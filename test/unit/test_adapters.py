import pytest
import sys
import pika
import os
from emit import adapters
from StringIO import StringIO
from emit.decorators import unreliable, slow
from emit.adapters import (
    Adapter, MultiAdapter, HttpAdapter, ListAdapter, RaisingAdapter,
    FileAdapter, StdoutAdapter, StderrAdapter, AmqpAdapter,
    AdapterError, AdapterEmitError, AdapterClosedError, AdapterEmitPermanentError)
from .test_decorators import assert_unreliable
from ..helpers import (TestCase, tevent, tjson)


try:
    _amqp_url = pytest.config.getvalue('amqp_url')
except:
    _amqp_url = None

try:
    _http_url = pytest.config.getvalue('http_url')
except:
    _http_url = None


def decorate_adapter(adapter, decorators=None, methods=None):
    methods = methods if not (methods is None) else ['emit']
    decorators = decorators if not (decorators is None) else [slow, unreliable]

    for method in methods:
        for decorator in decorators:
            setattr(adapter, method, decorator(getattr(adapter, method)))
    return adapter


def assert_adapter(adapter):
    assert isinstance(adapter, Adapter)
    assert callable(adapter)

    for given in [adapter, adapter()]:
        assert isinstance(given, Adapter)
        assert hasattr(given, 'open')
        assert hasattr(given, 'close')
        assert hasattr(given, 'emit')
        assert hasattr(given, 'flush')
        assert callable(given.open)
        assert callable(given.close)
        assert callable(given.emit)
        assert callable(given.flush)
        assert str(given).endswith(')')
        assert str(given).startswith(
            '{0}('.format(given.__class__.__name__))


def assert_amqp(adapter):
    assert_adapter(adapter)
    assert isinstance(adapter, AmqpAdapter)
    assert not (adapter.parameters is None)
    assert isinstance(adapter.parameters, pika.URLParameters)
    assert not (adapter.properties is None)
    assert isinstance(adapter.properties, pika.BasicProperties)
    assert adapter.properties.content_type == 'application/json'
    assert adapter.properties.delivery_mode == 1


def assert_amqp_closed(adapter):
    assert_amqp(adapter)
    assert adapter.connection is None
    assert adapter.channel is None


def assert_amqp_open(adapter, result):
    assert_amqp(adapter)
    assert result == adapter
    assert not (adapter.connection is None)
    assert isinstance(adapter.connection, pika.BlockingConnection)
    assert not (adapter.channel is None)
    assert isinstance(adapter.channel, pika.adapters.blocking_connection.BlockingChannel)


class AdapterTestsMixin(object):

    def test_interface(self):
        adapter = self.adapter_class()
        event = tevent()
        event_json = event.json

        assert_adapter(adapter)
        assert adapter.open() == adapter
        with adapter:
            assert adapter.close() is None
        assert adapter.close() is None
        with adapter:
            assert adapter.emit(event_json) is None
        with adapter:
            assert adapter.flush() is None

    def test__call__(self):
        adapter = self.adapter_class()
        cloned = adapter()
        assert not (cloned is None)
        assert not (id(cloned) == id(adapter))
        assert isinstance(cloned, adapter.__class__)

    def test_open(self):
        adapter = self.adapter_class()
        assert adapter.closed is True
        assert adapter.open() == adapter
        assert adapter.closed is False

    def test_close(self):
        adapter = self.adapter_class()
        assert adapter.closed is True
        assert adapter.close() is None
        assert adapter.closed is True
        assert adapter.open() == adapter
        assert adapter.closed is False
        assert adapter.close() is None
        assert adapter.closed is True

    def test_flush(self):
        adapter = self.adapter_class()
        with adapter:
            assert adapter.flush() is None

    def test_flush_closed(self):
        adapter = self.adapter_class()

        with pytest.raises(AdapterClosedError):
            assert adapter.flush() is None

    def test_emit(self):
        event = tevent()
        event_json = event.json
        adapter = self.adapter_class()
        with adapter:
            assert adapter.emit(event_json) is None

    def test_emit_closed(self):
        adapter = self.adapter_class()

        with pytest.raises(AdapterClosedError):
            assert adapter.emit(tevent().json) is None

    def test_emit_no_arg(self):
        adapter = self.adapter_class()

        with pytest.raises(TypeError) as excinfo:
            adapter.emit()
        assert 'emit() takes exactly 2 arguments (1 given)' == str(excinfo.value)

    def test_emit_errors(self):
        adapter = self.adapter_class()
        adapter.open()

        expect = set([
            AdapterError, AdapterClosedError,
            AdapterEmitError, AdapterEmitPermanentError])

        for e in expect:
            assert e in Adapter.errors

            with pytest.raises(AdapterError):
                adapter.emit(AdapterError)

    def test__enter__(self):
        adapter = self.adapter_class()
        assert adapter.closed is True

        with adapter as ctx:
            assert adapter.closed is False
            assert adapter == ctx
            assert adapter.open() == ctx
        assert adapter.closed is True

    def test__exit__(self):
        adapter = self.adapter_class()

        with adapter as ctx:
            assert adapter == ctx
            assert adapter.open() == ctx
        assert adapter.close() is None


class UnreliableTestsMixin(object):

    def test_unreliable_all(self):
        methods = ['open', 'close', 'flush', 'emit']
        decorators = [unreliable]
        adapter = decorate_adapter(self.adapter_class(), decorators=decorators, methods=methods)
        event = tevent()
        event_json = event.json

        assert_adapter(adapter)
        assert_unreliable(adapter.open)

        cases = [
            (adapter.close, 'close'),
            (adapter.flush, 'flush'),
            (lambda: adapter.emit(event_json), 'emit')]

        for case in cases:
            # Iter until we get an open
            for i in range(10000):
                try:
                    with adapter:
                        assert_unreliable(case[0], func_name=case[1])
                        break
                except RuntimeError:
                    pass


@pytest.mark.adapters
@pytest.mark.adapters_errors_classes
class TestAdapterErrorClasses(TestCase):

    def test_adapter_error(self):
        e = AdapterError()
        assert isinstance(e, Exception)

    def test_adapter_error_with_trigger(self):
        e_trigger = Exception()
        e = AdapterError(e_trigger)
        assert isinstance(e, Exception)
        assert e.trigger == e_trigger
        assert isinstance(e.trigger, Exception)

    def test_adapter_closed_error(self):
        e = AdapterClosedError()
        assert isinstance(e, Exception)
        assert isinstance(e, AdapterError)

    def test_adapter_emit_error(self):
        e = AdapterEmitError()
        assert isinstance(e, Exception)
        assert isinstance(e, AdapterError)

    def test_adapter_emit_permanent_error(self):
        e = AdapterEmitPermanentError()
        assert isinstance(e, Exception)
        assert isinstance(e, AdapterError)
        assert isinstance(e, AdapterEmitError)


@pytest.mark.adapters
@pytest.mark.base_adapter
class TestAdapter(AdapterTestsMixin, UnreliableTestsMixin, TestCase):
    adapter_class = Adapter

    def test_from(self):
        adapter_urls = [
            (ListAdapter, ['list', 'list://']),
            (StdoutAdapter, ['std://out']),
            (StderrAdapter, ['std://err']),
            (AmqpAdapter, ['amqp://user:pass@localhost:5379']),
            (AmqpAdapter, ['amqps://user:pass@localhost:5379']),
            (HttpAdapter, ['http://user:pass@localhost:5000']),
            (HttpAdapter, ['https://user:pass@localhost:5000']),
            (Adapter, ['noop', 'noop://']),
            (Adapter, ['default', 'default://'])]

        for (adapter_class, urls) in adapter_urls:
            for url in urls:
                adapter = Adapter.from_url(url)
                assert isinstance(adapter, adapter_class)
                assert isinstance(Adapter.from_url(url), adapter_class)
                assert isinstance(Adapter.from_url(adapter), adapter_class)

    def test_from_url_invalid(self):
        with pytest.raises(ValueError) as excinfo:
            Adapter.from_url('test_from_url_invalid-ad')
        assert '`test_from_url_invalid-ad` is not a known adapter type' == str(excinfo.value)

    def test_from_url_no_arg(self):
        with pytest.raises(TypeError) as excinfo:
            Adapter.from_url()
        assert 'from_url() takes at least 1 argument (0 given)' == str(excinfo.value)

    def test_raise_closed(self):
        errors = [
            AdapterError, AdapterClosedError,
            AdapterEmitError, AdapterEmitPermanentError]
        adapter = Adapter()
        adapter.open()

        for error in errors:
            with pytest.raises(error):
                adapter.emit(error)


@pytest.mark.adapters
@pytest.mark.list_adapter
class TestListAdapter(TestCase):

    def test_open(self):
        adapter = ListAdapter()
        assert adapter.closed is True
        with adapter:
            assert adapter.closed is False
        assert adapter.closed is True

    def test_append(self):
        adapter = ListAdapter()
        with adapter:
            event = tevent()
            expect = event.json
            adapter.emit(expect)
            got = adapter.pop()
            assert expect == got

    def test_record(self):
        adapter = ListAdapter()

        with adapter:
            expect = tjson()
            adapter._emit(expect)

            assert expect == adapter[0]
            assert adapter[0].flushed is False

            adapter.flush()
            assert adapter[0].flushed is True

    def test_cmp(self):
        adapter = ListAdapter()

        with adapter:
            expected = [tjson(), tjson(), tjson()]

            for index, expect in enumerate(expected):
                adapter._emit(expect)
                assert expect == adapter[index]
                assert adapter[index].flushed is False

                adapter.flush()
                assert adapter[index].flushed is True
            assert adapter == expected

    def test_str(self):
        adapter = ListAdapter()

        with adapter:
            event_json = tevent().json
            adapter.emit(event_json)
            adapter_str = str(adapter)

            assert event_json in adapter
            got = adapter.pop()
            assert event_json == got

            expect_lines = event_json.split('\n')
            for expect_line in expect_lines:
                assert expect_line in adapter_str


@pytest.mark.adapters
@pytest.mark.multi_adapter
class TestMultiAdapter(AdapterTestsMixin, UnreliableTestsMixin, TestCase):
    @staticmethod
    def adapter_factory():
        return MultiAdapter(ListAdapter())
    adapter_class = adapter_factory

    def test_errors_open(self):
        adapter = MultiAdapter(RaisingAdapter(AdapterClosedError))
        assert adapter.closed is True

        with pytest.raises(AdapterClosedError):
            with adapter:
                pass
        assert adapter.closed is True

    def test_errors_flush(self):
        adapter = MultiAdapter(RaisingAdapter(AdapterClosedError))
        assert adapter.closed is True

        with pytest.raises(AdapterClosedError):
            adapter.flush()
        assert adapter.closed is True

    def test_errors_emit(self):
        adapter = MultiAdapter(RaisingAdapter(AdapterClosedError))
        assert adapter.closed is True

        with pytest.raises(AdapterClosedError):
            adapter.emit(tevent().json)
        assert adapter.closed is True

    def test_errors_emit_no_adapters(self):
        adapter = MultiAdapter()
        assert adapter.closed is True
        with pytest.raises(AdapterEmitError):
            with adapter:
                adapter.emit(tevent().json)
        assert adapter.closed is True

    def test_errors_close_still_calls_on_adapters(self):
        t = ListAdapter()
        adapter = MultiAdapter(t, RaisingAdapter(AdapterClosedError))
        t.open()
        assert adapter.closed is True
        assert t.closed is False
        with pytest.raises(AdapterClosedError):
            adapter.open()
        assert adapter.closed is True
        assert t.closed is True

    def test_close_has_error(self):
        t = ListAdapter()
        tr = RaisingAdapter(AdapterClosedError, raising=False)
        adapter = MultiAdapter(t, tr)

        with adapter:
            assert adapter.closed is False
            assert tr.closed is False
            assert t.closed is False
            tr.raising = True
        assert adapter.closed is True
        assert tr.closed is True
        assert t.closed is True

    def test_one_closed(self):
        adapter = MultiAdapter(*[ListAdapter() for adapter in range(10)])
        assert adapter.closed is True
        adapter.open()
        assert adapter.closed is False
        adapter.adapters[5].close()
        assert adapter.closed is True

    def test_multi(self):
        for i in range(1, 10):
            adapters = [ListAdapter() for adapter in range(i)]
            adapter = MultiAdapter(*adapters)
            expect = tevent().json

            assert all(chk_adapter.closed is True for chk_adapter in adapters)
            assert adapter.closed is True

            with adapter:
                assert all(chk_adapter.closed is False for chk_adapter in adapters)
                assert adapter.closed is False
                adapter.emit(expect)

                for chk_adapter in adapters:
                    got = chk_adapter.pop()
                    assert expect == got
            assert all(chk_adapter.closed is True for chk_adapter in adapters)
            assert adapter.closed is True
            for chk_adapter in adapters:
                assert len(chk_adapter) == 0

    @pytest.mark.slow
    def test_multi_real_adapter(self):
        for i in range(1, 3):
            adapters = [Adapter() for adapter in range(i)]
            adapter = MultiAdapter(*adapters)
            expect = tevent().json

            assert all(chk_adapter.closed is True for chk_adapter in adapters)
            assert adapter.closed is True

            with adapter:
                assert all(chk_adapter.closed is False for chk_adapter in adapters)
                assert adapter.closed is False
                assert adapter.emit(expect) is None
            assert all(chk_adapter.closed is True for chk_adapter in adapters)
            assert adapter.closed is True


@pytest.mark.adapters
@pytest.mark.file_adapter
class TestFileAdapter(AdapterTestsMixin, UnreliableTestsMixin, TestCase):
    adapter_class = FileAdapter

    def test_writable(self):
        adapter = FileAdapter()
        adapter._file = StringIO()
        event = tevent()
        expect = event.json

        with adapter:
            assert adapter.closed is False
            adapter.emit(expect)
            got = adapter._file.getvalue()
            assert expect + '\n' == got

    def test_open_args(self):
        open_calls = []
        fsync_calls = []

        def mock_open(*args):
            open_calls.append(args)
            sio = StringIO()
            sio.fileno = lambda: None
            return sio

        def fsync(*args):
            fsync_calls.append(args)

        adapter = FileAdapter('/tmp/bar')

        try:
            adapters.open = mock_open
            adapters.os.fsync = fsync

            with adapter:
                event_json = tevent().json
                adapter.emit(event_json)
                assert adapter.flush() is None
                got = adapter._file.getvalue()
                assert event_json + '\n' == got
        finally:
            adapters.open = open
            adapters.os.fsync = os.fsync


@pytest.mark.adapters
@pytest.mark.stdout_adapter
class TestStdoutAdapter(TestCase):

    def test_writable(capsys):
        assert StdoutAdapter._file == sys.stdout


@pytest.mark.adapters
@pytest.mark.stderr_adapter
class TestStderrAdapter(TestCase):

    def test_writable(capsys):
        assert StderrAdapter._file == sys.stderr


@pytest.mark.skipif(not _amqp_url, reason='--amqp_url was not specified')
@pytest.mark.slow
@pytest.mark.adapters
@pytest.mark.amqp_adapter
class TestAmqpAdapter(AdapterTestsMixin, TestCase):
    @staticmethod
    def adapter_factory():
        return AmqpAdapter(_amqp_url)
    adapter_class = adapter_factory

    def test_interface(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

    def test_from_url(self):
        adapter = AmqpAdapter.from_url(_amqp_url)
        assert_amqp_closed(adapter)

    def test_init(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

    def test__call__(self):
        adapter = AmqpAdapter(_amqp_url)
        cloned = adapter()
        assert not (cloned is None)
        assert not (cloned == adapter)
        assert_amqp(cloned)
        assert dict(adapter.properties.__dict__) == dict(cloned.properties.__dict__)

    def test_open(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

    def test_open_failure(self):
        adapter = AmqpAdapter('amqps://invalid:invalid@example.com:5672/services')
        assert_amqp(adapter)

        with pytest.raises(AdapterClosedError) as excinfo:
            adapter.open()
        assert isinstance(excinfo.value, AdapterError)

    def test_close(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        assert adapter.close() is None
        assert_amqp_closed(adapter)  # this makes sure con/chan is None

        # Close should allow multiple close calls
        assert adapter.close() is None
        assert_amqp_closed(adapter)

    def test_close_never_opened(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        # Close should allow calls even if never opened
        assert adapter.close() is None
        assert_amqp_closed(adapter)

    def test_flush(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        # A noop
        with adapter:
            assert adapter.flush() is None
        assert_amqp_closed(adapter)

    def test_emit(self):
        event = tevent()
        event_json = event.json
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        assert adapter.emit(event_json) is None
        assert_amqp_open(adapter, result)

    def test_emit_ensure_connection_after_emit_failure(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        # This message is invalid so we should not be able to emit
        with pytest.raises(AdapterEmitPermanentError) as excinfo:
            assert adapter.emit(AdapterEmitPermanentError) is None
        assert isinstance(excinfo.value, AdapterEmitPermanentError)

        # Set system and ensure it's accepted
        event_json = tevent().json
        assert adapter.emit(event_json) is None
        assert_amqp_open(adapter, result)

    def test_emit_closed(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        with pytest.raises(AdapterClosedError):
            assert adapter.emit(tevent().json) is None
        assert_amqp_closed(adapter)

    def test_emit_closed_connection(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        adapter.connection.close()
        with pytest.raises(AdapterClosedError):
            assert adapter.emit(tevent().json) is None
        adapter.close()
        assert_amqp_closed(adapter)

    def test_emit_closed_channel(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        adapter.channel.close()
        with pytest.raises(AdapterClosedError):
            assert adapter.emit(tevent().json) is None
        adapter.close()
        assert_amqp_closed(adapter)

    def test_emit_no_channel(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        adapter.channel.close()
        adapter.channel = None
        with pytest.raises(AdapterClosedError):
            assert adapter.emit(tevent().json) is None
        adapter.close()
        assert_amqp_closed(adapter)

    def test_fail_emit(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        adapter.properties.content_type = '0' * 256

        with pytest.raises(AdapterEmitError):
            adapter.emit(tevent().json)

    def test_fail_emit_permanent(self):
        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        # Make this fail by assertion error for 100% cov
        restore = pika.spec.Basic.Ack
        try:
            with pytest.raises(AdapterEmitPermanentError) as excinfo:
                adapter.channel._delivery_confirmation = True
                pika.spec.Basic.Ack = None
                assert adapter.emit(tevent().json) is None
        finally:
            pika.spec.Basic.Ack = restore
        assert isinstance(excinfo.value, AdapterError)
        adapter.close()
        assert_amqp_closed(adapter)

    def test_fail_closed_via_pika(self):
        def _flush_output(*args, **kwargs):
            raise pika.exceptions.ProtocolSyntaxError

        adapter = AmqpAdapter(_amqp_url)
        assert_amqp_closed(adapter)

        result = adapter.open()
        assert_amqp_open(adapter, result)

        # Make this fail by assertion error for 100% cov
        restore = adapter.channel._flush_output

        try:
            with pytest.raises(AdapterClosedError) as excinfo:
                adapter.channel._flush_output = _flush_output
                assert adapter.emit(tevent().json) is None
        finally:
            adapter.channel._flush_output = restore
        assert isinstance(excinfo.value, AdapterClosedError)
        adapter.close()
        assert_amqp_closed(adapter)


@pytest.mark.skipif(not _http_url, reason='--http_url was not specified')
@pytest.mark.slow
@pytest.mark.adapters
@pytest.mark.http_adapter
class TestHttpAdapter(AdapterTestsMixin, TestCase):
    @staticmethod
    def adapter_factory():
        return HttpAdapter(_http_url)
    adapter_class = adapter_factory

    def test_init(self):
        adapter = self.adapter_factory()
        assert adapter.session is None, 'exp closed session for new adapter'

    def test__call__(self):
        adapter = self.adapter_factory()
        cloned = adapter()
        assert not (cloned is None)
        assert not (cloned == adapter)
        assert cloned.session is None, 'exp closed session for new adapter'
        assert adapter.url == cloned.url, 'exp same url'
        assert adapter.session == cloned.session, 'exp same session val'

    def test_emit(self):
        event = tevent()
        event_json = event.json
        adapter = self.adapter_factory()
        assert adapter.session is None

        adapter.open()
        assert adapter.session is not None

        assert adapter.emit(event_json) is None
        assert adapter.session is not None

    def test_emit_failure(self):
        event = tevent()
        event_json = event.json
        adapter = HttpAdapter.from_url(_http_url+"/invalid/url")
        adapter.open()
        with pytest.raises(AdapterEmitError) as excinfo:
            assert adapter.emit(event_json) is None
        assert isinstance(excinfo.value, AdapterError)

    def test_emit_closed(self):
        event = tevent()
        event_json = event.json
        adapter = self.adapter_factory()
        with pytest.raises(AdapterClosedError) as excinfo:
            assert adapter.emit(event_json) is None
        assert isinstance(excinfo.value, AdapterError)


@pytest.mark.adapters
@pytest.mark.raising_adapter
class TestRaisingAdapter(TestCase):

    def test_raises(self):
        adapters = [
            RaisingAdapter(), RaisingAdapter(AdapterError),
            RaisingAdapter(raising=False), RaisingAdapter(raising=True)]
        for a in adapters:
            if a.raising:
                with pytest.raises(AdapterError):
                    a._open()
                with pytest.raises(AdapterError):
                    a._close()
                with pytest.raises(AdapterError):
                    a._flush(None)
                with pytest.raises(AdapterError):
                    a._emit('')
            else:
                a._open()
                a._close()
                a._flush(None)
                a._emit('')
