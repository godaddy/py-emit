import sys
import os
import pika
from datetime import datetime
from .globals import log, conf
from pika.exceptions import (
    AMQPError, AMQPChannelError, AMQPConnectionError, ProtocolSyntaxError)
from .utils import _is_string, _timeout_seconds


Adapters = [
    'Adapter', 'MultiAdapter', 'ListAdapter', 'FileAdapter',
    'StdoutAdapter', 'StderrAdapter', 'AmqpAdapter']


__all__ = Adapters + [
    'Adapters', 'AdapterError', 'AdapterClosedError', 'AdapterEmitError']


class AdapterError(Exception):
    """Normalized error for adapters to share."""
    def __init__(self, trigger=None):
        self.trigger = trigger


class AdapterClosedError(AdapterError):
    """An error that indicates the adapter is no longer open."""


class AdapterEmitError(AdapterError):
    """Indicates the adapter failed to send a single event, but remains open for
    further events."""


class AdapterEmitPermanentError(AdapterEmitError):
    """Indicates the adapter will not be able to send another event."""


class Adapter(object):
    """Adapter is the base implementation of the emit adapter interface. An
    adapter is anything that when called returns an object that has an `open`
    method. This open method must return an object with a `close`, `flush` and
    `emit` method. The simple state of "opened" or "closed" will be managed by
    other modules in this package (see `Transport`). Adapters are expected to
    not require arguments. So if you intend on setting a custom adapter you
    should follow a pattern similar to the one here. Setup an initial adapter
    object with whatever params you need and have the __call__ method be a factory
    that returns an adapter setup with the paramters you want. The reason the
    adapters must be factories is they sometimes need to be instantiated within
    a new thread."""
    errors = set([AdapterError, AdapterClosedError, AdapterEmitError, AdapterEmitPermanentError])

    @classmethod
    def __call__(cls):
        """Looks for an "EMIT_ADAPTER_URL" environment variable, if not set will
        return a stdout adapter."""
        if len(conf.adapter_url):
            return cls.from_url(conf.adapter_url)
        return cls()

    @staticmethod
    def from_url(url, *args, **kwargs):
        if _is_string(url):
            if url.startswith('amqp'):
                return AmqpAdapter.from_url(url, *args, **kwargs)
            if url.startswith('list'):
                return ListAdapter(*args, **kwargs)
            if url == 'std://out':
                return StdoutAdapter(*args, **kwargs)
            if url == 'std://err':
                return StderrAdapter(*args, **kwargs)
            if url.startswith('noop') or url.startswith('default'):
                return Adapter(*args, **kwargs)
        if issubclass(url.__class__, Adapter):
            return url
        raise ValueError('`{0}` is not a known adapter type'.format(url))

    def __init__(self):
        self._closed = True

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    @classmethod
    def __repr__(cls):
        return '{0}()'.format(cls.__name__)

    @property
    def closed(self):
        return self._closed

    def open(self):
        self._open()
        self._closed = False
        return self

    def close(self):
        if self.closed:
            return
        try:
            self._close()
        finally:
            self._closed = True

    def flush(self, timeout=None):
        """Calls _flush with timeout in seconds."""
        timeout = _timeout_seconds(timeout, conf.max_flush_time.total_seconds())
        if self.closed:
            raise AdapterClosedError
        self._flush(timeout)

    def emit(self, event):
        if self.closed:
            raise AdapterClosedError
        if (event in self.errors):
            log.debug('Adapter.emit - event was of an error type {}, raising it'.format(event.__name__))
            raise event
        self._emit(event)

    def _open(self):
        pass

    def _close(self):
        pass

    def _flush(self, timeout):
        pass

    def _emit(self, event):
        pass


class MultiAdapter(Adapter):
    """Takes multiple adapters and emits to them, only for testing I would not use
    this to ensure delivery to multiple destinations."""
    def __init__(self, *adapters):
        super(MultiAdapter, self).__init__()
        self.adapters = adapters

    def __call__(self):
        return self.__class__(*[a() for a in self.adapters])

    @property
    def closed(self):
        return super(MultiAdapter, self).closed or any(a.closed for a in self.adapters)

    def _open(self):
        opened = []
        self._close()

        for adapter in self.adapters:
            try:
                adapter.open()
                opened.append(adapter)
            except AdapterError:
                continue
        if len(opened) != len(self.adapters):
            for adapter in opened:
                adapter.close()
            raise AdapterClosedError

    def _close(self):
        for adapter in self.adapters:
            try:
                adapter.close()
            except Exception:
                pass

    def _flush(self, timeout):
        for adapter in self.adapters:
            adapter.flush(timeout)

    def _emit(self, json):
        errors = []

        if not len(self.adapters):
            raise AdapterEmitError
        for adapter in self.adapters:
            try:
                adapter.emit(json)
            except AdapterError as e:
                errors.append(e)
                continue
        if len(errors):
            raise errors.pop()


class FileAdapter(Adapter):
    """If _file is set, will write the event json plus a single new line. If
    instantiated with `open_args` will call python's open() with them on
    adapter open and close, flush will fsync."""
    _file = None

    def __init__(self, *open_args):
        super(FileAdapter, self).__init__()
        self._open_args = open_args

    def _open(self):
        if self._open_args:
            self._file = open(*self._open_args)

    def _close(self):
        if self._open_args:
            try:
                self._file.close()
            finally:
                self._file = None

    def _flush(self, timeout):
        if self._file:
            self._file.flush()
            if self._open_args:
                os.fsync(self._file.fileno())

    def _emit(self, json):
        if self._file:
            self._file.write(json + "\n")


class StdoutAdapter(FileAdapter):
    _file = sys.stdout


class StderrAdapter(FileAdapter):
    _file = sys.stderr


class AmqpAdapter(Adapter):
    """Uses pika amqp python library to send events."""
    @classmethod
    def from_url(cls, url):
        return cls(pika.URLParameters(url))

    def __init__(self, parameters):
        super(AmqpAdapter, self).__init__()
        self.parameters = parameters if isinstance(
            parameters, pika.connection.Parameters) else pika.URLParameters(parameters)
        self.connection = None
        self.channel = None
        self.properties = pika.BasicProperties(
            content_type='application/json', delivery_mode=1)

    def __call__(self):
        return AmqpAdapter(self.parameters)

    def _open(self):
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
        except (AMQPChannelError, AMQPConnectionError, ProtocolSyntaxError) as e:
            raise AdapterClosedError(e)
        return self

    def _close(self):
        self._close_channel()
        self._close_connection()

    def _close_channel(self):
        if self.channel:
            try:
                self.channel.close()
            except (pika.ChannelClosed, pika.ChannelAlreadyClosing):
                pass
            finally:
                self.channel = None

    def _close_connection(self):
        if self.connection:
            try:
                self.connection.close()
            # Connection close calls channel close, though it says it can't raise
            # these are here just in case.
            except (pika.ChannelClosed, pika.ChannelAlreadyClosing):
                pass
            finally:
                self.connection = None

    def _flush(self, timeout):
        pass

    def _emit(self, json):
        if not self.channel:
            raise AdapterClosedError
        try:
            self.channel.publish(
                exchange='events', routing_key='emit.events', body=json,
                properties=self.properties)
        except (AMQPChannelError, AMQPConnectionError, ProtocolSyntaxError) as e:
            raise AdapterClosedError(e)
        except AMQPError as e:
            raise AdapterEmitError(e)
        except Exception as e:
            raise AdapterEmitPermanentError(e)


class ListAdapter(Adapter, list):
    """Stores each emitted event in a list along with it's creation time and a
    boolean indicating if it has been flushed or not. Useful for debugging."""
    class Record(object):
        def __init__(self, json):
            self.json = json
            self.flushed = False
            self.created = datetime.utcnow()

        def __cmp__(self, other):
            return 0 if self.json == other else -1

        def __str__(self):
            return self.json

    def __repr__(self):
        msg_list = ",\n".join([str(item) for item in self])

        return '{}([{}])'.format(
            self.__class__.__name__,
            "  ".join(msg_list.splitlines(True)))

    def __init__(self, *args, **kwargs):
        super(ListAdapter, self).__init__()
        list.__init__(self, *args, **kwargs)

    def _flush(self, timeout):
        for record in self:
            record.flushed = True

    def _emit(self, json):
        self.append(self.Record(json))


class RaisingAdapter(Adapter):
    """Raises given error class for each method, used for testing."""
    def __init__(self, error_class=AdapterError, raising=True):
        super(RaisingAdapter, self).__init__()
        self.error_class = error_class
        self.raising = raising

    def _open(self):
        if self.raising:
            raise self.error_class()

    def _close(self):
        if self.raising:
            raise self.error_class()

    def _flush(self, timeout):
        if self.raising:
            raise self.error_class()

    def _emit(self, json):
        if self.raising:
            raise self.error_class()
