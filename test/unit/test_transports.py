import pytest
import threading
from time import sleep
from datetime import datetime, timedelta
from emit import queue
from emit.decorators import defer, delay
from emit.transports import (
    Transport, Worker, Group, ThreadedWorker, WorkerError, WorkerStoppedError)
from emit.queue import Queue
from emit.utils import Called
from emit.adapters import (
    Adapter, ListAdapter, AdapterEmitError, AdapterClosedError, AdapterEmitPermanentError)
from ..helpers import (
    TestCase, tevent, tjson)


TD0 = timedelta()
TDS = timedelta(seconds=1)
TDM = timedelta(milliseconds=1)


def assert_transport(transport):
    expect_methods = [
        '__enter__', '__exit__', 'start', 'stop', 'halt', 'flush', 'emit']

    for expect_method in expect_methods:
        assert hasattr(transport, expect_method)
        assert callable(getattr(transport, expect_method))


def assert_worker(worker):
    assert issubclass(worker.__class__, Worker)
    assert issubclass(worker.transport.__class__, Transport)
    assert issubclass(worker.adapter.__class__, Adapter)


def assert_emit(transport, expect_flushed=False):
    event_json = tjson()
    transport.emit(event_json)
    chk(transport, event_json, False)


def eventually(func, *args, **kwargs):
    _eventually_delta = TDM * 100

    if '_eventually_delta' in kwargs:
        _eventually_delta = kwargs['_eventually_delta']
        del kwargs['_eventually_delta']
    expires = datetime.utcnow() + _eventually_delta
    td0 = timedelta()

    while not func(*args, **kwargs):
        sleep(TDM.total_seconds())
        remaining = expires - datetime.utcnow()

        if remaining <= td0:
            raise AssertionError(
                '{} did not return True after {} seconds'.format(
                    func.__name__, _eventually_delta.total_seconds()))


def check_delivered(worker, events, expect_flushed=False):
    if len(worker.adapter) != len(events):
        return False
    return all([event in worker.adapter for event in events]) and \
        all([expect_flushed is record.flushed for record in worker.adapter])


def chk(transport, event_json, expect_flushed=False, index=-1):
    chkw(transport.worker, event_json, expect_flushed, index)


def chkw(worker, event_json, expect_flushed=False, index=-1):
    if index < 0:
        index = len(worker.adapter) - 1
    record = worker.adapter[index]
    assert event_json == record.json
    assert expect_flushed == record.flushed


@pytest.mark.transport_error_classes
class TestTransportErrorClasses(TestCase):

    def test_worker_error(self, w):
        e = WorkerError()
        assert isinstance(e, Exception)

    def test_worker_error_with_trigger(self):
        e_trigger = Exception()
        e = WorkerError(e_trigger)
        assert isinstance(e, Exception)
        assert e.trigger == e_trigger
        assert isinstance(e.trigger, Exception)

    def test_worker_stopped_error(self):
        e = WorkerStoppedError()
        assert isinstance(e, Exception)

    def test_worker_stopped_error_with_trigger(self):
        e_trigger = Exception()
        e = WorkerStoppedError(e_trigger)
        assert isinstance(e, Exception)
        assert e.trigger == e_trigger
        assert isinstance(e.trigger, Exception)


@pytest.mark.worker
class TestCaseWorker(TestCase):
    transport_class = Transport
    worker_class = Worker

    def test_t(self, t):
        t.start()
        assert isinstance(t, self.transport_class)
        assert isinstance(t.worker, self.worker_class)
        assert t.worker.t == t

    def test_q(self, t):
        t.start()
        assert isinstance(t, self.transport_class)
        assert isinstance(t.worker, self.worker_class)
        assert isinstance(t.queue, Queue)
        assert t.worker.q == t.queue

    def test_adapter(self, t):
        t.start()
        assert isinstance(t, self.transport_class)
        assert isinstance(t.adapter, ListAdapter)
        assert t.worker.adapter == t.adapter
        assert t.worker.adapter.__class__ == t.adapter.__class__

    # process_item
    def test_process_item_ignores_none(self, w):
        w.process_item(None)

    def test_process_item_discarded_on_permanent_emit_error(self, w):
        item = queue.QueueItem(AdapterEmitPermanentError)

        with w.adapter:
            assert len(w.q) == 0
            w.process_item(item)
            assert len(w.q) == 0

    def test_process_item_attempt_count_increase_on_emit_error(self, w):
        item = queue.QueueItem(AdapterEmitError)

        with w.adapter:
            assert len(w.q) == 0
            assert item.attempts == 0
            w.process_item(item)
            assert len(w.q) == 1
            assert item.attempts == 1

    def test_process_item_attempt_count_increase_on_closed_error(self, w):
        item = queue.QueueItem(AdapterClosedError)

        with w.adapter:
            assert len(w.q) == 0
            assert item.attempts == 0
            with pytest.raises(AdapterClosedError):
                w.process_item(item)
            assert len(w.q) == 1
            assert item.attempts == 1

    # check_adapter
    def test_check_adapter_opens_when_closed_if_queue_exists(self, w):
        assert w.adapter.closed is True
        w.q.put(tjson(), True, None)
        w.check_adapter(TDS)
        assert w.adapter.closed is False

    def test_check_adapter_stays_open(self, w):
        w.start()
        assert w.adapter.closed is False
        w.check_adapter(TDS)
        assert w.adapter.closed is False

    def test_check_adapter_returns_immediate_when_in_backoff(self, w):
        for i in range(5):
            w.tracker.attempt()
        assert w.adapter.closed is True
        before = datetime.utcnow()
        w.check_adapter(TDM * 2)
        after = datetime.utcnow()
        assert (before - after) < TDM
        assert w.adapter.closed is True

    def test_check_adapter_waits_max_of_wait_time(self, w):
        for i in range(5):
            w.tracker.attempt()

        now = datetime.utcnow()

        # Set tracker expiration to 2ms from now
        w.tracker.last_attempt = now - (w.tracker.expires() - now) + (TDM * 2)

        # Wait for check adapter at most 10MS, expecting 5MS
        w.check_adapter(TDM * 10)
        assert (datetime.utcnow() - now) < (TDM * 5)
        assert w.adapter.closed is False

    def test_check_adapter_waits_max_when_attempts_out_of_band(self, w):
        @defer(duration=TDM)
        def attempt():
            for i in range(5):
                w.tracker.attempt()

        for i in range(5):
            w.tracker.attempt()

        now = datetime.utcnow()

        # Set tracker expiration to 2ms from now
        w.tracker.last_attempt = now - (w.tracker.expires() - now) + (TDM * 2)
        remaining = w.tracker.remaining()

        # This will fire off while in wait() in tracker, causing it to timeout
        attempt()

        # Wait for check adapter at most 10MS, expecting 5MS
        w.check_adapter(TDM * 24)
        assert w.tracker.expired() is False

        # Sould not be in check_adapter() for longer than remaining time
        # left for tracker.
        assert (datetime.utcnow() - now) < (remaining + TDM)
        assert w.tracker.expired() is False
        assert w.adapter.closed is True

    def test_check_adapter_waits_max_and_opens_if_expired_and_not_empty(self, w):
        @defer(duration=TDM)
        def reset():
            w.tracker.reset()

        w.q.put(tjson(), True, None)
        for i in range(5):
            w.tracker.attempt()

        now = datetime.utcnow()

        # Set tracker expiration to 2ms from now
        w.tracker.last_attempt = (now - w.tracker.remaining()) + (TDM * 2)
        remaining = w.tracker.remaining()

        # This will fire off while in wait() in tracker, causing it to be
        # ready when expired
        reset()

        # Wait for check adapter at most 10MS, expecting 5MS
        w.check_adapter(TDM * 4)

        # Sould not be in check_adapter() for longer than remaining time
        # left for tracker.
        assert (datetime.utcnow() - now) < (remaining + TDM)
        assert w.tracker.expired() is True
        assert w.adapter.closed is False

    # process_queue
    def test_process_queue_connects_when_closed_if_has_queue(self, w):
        expect_json = tjson()
        w.q.put(expect_json, True, None)
        assert len(w.q) > 0
        w.process_queue(TDS)
        chkw(w, expect_json)
        assert len(w.q) == 0

    def test_process_queue_adapter_stays_closed_if_queue_empty(self, w):
        assert w.adapter.closed is True
        w.process_queue(TDS)
        assert w.adapter.closed is True

    # fetch_item
    def test_fetch_item_no_blocking(self, w):
        assert w.adapter.closed is True

        now = datetime.utcnow()
        with pytest.raises(queue.Empty):
            w.fetch_item()
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is True

    def test_fetch_item_exit_on_empty_queue(self, w):
        w.adapter.open()
        assert w.adapter.closed is False

        now = datetime.utcnow()
        w.process(TDM * 4)
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is False

    # process
    def test_process_immediate_exit_on_negative_timeout(self, w):
        assert w.adapter.closed is True

        now = datetime.utcnow()
        w.process(-TDM)
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is True

    def test_process_immediate_exit_on_empty_queue(self, w):
        w.adapter.open()
        assert w.adapter.closed is False

        now = datetime.utcnow()
        w.process(TDM * 4)
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is False

    def test_process_adapter_closed_error(self, w):
        item = queue.QueueItem(AdapterClosedError)
        w.q.put_item(item)

        with w.adapter:
            assert len(w.q) == 1
            assert item.attempts == 0
            w.process(TDM)
            assert len(w.q) == 1
            assert item.attempts == 1
            assert w.adapter.closed is True

    # work
    def test_work(self, w):
        expect_json = tjson()
        w.q.put(expect_json, True, None)
        assert len(w.q) > 0
        w.start()
        w.work(TDS)
        chkw(w, expect_json)
        assert len(w.q) == 0

    def test_work_immediate_exit_on_negative_timeout(self, w):
        assert w.adapter.closed is True

        now = datetime.utcnow()
        w.work(-TDM)
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is True

    def test_work_immediate_exit_on_empty_queue(self, w):
        w.adapter.open()
        assert w.adapter.closed is False

        now = datetime.utcnow()
        w.work(TDM * 4)
        assert (now - datetime.utcnow()) < TDM
        assert w.adapter.closed is False

    def test_work_stopped(self, w):
        assert w.adapter.closed is True

        expected = [tjson() for i in range(3)]

        for json in expected:
            w.q.put(json, True, None)
        assert len(w.q) == len(expected)
        w.work(TDS)
        assert w.adapter.closed is False

    def test_work_catches_exceptions(self, w, logs):
        msg = 'TransportWorker.work - an uncaught exception occurred'

        # False will raise in process
        assert w.work(False) is None
        assert len(logs) == 2
        assert msg == logs[0].getMessage()

    # start
    def test_start(self, w):
        assert w.adapter.closed is True
        w.start()
        assert w.adapter.closed is False

    # flush
    def test_flush(self, w):
        assert w.adapter.closed is True
        w.start()
        assert w.adapter.closed is False

        expected = [tjson() for i in range(3)]

        for json in expected:
            w.q.put(json, True, None)
        assert len(w.q) == len(expected)
        w.work(TDS)
        assert w.adapter.closed is False

        for (index, expect) in enumerate(expected):
            chkw(w, expect, expect_flushed=False, index=index)
        assert w.adapter.closed is False

        w.flush(TDS)
        for (index, expect) in enumerate(expected):
            chkw(w, expect, expect_flushed=True, index=index)
        assert w.adapter.closed is False

    def test_flush_closed_no_op(self, w):
        assert w.adapter.closed is True
        w.flush(TDS)
        assert w.adapter.closed is True

    # stop
    def test_stop(self, w):
        assert w.adapter.closed is True
        w.start()
        assert w.adapter.closed is False
        expected = [tjson() for i in range(3)]

        for json in expected:
            w.q.put(json, True, None)
        assert len(w.q) == len(expected)

        w.stop(TDS)
        for (index, expect) in enumerate(expected):
            chkw(w, expect, expect_flushed=True, index=index)
        assert len(w.q) == 0
        assert w.adapter.closed is True

    def test_stop_without_starting(self, w):
        assert w.adapter.closed is True
        expected = [tjson() for i in range(3)]

        for json in expected:
            w.q.put(json, True, None)
        assert len(w.q) == len(expected)

        w.stop(TDS)
        for (index, expect) in enumerate(expected):
            chkw(w, expect, expect_flushed=True, index=index)
        assert len(w.q) == 0
        assert w.adapter.closed is True

    # reset
    def test_reset(self, w):
        expect_json = tjson()
        w.q.put(expect_json, True, None)
        assert len(w.q) > 0

        for i in range(6):
            w.tracker.attempt()
        assert w.tracker.attempts == 6
        called = Called(call_func=w.q.reset)
        w.q.reset = called
        assert len(called) == 0
        w.reset()
        assert len(called) == 1
        assert w.tracker.attempts == 0


@pytest.mark.threaded_worker
class TestCaseThreadedWorker(TestCase):
    worker_class = ThreadedWorker
    adapter_class = ListAdapter

    def test__init__(self, w):
        assert isinstance(w._adapter, ListAdapter)
        assert isinstance(w._started, threading._Event)
        assert isinstance(w._stopping, threading._Event)
        assert isinstance(w._halting, threading._Event)
        assert isinstance(w.t, Transport)
        assert isinstance(w.q, Queue)
        assert id(w._adapter) != id(w.t.adapter)

    def test_start(self, w):
        w.start()
        assert w.is_alive()

    def test_start_multi(self, w):
        w.start()
        assert w.is_alive()
        w.start()
        w.start()

    def test_work(self, w):
        w.start()
        w.work(TDM)
        w.stop(TDM)

    def test_work_no_started(self, w):
        with pytest.raises(WorkerStoppedError):
            w.work(TDM)

    def test_work_when_started_but_not_alive(self, w):
        w._started.set()
        with pytest.raises(WorkerStoppedError):
            w.work(TDM)

    def test_halt_no_started(self, w):
        with pytest.raises(WorkerStoppedError):
            w.halt()

    def test_halt_when_started_but_not_alive(self, w):
        w._started.set()
        with pytest.raises(WorkerStoppedError):
            w.halt()

    def test_flush(self, w):
        w.start()
        assert w.is_alive()
        w.flush(TDM)

    def test_check_flush_error(self, w):
        w.start()
        w._flush_pending = True
        w.adapter.close()
        w.check_flush()

    def test_flush_no_started(self, w):
        with pytest.raises(WorkerStoppedError):
            w.flush(TDM)

    def test_flush_when_started_but_not_alive(self, w):
        w._started.set()
        with pytest.raises(WorkerStoppedError):
            w.flush(TDM)

    def test_run_catch_all(self, w, logs):
        events_json = [tjson() for i in range(5)]

        for event_json in events_json:
            w.q.put(event_json, True, None)
        w.start()
        w.check_flush = None

        eventually(
            lambda: not w.is_alive(),
            _eventually_delta=TDS)

        expect_str = "'NoneType' object is not callable"
        assert any(expect_str in log.getMessage() for log in logs)

    @pytest.mark.slow
    def test_stop(self, w):
        events_json = [tjson() for i in range(5)]

        for event_json in events_json:
            w.q.put(event_json, True, None)

        w.start()
        w.stop(TDM * 20)
        eventually(
            check_delivered, w, events_json,
            expect_flushed=True, _eventually_delta=TDM*40)
        eventually(w._stopping.isSet, _eventually_delta=TDM*10)
        eventually(w._halting.isSet, _eventually_delta=TDM*10)
        eventually(
            lambda: not w.is_alive(),
            _eventually_delta=TDM*60)

    @pytest.mark.slow
    def test_stop_depletes_queue(self, w):

        @delay(duration=TDM)
        def process_item_wrapper(item):
            w_process_item(item)

        events_json = [tjson() for i in range(5)]
        for event_json in events_json:
            w.q.put(event_json, True, None)

        w_process_item = w.process_item
        w.process_item = process_item_wrapper
        w.start()
        w.stop(TDM * 20)

        eventually(
            check_delivered, w, events_json,
            expect_flushed=True, _eventually_delta=TDM*40)
        eventually(w._stopping.isSet, _eventually_delta=TDM*10)
        eventually(w._halting.isSet, _eventually_delta=TDM*10)
        eventually(
            lambda: not w.is_alive(),
            _eventually_delta=TDM*100)

    @pytest.mark.slow
    def test_halt(self, w):
        events_json = [tjson() for i in range(5)]

        for event_json in events_json:
            w.q.put(event_json, True, None)
        w.start()
        w.halt()
        eventually(w._stopping.isSet)
        eventually(w._halting.isSet)
        eventually(
            lambda: not w.is_alive(),
            _eventually_delta=TDM*100)

    @pytest.mark.slow
    def test_halt_leaves_queue(self, w):

        @delay(duration=TDS * 2)
        def process_item_wrapper(item):
            w_process_item(item)

        events_json = [tjson() for i in range(10)]
        for event_json in events_json:
            w.q.put(event_json, True, None)

        w_process_item = w.process_item
        w.process_item = process_item_wrapper
        w.start()
        w.halt()

        eventually(w._stopping.isSet)
        eventually(w._halting.isSet)
        eventually(
            lambda: not w.is_alive(),
            _eventually_delta=TDS*100)
        assert len(w.q) > 0

    @pytest.mark.slow
    def test_check_orphaned_evil_twins(self, w, logs):
        w.start()
        evil_twins = [w] + [ThreadedWorker(w.t) for x in range(5)]
        map(lambda ew: ew.start(), evil_twins)

        def there_can_only_be_one():
            return 1 == len(filter(lambda ew: ew.is_alive(), evil_twins))

        eventually(there_can_only_be_one, _eventually_delta=TDS)

        expect_str = "found another worker belonging to this transport, halting"
        assert 5 == len([expect_str in log.getMessage() for log in logs])
        assert there_can_only_be_one()

    @pytest.mark.slow
    def test_halt_when_main_thread_dead(self, w):
        '''
        @TODO: Not sure how to mock this.

        def set_is_alive(to):
            for t in threading.enumerate():
                if isinstance(t, threading._MainThread):
                    was = t.is_alive
                    t.is_alive = to
                    return was

        events_json = [tjson() for i in range(5)]
        for event_json in events_json:
            w.q.put(event_json, True, None)

        w.start()
        try:
            restore = set_is_alive(lambda: False)
            eventually(w._stopping.isSet)
            eventually(w._halting.isSet)
            eventually(
                lambda: not w.is_alive(),
                _eventually_delta=TDS*5)
        finally:
            set_is_alive(restore)
        '''
        pass


@pytest.mark.transport
class TestTransport(TestCase):
    transport_class = Transport
    worker_class = Worker

    class TAdapter(ListAdapter):
        pass

    class TWorker(Worker):
        pass

    class TQueue(Queue):
        pass

    def test__init__(self):
        t = Transport()
        assert_transport(t)
        assert t.worker is None

    def test__init__kwargs_assigned(self):
        cases = {
            'adapter_class': self.TAdapter,
            'worker_class': self.TWorker,
            'queue_class': self.TQueue,
            'max_queue_size': timedelta(1),
            'max_flush_time': timedelta(2),
            'max_work_time': timedelta(3),
            'max_stopping_time': timedelta(4)}

        for case in cases:
            kwargs = dict()
            kwargs[case] = cases[case]
            t = Transport(**kwargs)
            assert_transport(t)
            assert getattr(t, case) == cases[case]

    def test__init__kwargs_adapter(self):
        adapter = self.TAdapter()
        t = Transport(adapter=adapter)
        assert_transport(t)
        assert t.adapter == adapter

    def test__init__kwargs_adapter_class(self):
        adapter_class = self.TAdapter
        t = Transport(adapter_class=adapter_class)
        assert_transport(t)
        assert isinstance(t.adapter, adapter_class)

    def test__init__kwargs_worker(self):
        sentinel = object()
        t = Transport(worker=sentinel)
        assert_transport(t)
        assert t.worker == sentinel

    def test__init__kwargs_worker_class(self):
        t = Transport(worker_class=self.TWorker)
        assert_transport(t)
        t.start()
        assert isinstance(t.worker, self.TWorker)

    def test__init__kwargs_queue(self):
        queue = self.TQueue()
        t = Transport(queue=queue)
        assert_transport(t)
        assert t.queue == queue

    def test__init__kwargs_queue_class(self):
        t = Transport(queue_class=self.TQueue)
        assert_transport(t)
        assert isinstance(t.queue, self.TQueue)

    def test_start(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True

    def test_start_multiple(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True
        expect_worker = t.worker

        t.start()
        assert t.running is True
        assert t.worker == expect_worker

    def test_stop(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True

        t.stop()
        assert t.running is False

    def test_stop_when_worker_none(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True
        t.worker = None
        t.stop()
        assert t.running is False

    def test_stop_when_worker_stopped(self, t):
        t = Transport()
        t.worker = ThreadedWorker(t)
        assert_transport(t)
        assert t.running is True
        t.stop()
        assert t.running is False

    def test_halt_doesnt_do_work(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True

        expect_json = tjson()
        t.queue.put(expect_json, True, None)
        assert len(t.queue) > 0

        t.halt()
        assert t.running is False
        assert len(t.queue) > 0

    def test_halt_when_worker_stopped(self, t):
        t = Transport()
        t.worker = ThreadedWorker(t)
        assert_transport(t)
        assert t.running is True
        t.halt()
        assert t.running is False

    def test_halt_when_worker_none(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True
        t.worker = None
        t.halt()
        assert t.running is False

    def test_emit(self, t):
        assert_transport(t)
        assert t.running is False

        t.start()
        assert t.running is True
        assert_emit(t)
        assert t.running is True

        t.stop()
        assert t.running is False

    def test_emit_autostart(self, t):
        assert_transport(t)
        assert t.running is False

        assert_emit(t)
        assert t.running is True

        t.stop()
        assert t.running is False

    def test_emit_when_worker_stopped_halts(self, t):
        class TWorker(Worker):
            def work(self, timeout):
                raise WorkerStoppedError

        t = Transport()
        t.worker = TWorker(t)
        assert_transport(t)
        assert t.running is True
        t.emit(tjson())
        assert t.running is False

    def test_flush(self, t):
        assert_transport(t)
        assert t.running is False

        events_json = [tjson() for i in range(10)]
        for event_json in events_json:
            t.emit(event_json)
            chk(t, event_json, False)

        t.flush()
        for index, event_json in enumerate(events_json):
            chk(t, event_json, True, index)

        t.stop()
        assert t.running is False

    def test_flush_when_none(self, t):
        assert_transport(t)
        assert t.running is False
        t.worker = None
        t.flush()
        assert t.running is False

    def test_flush_when_worker_stopped_halts(self, t):
        t = Transport()
        t.worker = ThreadedWorker(t)
        assert_transport(t)
        assert t.running is True
        t.flush()
        assert t.running is False

    def test_ctx_manager(self, t):
        assert_transport(t)
        assert t.running is False

        with t:
            assert t.running is True
        assert t.running is False

    def test__repr__(self, t):
        event_json = tevent().json
        t.start()
        t.adapter.emit(event_json)
        t_str = str(t)
        expect_str = 'Transport('
        expect_str += 'running=True,'
        expect_str += ' queue=Queue(size=0,'
        expect_str += ' backoff=Backoff(max_attempts=15)),'
        expect_str += ' adapter=ListAdapter([{'
        assert t_str.startswith(expect_str)


def assert_group_stopped(g, tps):
    assert not g.running, 'group should not be running'
    for tp in tps:
        assert_transport(tp)
        assert tp in g.transports, \
            'each transport should be added to Group'
        assert not tp.running, 'transport should not be running'


def assert_group_running(g, tps):
    assert g.running, 'group should be running'
    for tp in tps:
        assert_transport(tp)
        assert tp in g.transports, \
            'each transport should be added to Group'
        assert tp.running, 'transport should be running'


def assert_group_emit(g, tps):
    event_json = tjson()
    g.emit(event_json)
    for tp in tps:
        chk(tp, event_json, False)


@pytest.mark.transport_group
class TestTransportGroup(TestCase):
    transport_class = Transport
    worker_class = Worker

    class TAdapter(ListAdapter):
        pass

    class TWorker(Worker):
        pass

    class TQueue(Queue):
        pass

    def test__init__(self):
        tps = [Transport(), Transport(), Transport()]
        g = Group(*tps)
        assert_group_stopped(g, tps)

    def test_running(self):
        # test multiple transport counts
        for n in range(1, 4):
            tps = [Transport() for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            g.start()  # multiple calls to start are okay
            assert_group_running(g, tps)

            g.stop()  # multiple calls to stop are okay
            assert_group_stopped(g, tps)

    def test_stop_when_transport_worker_is_none(self):
        for n in range(1, 4):
            tps = [Transport() for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                tp.worker = None

            g.stop()
            assert_group_stopped(g, tps)

    def test_stop_when_transport_stopped(self):
        for n in range(1, 4):
            tps = [Transport() for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                tp.stop()

            g.stop()
            assert_group_stopped(g, tps)

    def test_halt_group_doesnt_do_work(self):
        for n in range(1, 4):
            tps = [Transport(worker_class=Worker) for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                expect_json = tjson()
                tp.queue.put(expect_json, True, None)
                assert len(tp.queue) == 1

            g.halt()
            assert_group_stopped(g, tps)
            for tp in tps:
                assert len(tp.queue) == 1, 'exp no work on call to halt'

    def test_halt_group_when_stopped(self):
        for n in range(1, 4):
            tps = [Transport(worker_class=Worker) for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.halt()
            assert_group_stopped(g, tps)

    def test_halt_when_transport_worker_is_none(self):
        for n in range(1, 4):
            tps = [Transport() for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                tp.worker = None

            g.halt()
            assert_group_stopped(g, tps)

    def test_halt_when_transport_stopped(self):
        for n in range(1, 4):
            tps = [Transport() for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                tp.stop()

            g.halt()
            assert_group_stopped(g, tps)

    def test_emit(self):
        for n in range(1, 4):
            tps = [
                Transport(worker_class=Worker, adapter_class=ListAdapter) \
                    for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)
            assert_group_emit(g, tps)

            g.stop()
            assert_group_stopped(g, tps)

    def test_emit_autostart(self):
        for n in range(1, 4):
            tps = [
                Transport(worker_class=Worker, adapter_class=ListAdapter) \
                    for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)
            assert_group_emit(g, tps)

            g.stop()
            assert_group_stopped(g, tps)

    def test_emit_underlying_stopped(self):
        for n in range(1, 4):
            tps = [
                Transport(worker_class=Worker, adapter_class=ListAdapter) \
                    for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)
            assert_group_emit(g, tps)
            assert_group_running(g, tps)

            assert all(tp.running for tp in tps)
            assert g.running, 'should be running'
            for tp in tps:
                tp.stop()

            assert all(not tp.running for tp in tps)
            assert not g.running, 'should be stopped'

    def test_emit_flush(self):
        for n in range(1, 4):
            tps = [
                Transport(worker_class=Worker, adapter_class=ListAdapter) \
                    for i in range(n)]

            g = Group(*tps)
            assert_group_stopped(g, tps)

            g.start()
            assert_group_running(g, tps)

            for tp in tps:
                expect_json = tjson()
                tp.queue.put(expect_json, True, None)
                assert len(tp.queue) == 1

            g.flush()
            for tp in tps:
                assert len(tp.queue) == 0, 'exp empty queue after flush'

    def test_ctx_manager(self):
        for n in range(1, 4):
            tps = [
                Transport(worker_class=Worker, adapter_class=ListAdapter) \
                    for i in range(n)]
            g = Group(*tps)
            assert_group_stopped(g, tps)

            with g:
                assert_group_running(g, tps)
            assert_group_stopped(g, tps)

    def test__repr__(self, t):
        g = Group(t)
        g.start()
        assert_group_running(g, [t])

        g_str = str(g)
        print g_str

        expect_str = 'Group(running=True, transports=[Transport('
        expect_str += 'running=True,'
        expect_str += ' queue=Queue(size=0,'
        expect_str += ' backoff=Backoff(max_attempts=15)),'
        expect_str += ' adapter=ListAdapter('
        assert g_str.startswith(expect_str)
