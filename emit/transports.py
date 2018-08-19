import threading
from datetime import timedelta, datetime
from .queue import Empty
from .utils import Backoff, Tracker, _timeout_delta
from .globals import log, ConfigDescriptor
from .adapters import (AdapterError, AdapterClosedError, AdapterEmitError, AdapterEmitPermanentError)


Transports = ['Transport']
Workers = ['Worker', 'ThreadedWorker']


__all__ = Transports + Workers + [
    'Transports', 'Workers', 'WorkerError', 'WorkerStoppedError']


class WorkerError(Exception):
    """Base error for workers to share."""
    def __init__(self, trigger=None):
        self.trigger = trigger


class WorkerStoppedError(WorkerError):
    """Indicates the worker has stopped abnormally."""


class Worker(object):
    def __init__(self, transport, tracker=None):
        self.tracker = tracker if tracker is not None else Tracker(Backoff(10))
        self.transport = transport

    def __repr__(self):
        return '{}(tracker={}, transport={})'.format(
            self.__class__.__name__, self.tracker, self.transport)

    @property
    def t(self):
        """Forwards to transport."""
        return self.transport

    @property
    def q(self):
        """Forwards to transports queue."""
        return self.transport.queue

    @property
    def adapter(self):
        """Forwards to transports adapter."""
        return self.transport.adapter

    def flush(self, timeout):
        """Forwards to adapter flush."""
        try:
            self.adapter.flush(timeout)
        except AdapterClosedError:
            # No point in throwing here, nothing to flush
            pass

    def start(self):
        """Forwards to adapter open."""
        self.adapter.open()

    def stop(self, timeout):
        """Begins the shutdown process, for this worker it simply will work()
        for a given period of time after resetting queue and tracker."""
        self.reset()
        self.work(timeout)
        self.flush(self.t.max_flush_time)
        self.adapter.close()

    def reset(self):
        """Reset queue and tracker. It does ensure tracker attempts are more
        than 4 attempts first, to prevent any sort of relentless start/stop
        loops in user space."""
        if self.tracker.attempts > 5:
            self.tracker.reset()
        self.q.reset()

    def work(self, timeout):
        """Calls process with timeout and catches any exceptions. It's the run()
        implementation for a regular Worker."""
        try:
            expires = datetime.utcnow() + timeout
            td0 = timedelta()

            while True:
                remaining = expires - datetime.utcnow()

                if remaining <= td0:
                    break

                # Queue is empty if this returns False
                if not self.process_queue(remaining):
                    break
        except Exception as e:
            log('TransportWorker.work - an uncaught exception occurred')
            log.exception(e)

    def process(self, timeout):
        """Should process items in queue until timeout has been exceeded or
        queue is empty."""
        try:

            # Queue is empty if this returns False
            return self.process_queue(timeout)

        # If an adapter closed error was thrown then our open() attempt failed
        # or there was a connection drop while emitting. We will manually close
        # the adapter to trigger a reopen in case adapter gets in a bad state.
        except AdapterClosedError as e:
            self.adapter.close()
            log('Worker.check_adapter - adapter is currently closed and can'
                ' not be opened. Tracker: {0}'.format(self.tracker))
            log.exception(e)

    def process_queue(self, timeout):
        item = None

        try:
            item = self.fetch_item()

            # check the adapter
            self.check_adapter(timeout)

            # Process our item
            self.process_item(item)

            # We may have more items remaining since we were not empty
            return True

        except Empty:
            return False

        finally:
            if item:
                self.q.task_done()

    def fetch_item(self):
        return self.q.get(False)

    def check_adapter(self, timeout):
        """Ensures the adapter is ready to emit messages. Returns True when the
        adapter is ready, False otherwise. May raise any adapter exception."""
        if self.adapter.closed or self.tracker.attempts > 0:

            # No point in blocking the application if the backoff period
            # remaining exceeds the current timeout.
            if self.tracker.remaining() > timeout:
                log('Worker.check_adapter - the timeout would exceed the'
                    ' remaining backoff duration. Tracker: {}'.format(self.tracker))
                return False

            # Don't try to wait longer then the max stopping seconds, this is
            # just to prevent edge cases / threading issues in user space. We
            # should be guarded from this above ^
            self.tracker.wait(max_seconds=timeout.total_seconds())

            # After waiting for tracker backoff we still haven't expired
            # something is probably wrong.
            if not self.tracker.expired():
                log.warn('Worker.check_adapter - tracker did not expire despite'
                         ' waiting for the remaining duration. Tracker: {}'.format(self.tracker))
                return False

            # Open adapter, if the adapter does not throw then it was a success.
            self.tracker.attempt()
            self.adapter.open()
            self.tracker.reset()

        # We have an open adapter
        return True

    def process_item(self, item):
        if item is None:
            return
        try:

            # attempt to deliver the item via adapter
            item.attempt()
            self.adapter.emit(item.payload)
            item.reset()

        # Event can't be sent, we won't return it to the queue
        except AdapterEmitPermanentError:
            log.error('TransportWorker.process_queue - permanent failure for item({0})'.format(item))

        # Event wasn't sent, but adapter doesn't think the error is
        # permanent so return it to queue, no need to raise.
        except AdapterEmitError:
            self.q.put_item(item)

        # Return this item to the queue and notify caller the adapter
        # has been closed unexpectedly.
        except AdapterClosedError:
            self.q.put_item(item)
            raise


class ThreadedWorker(Worker, threading.Thread):

    class HaltWorker(object):
        """Signals for the transport worker to immediately exit."""

    class StopWorker(timedelta):
        """Signals for the transport worker to begin a graceful exit. It's value
        should be the amount of time it may spend stopping."""

    class FlushWorker(timedelta):
        """Signals for the transport worker to flush per user request."""

    def __init__(self, transport):
        Worker.__init__(self, transport)
        threading.Thread.__init__(self)
        self._flush_pending = False
        self._adapter = self.transport.adapter()
        self._started = threading.Event()
        self._halting = threading.Event()
        self._stopping = threading.Event()
        self._stopping_timer = None

    @property
    def adapter(self):
        """We use a copy of the adapter instead of the transports."""
        return self._adapter

    def start(self):
        """Called from transport thread. Starts the Worker. Does nothing if it
        has already been started."""
        if not self._started.isSet():
            self._started.set()
            threading.Thread.start(self)

    def stop(self, timeout):
        """Called from transport thread. Asks the worker to stop. We will put a
        _stop_worker sentinel in the queue then join(). The worker thread context
        will pick up the sentinel item and begin shutdown."""
        if not self._started.isSet() or (not self.is_alive()):
            # Do not attempt to join() multiple times
            raise WorkerStoppedError
        self.q.put_head(self.StopWorker(seconds=timeout.total_seconds()))
        self.join()

    def halt(self):
        """Called from transport thread. Request for the worker to halt."""
        if not self._started.isSet() or (not self.is_alive()):
            # Do not attempt to join() multiple times
            raise WorkerStoppedError
        self.q.put_head(self.HaltWorker())
        self.join()

    def flush(self, timeout):
        """Called from transport thread. Requests for adapter to be flushed."""
        if not self._started.isSet() or (not self.is_alive()):
            # If we have not started OR the thread is not alive we shouldn't
            # try to place queue items onto it.
            raise WorkerStoppedError
        self.q.put_tail(self.FlushWorker())

    def work(self, timeout):
        """Called from transport thread. Check if alive and raise if not."""
        if (not self._started.isSet()) or (self._started.isSet() and not self.is_alive()):
            # If we already "started" the thread should be alive.
            raise WorkerStoppedError

    def run(self):
        """The threads run() implementation."""
        process = super(ThreadedWorker, self).process

        try:
            # Just work() until we are orphaned or stopping event
            while not self._stopping.isSet():
                process(self.t.max_work_time)
                self.check_orphaned()
                self.check_flush()

            try:
                # If we are halting just return
                if self._halting.isSet():
                    return

                # When this expires halting will be set, after reset the queue
                if self._stopping_timer:
                    self._stopping_timer.start()
                self.reset()

                # Acquire all tasks done condition and wait till we deplete
                # queue or time out.
                # with self.q.all_tasks_done:
                while not self._halting.isSet():
                    if self.q.empty():
                        self._halting.set()
                    else:
                        process(self.t.max_work_time)
                    self.check_flush()
                if not self.q.empty():
                    log('ThreadedWorker.run - worker exiting with {}'
                        ' items stil in the queue'.format(len(self.q)))
            finally:
                if self._stopping_timer:
                    self._stopping_timer.cancel()
        except Exception as e:
            log('ThreadedWorker.run - uncaught exception')
            log.exception(e)

    def check_orphaned(self):
        """Check to see if we are an orphan or starting on top of ourself, if so
        we will stop / halt."""
        if self._stopping.isSet():
            return
        for t in threading.enumerate():
            if isinstance(t, self.__class__) and t != self and t.t == self.t and t.is_alive():
                with t.t.lock:
                    if not t._stopping.isSet():
                        self._stopping.set()
                        self._halting.set()
                        log('ThreadedWorker.check_orphaned - found another '
                            'worker belonging to this transport, halting')
                        return
            if isinstance(t, threading._MainThread) and not t.is_alive():
                stop_seconds = self.t.max_stopping_time.total_seconds()
                msg = 'ThreadedWorker.check_orphaned - main thread has ' \
                      'died, flushing queue for up to {} seconds'
                log(msg.format(stop_seconds))
                self._stopping.set()
                self._stopping_timer = threading.Timer(
                    stop_seconds, self._halting.set)
                return

    def check_flush(self):
        """This just checks a flag _flush_pending to be called when the worker
        is idle. Whenever a event is sent this is set to True, then the next
        work iteration with an empty queue we flush."""
        if self._flush_pending:
            try:
                self.adapter.flush()
            except AdapterError:
                pass
            finally:
                self._flush_pending = False

    def fetch_item(self):
        """We override the Worker.fetch_item to make the request blocking."""
        return self.q.get(True, self.t.max_work_time.total_seconds())

    def process_item(self, item):
        """Override fetch item to check for our sentinels."""
        if isinstance(item.payload, self.StopWorker):
            self._stopping.set()
            self._stopping_timer = threading.Timer(
                item.payload.total_seconds(), self._halting.set)
        elif isinstance(item.payload, self.HaltWorker):
            self._stopping.set()
            self._halting.set()
        elif isinstance(item.payload, self.FlushWorker):
            self._flush_pending = True
        else:
            super(ThreadedWorker, self).process_item(item)
            self._flush_pending = True


class Transport(object):
    adapter_class = ConfigDescriptor('adapter_class')
    worker_class = ConfigDescriptor('worker_class')
    queue_class = ConfigDescriptor('queue_class')
    max_stopping_time = ConfigDescriptor('max_stopping_time')
    max_flush_time = ConfigDescriptor('max_flush_time')
    max_work_time = ConfigDescriptor('max_work_time')
    max_queue_size = ConfigDescriptor('max_queue_size')

    def __init__(
            self, adapter=None, worker=None, queue=None, max_queue_size=None,
            max_flush_time=None, max_work_time=None, max_stopping_time=None,
            adapter_class=None, worker_class=None, queue_class=None):
        if adapter_class is not None:
            self.adapter_class = adapter_class
        if worker_class is not None:
            self.worker_class = worker_class
        if queue_class is not None:
            self.queue_class = queue_class
        if max_queue_size is not None:
            self.max_queue_size = max_queue_size
        if max_flush_time is not None:
            self.max_flush_time = max_flush_time
        if max_work_time is not None:
            self.max_work_time = max_work_time
        if max_stopping_time is not None:
            self.max_stopping_time = max_stopping_time

        self.queue = queue if queue is not None else self.queue_class()
        self.adapter = adapter if adapter is not None else self.adapter_class()
        self.lock = threading.RLock()
        self.worker = worker

    def __repr__(self):
        return '{}(running={}, queue={}, adapter={})'.format(
            self.__class__.__name__, self.running, self.queue, self.adapter)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, tb):
        self.stop()

    @property
    def running(self):
        return self.worker is not None

    def start(self):
        with self.lock:
            if self.worker is not None:
                return
            self.worker = self.worker_class(self)
            self.worker.start()

    def stop(self, timeout=None):
        """Timeout is the max time spent stopping in seconds, does not include
        the flush timeout which is a separate configuration item."""
        with self.lock:
            if self.worker is None:
                return
            try:
                timeout = _timeout_delta(timeout, self.max_stopping_time)
                self.worker.stop(timeout)
            except WorkerStoppedError:
                pass
            finally:
                self.worker = None

    def halt(self):
        """Halts the worker as soon as possible without working on any items in
        the queue or giving the adapter the opportunity to relinquish it's buffers."""
        with self.lock:
            if self.worker is None:
                return
            try:
                self.worker.stop(-1)
            except WorkerStoppedError:
                pass
            finally:
                self.worker = None

    def flush(self, timeout=None):
        """Flush will notify the adapter we want to spend `timeout` time letting
        the adapter relinquish any buffers."""
        with self.lock:
            if self.worker is None:
                return
            try:
                timeout = _timeout_delta(timeout, self.max_flush_time)
                self.worker.flush(timeout)
            except WorkerStoppedError:
                self.halt()

    def emit(self, event_json, timeout=None):
        """Places a message into the queue then notifies the worker."""
        self.queue.put(event_json, True, None)

        if self.worker is None:
            log('Transport.emit - starting worker')
            self.start()
        try:
            timeout = _timeout_delta(timeout, self.max_work_time)
            self.worker.work(timeout)
        except WorkerStoppedError:
            self.halt()
