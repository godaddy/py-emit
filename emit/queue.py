from __future__ import absolute_import
import sys
import time
import operator
from datetime import datetime
from .utils import Backoff, Tracker
from .globals import log


try:
    import queue
except ImportError:
    import Queue as queue


Queues = ['Queue']


__all__ = Queues + [
    'Queues', 'Empty', 'QueueItem', 'TailQueueItem', 'HeadQueueItem']


class QueueItem(Tracker, object):
    def __repr__(self):
        return '{0}(attempts={1}, last={2}, payload={3})'.format(
            self.__class__.__name__, self.attempts, self.last_attempt, repr(self.payload))

    def __init__(self, payload, *args, **kwargs):
        super(QueueItem, self).__init__(*args, **kwargs)
        self.payload = payload
        self.created = datetime.utcnow()


class TailQueueItem(QueueItem, object):
    """Queue item which will always be placed at the tail of the queue."""
    def __init__(self, payload=None, *args, **kwargs):
        super(TailQueueItem, self).__init__(payload, *args, **kwargs)
        self.attempts = sys.maxint
        self.last_attempt = datetime.min


class HeadQueueItem(QueueItem, object):
    """Queue item which will always be placed at the head of the queue."""
    def __init__(self, payload=None, *args, **kwargs):
        super(HeadQueueItem, self).__init__(payload, *args, **kwargs)
        self.attempts = -sys.maxint
        self.last_attempt = datetime.min


class Empty(queue.Empty):
    pass


class QueueStat(object):
    def __repr__(self):
        return 'QueueStat(size={0} ready={1} oldest={2})'.format(
            self.size, self.ready, self.oldest)

    def __init__(self, q):
        self.oldest = None
        self.ready = 0
        self.size = len(q.queue)

        if self.size == 0:
            return
        for item in q.queue:
            if item.expired():
                self.ready += 1
            if (self.oldest is None) or self.oldest.expires() <= item.expires():
                self.oldest = item


class Queue(queue.Queue):
    """Number of items which may be enqueued before blocking."""
    MAX_SIZE = 0  # Never block, queue forever

    def __len__(self):
        return self.qsize()

    def __repr__(self):
        return '{0}(size={1}, backoff={2})'.format(
            self.__class__.__name__, self._qsize(), self._backoff)

    def __init__(self, **kwargs):
        queue.Queue.__init__(self, kwargs.get('max_size', Queue.MAX_SIZE))
        self._backoff = kwargs.get('backoff', Backoff())

    def put(self, payload, block=True, timeout=None, queue_item_class=QueueItem):
        return self.put_item(
            queue_item_class(payload, backoff=self._backoff), block, timeout)

    def put_item(self, item, block=True, timeout=None):
        assert isinstance(item, QueueItem), '`item` must be a QueueItem obj'
        queue.Queue.put(self, item, block, timeout)
        return item

    def put_head(self, payload, block=True, timeout=None):
        return self.put(
            payload, block, timeout, queue_item_class=HeadQueueItem)

    def put_tail(self, payload, block=True, timeout=None):
        return self.put(
            payload, block, timeout, queue_item_class=TailQueueItem)

    def get(self, block=True, timeout=None):
        # https://github.com/python/cpython/blob/2.7/Lib/Queue.py#L150
        # Modified slightly to keep an identical interface while allowing
        # exponential backoff of queue items.
        with self.not_empty:
            item = self._get()

            if not block:
                if item is None:
                    raise Empty
            elif timeout is None:
                while not item:
                    self.not_empty.wait()
                    item = self._get()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time.time() + timeout

                while not item:
                    remaining = endtime - time.time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
                    item = self._get()
            self.not_full.notify()
            return item

    def stat(self):
        with self.mutex:
            self._sort()
            return QueueStat(self)

    def reset(self):
        with self.mutex:
            log('Queue.reset() - resetting queue')
            for item in self.queue:
                item.reset()

    def clear(self):
        with self.mutex:
            log('Queue.clear() - clearing all items from queue')
            del self.queue[:]
            self.all_tasks_done.notify_all()
            self.unfinished_tasks = 0

    def _sort(self):
        self.queue.sort(key=operator.attrgetter('attempts', 'last_attempt'))

    def _init(self, maxsize):
        self.queue = []

    def _qsize(self, len=len):
        return len(self.queue)

    def _put(self, item):
        log('Queue._put() - put item {} into queue'.format(item))
        self.queue.append(item)

    def _get(self):
        self._sort()

        for (index, item) in enumerate(self.queue):
            if item.attempts > 0:
                if item.expired():
                    log.debug('Queue._get() - {} has {} attempts, elapsed is {} making it eligible for retry'.format(
                        item, item.attempts, item.elapsed()))
                    return self.queue.pop(index)
                else:
                    log.debug('Queue._get() - {} has {} attempts and is ineligible for retry until {}'.format(
                        item, item.attempts, item.expires()))
            else:
                log.debug('Queue._get() - {} is ready for first attempt'.format(item))
                return self.queue.pop(index)
        return None
