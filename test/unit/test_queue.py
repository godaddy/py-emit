import pytest
import sys
from datetime import datetime, timedelta
from emit.decorators import defer
from emit.utils import Backoff
from emit.queue import (
    Queue, Empty, QueueStat, QueueItem, TailQueueItem, HeadQueueItem)
from ..helpers import TestCase, tevent


def fill_queue(count=100, queue=None, event_factory=None, item_factory=None):
    event_factory = event_factory if not (event_factory is None) else tevent
    item_factory = item_factory if not (item_factory is None) else QueueItem
    queue = queue if not (queue is None) else Queue()

    # Put q_count items into queue
    for i in range(count):
        qi = item_factory(event_factory().json)
        queue.put(qi)
    assert_queue(queue)
    return queue


def assert_queue(q):
    assert str(q).startswith('Queue(size={0}'.format(len(q.queue)))
    assert str(q).endswith(')')
    assert len(q.queue)
    assert len(q.queue) == q._qsize()


def assert_queue_empty(q):
    assert_queue(q)
    with pytest.raises(Empty) as excinfo:
        q.get(False)
    assert '' == str(excinfo.value)


@pytest.mark.queue
class TestQueue(TestCase):

    def test_init(self):
        q = Queue()
        assert q.maxsize == Queue.MAX_SIZE
        assert isinstance(q._backoff, Backoff)

    def test_init_max_size(self):
        q = Queue(max_size=250)
        assert q.maxsize == 250
        assert isinstance(q._backoff, Backoff)

    def test_init_backoff(self):
        b = Backoff(10)
        assert b.max_attempts == 10
        q = Queue(backoff=b)
        assert q.maxsize == Queue.MAX_SIZE
        assert isinstance(q._backoff, Backoff)
        assert q._backoff.max_attempts == 10

    def test_private_init(self):
        q = Queue()
        q.queue = 'test_private_init'
        assert not isinstance(q.queue, list)
        q._init(q.maxsize)
        assert isinstance(q.queue, list)

    def test_private_qsize(self):
        q = Queue()
        q.queue.append('test_private_init')
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()

    def test_private_qsize_len(self):
        q = Queue()
        q.queue.append('test_private_init')
        assert q._qsize() == 1
        assert len(q) == 1
        assert len(q.queue) == q._qsize()
        assert len(q) == q._qsize()

    def test_private_put(self):
        q = Queue()
        q.put('test_private_init')
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()

    def test_private_clear(self):
        payload = tevent().json
        qi = QueueItem(payload)
        q = Queue()
        q.put_item(qi)
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()
        q.clear()
        with pytest.raises(Empty) as excinfo:
            q.get(False)
        assert '' == str(excinfo.value)

    def test_private_get(self):
        payload = tevent().json
        q = Queue()
        qi = q.put(payload)
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()
        item = q._get()
        assert item == qi
        assert item.payload == payload
        item = q._get()
        assert item is None

    def test_private_get_many(self):
        q = Queue()
        q_count = 100
        total = item = 0

        # Put q_count items into queue
        for i in range(q_count):
            assert q._qsize() == i
            assert len(q.queue) == q._qsize()
            payload = tevent().json
            qi = QueueItem(payload)
            q.put_item(qi)
            assert q._qsize() == i + 1
            assert len(q.queue) == q._qsize()

        # Get back q_count items
        while not (item is None):
            assert q._qsize() == q_count - total
            assert len(q.queue) == q._qsize()
            item = q._get()
            total += 1
            assert q._qsize() == max(q_count - total, 0)
            assert len(q.queue) == q._qsize()

    @pytest.mark.slow
    def test_private_get_many_with_failures(self):
        q = Queue()
        q_count = 100
        attempted_list = []
        total = item = 0

        # Put q_count items into queue
        for i in range(q_count):
            assert q._qsize() == i
            assert len(q.queue) == q._qsize()
            payload = tevent().json
            q.put(payload)
            assert q._qsize() == i + 1
            assert len(q.queue) == q._qsize()

        # Every 10th item should fail as many times as it's index. Item 10
        # fails 1 time, item 20 fails twice and so on.
        for index, item in enumerate(q.queue[0::10]):
            attempted_list.append(item)
            for i in range(index + 1):
                item.attempt()

        # Pull from queue until only our failed items are left
        while not (item is None):
            if (q_count - total) < q_count / 10:
                break
            assert q._qsize() == q_count - total
            assert len(q.queue) == q._qsize()

            # Fetch items until it's none, which should be the qcount / 10
            item = q._get()
            if item is None:
                assert q._qsize() == q_count / 10
                break

            total += 1
            assert q._qsize() == max(q_count - total, 0)
            assert len(q.queue) == q._qsize()

        # Make sure all items in queue are failures only
        for item in q.queue:
            assert item in attempted_list

        # Assert none still
        item = q._get()
        assert item is None
        assert q._qsize() == (q_count / 10)
        assert len(q.queue) == q._qsize()

        # Now set time back 2 seconds and we should get the lowest attempt
        # count item in the queue.
        attempted_list[0].last_attempt -= timedelta(seconds=2)
        item = q._get()
        assert item == attempted_list[0]

        # Next item shouldn't be ready
        item = q._get()
        assert item is None

    def test_private_get_backoff(self):
        payload = tevent().json
        q = Queue()
        qi = q.put(payload)
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()

        item = q._get()
        assert item.payload == payload

        item.attempt()
        q.put_item(item)
        assert q._qsize() == 1
        item = q._get()
        assert item is None
        assert q._qsize() == 1

        # Set time back 4 seconds
        qi.last_attempt -= timedelta(seconds=2)
        item = q._get()
        assert item.payload == payload
        item.attempt()
        q.put_item(item)
        assert q._qsize() == 1

        # Shouldn't have a ready item
        item = q._get()
        assert item is None
        assert q._qsize() == 1

        # Set the time back 4 seconds to see if it's ready to fetch now
        qi.last_attempt -= timedelta(seconds=4)
        item = q._get()
        assert item.payload == payload

    def test_get(self):
        payload = tevent().json
        q = Queue()
        qi = q.put(payload)
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()
        item = q.get()
        assert item == qi
        assert item.payload == payload
        assert q._qsize() == 0
        assert len(q.queue) == q._qsize()

    @pytest.mark.slow
    def test_get_blocking(self):
        payload = tevent().json
        q = Queue()
        qi = q.put(payload)
        assert q._qsize() == 1
        assert len(q.queue) == q._qsize()
        item = q.get(True)
        assert item == qi
        assert item.payload == payload
        assert q._qsize() == 0
        assert len(q.queue) == q._qsize()

        sleep_delta = timedelta(0, 0, 0, 100)  # 1/10th second sleep
        item_stack = []

        @defer(duration=sleep_delta)
        def deferred():
            msg = tevent().json
            item_stack.append(q.put(msg))

        for i in range(5):
            deferred()
            item = q.get(True)
            assert item.payload == item_stack[0].payload
            assert q._qsize() == 0
            assert len(q.queue) == q._qsize()
            item_stack.pop()
        assert True, 'fetched every item'

    def test_get_blocking_timeout(self):
        q = Queue()
        wait_seconds = .005
        before = datetime.utcnow()
        with pytest.raises(Empty) as excinfo:
            q.get(True, wait_seconds)
        after = datetime.utcnow()
        assert '' == str(excinfo.value)
        assert q._qsize() == 0
        assert len(q.queue) == q._qsize()
        assert (after - before).total_seconds() >= wait_seconds

    def test_get_blocking_timeout_neg(self):
        q = Queue()
        with pytest.raises(ValueError) as excinfo:
            q.get(True, -1)
        assert "'timeout' must be a non-negative number" == str(excinfo.value)
        assert q._qsize() == 0
        assert len(q.queue) == q._qsize()

    def test_get_no_blocking(self):
        q = Queue()
        with pytest.raises(Empty) as excinfo:
            q.get(False)
        assert '' == str(excinfo.value)
        assert q._qsize() == 0
        assert len(q.queue) == q._qsize()

    def test_reset(self):
        q_count = 100
        q = fill_queue(q_count)
        item = 0

        # fail every item in queue and assert we are empty
        for qi in q.queue:
            for i in range(10):
                qi.attempt()
        assert_queue_empty(q)

        # Reset queue and assert all items are ready
        q.reset()

        while not (item is None):
            item = q.get(False)

            if q._qsize() == 0:
                break


@pytest.mark.queue
@pytest.mark.queue_stat
class TestQueueStat(TestCase):

    def test_repr(self):
        stat = QueueStat(Queue())
        assert str(stat).startswith('QueueStat(')
        assert str(stat).endswith(')')

    def test_basic(self):
        q_count = 250
        q = fill_queue(q_count)
        assert_queue(q)

        # Every 10th item should fail as many times as it's index. Item 10
        # fails 1 time, item 20 fails twice and so on.
        for index, item in enumerate(q.queue[0::10]):
            for i in range(index + 1):
                item.attempt()

        for i in [2, 4, 6]:
            for index, item in enumerate(q.queue[0::i]):
                for x in range(i / 2):
                    item.attempt()
        stat = q.stat()
        assert isinstance(stat, QueueStat)
        assert stat.ready == q_count / 2


@pytest.mark.queue
@pytest.mark.queue_item
class TestQueueItem(TestCase):

    def test_init(self):
        payload = tevent().json
        qi = QueueItem(payload)
        assert qi.payload == payload
        assert qi.attempts == 0
        assert qi.last_attempt is None


@pytest.mark.queue
@pytest.mark.queue_item
@pytest.mark.queue_head_item
class TestHeadQueueItem(TestCase):

    def test_init(self):
        qi = HeadQueueItem()
        assert qi.payload is None
        assert qi.attempts == -sys.maxint
        assert qi.last_attempt == datetime.min

    def test_when_inserted_at_head(self):
        q_count = 100

        # Put a HeadQueueItem at head of queue
        q = Queue()
        head_item = q.put_head(None)
        fill_queue(q_count, q)
        assert len(q.queue) == q_count + 1
        assert len(q.queue) == q._qsize()

        # Assert head of queue is our HeadQueueItem
        assert q._get() == head_item

    def test_when_inserted_at_center(self):
        q_count = 100

        # Put a HeadQueueItem at tail of queue
        q = fill_queue(q_count)
        head_item = q.put_head(None)
        fill_queue(q_count, q)
        assert len(q.queue) == q_count * 2 + 1
        assert len(q.queue) == q._qsize()

        # Assert head of queue is our HeadQueueItem
        assert q._get() == head_item

    def test_when_inserted_at_tail(self):
        q_count = 100

        # Put a HeadQueueItem at tail of queue
        head_item = HeadQueueItem()
        q = fill_queue(q_count)
        head_item = q.put_head(None)
        assert len(q.queue) == q_count + 1
        assert len(q.queue) == q._qsize()

        # Assert head of queue is our HeadQueueItem
        assert q._get() == head_item


@pytest.mark.queue
@pytest.mark.queue_item
@pytest.mark.queue_tail_item
class TestTailQueueItem(TestCase):

    def test_init(self):
        qi = TailQueueItem()
        assert qi.payload is None
        assert qi.attempts == sys.maxint
        assert qi.last_attempt == datetime.min

    def test_when_inserted_at_head(self):
        q_count = 100
        item = 0

        # Put a TailQueueItem at head of queue
        q = Queue()
        tail_item = q.put_tail(None)
        fill_queue(q_count, q)
        assert len(q.queue) == q_count + 1
        assert len(q.queue) == q._qsize()

        # Assert tail of queue is our TailQueueItem
        while not (item is None):
            item = q._get()

            if item == tail_item:
                assert q._qsize() == 0

    def test_when_inserted_at_center(self):
        q_count = 100
        item = 0

        # Put a TailQueueItem at tail of queue
        q = fill_queue(q_count)
        tail_item = q.put_tail(None)
        fill_queue(q_count, q)
        assert len(q.queue) == q_count * 2 + 1
        assert len(q.queue) == q._qsize()

        # Assert tail of queue is our TailQueueItem
        while not (item is None):
            item = q._get()

            if item == tail_item:
                assert q._qsize() == 0

    def test_when_inserted_at_tail(self):
        q_count = 100
        item = 0

        # Put a TailQueueItem at tail of queue
        q = fill_queue(q_count)
        tail_item = q.put_tail(None)
        assert len(q.queue) == q_count + 1
        assert len(q.queue) == q._qsize()

        # Assert tail of queue is our TailQueueItem
        while not (item is None):
            item = q._get()

            if item == tail_item:
                assert q._qsize() == 0
