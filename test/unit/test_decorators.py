import pytest
import re
from time import sleep
from datetime import datetime, timedelta
from emit.decorators import backoff_barrier, defer, unreliable, slow, delay
from ..helpers import TestCase


class BackoffFixture(object):
    def __init__(self):
        self.raise_exception = False

    @backoff_barrier
    def barrier_no_args(self):
        if self.raise_exception:
            raise Exception('raise_exception=True')

    @backoff_barrier(max_attempts=10)
    def barrier_max_attempts(self):
        if self.raise_exception:
            raise Exception('raise_exception=True')


def assert_unreliable(func, func_name=None, iterations=5, exc_class=None, exc_match=None):
    func_name = func_name if not (func_name is None) else func.__name__
    exc_class = exc_class if not (exc_class is None) else RuntimeError
    exc_match = exc_match if not (exc_match is None) else \
        '^unreliable func `' + func_name + '` rolled [0-9]{2,3}% exceeding' \
        ' [0-9]{2,3}% success rate causing ' + exc_class.__name__ + '$'
    assert callable(func)

    for i in range(iterations):
        calls = 0

        with pytest.raises(exc_class) as excinfo:
            while calls < 10000:
                calls += 1
                func()
        exc_val = str(excinfo.value)

        matches = re.match(exc_match, exc_val)
        assert exc_val, 'empty exception'
        assert_msg = 'failed to match matches={matches} exc_val={exc_val}' \
                     ' exc_class={exc_class} exc_match={exc_match}'
        assert matches, assert_msg.format(
            matches=matches, exc_val=exc_val, exc_class=exc_class.__name__, exc_match=exc_match)


@pytest.mark.decorators
@pytest.mark.backoff
@pytest.mark.backoff_barrier
class TestBackoffBarrier(TestCase):

    @pytest.mark.slow
    def test_barrier_wait(self):
        before = datetime.utcnow()
        b = BackoffFixture()
        b.barrier_no_args()
        assert datetime.utcnow() < (before + timedelta(seconds=1))

        b.raise_exception = True
        with pytest.raises(Exception) as excinfo:
            b.barrier_no_args()
        assert 'raise_exception=True' == str(excinfo.value)

        b.raise_exception = False
        b.barrier_no_args()
        assert datetime.utcnow() > (before + timedelta(seconds=1))


@pytest.mark.decorators
@pytest.mark.defer
class TestDefer(TestCase):

    @pytest.mark.slow
    def test_basic(self):
        began = datetime.utcnow()
        sleep_delta = timedelta(0, 0, 0, 100)  # 1/10th second sleep
        deferred_stack = []

        @defer(duration=sleep_delta)
        def deferred():
            deferred_stack.append(True)

        for i in range(5):
            deferred()

            while not len(deferred_stack):
                sleep((sleep_delta).total_seconds())
                if (datetime.utcnow() - began) > timedelta(seconds=4):
                    raise Exception('timed out waiting for deferred #{0}'.format(i))
            deferred_stack.pop()
        assert True, 'called all deferreds'


@pytest.mark.decorators
@pytest.mark.unreliable
class TestUnreliable(TestCase):
    def test_basic(self):
        @unreliable
        def test_basic_call():
            pass
        assert_unreliable(test_basic_call)

        @unreliable()
        def test_basic_call_as_func():
            pass
        assert_unreliable(test_basic_call_as_func)

    def test_args(self):
        assert_unreliable(unreliable(lambda: None))
        assert_unreliable(unreliable()(lambda: None))
        assert_unreliable(unreliable(success_rate=.2)(lambda: None))
        assert_unreliable(unreliable(exc_class=ValueError)(lambda: None), exc_class=ValueError)
        assert_unreliable(unreliable(success_rate=.2, exc_class=ValueError)(lambda: None), exc_class=ValueError)
        assert_unreliable(unreliable(exc_format='test_args')(lambda: None), exc_match='test_args')


@pytest.mark.decorators
@pytest.mark.slow
class TestSlow(TestCase):

    @pytest.mark.slow
    def test_basic(self):
        began = datetime.utcnow()
        min_duration = timedelta(0, 0, 0, 100)
        max_duration = timedelta(0, 0, 0, 250)
        slow_stack = []

        @slow(min_duration=min_duration, max_duration=max_duration)
        def slow_func():
            slow_stack.append(True)
        slow_func()

        assert len(slow_stack)
        assert datetime.utcnow() < (began + max_duration + min_duration), \
            'slow func took too long'


@pytest.mark.decorators
@pytest.mark.delay
class TestDelay(TestCase):

    @pytest.mark.slow
    def test_basic(self):
        began = datetime.utcnow()
        duration = timedelta(0, 0, 0, 100)
        margin = duration / 10
        slow_stack = []

        @delay(duration=duration)
        def slow_func():
            slow_stack.append(True)
        slow_func()
        assert datetime.utcnow() < (began + duration) + margin, \
            'delay func slower than duration'
        assert datetime.utcnow() > began + duration, \
            'delay func faster than duration'
        assert len(slow_stack), 'delay func did not run'
