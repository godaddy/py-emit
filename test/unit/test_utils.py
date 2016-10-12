import pytest
from datetime import datetime, timedelta
from time import sleep
from emit.utils import (
    Backoff, Tracker, Called, _debug_assert, _is_string, _is_value,
    _timeout_seconds, _timeout_delta)
from emit.globals import conf
from ..helpers import TestCase


@pytest.mark.utils
@pytest.mark.utils_functions
class TestUtilFunctions(TestCase):

    def test_is_string(self):
        cases = {
            True: ['', u'', unicode(), str()],
            False: [dict(), set([]), False, True]}

        for expect, tests in cases.iteritems():
            for test in tests:
                assert _is_string(test) == expect

    def test_timeouts(self):
        cases = [
            [0, None, None],
            [5, 5, None],
            [5, None, 5],
            [5, 5, 10],
            [5, 5, 5]]

        def d(v):
            if v is None:
                return None
            return timedelta(seconds=v)

        ts = _timeout_seconds
        td = _timeout_delta

        for case in cases:
            expect, timeout, default = case
            assert expect == ts(timeout, default)
            assert d(expect) == td(timeout, default)

            assert expect == ts(d(timeout), default)
            assert d(expect) == td(d(timeout), default)

            assert expect == ts(timeout, d(default))
            assert d(expect) == td(timeout, d(default))

            assert expect == ts(d(timeout), d(default))
            assert d(expect) == td(d(timeout), d(default))

    def test_is_value(self):
        cases = {
            True: ['a', u'a', ['a'], {'a': ''}, datetime.utcnow(), set(['a', 'b'])],
            False: [dict(), False, True, None]}

        for expect, tests in cases.iteritems():
            for test in tests:
                assert _is_value(test) == expect

    def test_debug_assert_on(self):
        restore = conf.debug

        try:
            conf.debug = False
            _debug_assert(False, 'test_debug_assert_off')
            _debug_assert(True, 'test_debug_assert_off')
            _debug_assert(False)
            _debug_assert(True)
        finally:
            conf.debug = restore

    def test_debug_assert_off(self):
        restore = conf.debug

        try:
            conf.debug = True
            expect_msg = '. Disable debug mode to remove this assertion.'

            with pytest.raises(AssertionError) as excinfo:
                _debug_assert(False, 'test_debug_assert_on')
            assert str(excinfo.value) == 'test_debug_assert_on' + expect_msg
            _debug_assert(True, 'test_debug_assert_on')

            with pytest.raises(AssertionError) as excinfo:
                _debug_assert(False)
            assert str(excinfo.value) == 'AssertionError' + expect_msg
            _debug_assert(True)
        finally:
            conf.debug = restore


@pytest.mark.utils
@pytest.mark.backoff
class TestBackoff(TestCase):
    def test_defaults(self):
        assert Backoff.max_attempts == 15
        assert Backoff.zero == timedelta()

    def test_str(self):
        assert str(Backoff()) == 'Backoff(max_attempts={0})'.format(Backoff.max_attempts)

    def test_init(self):
        backoff = Backoff()
        assert backoff.max_attempts == Backoff.max_attempts
        assert len(backoff.deltas) == 16

    def test_init_max_attempts(self):
        backoff = Backoff(10)
        assert backoff.max_attempts == 10
        assert len(backoff.deltas) == 11

    def test_init_deltas(self):
        count = 30
        backoff = Backoff(count, [
            timedelta(seconds=2 ** attempt if attempt > 0 else 0)
            for attempt in range(count + 1)])
        for attempt in range(count):
            if attempt == 0:
                assert backoff.delta(attempt) == timedelta()
                continue
            assert backoff.delta(attempt) == backoff.deltas[attempt]
            assert backoff.delta(attempt) == timedelta(seconds=2 ** attempt)

    def test_init_max_attempts_is_zero(self):
        with pytest.raises(ValueError) as excinfo:
            Backoff(0)
        assert '`max_attempts` must be greater than zero' == str(excinfo.value)

    def test_init_max_attempts_exceeds_deltas(self):
        with pytest.raises(IndexError) as excinfo:
            Backoff(2, deltas=[])
        assert '`max_attempts` must not exceed the length of deltas' == str(excinfo.value)

    def test_all_attempts_edge(self):
        backoff = Backoff()
        assert backoff.max_attempts == Backoff.max_attempts
        assert len(backoff.deltas) == 16
        for edge in [-1, 0, 15, 16, 17]:
            assert backoff.delta(edge) >= backoff.zero
            assert backoff.remaining(edge, datetime.utcnow()) >= backoff.zero
            if edge > 0:
                assert backoff.expires(edge, datetime.utcnow()) >= datetime.utcnow()
                assert backoff.expired(edge, datetime.utcnow()) == False
            else:
                assert backoff.expires(edge, datetime.utcnow()) <= datetime.utcnow()
                assert backoff.expired(edge, datetime.utcnow())

    def test_delta(self):
        backoff = Backoff()
        for attempt in range(backoff.max_attempts):
            if attempt == 0:
                assert backoff.delta(attempt) == timedelta()
                continue
            assert backoff.delta(attempt) == backoff.deltas[attempt]
            assert backoff.delta(attempt) == timedelta(seconds=2 ** attempt)
        # Should cap at max_attempts to highest delta
        assert backoff.delta(backoff.max_attempts + 20) == timedelta(seconds=2 ** backoff.max_attempts)

    def test_elapsed(self):
        backoff = Backoff()
        assert backoff.elapsed(None) == backoff.zero
        now = datetime.utcnow()
        sleep_delta = timedelta(0, 0, 0, 1)
        sleep((sleep_delta * 2).total_seconds())
        assert backoff.elapsed(now) > sleep_delta

    def test_remaining(self):
        backoff = Backoff()
        for attempt in range(backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                assert backoff.remaining(attempt, now) == timedelta()
                continue

            expect_delta = timedelta(seconds=2 ** attempt)
            assert backoff.remaining(attempt, now) > timedelta()
            assert backoff.remaining(attempt, now) < expect_delta
            assert backoff.remaining(attempt, now - expect_delta) == timedelta()
            assert backoff.remaining(attempt, now + expect_delta) > expect_delta

    def test_expires(self):
        backoff = Backoff()
        for attempt in range(backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                assert backoff.expires(attempt, now) < datetime.utcnow()
                continue
            expect_delta = timedelta(seconds=2 ** attempt)
            assert backoff.expires(attempt, now) > datetime.utcnow()
            assert backoff.expires(attempt, now - expect_delta) < datetime.utcnow()
            assert backoff.expires(attempt, now - (expect_delta * 2)) < datetime.utcnow()
            assert backoff.expires(attempt, now + expect_delta) > datetime.utcnow()
            assert backoff.expires(attempt, now + (expect_delta * 2)) > datetime.utcnow()

    def test_expires_overflow(self):
        backoff = Backoff()
        assert backoff.expires(15, datetime.max) == datetime.max
        assert backoff.expires(16, datetime.max) == datetime.max

    def test_expired(self):
        backoff = Backoff()
        for attempt in range(backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                assert backoff.expired(0, now) == True
                continue
            expect_delta = timedelta(seconds=2 ** attempt)
            assert backoff.expired(attempt, datetime.utcnow()) == False
            assert backoff.expired(attempt, datetime.utcnow() - (expect_delta / 2)) == False
            assert backoff.expired(attempt, datetime.utcnow() - expect_delta) == True
            assert backoff.expired(attempt, datetime.utcnow() - (expect_delta * 2)) == True

    @pytest.mark.slow
    def test_wait(self):
        backoff = Backoff()
        sleep_delta = timedelta(0, 0, 0, 5)

        for attempt in range(backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                # Wait with 0 attempts should always be instant, 500 microsec buffer
                # here for when that carbon black is going hard at 100%
                backoff.wait(0, now)
                assert datetime.utcnow() < (now + timedelta(0, 0, 500))
                continue

            # Every attempt we expect a (5 millisec) sleep when calling wait
            delta = timedelta(seconds=2 ** attempt)
            last_attempt = (now - delta) + sleep_delta
            expect = now - delta
            backoff.wait(attempt, last_attempt)
            assert datetime.utcnow() > expect


@pytest.mark.utils
@pytest.mark.backoff
@pytest.mark.tracker
class TestBackoffTracker(TestCase):

    def test_init(self):
        tracker = Tracker()
        assert tracker.attempts == 0
        assert tracker.last_attempt is None

    def test_str(self):
        assert str(Tracker()) == 'Tracker(attempts=0, remaining=0:00:00)'

    def test_init_backoff(self):
        b = Backoff()
        tracker = Tracker(b)
        tracker2 = Tracker()
        assert tracker.backoff == b
        assert tracker2.backoff != b
        assert tracker.attempts == 0
        assert tracker.last_attempt is None

    def test_attempt(self):
        now = datetime.utcnow()
        tracker = Tracker()
        tracker.attempt()
        assert tracker.attempts == 1
        assert tracker.last_attempt > now

    def test_reset(self):
        now = datetime.utcnow()
        tracker = Tracker()
        tracker.attempt()
        assert tracker.attempts == 1
        assert tracker.last_attempt > now
        tracker.reset()
        assert tracker.attempts == 0
        assert tracker.last_attempt is None

    def test_delta(self):
        tracker = Tracker()
        for attempt in range(tracker.backoff.max_attempts):
            if attempt == 0:
                assert tracker.delta() == timedelta()
                continue
            tracker.attempt()
            assert tracker.delta() == tracker.backoff.deltas[attempt]
            assert tracker.delta() == timedelta(seconds=2 ** attempt)
        # Should cap at max_attempts to highest delta
        tracker.attempts = tracker.backoff.max_attempts + 20
        assert tracker.delta() == timedelta(seconds=2 ** tracker.backoff.max_attempts)

    def test_elapsed(self):
        tracker = Tracker()
        assert tracker.elapsed() == Backoff.zero
        tracker.attempt()
        sleep_delta = timedelta(0, 0, 0, 1)
        sleep((sleep_delta * 2).total_seconds())
        assert tracker.elapsed() > sleep_delta

    def test_remaining(self):
        tracker = Tracker()
        for attempt in range(tracker.backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                assert tracker.remaining() == timedelta()
                continue
            tracker.attempt()
            expect_delta = timedelta(seconds=2 ** attempt)
            assert tracker.remaining() > timedelta()
            assert tracker.remaining() < expect_delta

            tracker.last_attempt = now - expect_delta
            assert tracker.remaining() == timedelta()

            tracker.last_attempt = now + expect_delta
            assert tracker.remaining() > expect_delta

    def test_expires(self):
        tracker = Tracker()
        for attempt in range(tracker.backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                assert tracker.expires() < datetime.utcnow()
                continue

            tracker.attempt()
            expect_delta = timedelta(seconds=2 ** attempt)
            assert tracker.expires() > datetime.utcnow()

            tracker.last_attempt = now - expect_delta
            assert tracker.expires() < datetime.utcnow()

            tracker.last_attempt = now - (expect_delta * 2)
            assert tracker.expires() < datetime.utcnow()

            tracker.last_attempt = now + expect_delta
            assert tracker.expires() > datetime.utcnow()

            tracker.last_attempt = now + (expect_delta * 2)
            assert tracker.expires() > datetime.utcnow()

    def test_expires_overflow(self):
        tracker = Tracker()
        tracker.attempts = 15
        tracker.last_attempt = datetime.max
        assert tracker.expires() == datetime.max

    def test_expired(self):
        tracker = Tracker()
        for attempt in range(tracker.backoff.max_attempts):
            if attempt == 0:
                assert tracker.expired() == True
                continue

            tracker.attempt()
            expect_delta = timedelta(seconds=2 ** attempt)

            tracker.last_attempt = datetime.utcnow()
            assert tracker.expired() == False

            tracker.last_attempt = datetime.utcnow() - (expect_delta / 2)
            assert tracker.expired() == False

            tracker.last_attempt = datetime.utcnow() - expect_delta
            assert tracker.expired() == True

            tracker.last_attempt = datetime.utcnow() - (expect_delta * 2)
            assert tracker.expired() == True

    @pytest.mark.slow
    def test_wait(self):
        tracker = Tracker()
        sleep_delta = timedelta(0, 0, 0, 5)

        for attempt in range(tracker.backoff.max_attempts):
            now = datetime.utcnow()

            if attempt == 0:
                # Wait with 0 attempts should always be instant, 500 microsec buffer
                # here for when that carbon black is going hard at 100%
                tracker.wait()
                assert datetime.utcnow() < (now + timedelta(0, 0, 500))
                continue

            # Every attempt we expect a (5 millisec) sleep when calling wait
            tracker.attempt()
            delta = timedelta(seconds=2 ** attempt)
            tracker.last_attempt = (now - delta) + sleep_delta
            expect = now - delta
            tracker.wait()
            assert datetime.utcnow() > expect


@pytest.mark.utils
@pytest.mark.utils_called
class TestCalled(TestCase):

    def test_basic(self):
        called = Called()
        res = called('foo', bar='foo')
        assert res == called
        assert len(res) == 1
        assert len(called) == 1

        call = called.pop()
        args, kwargs = call
        assert args[0] == 'foo'
        assert kwargs['bar'] == 'foo'

    def test_with_func(self):
        call_mes = []
        sentinel = object()

        def call_me(*args, **kwargs):
            call_mes.append([args, kwargs])
            return sentinel

        called = Called(call_func=call_me)
        res = called('foo', bar='foo')
        assert res == sentinel
        assert len(called) == 1

        for call in [called.pop(), call_mes.pop()]:
            args, kwargs = call
            assert args[0] == 'foo'
            assert kwargs['bar'] == 'foo'
