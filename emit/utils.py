import time
import sys
from datetime import datetime, timedelta
from dateutil.parser import parse
from .globals import conf


__all__ = [
    'Backoff', 'Tracker', 'Called', '_debug_assert', '_is_string', '_is_value',
    '_timeout_seconds', '_timeout_delta']


def _is_date(value):
    if isinstance(value, datetime):
        return True
    if not _is_string(value):
        return False
    try:
        return isinstance(parse(value), datetime)
    except (ValueError, OverflowError):
        return False


def _is_string(value):
    return isinstance(value, (str, unicode, basestring))


def _is_value(value):
    try:
        return value is not None and \
          (_is_date(value) or len(value) > 0)
    except TypeError:
        return False


def _timeout_seconds(timeout, default=None):
    """Timeout functions here are used where `timeout` arguments are accepted. I
    don't differentiate anywhere in the API between seconds and timedeltas and
    leave it up to the call site using the timeout to call one of these."""
    if timeout is None:
        timeout = _timeout_seconds(default, 0)
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return timeout


def _timeout_delta(timeout, default=None):
    if timeout is None:
        timeout = _timeout_delta(default, timedelta())
    if not isinstance(timeout, timedelta):
        return timedelta(seconds=timeout)
    return timeout


def _debug_assert(condition, msg=None):
    if conf.debug:
        if msg is None:
            msg = 'AssertionError'
        assert condition, '{}. Disable debug mode to remove this assertion.'.format(msg)


class Backoff(object):
    """Implements basic exponential backoff, used by transports for adapter
    connections and queue item retry."""
    zero = timedelta()
    max_attempts = 15  # Backoff at most 15 times, after this always return MAX delta

    def __repr__(self):
        return 'Backoff(max_attempts={0})'.format(self.max_attempts)

    def __init__(self, max_attempts=None, deltas=None, attempts=0, last_attempt=None):
        if max_attempts is not None:
            self.max_attempts = max_attempts
        self.deltas = deltas if deltas is not None else \
            [timedelta(seconds=2 ** i if i > 0 else 0) for i in range(self.max_attempts + 1)]

        if self.max_attempts <= 0:
            raise ValueError('`max_attempts` must be greater than zero')
        if self.max_attempts > (len(self.deltas) - 1):
            raise IndexError('`max_attempts` must not exceed the length of deltas')

    def delta(self, attempts):
        """Get the time delta for a given attempt number."""
        try:
            return self.deltas[max(attempts, 0)]
        except IndexError:
            return self.deltas[len(self.deltas) - 1]

    def elapsed(self, last_attempt):
        """Get the time elapsed since the last_attempt, if no attempts have been
        made it returns self.zero."""
        if last_attempt is None:
            return self.zero
        return datetime.utcnow() - last_attempt

    def remaining(self, attempts, last_attempt):
        """Get the time delta remaining for the current backoff period relative to the last_attempt."""
        if attempts == 0 or last_attempt is None:
            return self.zero
        return max(self.delta(attempts) - self.elapsed(last_attempt), self.zero)

    def expires(self, attempts, last_attempt):
        """Get the date which the current backoff period expires."""
        if attempts == 0:
            return datetime.min
        try:
            return datetime.utcnow() + self.remaining(attempts, last_attempt)
        except OverflowError:
            return datetime.max

    def expired(self, attempts, last_attempt):
        """Returns True if we are outside the backoff period, False otherwise."""
        return self.remaining(attempts, last_attempt) <= self.zero

    def wait(self, attempts, last_attempt, max_seconds=None):
        """Wait for current `remaining()` time."""
        remaining = self.remaining(attempts, last_attempt)
        if attempts == 0 or remaining <= self.zero:
            return
        if max_seconds is None:
            max_seconds = sys.maxint
        time.sleep(min(remaining.total_seconds(), max_seconds))


class Tracker(object):
    """Simple object to help track backoff state."""
    def __repr__(self):
        return 'Tracker(attempts={0}, remaining={1})'.format(
            self.attempts, self.remaining())

    def __init__(self, backoff=None, *args, **kwargs):
        self.backoff = backoff if backoff is not None else Backoff(*args, **kwargs)
        self.attempts = 0
        self.last_attempt = None

    def delta(self):
        return self.backoff.delta(self.attempts)

    def elapsed(self):
        return self.backoff.elapsed(self.last_attempt)

    def remaining(self):
        return self.backoff.remaining(self.attempts, self.last_attempt)

    def expires(self):
        return self.backoff.expires(self.attempts, self.last_attempt)

    def expired(self):
        return self.backoff.expired(self.attempts, self.last_attempt)

    def wait(self, max_seconds=None):
        return self.backoff.wait(self.attempts, self.last_attempt, max_seconds=max_seconds)

    def attempt(self):
        self.attempts += 1
        self.last_attempt = datetime.utcnow()

    def reset(self):
        self.attempts = 0
        self.last_attempt = None


class Called(list):
    def __call__(self, *args, **kwargs):
        self.append([args, kwargs])
        if self.call_func is None:
            return self
        return self.call_func(*args, **kwargs)

    def __init__(self, call_func=None):
        super(Called, self).__init__()
        self.call_func = call_func
