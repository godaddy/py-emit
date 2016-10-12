import threading
import random
import time
from datetime import timedelta
from functools import wraps
from .utils import Backoff, Tracker


__all__ = ['as_callable', 'slow', 'backoff_barrier', 'unreliable', 'defer', 'delay']


def as_callable(factory):
    """Decorator to allow decorated functions with all optional parameters to be
    applied without being called.

    Given:
        decorator(f, arg1=True, arg2=1)

    Allows:
        @decorator

    To look identical to:
        @decorator(f, True, 1)
    """
    @wraps(factory)
    def decorator(*args, **kwargs):

        # Called as "@decorator" if args[0] is callable
        if len(args) and callable(args[0]):
            return factory(args[0])

        # Called as "@decorator(...)"
        @wraps(factory)
        def dec_factory(func):
            return factory(func, *args, **kwargs)
        return dec_factory
    return decorator


@as_callable
def backoff_barrier(f, backoff=None, **kwargs):
    """Adds exponential backoff via Tracker() class around a function."""
    backoff = backoff if backoff is not None else Backoff(**kwargs)
    tracker = Tracker(backoff)

    @wraps(f)
    def decorator(*args, **kwargs):
        tracker.wait()
        tracker.attempt()
        result = f(*args, **kwargs)
        tracker.reset()
        return result
    return decorator


@as_callable
def unreliable(f, success_rate=.60, exc_class=None, exc_format=None):
    """Utility for unit testing that makes a function unreliable. Meaning it has
    as `success_rate` chance of raising exc_class."""
    exc_class = exc_class if exc_class is not None else RuntimeError
    exc_format = exc_format if exc_format is not None else \
        'unreliable func `{name}` rolled {roll:.0%} exceeding {success_rate:.0%} success rate causing {exc_class}'

    @wraps(f)
    def decorator(*args, **kwargs):
        roll = random.random()

        if success_rate >= roll:
            return f(*args, **kwargs)
        raise exc_class(exc_format.format(
            name=f.__name__, success_rate=success_rate, roll=roll, exc_class=exc_class.__name__))
    return decorator


@as_callable
def slow(f, min_duration=None, max_duration=None):
    """Utility for unit testing that adds a sleep between min/max duration to any function."""
    min_duration = min_duration if min_duration is not None else \
        timedelta(seconds=1)
    max_duration = max_duration if max_duration is not None else \
        timedelta(seconds=4)

    @wraps(f)
    def decorator(*args, **kwargs):
        time.sleep(random.uniform(min_duration.total_seconds(), max_duration.total_seconds()))
        return f(*args, **kwargs)
    return decorator


@as_callable
def delay(f, duration=None):
    """Utility for unit testing that will delay a function by a constant duration."""
    duration = duration if duration is not None else \
        timedelta(seconds=1)

    @wraps(f)
    def decorator(*args, **kwargs):
        time.sleep(duration.total_seconds())
        return f(*args, **kwargs)
    return decorator


@as_callable
def defer(f, duration=None, *args, **kwargs):
    """Utility for unit testing that defers function execution to a separate
    thread after `duration` has elapsed."""
    duration = duration if duration is not None else timedelta(*args, **kwargs)

    @wraps(f)
    def decorator(*args, **kwargs):
        t = threading.Timer(duration.total_seconds(), f)
        t.start()
    return decorator
