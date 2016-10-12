import sys
from uuid import uuid4
from datetime import datetime
from emit.adapters import ListAdapter
from emit.transports import Transport, Worker
from emit.emitters import Emitter
from emit.event import Event, EventStack
from emit.decorators import slow, unreliable
from StringIO import StringIO


DEFAULT_DATETIME = datetime.fromtimestamp(1461631957.123456)
SYSTEM = COMPONENT = OPERATION = 'test.pyemit'


def tevent_stack(cls=EventStack):
    event_stack = cls()
    event_stack.append(Event(system='test.pyemit', tid='test.event_stack_tid'))
    event_stack.append(Event(component='event_stack'))
    event_stack.append(Event(operation='testing'))
    event_stack.append(Event(name='base'))
    event_stack.append(Event(name='one'))
    event_stack.append(Event(name='two'))
    event_stack.append(Event(name='three'))
    return event_stack


tevent_stack._as_str = '''EventStack(test.event_stack_tid)
  ->{'name': 'three'}
    ->{'name': 'two'}
      ->{'name': 'one'}
        ->{'name': 'base'}
          ->{'operation': 'testing'}
            ->{'component': 'event_stack'}
              ->{'tid': 'test.event_stack_tid', 'system': 'test.pyemit'}
'''


def tevent_expect(**kwargs):
    expect = dict(
        tid='test.tid',
        # time=DEFAULT_DATETIME,
        system=SYSTEM,
        component=COMPONENT,
        operation=OPERATION,
        name='test.event')
    expect.update(**kwargs)
    evt = Event(**expect)
    return evt, expect


def teventf_expect(*args, **kwargs):
    r = dict(
        replay='protect://blackhole:create/data/replay_data',
        tags=['test.tag'],
        fields={'my_lookup_str': 'foo'},
        data={'replay_data': {'headers': {'Content-Type': 'application/json'}}})
    evt, expect = tevent_expect(**r)
    evt.update(**kwargs)
    expect.update(**kwargs)
    return evt, expect


def teventr_expect(**kwargs):
    r = dict(
        tid=str(uuid4()),
        time=datetime.utcnow())
    evt, expect = tevent_expect(**r)
    evt.update(**kwargs)
    expect.update(**kwargs)
    return evt, expect


def tevent(**kwargs):
    evt, exp = tevent_expect(**kwargs)
    return evt


def teventr(**kwargs):
    evt, exp = teventr_expect(**kwargs)
    return evt


def teventf(**kwargs):
    evt, exp = teventf_expect(**kwargs)
    return evt


def tjson(**kwargs):
    evt, exp = tevent_expect(**kwargs)
    return evt.json


def tjsonf(**kwargs):
    evt, exp = teventf_expect(**kwargs)
    return evt.json


def temitter(**kwargs):
    if not ('adapter' in kwargs):
        kwargs['adapter'] = ListAdapter()
    if not ('transport' in kwargs):
        if not ('worker_class' in kwargs):
            kwargs['worker_class'] = Worker
        kwargs['transport'] = Transport(
          kwargs['adapter'], worker_class=kwargs['worker_class'])
        del kwargs['adapter']
        del kwargs['worker_class']
    if not ('event_stack' in kwargs):
        kwargs['event_stack'] = tevent_stack()
    return Emitter(**kwargs)


def twrap(obj, func, **kwargs):
    attrs = [item for item in dir(obj) if not item.startswith('__')]
    for attr in attrs:
        setattr(obj, attr, func(
          getattr(obj, attr), **kwargs))
    return obj


def tslow(func, min_duration=None, max_duration=None):
    return slow(func, min_duration=min_duration, max_duration=max_duration)


def tslowo(obj, min_duration=None, max_duration=None):
    return twrap(
      obj, slow, min_duration=min_duration, max_duration=max_duration)


def tunreliable(func, success_rate=.60, exc_class=None, exc_format=None):
    return unreliable(
      func, success_rate=success_rate, exc_class=exc_class,
      exc_format=exc_format)


def tunreliableo(obj, success_rate=.60, exc_class=None, exc_format=None):
    return twrap(
      obj, unreliable, success_rate=success_rate,
      exc_class=exc_class, exc_format=exc_format)


class StdoutCapturing(object):
    """ @TODO This doesn't work with py.test and their "capsys" fixture doesn't seem to work either."""
    def __init__(self, target=None):
        self.target = target if not (target is None) else StringIO()

    def __enter__(self):
        sys.stdout = self.target
        return self.target

    def __exit__(self, *args):
        sys.stdout = sys.__stdout__


class StderrCapturing(object):
    def __init__(self, target=None):
        self.target = target if not (target is None) else StringIO()

    def __enter__(self):
        sys.stderr = self.target
        return self.target

    def __exit__(self, *args):
        sys.stderr = sys.__stderr__


class TestCase(object):
    pass


def Generatable(cls):
    """Just a simple decorator to make the process of generating tests easier."""
    if hasattr(cls, 'generate_tests') and callable(cls.generate_tests):
        def create_test_func(name, test_func):
            setattr(cls, 'test_' + name.replace(' ', '_').lower(), test_func)
        cls.generate_tests(create_test_func)
    return cls
