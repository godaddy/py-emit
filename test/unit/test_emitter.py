import pytest
from datetime import datetime
from uuid import uuid4
from emit import transports, adapters, event
from emit.globals import conf
from emit.event import Event
from emit.emitters import Emitter, EmittingEventContext
from ..helpers import (
    TestCase, tevent, teventr_expect, tevent_stack, temitter)


class EmitterTestCase(TestCase):

    @classmethod
    def event_from_callback(cls, event, **kwargs):
        events = []

        def cb(message):
            events.append(message)

        adapter = adapters.ListAdapter()
        transport = transports.Transport(
            adapter, worker_class=transports.Worker)
        emitter = Emitter(transport=transport, callbacks=[cb], **kwargs)

        emitter.emit(event)
        assert len(events) == 1

        event = events.pop()
        assert isinstance(event, Event)
        return event


@pytest.mark.emitter_init
class TestEmitterInit(EmitterTestCase):

    def test_class_defaults(self):
        assert Emitter.event_stack_class == event.EventStack
        assert Emitter.event_class == event.Event
        assert Emitter.adapter_class == adapters.Adapter
        assert Emitter.transport_class == transports.Transport

    def test_instance_defaults(self):
        assert Emitter().event_stack_class == event.EventStack
        assert Emitter().event_class == event.Event
        assert Emitter().adapter_class == adapters.Adapter
        assert Emitter().transport_class == transports.Transport
        assert Emitter.event_stack_class == event.EventStack
        assert Emitter.event_class == event.Event
        assert Emitter.adapter_class == adapters.Adapter
        assert Emitter.transport_class == transports.Transport

    def test__init__(self):
        emitter = Emitter()
        assert isinstance(emitter.transport, emitter.transport_class)
        assert isinstance(emitter.transport.adapter, emitter.adapter_class)
        assert isinstance(emitter.event_stack, emitter.event_stack_class)
        assert isinstance(emitter.event_stack.top, emitter.event_class)

    def test__init__adapter(self):
        adapter = adapters.StdoutAdapter()
        emitter = Emitter(adapter=adapter)
        assert emitter.transport.adapter == adapter

    def test__init__transport(self):
        adapter = adapters.StdoutAdapter()
        transport = transports.Transport(adapter)
        emitter = Emitter(transport=transport)
        assert emitter.transport == transport
        assert emitter.transport.adapter == adapter

    def test__init__transport_and_adapter(self):
        restore = conf.debug

        try:
            conf.debug = False
            adapter = adapters.StdoutAdapter()
            adapter_kwarg = adapters.StdoutAdapter()
            transport = transports.Transport(adapter)
            emitter = Emitter(adapter=adapter_kwarg, transport=transport)
            assert emitter.transport == transport
            assert emitter.transport.adapter == adapter
        finally:
            conf.debug = restore

    def test__init__transport_and_adapter_debug(self):
        restore = conf.debug

        try:
            conf.debug = True
            adapter = adapters.StdoutAdapter()
            adapter_kwarg = adapters.StdoutAdapter()
            transport = transports.Transport(adapter)
            expect_msg = '`adapter` argument is discarded when `transport`.'
            expect_msg += ' Disable debug mode to remove this assertion.'

            with pytest.raises(AssertionError) as excinfo:
                Emitter(adapter=adapter_kwarg, transport=transport)
            assert expect_msg == str(excinfo.value)
        finally:
            conf.debug = restore

    def test__init__event_stack(self):
        event_stack = event.EventStack()
        emitter = Emitter(event_stack=event_stack)
        assert emitter.event_stack == event_stack

    def test__init__event_stack_and_defaults(self):
        event_stack = event.EventStack()
        defaults = dict(system='test__init__event_stack_and_defaults')
        emitter = Emitter(event_stack=event_stack, defaults=defaults)
        assert emitter.event_stack == event_stack
        assert emitter.event_stack.top.system == defaults['system']

    def test__init__event_stack_and_kwargs(self):
        event_stack = event.EventStack()
        emitter = Emitter(event_stack=event_stack, system='test__init__event_stack_and_defaults')
        assert emitter.event_stack == event_stack
        assert emitter.event_stack.top.system == 'test__init__event_stack_and_defaults'

    def test__init__event_stack_and_defaults_and_kwargs(self):
        system = 'test__init__event_stack_and_defaults_and_kwargs_system'
        component = 'test__init__event_stack_and_defaults_and_kwargs_component'
        event_stack = event.EventStack()
        defaults = Event(system=system)
        emitter = Emitter(event_stack=event_stack, defaults=defaults, component=component)
        assert emitter.event_stack == event_stack
        assert emitter.event_stack.top.system == system
        assert emitter.event_stack.top.system == system
        assert emitter.event_stack.top.component == component

    def test__init__event_stack_class(self):
        class TClass(event.EventStack):
            pass
        emitter = Emitter(event_stack_class=TClass)

        assert Emitter.event_stack_class == event.EventStack
        assert emitter.event_stack_class == TClass

    def test__init__event_class(self):
        class TClass(event.Event):
            pass
        emitter = Emitter(event_class=TClass)
        assert Emitter.event_class == event.Event
        assert emitter.event_class == TClass
        assert isinstance(emitter.event_stack.top, TClass)

    def test__init__adapter_class(self):
        class TClass(adapters.StdoutAdapter):
            pass
        emitter = Emitter(adapter_class=TClass)
        assert Emitter.adapter_class == adapters.Adapter
        assert emitter.adapter_class == TClass
        assert isinstance(emitter.transport.adapter, TClass)

    def test__init__transport_class(self):
        class TClass(transports.Transport):
            pass
        emitter = Emitter(transport_class=TClass)
        assert Emitter.transport_class == transports.Transport
        assert emitter.transport_class == TClass
        assert isinstance(emitter.transport, TClass)

    def test__init__callbacks(self):
        callbacks = [lambda event: event]
        emitter = Emitter(callbacks=callbacks)
        assert emitter.callbacks == callbacks

    def test__init__defaults(self):
        defaults = Event(tid='test__init__defaults')
        emitter = Emitter(defaults=defaults)
        assert emitter.top == defaults
        assert emitter.top.tid == 'test__init__defaults'

    def test__init__kwarg(self):
        emitter = Emitter(tid='t_tid')
        assert emitter.top.tid == 't_tid'

    def test__init__kwarg_and_named(self):
        event_stack = event.EventStack()
        emitter = Emitter(event_stack=event_stack, tid='t_tid')
        assert emitter.event_stack == event_stack
        assert emitter.top.tid == 't_tid'

    def test__init__kwargs(self):
        emitter = Emitter(
            tid='t_tid',
            system='t_system',
            component='t_component',
            operation='t_operation')
        assert emitter.top.tid == 't_tid'
        assert emitter.top.system == 't_system'
        assert emitter.top.component == 't_component'
        assert emitter.top.operation == 't_operation'

    def test__init__kwargs_and_named(self):
        event_stack = event.EventStack()
        emitter = Emitter(
            event_stack=event_stack,
            tid='t_tid',
            system='t_system',
            component='t_component',
            operation='t_operation')
        assert emitter.top.tid == 't_tid'
        assert emitter.top.system == 't_system'
        assert emitter.top.component == 't_component'
        assert emitter.top.operation == 't_operation'
        assert emitter.event_stack == event_stack


class TestEmitterProperties(EmitterTestCase):

    def test_top(self):
        emitter = Emitter()
        assert emitter.top == emitter.event_stack.top

    def test_bot(self):
        emitter = Emitter()
        assert emitter.bot == emitter.event_stack.bot

    def test_system(self):
        emitter = Emitter()
        assert emitter.system == ''

        val = 'test_set_system'
        emitter.system = val
        emitter.top.system = val
        assert emitter.system == val

    def test_component(self):
        emitter = Emitter()
        assert emitter.component == ''

        val = 'test_set_component'
        emitter.component = val
        emitter.top.component = val
        assert emitter.component == val

    def test_operation(self):
        emitter = Emitter()
        assert emitter.operation == ''

        val = 'test_set_operation'
        emitter.operation = val
        emitter.top.operation = val
        assert emitter.operation == val

    def test_name(self):
        emitter = Emitter()
        assert emitter.name == ''

        val = 'test_set_name'
        emitter.name = val
        emitter.top.name = val
        assert emitter.name == val

    def test_tid(self):
        emitter = Emitter()
        assert emitter.tid == ''

        val = 'test_set_tid'
        emitter.tid = val
        assert emitter.tid == val
        assert emitter.top.tid == val

    def test_time(self):
        emitter = Emitter()
        assert isinstance(emitter.time, datetime)

        val = datetime.utcnow()
        emitter.time = val
        assert emitter.time == val
        assert emitter.top.time == val

    def test_tags(self):
        emitter = Emitter()
        assert not len(emitter.tags)

        vals = ['tag_1', 'tag_2', 'tag_3']
        emitter.tags = vals
        for val in vals:
            assert val in emitter.tags
            assert val in emitter.top.tags

    def test_fields(self):
        emitter = Emitter()
        assert not len(emitter.fields)

        val = {'f1': 'one', 'f2': 'two'}
        emitter.fields = val
        assert emitter.fields == val
        assert emitter.top.fields == val

    def test_data(self):
        emitter = Emitter()
        assert not len(emitter.data)

        val = {'f1': 'one', 'f2': 'two'}
        emitter.data = val
        assert emitter.data == val
        assert emitter.top.data == val

    def test_replay(self):
        emitter = Emitter()
        assert emitter.replay == ''

        val = 'test_set_replay'
        emitter.replay = val
        assert emitter.replay == val
        assert emitter.top.replay == val


@pytest.mark.emitter_methods
class TestEmitterMethods(EmitterTestCase):

    def test_open(self):
        emitter = Emitter()
        result = emitter.open('test_open')
        assert isinstance(result, EmittingEventContext)
        assert result.name == 'test_open'
        assert result.enter_event.name == 'open'
        assert result.exit_event.name == 'close'
        assert id(result.enter_event) != id(result.exit_event)

    def test_enter(self):
        emitter = Emitter()
        result = emitter.enter('test_enter')
        assert isinstance(result, EmittingEventContext)
        assert result.name == 'test_enter'
        assert result.enter_event.name == 'enter'
        assert result.exit_event.name == 'exit'
        assert id(result.enter_event) != id(result.exit_event)

    def test_emit(self):
        emitter = temitter()
        event, expect = teventr_expect()
        result = emitter.emit(event)
        assert isinstance(result, EmittingEventContext)
        assert result.name == expect['name']
        assert result.enter_event.name == 'enter'
        assert result.exit_event.name == 'exit'
        assert id(result.enter_event) != id(result.exit_event)
        assert len(emitter.transport.adapter) == 1

        got = Event.from_json(emitter.transport.adapter[0])
        want = (emitter.event_stack | Event(**expect))
        assert got == want

    def test_ping(self):
        conf.debug = True

        emitter = temitter()
        result = emitter.ping()

        assert len(emitter.transport.adapter) == 3
        for expect in ['open', 'ping', 'close']:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == expect
            assert e.tid == result
            assert e.system == 'test.pyemit'
            assert e.component == 'emitter'
            assert e.operation == 'ping'

    def test_emit_debug_false(self):
        restore = conf.debug

        try:
            conf.debug = True
            emitter = Emitter()

            with pytest.raises(ValueError) as excinfo:
                emitter.emit('foo')
            assert '`operation` must not be empty' == str(excinfo.value)
        finally:
            conf.debug = restore

    def test_emit_debug_true(self):
        restore = conf.debug

        try:
            conf.debug = False
            emitter = Emitter()
            emitter.callbacks = 'str'
            emitter.emit('foo')
        finally:
            conf.debug = restore

    def test_callbacks(self):
        events = []

        def cb(message):
            events.append(message)

        adapter = adapters.ListAdapter()
        transport = transports.Transport(adapter)
        emitter = Emitter(transport=transport, callbacks=[cb])
        event_sent = tevent()

        emitter(event_sent)
        assert len(events) == 1

        event = events.pop()
        assert isinstance(event, Event)
        assert event_sent == event

    def test_str(self):
        emitter = Emitter()
        to_str = str(emitter)
        assert to_str.startswith('Emitter(')
        assert to_str.endswith(')')


@pytest.mark.emitter_defaults
class TestEmitterDefaults(EmitterTestCase):

    def test_defaults_system(self):
        event_defaults = Event(system='test_defaults_system_getter')
        emitter = Emitter(defaults=event_defaults)
        assert event_defaults.system == 'test_defaults_system_getter'
        assert emitter.system == 'test_defaults_system_getter'
        assert event_defaults.system == emitter.system

    def test_defaults_component(self):
        event_defaults = Event(component='test_component_getter')
        emitter = Emitter(defaults=event_defaults)
        assert event_defaults.component == 'test_component_getter'
        assert emitter.component == 'test_component_getter'
        assert event_defaults.component == emitter.component

    def test_defaults_operation(self):
        event_defaults = Event(operation='test_defaults_operation_getter')
        emitter = Emitter(defaults=event_defaults)
        assert event_defaults.operation == 'test_defaults_operation_getter'
        assert emitter.operation == 'test_defaults_operation_getter'
        assert event_defaults.operation == emitter.operation


class TestEmitterEventStack(EmitterTestCase):

    def test_top(self):
        event_stack = tevent_stack()
        emitter = Emitter(event_stack=event_stack)
        assert emitter.top == event_stack.top

    def test_bot(self):
        event_stack = tevent_stack()
        emitter = Emitter(event_stack=event_stack)
        assert emitter.bot == event_stack.bot

    def test_defaults(self):
        event_stack = tevent_stack()
        emitter = Emitter(event_stack=event_stack)

        event = Event(system='test_defaults')
        assert event.system == 'test_defaults'
        assert emitter.system == ''
        assert emitter.top.system == ''
        assert emitter.to_event.system == 'test.pyemit'

        # Defaults should set the values of the top most ctx
        emitter.defaults(event)
        assert event.system == 'test_defaults'
        assert emitter.system == 'test_defaults'
        assert emitter.top.system == 'test_defaults'
        assert emitter.to_event.system == 'test_defaults'

        event_stack.pop()
        assert event.system == 'test_defaults'
        assert emitter.system == ''
        assert emitter.top.system == ''
        assert emitter.to_event.system == 'test.pyemit'

    def test_setters_are_event_top(self):
        event_stack = tevent_stack()
        emitter = Emitter(event_stack=event_stack)
        emitter.system = 'test_setters_are_event_top'
        assert emitter.system == 'test_setters_are_event_top'
        assert emitter.event_stack.top.system == 'test_setters_are_event_top'
        assert emitter.event_stack.bot.system == 'test.pyemit'

        top = emitter.event_stack.pop()
        assert top.system == 'test_setters_are_event_top'
        assert emitter.system == ''
        assert emitter.event_stack.top.system == ''
        assert emitter.event_stack.bot.system == 'test.pyemit'


class TestEmitterContext(EmitterTestCase):

    def test__enter__(self):
        emitter = temitter()

        with emitter:
            emitter('hello')

        assert len(emitter.transport.adapter) == 3
        for expect in ['enter', 'hello', 'exit']:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == '{}.{}'.format('base.one.two.three', expect)
        assert len(emitter.transport.adapter) == 0

    def test__enter__as_callable(self):
        emitter = temitter()

        with emitter('called'):
            emitter('hello')
        for expect in ['called', 'called.enter', 'called.hello', 'called.exit']:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == '{}.{}'.format('base.one.two.three', expect)
        assert len(emitter.transport.adapter) == 0

    def test__enter__enter_method(self):
        emitter = temitter()

        emitter('called')
        with emitter.enter('called'):
            emitter('hello')
        for expect in ['called', 'called.enter', 'called.hello', 'called.exit']:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == '{}.{}'.format('base.one.two.three', expect)
        assert len(emitter.transport.adapter) == 0

    def test__enter__direct(self):
        emitter = temitter()

        emitter('called')
        with emitter:
            emitter('hello')
        for expect in ['called', 'enter', 'hello', 'exit']:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == '{}.{}'.format('base.one.two.three', expect)
        assert len(emitter.transport.adapter) == 0

    def test__enter__as_callable_nested(self):
        emitter = temitter()

        with emitter('called'):
            emitter('hello')
            with emitter('called2'):
                emitter('hello2')

        expected = [
            'called',
            'called.enter',
            'called.hello',
            'called.called2',
            'called.called2.enter',
            'called.called2.hello2',
            'called.called2.exit',
            'called.exit']

        assert len(emitter.transport.adapter) == len(expected)
        for expect in expected:
            e = emitter.transport.adapter.pop(0)
            e = Event.from_json(e)
            assert e.name == '{}.{}'.format('base.one.two.three', expect)
        assert len(emitter.transport.adapter) == 0

    def test__enter__without_valid_ctx(self):
        restore = conf.debug

        try:
            conf.debug = True
            emitter = Emitter()

            with pytest.raises(ValueError) as excinfo:
                with emitter:
                    pass
            assert '`operation` must not be empty' == str(excinfo.value)
        finally:
            conf.debug = restore

    def test_ctx_manager(self):
        emitter = temitter()
        tid = str(uuid4())

        def chk(expect):
            event = Event.from_json(emitter.transport.adapter.pop())
            assert event.system == 'test.pyemit'
            assert event.tid == tid
            assert event.component == 'event_stack'
            assert event.operation == 'testing'
            assert event.name == '{}.{}'.format('base.one.two.three', expect)
            assert event.valid

        with emitter.open(tid=tid) as ctx:

            # The ctx returned should be empty
            assert ctx == emitter.top
            assert ctx.name == ''

            # The tid should for the top should be empty, the to_event should
            # be the passed tid
            assert emitter.tid == ''
            assert emitter.to_event.tid == tid

            # Check for open event
            chk('open')

            emitter('init0')
            chk('init0')

            emitter('init2')
            chk('init2')

            with emitter:
                chk('enter')
            chk('exit')

            with emitter('a'):
                # This is because pop() ordering in chk, it's emitted as a -> a.enter
                chk('a.enter')
                chk('a')
                emitter('b')
                chk('a.b')
            chk('a.exit')

            with emitter.enter('a'):
                chk('a.enter')
                emitter('b')
                chk('a.b')
            chk('a.exit')

            emitter('a')
            chk('a')
            with emitter:
                chk('enter')
            chk('exit')

            with emitter.enter('b'):
                chk('b.enter')
            chk('b.exit')

            with emitter.enter('one'):
                chk('one.enter')

                emitter('init1')
                chk('one.init1')

                emitter('init2')
                chk('one.init2')

                with emitter.enter('two'):
                    chk('one.two.enter')

                    with emitter.enter('three'):
                        chk('one.two.three.enter')

                        with emitter.enter('four'):
                            chk('one.two.three.four.enter')
                        chk('one.two.three.four.exit')
                    chk('one.two.three.exit')

                    emitter('init2')
                    chk('one.two.init2')
                    emitter('init3')
                    chk('one.two.init3')
                    emitter('init4')
                    chk('one.two.init4')

                chk('one.two.exit')
            chk('one.exit')

            emitter('zero_01')
            chk('zero_01')

            emitter('zero_02')
            chk('zero_02')
        assert len(emitter.transport.adapter) == 1
        chk('close')
        assert len(emitter.transport.adapter) == 0
