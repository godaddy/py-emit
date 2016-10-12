from uuid import uuid4
from .globals import log, conf, ConfigDescriptor
from .utils import _debug_assert
from .event import EventContext


Emitters = ['Emitter']


__all__ = Emitters + ['Emitters', 'EmittingEventContext']


class Emitter(object):
    """Base functionality needed to `emit()` events."""
    event_stack_class = ConfigDescriptor('event_stack_class')
    event_class = ConfigDescriptor('event_class')
    adapter_class = ConfigDescriptor('adapter_class')
    transport_class = ConfigDescriptor('transport_class')

    def __init__(
            self, adapter=None, transport=None, event_stack=None,
            event_stack_class=None, event_class=None, adapter_class=None,
            transport_class=None, callbacks=None, defaults=None, **kwargs):

        # If you do not pass any of these, then conf.<kwarg> is used instead
        # due to the `ConfigDescriptor`
        if event_stack_class is not None:
            self.event_stack_class = event_stack_class
        if event_class is not None:
            self.event_class = event_class
        if adapter_class is not None:
            self.adapter_class = adapter_class
        if transport_class is not None:
            self.transport_class = transport_class

        # Event stack if not passed uses event_stack_class and pushes defaults to it.
        self.event_stack = event_stack if event_stack is not None else \
            self.event_stack_class()

        # Defaults may be passed in or event_class will be used, it's updated
        # with kwargs to allow:
        #   emitter = Emitter(system='foo', component='bar')
        #   assert emitter.system == 'foo' ...
        self.event_stack.append(
            self.event_class(defaults or dict()).update(**kwargs))

        # Callbacks is an array of funcs to call with each emitted message.
        self.callbacks = callbacks if callbacks is not None else []

        # If given a transport the adapter is discarded. If debug mode is enabled
        # we will throw for a hint at this behavior.
        if transport is not None:
            _debug_assert(
                adapter is None,
                '`adapter` argument is discarded when `transport`')
            self.transport = transport
        else:
            # Adapter is passed or default of adapter_class is used
            self.transport = self.transport_class(
                adapter if adapter is not None else self.adapter_class())

    def __enter__(self):
        """Enters a new context."""
        self.emit('enter')
        return self.event_stack.__enter__(self.event_class())

    def __exit__(self, exc_type, exc_value, tb):
        """Exits current context."""
        self.event_stack.__exit__(exc_type, exc_value, tb)
        self.emit('exit')

    def __call__(self, *args, **kwargs):
        """Makes the Emitter callable, passing all arguments to the `emit()` method."""
        return self.emit(*args, **kwargs)

    def __str__(self):
        """Returns the transport name and event stack."""
        return '{}({}, {})'.format(
            self.__class__.__name__, self.transport, self.event_stack)

    def open(self, *args, **kwargs):
        """Begins an `open` event. Returns an `EmittingEventContext` which
        is simply an `EventContext` that will emit an event on enter/exit."""
        return EmittingEventContext(
            self,
            self.event_stack,
            self.event_class('open'),
            self.event_class('close'),
            *args, **kwargs)

    def enter(self, *args, **kwargs):
        """Begins an `enter` event. It functions like entering the Emitter
        context directly except you may augment the context with some
        args if you would like. i.e.:
        emitter.operation = 'foo'
        with emitter as ctx:
            print ctx.operation -> foo
        with emitter.enter(operation=bar) as ctx:
            print ctx.operation -> bar
        """
        return EmittingEventContext(
            self,
            self.event_stack,
            self.event_class('enter'),
            self.event_class('exit'),
            *args, **kwargs)

    def ping(self):
        """Send open, ping and close events as a ping operation. Returns TID to
        look up at endpoint if desired. Will set system=test.pyemit,
        component=emitter and operation=ping."""
        tid = str(uuid4())
        event = conf.event_class(
            tid=tid, system='test.pyemit', component='emitter', operation='ping')

        self.transport.emit(event(name='open').json)
        self.transport.emit(event(name='ping').json)
        self.transport.emit(event(name='close').json)
        return tid

    def emit(self, *args, **kwargs):
        """Emit an event. It will use the current event stack for the events
        context. It returns a context manager which will use the emitted event
        as a base for this event stack as well as emit an 'enter' and 'exit'
        event. This hides exceptions unless debug mode is enabled."""
        try:
            event = self.event_stack | self.event_class(*args, **kwargs)
            event.validate()

            if len(self.callbacks):
                map(lambda f: f(event), self.callbacks)
            self.transport.emit(event.json)
            return self.enter(*args, **kwargs)

        except Exception:
            if conf.debug:
                raise
            log.exception('`emit(*{}, **{})` caught exception (set debug = True to throw)', args, kwargs)
        return self

    """
    Below here all methods forward / interact with the current context. For properties
    They are all set and accessed the same, pointing to the top of the current context i.e.:
        emitter.system = 'system'
            -> emitter.top.system
                -> (emitter.context.top.system = 'system')

    If you want the default values at the bottom of the context, which is a good
    place to set system and component, use emitter.bot:
        emitter.bot.system = 'system'
            -> (emitter.context.bot.system = 'system')

    Or you could provide them when you construct your emitter, i.e.:
        Emitter(system='system')

    If you want to get the resolved defaults use to_event, i.e.:
        emitter.to_event.system
            -> (rolls up all context into a event)
    """
    def defaults(self, *args, **kwargs):
        return self.event_stack.top.defaults(*args, **kwargs)

    @property
    def to_event(self):
        return self.event_stack.to_event

    @property
    def top(self):
        return self.event_stack.top

    @property
    def bot(self):
        return self.event_stack.bot

    @property
    def system(self):
        return self.event_stack.top.system

    @system.setter
    def system(self, system):
        self.event_stack.top.system = system

    @property
    def component(self):
        return self.event_stack.top.component

    @component.setter
    def component(self, component):
        self.event_stack.top.component = component

    @property
    def operation(self):
        return self.event_stack.top.operation

    @operation.setter
    def operation(self, operation):
        self.event_stack.top.operation = operation

    @property
    def name(self):
        return self.event_stack.top.name

    @name.setter
    def name(self, name):
        self.event_stack.top.name = name

    @property
    def tid(self):
        return self.event_stack.top.tid

    @tid.setter
    def tid(self, tid):
        self.event_stack.top.tid = tid

    @property
    def time(self):
        return self.event_stack.top.time

    @time.setter
    def time(self, time):
        self.event_stack.top.time = time

    @property
    def tags(self):
        return self.event_stack.top.tags

    @tags.setter
    def tags(self, tags):
        self.event_stack.top.tags = tags

    @property
    def replay(self):
        return self.event_stack.top.replay

    @replay.setter
    def replay(self, replay):
        self.event_stack.top.replay = replay

    @property
    def fields(self):
        return self.event_stack.top.fields

    @fields.setter
    def fields(self, fields):
        self.event_stack.top.fields = fields

    @property
    def data(self):
        return self.event_stack.top.data

    @data.setter
    def data(self, data):
        self.event_stack.top.data = data


class EmittingEventContext(EventContext):
    """Short lived object to bridge into a contextual event."""
    def __init__(self, emitter, event_stack, enter_event=None, exit_event=None, *args, **kwargs):
        super(EmittingEventContext, self).__init__(event_stack, *args, **kwargs)
        self.emitter = emitter
        self.enter_event = enter_event
        self.exit_event = exit_event

    def __enter__(self):
        # We append ourself to the base of the event stack before giving an
        # overlay for transient values that won't get inherited on exit
        self.event_stack.append(self)
        ctx = self.event_stack.__enter__(self.emitter.event_class())

        # We want the current context to be part of the emit event for entering
        # so we prepend ctx first.
        if self.enter_event:
            self.emitter.emit(self.enter_event)
        return ctx

    def __exit__(self, exc_type, exc_value, tb):
        # Pop transient context
        self.event_stack.__exit__(exc_type, exc_value, tb)

        # Emit exit event
        if self.exit_event:
            self.emitter.emit(self.exit_event)

        # Pop the base
        self.event_stack.pop()
