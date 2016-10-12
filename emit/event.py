import inspect
import itertools
from json import dumps, loads, JSONEncoder
from datetime import datetime
from dateutil import parser
from collections import Mapping, Iterable
from .globals import conf, log
from .utils import (_is_string, _is_value, _is_date)


Events = ['Event']
EventStacks = ['EventStack']


__all__ = Events + EventStacks + ['Events', 'EventStacks', 'EventJsonEncoder']


class EventJsonEncoder(JSONEncoder):
    """Just makes dates valid to spec."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat('T') + 'Z'
        elif isinstance(obj, set):
            return list(obj)
        try:
            return JSONEncoder.default(self, obj)
        except TypeError as e:
            obj_src = '(unknown)'

            try:
                obj_src = inspect.getsource(obj)
            except (IOError, TypeError):
                pass
            msg = 'EventJsonEncoder.default - unable to decode obj of type({0}) str({1}) src({2})'.format(
                str(type(obj)), str(obj), obj_src)
            log.debug(msg)
            log.exception(e)
            return dict(error='EventJsonEncoder', message=msg)


class EventProperty(object):
    """Just like a @property, but adds a validator func, see:
         https://docs.python.org/2/howto/descriptor.html#properties"""
    def __init__(self, fget=None, fset=None, fdel=None, fval=None, fnorm=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.fval = fval
        self.fnorm = fnorm
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        if self.fval:
            self.fval(obj, value)
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        self.fdel(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.fval, self.fnorm, self.__doc__)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.fval, self.fnorm, self.__doc__)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.fval, self.fnorm, self.__doc__)

    def validate(self, fval):
        return type(self)(self.fget, self.fset, self.fdel, fval, self.fnorm, self.__doc__)

    def normalize(self, fnorm):
        return type(self)(self.fget, self.fset, self.fdel, self.fval, fnorm, self.__doc__)


class Event(dict):
    keys_allowed = ['name', 'operation', 'component', 'system', 'fields', 'data', 'tags', 'replay', 'tid', 'time']
    keys_required = ['name', 'operation', 'component', 'system', 'tid', 'time']
    keys_optional = ['fields', 'data', 'tags', 'replay']

    _fields_rules = {
        'date': lambda v: _is_date(v),
        'boolean': lambda v: isinstance(v, bool),
        'double': lambda v: isinstance(v, float),
        'long': lambda v: (not isinstance(v, bool)) and isinstance(v, (int, long, complex)),
        'string': _is_string,
        'array': _is_string}

    _fields_lookups = list(itertools.chain.from_iterable(
        [k] if k == 'array' else ['array_{}'.format(k), k]
        for k in _fields_rules.keys()))

    @classmethod
    def from_json(cls, event_json):
        event_dict = loads(str(event_json))

        if 'time' in event_dict:
            event_dict['time'] = parser.parse(event_dict['time']).replace(tzinfo=None)
        return cls(**event_dict)

    def __call__(self, *args, **kwargs):
        """Calling an event returns a new Event inheriting all keys. Allows a
        useful construct such as:
          base = Event(system='foo')
          event = base(name='foo')
        """
        return type(self)(self, **self).update(*args, **kwargs)

    def __repr__(self):
        return '{}(tid={})'.format(self.__class__.__name__, self.tid)

    def __str__(self):
        """Returns json"""
        return self.json

    def canonicalized(self, base):
        """Returns the canonical event name for this event when derived from a
        given base. I.E.:
            self['name'] = 'foo'
            self.canonicalized('one.two.three')
                -> 'one.two.three.foo'
            self.canonicalized('one.two.three.foo')
                -> 'one.two.three.foo'
        """
        name = self['name']

        if not len(name) or base.endswith(name):
            return base
        if not len(base) or name.startswith(base):
            return name
        return '{0}.{1}'.format(base, name)

    def __or__(a, b):
        """Behaves like __add__ except prefixes event name"""
        event = a()
        event |= b
        return event

    def __ior__(self, right):
        """In place __or__."""
        right = right.to_event
        canonicalized_name = right.canonicalized(self.name)
        self.update(**right)
        self.name = canonicalized_name
        return self

    def __add__(a, b):
        """Allows Event() + `Eventable` to be a create + update operation."""
        event = a()
        event += b
        return event

    def __iadd__(self, other):
        """In place __add__."""
        return self.update(**other.to_event)

    def __init__(self, *args, **kwargs):
        """Makes sure the required keys exists, sets the time and calls `update`
        with any additional arguments."""
        super(Event, self).__init__(
            tid='',
            system='',
            component='',
            operation='',
            name='',
            time=datetime.utcnow())
        self.update(*args, **kwargs)

    def _list_from_sequence_by_allowed(self, sequence):
        """Helper for validation, just used for finding missing keys."""
        return list(itertools.chain(
            [allowed for allowed in self.keys_allowed if allowed in sequence],
            [item for item in sequence if not (item in self.keys_allowed)]))

    def _args_slurp(self, vals, *args):
        """Takes a list of `args` and merges them into `vals` container based
        on a set of rules.. see comments."""
        keys = ['tid', 'system', 'component', 'operation', 'name']
        args = list(args)

        while len(args):
            arg = args.pop(0)

            # String types are assigned to keys by order, even if they are empty:
            #   event, operation, component, system
            if _is_string(arg):
                if not len(keys):
                    raise TypeError(
                        'update() takes exactly 5 `string` arguments (6 given), extra was ({})'.format(arg))
                vals[keys.pop()] = arg

            # `Events` and mappings are merged in if they have a value, or if
            # the current value is also empty to keeps references in order
            # of precedence.
            elif isinstance(arg, (Event, self.__class__, Mapping)):
                for k, v in arg.iteritems():
                    if _is_value(v) or ((k in vals) and not _is_value(vals[k])):
                        vals[k] = v

            # Datetime to time
            elif isinstance(arg, datetime):
                vals['time'] = arg

            # Iterables merged into tags
            elif isinstance(arg, Iterable):
                if not ('tags' in vals):
                    vals['tags'] = []
                for tag in arg:
                    vals['tags'].append(tag)

            # Other types don't make sense, TypeError
            else:
                raise TypeError('`{}` type of ({}) can not be assigned'.format(
                    type(arg).__name__, arg))

    def _kwargs_slurp(self, vals, **kwargs):
        """Takes `kwargs` and merges them into `vals` if they are not empty."""

        # Merge any keywoard arguments
        if len(kwargs):
            for k, v in kwargs.iteritems():
                if _is_value(v):
                    vals[k] = v

    def update(self, *args, **kwargs):
        """Updates the event with the given args."""
        vals = dict()
        self._args_slurp(vals, *args)
        self._kwargs_slurp(vals, **kwargs)

        # Set vals through attributes for validation
        for k, v in vals.iteritems():
            setattr(self, k, v)
        return self

    def defaults(self, defaults=None, **kwargs):
        """Assign default values for `Event` from a `Mapping` type."""
        if defaults is None:
            defaults = dict()
        if not isinstance(defaults, (Event, self.__class__, Mapping)):
            raise ValueError('`defaults` must be an Event or Mapping')
        for map_iter in (defaults, kwargs):
            for key in map_iter:
                self.default(key, map_iter[key])
        return self

    def default(self, key, value):
        """Assign default value for any allowed keys that are empty or missing."""
        if not (key in self.keys_allowed):
            raise LookupError('`{0}` key is not allowed'.format(key))
        if key == 'time':
            return  # Not possible, set in __init__
        if not _is_value(getattr(self, key)):
            setattr(self, key, value)

    def validate(self):
        """Performs valiations by calling the key validate methods."""
        key_set = set(self.keys())
        keys_required_diff = self._list_from_sequence_by_allowed(
            set(self.keys_required) - key_set)
        keys_allowed_diff = self._list_from_sequence_by_allowed(
            key_set - set(self.keys_allowed))

        if len(keys_required_diff):
            raise ValueError('`Event` was missing required keys: {0}'.format(', '.join(keys_required_diff)))
        if len(keys_allowed_diff):
            raise ValueError('`Event` had extraneous keys: {0}'.format(', '.join(keys_allowed_diff)))
        for key in self.keys_allowed:
            if key in self:
                # Event is a descriptor, returns self on class access, use self
                # ref to call fval bound to this event instance
                object.__getattribute__(Event, key).fval(
                    self, getattr(self, key), final=True)
        return True

    @property
    def valid(self):
        """Returns True if the Event is currently valid, False otherwise."""
        try:
            self.validate()
        except ValueError:
            return False
        return True

    @property
    def to_dict(self):
        """Returns a plain dict and finalizes the event. This is where operation
        is set to component if it wasn't set."""
        out = dict(**self.to_event)

        if not _is_value(out['operation']) and _is_value(out['component']):
            out['operation'] = out['component']
        return out

    @property
    def to_event(self):
        """Satisfies an eventable interface, just does some normalizing and
        returns itself."""
        for key in self.keys_allowed:
            # Event is a descriptor, returns self on class access, use self
            # ref to call fnorm bound to this event instance
            if object.__getattribute__(Event, key).fnorm:
                object.__getattribute__(Event, key).fnorm(self)
        return self

    @property
    def json(self):
        """Returns JSON string of the current `to_event` property. It will output
        pretty JSON if conf.debug is set."""
        if conf.debug or conf.pretty:
            return dumps(
                self.to_dict, cls=EventJsonEncoder,
                indent=2, separators=(',', ': '))
        else:
            return dumps(self.to_dict, cls=EventJsonEncoder)

    # tid
    @EventProperty
    def tid(self):
        return self['tid']

    @tid.setter
    def tid(self, tid):
        self['tid'] = tid

    @tid.validate
    def tid(self, tid, final=False):
        if not _is_string(tid):
            raise ValueError('`tid` must be a string')
        if final and not len(tid):
            raise ValueError('`tid` must not be empty')

    # time
    @EventProperty
    def time(self):
        return self['time']

    @time.setter
    def time(self, time):
        self['time'] = time

    @time.validate
    def time(self, time, final=False):
        if not isinstance(time, datetime):
            raise ValueError('`time` must be an instance of datetime')

    # system
    @EventProperty
    def system(self):
        return self['system']

    @system.setter
    def system(self, system):
        self['system'] = system

    @system.validate
    def system(self, system, final=False):
        if not _is_string(system):
            raise ValueError('`system` must be a string')
        if final and not len(system):
            raise ValueError('`system` must not be empty')

    # component
    @EventProperty
    def component(self):
        return self['component']

    @component.setter
    def component(self, component):
        self['component'] = component

    @component.validate
    def component(self, component, final=False):
        if not _is_string(component):
            raise ValueError('`component` must be a string')
        if final and not len(component):
            raise ValueError('`component` must not be empty')

    # operation
    @EventProperty
    def operation(self):
        return self['operation']

    @operation.setter
    def operation(self, operation):
        self['operation'] = operation

    @operation.validate
    def operation(self, operation, final=False):
        if not _is_string(operation):
            raise ValueError('`operation` must be a string')
        if final and (not len(operation)) and (not len(self['component'])):
            raise ValueError('`operation` must not be empty')

    # name
    @EventProperty
    def name(self):
        return self['name']

    @name.setter
    def name(self, name):
        self['name'] = name

    @name.validate
    def name(self, name, final=False):
        if not _is_string(name):
            raise ValueError('`name` must be a string')
        if final and not len(name):
            raise ValueError('`name` must not be empty')

    @EventProperty
    def tags(self):
        if not ('tags' in self):
            self['tags'] = set()
        return self['tags']

    @tags.setter
    def tags(self, tags):
        self['tags'] = set()
        for tag in tags:
            self.tags.add(tag)
        return self

    @tags.deleter
    def tags(self):
        if 'tags' in self:
            del self['tags']

    @tags.validate
    def tags(self, tags, final=False):
        if not isinstance(tags, Iterable) or _is_string(tags):
            raise ValueError('`tags` must be an Iterable of strings')
        if not all(map(_is_string, tags)):
            raise ValueError('`tags` must only contain strings')
        if final:
            if not all(map(len, tags)):
                raise ValueError('`tags` must not contain empty strings')

    @tags.normalize
    def tags(self):
        if not ('tags' in self):
            return
        if not isinstance(self.tags, set):
            tags = set()
            for tag in self.tags:
                tags.add(tag)
            self['tags'] = tags

    # replay
    @EventProperty
    def replay(self):
        if not ('replay' in self):
            self['replay'] = ''
        return self['replay']

    @replay.setter
    def replay(self, replay):
        self['replay'] = replay

    @replay.deleter
    def replay(self):
        if 'replay' in self:
            del self['replay']

    @replay.validate
    def replay(self, replay, final=False):
        if not _is_string(replay):
            raise ValueError('`replay` must be a string')

    # fields
    @EventProperty
    def fields(self):
        if not ('fields' in self):
            self['fields'] = dict()
        return self['fields']

    @fields.setter
    def fields(self, fields):
        self['fields'] = fields

    @fields.deleter
    def fields(self):
        if 'fields' in self:
            del self['fields']

    @fields.validate
    def fields(self, fields, final=False):
        """@TODO Allow the empty arrays?"""
        if not isinstance(fields, Mapping):
            raise ValueError('`fields` must be a Mapping')
        if final:
            for k, v in self['fields'].iteritems():
                if not self.fields_validate(k, v):
                    raise ValueError('`{}` value `{}` did not match suffix type'.format(k, v))

    def fields_lookup(self, name):
        for l in self._fields_lookups:
            if name.rfind(l) >= 0:
                return l
        return 'string'

    def fields_validate(self, name, value):
        l = self.fields_lookup(name)

        if l.startswith('array'):
            if (not isinstance(value, Iterable)) or _is_string(value):
                return False
            func = self._fields_rules[l.split('_').pop()]
            return all(map(func, value))
        else:
            return self._fields_rules[l](value)

    @fields.normalize
    def fields(self):
        if not ('fields' in self):
            return

    # data
    @EventProperty
    def data(self):
        if not ('data' in self):
            self['data'] = dict()
        return self['data']

    @data.setter
    def data(self, data):
        self['data'] = data

    @data.deleter
    def data(self):
        if 'data' in self:
            del self['data']

    @data.validate
    def data(self, data, final=False):
        if not isinstance(data, Mapping):
            raise ValueError('`data` must be a Mapping')


class EventContext(Event):
    """Short lived object to bridge into a contextual event."""
    def __init__(self, event_stack, *args, **kwargs):
        super(EventContext, self).__init__(*args, **kwargs)
        self.event_stack = event_stack

    def __enter__(self):
        return self.event_stack.__enter__(self)

    def __exit__(self, exc_type, exc_value, tb):
        self.event_stack.__exit__(exc_type, exc_value, tb)


class EventStack(list):
    """Lifo stack of events for tracking context."""

    @property
    def to_event(self):
        """Returns rolled up Event using this context stack."""
        if not len(self):
            return conf.event_class()
        out = self[0]()

        for event in self[1:]:
            out |= event

        return out

    @property
    def bot(self):
        """Returns the bottom of the stack, or `None` if empty."""
        try:
            return self[0]
        except (AttributeError, IndexError):
            return None

    @property
    def top(self):
        """The topmost item on the stack, or `None` if empty."""
        try:
            return self[-1]
        except (AttributeError, IndexError):
            return None

    def __or__(a, b):
        """Acts just like Event() __or__."""
        return a.to_event | b.to_event

    def __add__(a, b):
        """Acts just like Event() __add__."""
        return a.to_event + b.to_event

    def __enter__(self, evt=None):
        """Entering the event stack adds an event to it."""
        if evt is None:
            evt = conf.event_class()
        self.append(evt)
        return evt

    def __exit__(self, exc_type, exc_value, tb):
        """Exiting event stack pops current event context."""
        self.pop()

    def __call__(self, *args, **kwargs):
        """Returns an `EventContext` which may be entered to add to this stack."""
        return EventContext(self, *args, **kwargs)

    def __getslice__(self, begin, end):
        """Just implements slicing, i.e.: foo[begin:end]."""
        return self.__class__(list.__getslice__(self, begin, end))

    def __getitem__(self, k):
        """EventStack[key] will access the to_event item, meaning it will have
        the canonicalized event name `base.one.two.three` vs EventStack.attribute
        being `three` given a stack containing [name=base, one, ..three]."""
        if isinstance(k, int):
            return list.__getitem__(self, k)
        return dict.__getitem__(self.to_event, k)

    def __contains__(self, k):
        """Checks if a key exists in current event stack's full event."""
        return dict.__contains__(self.to_event, k)

    def __getattr__(self, name):
        """EventStack.attribute will access the top item."""
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return object.__getattribute__(self.top, name)

    def __str__(self):
        """Returns a helpful view of the event stack for troubleshooting."""
        evt = self.to_event
        out = '{}({})'.format(self.__class__.__name__, dict.__getitem__(evt, 'tid'))

        for index, evt in enumerate(reversed(self)):
            out += '\n{0}->{1}'.format(
                ('  ' * (index + 1)),
                dict((k, v) for k, v in evt.iteritems() if v and k != 'time'))
        return out
