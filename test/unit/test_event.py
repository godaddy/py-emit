import pytest
import itertools
import json
import emit
from emit.globals import conf
from datetime import datetime
from emit.event import Event, EventJsonEncoder, EventProperty, EventStack
from ..helpers import (
    TestCase, tevent, tevent_expect, teventf, teventf_expect, tevent_stack)


keys_allowed = ['name', 'operation', 'component', 'system', 'tid', 'fields', 'data', 'tags', 'replay', 'time']
keys_required = ['name', 'operation', 'component', 'system', 'tid', 'time']
keys_optional = ['fields', 'data', 'tags', 'replay']

expect_name_value = 'test_event'
expect_name = dict(name=expect_name_value)
expect_operation_value = 'test_operation'
expect_operation = dict(operation=expect_operation_value)
expect_component_value = 'test_component'
expect_component = dict(component=expect_component_value)
expect_system_value = 'test_system'
expect_system = dict(system=expect_system_value)
expect_fields_value = dict(fields_key_1='value_1')
expect_fields = dict(fields=expect_fields_value)
expect_data_value = dict(data_key_1='value_1')
expect_data = dict(data=expect_data_value)
expect_tags_value = ['tag_1', 'tag_2']
expect_tags = dict(tags=expect_tags_value)
expect_replay_value = 'test_replay'
expect_replay = dict(replay=expect_replay_value)
expect_tid_value = 'test_tid'
expect_tid = dict(tid=expect_tid_value)
expect_time_value = datetime.utcnow()
expect_time = dict(time=expect_time_value)

expect0 = dict()
expect1 = dict(name=expect_name_value)
expect2 = dict(operation=expect_operation_value, **expect1)
expect3 = dict(component=expect_component_value, **expect2)
expect4 = dict(system=expect_system_value, **expect3)
expect5 = dict(tid=expect_tid_value, **expect4)
expect6 = dict(fields=expect_fields_value, **expect5)
expect7 = dict(data=expect_data_value, **expect6)
expect8 = dict(tags=expect_tags_value, **expect7)
expect9 = dict(replay=expect_replay_value, **expect8)
expect10 = dict(time=expect_time_value, **expect9)

expect_string_field_values = [
    expect_name_value, expect_operation_value,
    expect_component_value, expect_system_value,
    expect_tid_value]
expect_string_keys = ['name', 'operation', 'component', 'system', 'tid']
expect_string_keys_dict = dict(zip(expect_string_keys, expect_string_field_values))

expect_empty = dict(
    tid='', system='', component='', operation='', name='')
expect_empty_full = dict(
    tid='', system='', component='', operation='', name='',
    replay='', tags=set(), fields=dict(), data=dict())

event_dict_required = dict(
    tid='test_tid', system='test_system', component='test_component',
    operation='test_operation', name='test_event', time=datetime.utcnow())
event_dict_allowed = dict(
    replay='test_replay', tags=set(['tag_1', 'tag_2', 'tag_3']),
    fields=dict(
        fields_key_1='fields_value_1', fields_key_2='fields_value_2'),
    data=dict(
        data_key_1='data_value_1', data_key_2='data_value_2'),
    **event_dict_required)
event_factories = [
    lambda *a, **k: Event(*a, **k),
    lambda *a, **k: Event().update(*a, **k),
    lambda *a, **k: Event() + Event(*a, **k),
    lambda *a, **k: Event(*a) + Event(**k)]


def _assert_event_perms(expect, *args, **kwargs):
    event_rolling = Event()

    for factories in itertools.permutations(event_factories):
        factories = list(factories)
        events = []
        event_set = Event()

        while len(factories):
            factory = factories.pop(0)
            event = factory(*args, **kwargs)
            _assert_event(event, expect)

            event_set.update(event)
            _assert_event(event_set, expect)

            event_rolling.update(event)
            _assert_event(event_rolling, expect)

            events.append(event)
        _assert_identical(expect, event_rolling, event_set, *events)


def _assert_events(*events, **kwargs):
    expect = kwargs['expect'] if 'expect' in kwargs else None
    for event in events:
        _assert_event(event, expect=expect)


def _assert_event(event, expect=None):
    if expect is None:
        expect = expect_empty
    assert isinstance(event, Event)

    for field in keys_required:
        assert field in event
    assert isinstance(event.time, datetime)

    if 'name' in expect:
        assert emit.event._is_string(event.name)
        assert event.name == expect['name']
    if 'operation' in expect:
        assert emit.event._is_string(event.operation)
        assert event.operation == expect['operation']
    if 'component' in expect:
        assert emit.event._is_string(event.component)
        assert event.component == expect['component']
    if 'system' in expect:
        assert emit.event._is_string(event.system)
        assert event.system == expect['system']
    if 'tid' in expect:
        assert emit.event._is_string(event.tid)
        assert event.tid == expect['tid']
    if 'time' in expect:
        assert isinstance(event.time, datetime)
        assert event.time == expect['time']
    if 'replay' in expect:
        assert 'replay' in event
        assert emit.event._is_string(event.replay)
        assert event.replay == expect['replay']
    if 'tags' in expect:
        assert 'tags' in event
        assert isinstance(event.tags, set)
        assert all(tag in event.tags for tag in expect['tags'])
    if 'data' in expect:
        assert 'data' in event
        assert isinstance(event.data, dict)
        assert event.data == expect['data']
    if 'fields' in expect:
        assert 'fields' in event
        assert isinstance(event.fields, dict)
        assert event.fields == expect['fields']


def _assert_identical(expect, *events):
    for event1, event2 in itertools.combinations(events, 2):
        _assert_event(event1, expect)
        _assert_event(event2, expect)


class TestEventInit(TestCase):

    def test_init(self):
        event = Event()
        _assert_event(event)

    def test_init_str(self):
        event = Event(expect_name_value)
        _assert_event(event, expect_name)

    def test_init_datetime(self):
        event = Event(expect_time_value)
        _assert_event(event, expect_time)

    def test_init_iterable(self):
        event = Event(expect_tags_value)
        _assert_event(event, expect_tags)

    def test_init_dict(self):
        event = Event(expect_name)
        _assert_event(event, expect_name)

    def test_init_kwargs(self):
        event = Event(name=expect_name_value)
        _assert_event(event, expect_name)

    def test_init_event(self):
        event = Event(Event(name=expect_name_value))
        _assert_event(event, expect_name)

    def test_init__str_str(self):
        event = Event(expect_name_value, expect_operation_value)
        _assert_event(event, expect2)

    def test_init_str_dict(self):
        event = Event(expect_name_value, expect_operation_value)
        _assert_event(event, expect2)

    def test_init_str_kwargs(self):
        event = Event(expect_name_value, **expect_operation)
        _assert_event(event, expect2)

    def test_init_str_event(self):
        event = Event(expect_name_value, Event(operation=expect_operation_value))
        _assert_event(event, expect2)

    def test_init_str_str_str(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            expect_component_value)
        _assert_event(event, expect3)

    def test_init_str_str_dict(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            expect_component)
        _assert_event(event, expect3)

    def test_init_str_str_kwargs(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            **expect_component)
        _assert_event(event, expect3)

    def test_init_str_str_event(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            Event(component=expect_component_value))
        _assert_event(event, expect3)

    def test_init_str_dict_dict(self):
        event = Event(
            expect_name_value,
            expect_operation,
            expect_component)
        _assert_event(event, expect3)

    def test_init_str_dict_kwargs(self):
        event = Event(
            expect_name_value,
            expect_operation,
            component=expect_component_value)
        _assert_event(event, expect3)

    def test_init_str_dict_event(self):
        event = Event(
            expect_name_value,
            expect_operation,
            Event(component=expect_component_value))
        _assert_event(event, expect3)

    def test_init_str_dict_event_kwargs(self):
        event = Event(
            expect_name_value,
            expect_operation,
            Event(component=expect_component_value),
            system=expect_system_value)
        _assert_event(event, expect4)

    def test_init_str_datetime(self):
        event = Event(expect_name, expect_time_value)
        _assert_event(event, Event(expect_time, expect_name))

    def test_init_str_datetime_kwargs(self):
        event = Event(
            expect_name,
            expect_time_value,
            system=expect_system_value)
        _assert_event(event, dict(
            name=expect_name_value,
            time=expect_time_value,
            system=expect_system_value))

    def test_init_str_datetime_str_kwargs(self):
        event = Event(
            expect_name_value,
            expect_time_value,
            expect_operation_value,
            system=expect_system_value)
        _assert_event(event, dict(
            name=expect_name_value,
            time=expect_time_value,
            system=expect_system_value,
            operation=expect_operation_value))

    def test_init_str_datetime_str_kwargs_empty_ignore_mutable(self):
        event = Event(
            expect_name_value,
            expect_time_value,
            expect_operation_value,
            system='')
        _assert_event(event, dict(
            name=expect_name_value,
            time=expect_time_value,
            operation=expect_operation_value))

    def test_init_str_datetime_str_kwargs_empty_set_mutable(self):
        event = Event(
            expect_name_value,
            expect_time_value,
            expect_operation_value,
            data=expect_data_value)
        _assert_event(event, dict(
            name=expect_name_value,
            time=expect_time_value,
            operation=expect_operation_value,
            data=expect_data_value))

    def test_init_strings_kwargs(self):
        event = Event(*expect_string_field_values, data=expect_data_value)
        _assert_event(event, dict(
            name=expect_name_value,
            operation=expect_operation_value,
            component=expect_component_value,
            system=expect_system_value,
            tid=expect_tid_value,
            data=expect_data_value))

    def test_init_all_strings(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            expect_component_value,
            expect_system_value,
            expect_tid_value)
        _assert_event(event, dict(
            name=expect_name_value,
            operation=expect_operation_value,
            component=expect_component_value,
            system=expect_system_value,
            tid=expect_tid_value))

    def test_init_all_strings_dict(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            expect_component_value,
            expect_system_value,
            expect_tid_value,
            expect_data)
        _assert_event(event, dict(
            name=expect_name_value,
            operation=expect_operation_value,
            component=expect_component_value,
            system=expect_system_value,
            tid=expect_tid_value,
            data=expect_data_value))

    def test_init_all_strings_dict_kwargs(self):
        event = Event(
            expect_name_value,
            expect_operation_value,
            expect_component_value,
            expect_system_value,
            expect_tid_value,
            expect_fields,
            data=expect_data_value)
        _assert_event(event, dict(
            name=expect_name_value,
            operation=expect_operation_value,
            component=expect_component_value,
            system=expect_system_value,
            tid=expect_tid_value,
            fields=expect_fields_value,
            data=expect_data_value))

    def test_init_precedence(self):
        event_value = 'test_uses_precedence_event'
        data = dict(key='val')

        # args -> dicts
        event = Event(
            expect_name_value,
            dict(name=event_value))
        _assert_event(event, dict(
            name=event_value))

        # args -> kwargs
        event = Event(
            expect_name_value,
            name=event_value)
        _assert_event(event, dict(
            name=event_value))

        # args -> dict -> kwargs
        event = Event(
            expect_name_value,
            dict(name='!!!override!!!'),
            name=event_value)
        _assert_event(event, dict(
            name=event_value))

        # args x 2 -> dict -> kwargs
        event = Event(
            expect_name_value,
            expect_operation_value,
            dict(name='!!!override!!!'),
            name=event_value)
        _assert_event(event, dict(
            name=event_value,
            operation=expect_operation_value))

        # args -> dict -> kwargs map
        event = Event(
            expect_name_value,
            expect_data,
            data=data)
        _assert_event(event, dict(
            name=expect_name_value,
            data=data))

    def test_init_non_required_keys_missing(self):
        event = Event()
        for key in keys_optional:
            assert not (key in event)

    def test_init_required_keys_exist(self):
        event = Event()
        for key in keys_required:
            assert key in event

    def test_init_neg(self):
        sentinel = object()
        cases = [123456, True, False, None, 123.456]

        for case in cases:
            event = Event()
            with pytest.raises(TypeError) as excinfo:
                event.update(case, data=sentinel)
            expect_msg = '`{}` type of ({}) can not be assigned'.format(
                type(case).__name__, case)
            assert expect_msg == str(excinfo.value)
            assert event.data != sentinel

        with pytest.raises(TypeError) as excinfo:
            Event(*expect_string_field_values + ['!!EXTRA!!'])
        expect_msg = 'update() takes exactly 5 `string` arguments (6 given), extra was (!!EXTRA!!)'
        assert expect_msg == str(excinfo.value)

        event = Event()
        with pytest.raises(TypeError) as excinfo:
            event.update(dict(data=sentinel), *expect_string_field_values + ['!!EXTRA!!'])
        expect_msg = 'update() takes exactly 5 `string` arguments (6 given), extra was (!!EXTRA!!)'
        assert expect_msg == str(excinfo.value)
        assert event.data != sentinel

    def test_callable(self):
        event = Event(expect_name_value, expect_operation_value)
        _assert_event(event, expect2)

        got = event(name='test_callable')
        _assert_event(got, dict(name='test_callable', operation=expect_operation_value))

    def test_callable_nested(self):
        event = Event(expect_name_value, expect_operation_value)
        _assert_event(event, expect2)

        trail = 'init'
        for i in range(10):
            trail += '.' + str(i)
            got = event(name=trail)
            _assert_event(got, dict(name=trail, operation=expect_operation_value))


@pytest.mark.name_fields
class TestEventFields(TestCase):
    def test_fields_types(self):
        all_valid_fields = dict(
            t='t',
            t_array=['t1', 't2', 't3', 't4'],
            tstring='t',
            tstring_array_string=['t1', 't2', 't3', 't4'],
            t_boolean=True,
            t_array_boolean=[True, True, False, True],
            t_double=123.456,
            t_array_double=[1.2, 3.4, 5.6],
            tlong_long=long(123456),
            tlong_array_long=[long(12), long(34), long(56)],
            tint_long=int(123456),
            tint_array_long=[int(12), int(34), int(56)],
            tcomplex_long=complex(123456),
            tcomplex_array_long=[complex(12), complex(34), complex(56)],
            tmixed_array_long=[int(12), long(34), complex(56)],
            t_date=datetime.utcnow(),
            tstr_date='2016-07-08T17:37:43Z',
            t_array_date=[datetime.utcnow(), datetime.utcnow(), datetime.utcnow()],
            tstr_array_date=['2016-01-08T11:23:56Z', '2016-02-08T11:23:56Z', '2016-03-08T11:23:56Z'],
            tstrmix_array_date=[datetime.utcnow(), '2016-03-08T11:23:56Z', datetime.utcnow()])
        event = tevent()
        event.fields = dict(**all_valid_fields)
        event.validate()

    def test_fields_types_perm(self):
        def run_cases(expect, cases):
            for case in cases:
                if expect:
                    event = tevent()
                    event.fields = dict(**case)
                    event.validate()
                else:
                    with pytest.raises(ValueError) as excinfo:
                        event = tevent()
                        event.fields = dict(**case)
                        event.validate()
                        assert False, '{} did not raise'.format(dict(**case))
                    assert str(excinfo.value).endswith('did not match suffix type')

        def iterf():
            for k, vals in fields.iteritems():
                yield 't_{}'.format(k), vals

        def iteraf():
            for k, vals in array_fields.iteritems():
                yield 't_array_{}'.format(k), vals

        def iterneg(iterable):
            for k, vals in iterable:
                for neg_k, neg_vals in fields.iteritems():
                    if k.endswith(neg_k):
                        continue
                    for v in neg_vals:
                        yield k, v

        def iterswap(iterable):
            for k, vals in iterable:
                name = k.split('_').pop()
                values = [sv for sk, sv in iterf() if not (name in sk)]

                for index in range(3):
                    for kswap, vswap in iterswapv(index, values, iterable):
                        yield kswap, vswap

        def iterswapv(index, values, iterable):
            for k, vals in iterable:
                for value in values:
                    v = vals[:]
                    v[index] = value
                    yield k, v

        def iterslice(start, end, iterable):
            for k, vals in iterable:
                for value in vals:
                    v = vals[start:end]
                    yield k, v

        fields = dict(
            string=['', 'a', 'aaa'],
            date=[datetime.utcnow(), datetime.min, datetime.max],
            boolean=[True, False, False],
            double=[123.456, float(0), float(1)],
            long=[123, long(123), complex(123)])
        array_fields = {'array_{}'.format(k): v for k, v in fields.iteritems()}

        # Basic fields
        run_cases(True, [{k: v} for k, vals in iterf() for v in vals])

        # Array fields
        run_cases(True, [{k: vals} for k, vals in iteraf()])

        # Array fields len 0
        run_cases(True, [{k: []} for k, vals in iteraf()])

        # Array fields len 1
        run_cases(True, [{k: vals} for k, vals in iterslice(0, 1, iteraf())])

        # Array fields len 2
        run_cases(True, [{k: vals} for k, vals in iterslice(0, 2, iteraf())])

        # Basic fields as arrays
        run_cases(False, [{'t_array_{}'.format(k): v} for k, vals in fields.iteritems() for v in vals])

        # Basic bad fields
        run_cases(False, [{k: vals} for k, vals in iterneg(iterf())])

        # Basic bad array fields
        run_cases(False, [{k: vals} for k, vals in iterneg(iteraf())])

        # Array fields with single value of each index made invalid
        run_cases(False, [{k: vals} for k, vals in iterswap(iteraf())])


@pytest.mark.name_properties
class TestEventProperties(TestCase):

    def test_properties(self):
        event_dict = dict(**event_dict_allowed)
        event = Event(**event_dict)
        _assert_event(event, event_dict)
        setvals = dict(
            tags=set(['tag1', 'tag2']), data=dict(dkey=1),
            fields=dict(fkey=1), time=datetime.utcnow())

        for key, value in event_dict.iteritems():
            assert getattr(event, key) == event_dict[key]
            assert event[key] == event_dict[key]

            setval = 'setval' if not (key in setvals) else setvals[key]
            setattr(event, key, setval)
            assert getattr(event, key) == setval

            if key in keys_optional:
                assert key in event
                delattr(event, key)
                assert not (key in event)

    def test_properties_set_on_access(self):
        event = Event()
        _assert_event(event)

        for key in keys_optional:
            assert not (key in event)
            value = getattr(event, key)
            assert not (value is None)
            assert not (value is event)
            assert key in event
        assert len(event.tags) == 0
        assert len(event.replay) == 0
        assert len(event.fields) == 0
        assert len(event.data) == 0
        _assert_event(event)

    def test_tid(self):
        event = teventf()
        assert len(event.tid)
        event.tid = 'test_tid'
        assert event.tid == 'test_tid'

    def test_time(self):
        now = datetime.utcnow()
        event = teventf()
        assert event.time > now
        event.time = now
        assert event.time == now

    def test_system(self):
        event, expect = tevent_expect()
        assert event.system == expect['system']
        event.system = expect['system']
        assert event.system == expect['system']

    def test_component(self):
        event, expect = teventf_expect()
        assert event.component == expect['component']
        event.component = expect['component']
        assert event.component == expect['component']

    def test_event(self):
        event, expect = teventf_expect()
        assert event.name == expect['name']
        event.name = expect['name']
        assert event.name == expect['name']

    def test_operation(self):
        event, expect = teventf_expect()
        assert event.operation == expect['operation']
        event.operation = expect['operation']
        assert event.operation == expect['operation']

    def test_operation_default(self):
        """Operation is left unchanged until a message is converted
        to json at that point it will default to component if it
        was not set."""
        event = Event()
        event.system = 'test_operation_default_system'
        event.component = 'test_operation_default_component'
        assert event.operation == ''
        assert event.to_dict['operation'] == 'test_operation_default_component'
        assert event.to_dict['component'] == 'test_operation_default_component'

    def test_tags(self):
        event = teventf()
        assert 'test.tag' in event.tags
        event.tags = ['test_tags', 'test_tags2']
        assert 'test_tags' in event.tags
        assert 'test_tags2' in event.tags
        del event.tags
        assert len(event.tags) == 0

        # Check tags.normalize changes to set (prevent dupes)
        event['tags'] = ['test_tags', 'test_tags2']
        assert isinstance(event.to_event.tags, set)

    def test_replay(self):
        event = teventf()
        assert event.replay == 'protect://blackhole:create/data/replay_data'
        event.replay = 'tcp://127.0.0.1:1924/data/replay_data'
        assert event.replay == 'tcp://127.0.0.1:1924/data/replay_data'

    def test_data(self):
        event = teventf()
        assert event.data == {'replay_data': {'headers': {'Content-Type': 'application/json'}}}
        event.data = {'test_data': 'test_data'}
        assert event.data == {'test_data': 'test_data'}


@pytest.mark.event_properties
class TestEventProperty(TestCase):

    def test_basic(self):
        class EventPropertyTest(dict):
            @EventProperty
            def bad_field(self):
                pass

            @bad_field.getter
            def bad_field(self):
                pass

            @EventProperty
            def event(self):
                return self['name']

            @event.deleter
            def name(self):
                del self['name']

        ft = EventPropertyTest(name='foo')
        assert isinstance(EventPropertyTest.bad_field, EventProperty)
        assert ft.name == 'foo'

        EventPropertyTest.bad_field.fget = None
        with pytest.raises(AttributeError):
            ft.bad_field
        with pytest.raises(AttributeError):
            ft.bad_field = 'set'
        with pytest.raises(AttributeError):
            del ft.bad_field
        del ft.name


class TestEventMethods(TestCase):

    def test_json(self):
        event, expect = teventf_expect()
        got = json.loads(event.json)
        expect = json.loads(json.dumps(expect, cls=EventJsonEncoder))

        for key in expect:
            assert got[key] == expect[key]

    def test_str(self):
        event, expect = teventf_expect()
        got = json.loads(str(event))
        expect = json.loads(json.dumps(expect, cls=EventJsonEncoder))

        for key in expect:
            assert got[key] == expect[key]

    def test_repr(self):
        event = tevent()
        assert repr(event) == 'Event(tid=test.tid)'

    def test_canonicalize(self):
        assert Event(name='end').canonicalized('a.b.c') == 'a.b.c.end'
        # Existing stutter isnt removed in canonicalized
        assert Event(name='end').canonicalized('a.b.c.c.c') == 'a.b.c.c.c.end'
        # But new stuttering isn't added
        assert Event(name='a').canonicalized('a') == 'a'
        assert Event(name='a').canonicalized('a.a') == 'a.a'
        assert Event(name='a.a').canonicalized('a') == 'a.a'
        assert Event(name='end').canonicalized('a.b.c.end') == 'a.b.c.end'

    def test__or__(self):
        event = Event('a') | Event('b')
        assert event.name == 'a.b'

        event = Event('a', system='system_a', component='component_a') | Event('b', system='system_b')
        assert event.name == 'a.b'
        assert event.system == 'system_b'
        assert event.component == 'component_a'

        event = Event('a') | Event('b') | Event('c')
        assert event.name == 'a.b.c'

        event = Event('a') | Event('b') | Event('c') | Event('d')
        assert event.name == 'a.b.c.d'

        event_a = Event(name='a', system='t_system_a', component='t_component_a')
        event_b = Event(name='b', system='t_system_b', operation='t_operation_b')
        event_c = Event(name='c', data={'key_c': 'c'})

        event = event_a | event_b
        assert event != event_a
        assert event != event_b
        assert event.name == 'a.b'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'

        event = event_a | event_b | event_c
        assert event != event_a
        assert event != event_b
        assert event != event_c
        assert event.name == 'a.b.c'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'
        assert 'key_c' in event.data
        assert event.data['key_c'] == 'c'

    def test__ior__(self):
        event = Event('a')
        event |= Event('b')
        assert event.name == 'a.b'

        event = Event('a', system='system_a', component='component_a')
        event |= Event('b', system='system_b')
        assert event.name == 'a.b'
        assert event.system == 'system_b'
        assert event.component == 'component_a'

        event = Event('a')
        event |= Event('b')
        event |= Event('c')
        assert event.name == 'a.b.c'

        event = Event('a')
        event |= Event('b')
        event |= Event('c')
        event |= Event('d')
        assert event.name == 'a.b.c.d'

        event_a = Event(name='a', system='t_system_a', component='t_component_a')
        event_b = Event(name='b', system='t_system_b', operation='t_operation_b')
        event_c = Event(name='c', data={'key_c': 'c'})

        event = event_a()
        event |= event_b
        assert event != event_a
        assert event != event_b
        assert event.name == 'a.b'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'

        event = event_a()
        event |= event_b
        event |= event_c
        assert event != event_a
        assert event != event_b
        assert event != event_c
        assert event.name == 'a.b.c'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'
        assert 'key_c' in event.data
        assert event.data['key_c'] == 'c'

    def test__add__(self):
        event_a = Event(name='a', system='t_system_a', component='t_component_a')
        event_b = Event(name='b', system='t_system_b', operation='t_operation_b')
        event_c = Event(name='c', data={'key_c': 'c'})

        event = event_a + event_b
        assert event != event_a
        assert event != event_b
        assert event.name == 'b'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'

        event = event_b + event_a
        assert event != event_a
        assert event != event_b
        assert event.name == 'a'
        assert event.system == 't_system_a'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'

        event = event_a + event_b + event_c
        assert event != event_a
        assert event != event_b
        assert event != event_c
        assert event.name == 'c'
        assert event.system == 't_system_b'
        assert event.component == 't_component_a'
        assert event.operation == 't_operation_b'
        assert 'key_c' in event.data
        assert event.data['key_c'] == 'c'

    def test_pretty_with_flags_set(self):
        restore_debug = conf.debug
        restore_pretty = conf.pretty

        cases = [
            {'debug': False, 'pretty': False, 'expect': 0},
            {'debug': True, 'pretty': False, 'expect': 21},
            {'debug': False, 'pretty': True, 'expect': 21},
            {'debug': True, 'pretty': True, 'expect': 21},
        ]

        try:
            for case in cases:
                conf.debug = case['debug']
                conf.pretty = case['pretty']
                event = teventf()
                assert event.json.count('\n') == case['expect']
        finally:
            conf.debug = restore_debug
            conf.pretty = restore_pretty

    def test_list_from_sequence_by_allowed(self):
        tests = []
        tests.append(
            (('system', 'name', 'component', 'name'), ['name', 'component', 'system']))
        tests.append(
            (('name', 'component'), ['name', 'component']))
        tests.append(
            (('name', 'component', 'name'), ['name', 'component']))
        tests.append(
            (('name', 'component', 'name', 'component'), ['name', 'component']))

        event = Event()
        for (given, expect) in tests:
            got = event._list_from_sequence_by_allowed(given)
            assert got == expect


@pytest.mark.event_mutations
class TestEventMutation(TestCase):

    def test_args(self):
        cases = [
            ['name'], ['name', 'operation'],
            ['name', 'operation', 'component'],
            ['name', 'operation', 'component', 'system'],
            ['name', 'operation', 'component', 'system', 'tid']]

        for case in cases:
            expect = dict()
            expect.update(**expect_empty)
            for key in case:
                expect[key] = key
            _assert_event_perms(expect, *case)

    def test_args_only(self):
        _assert_event_perms(expect_string_keys_dict, *expect_string_field_values)

    def test_kwargs_only(self):
        t = datetime.utcnow()
        tags = ['tag_1', 'tag_2']
        data = dict(data_key_1='value_1')
        fields = dict(fields_key_1='value_1')
        kwargs = dict(
            operation='test_operation',
            name='test_event', component='test_component', tags=tags,
            system='test_system', tid='test_tid', replay='test_replay',
            fields=fields, data=data, time=t)
        _assert_event_perms(kwargs, **kwargs)

    def test_with_arg_and_kwarg(self):
        event = Event('test_emittable_with_event_name')
        assert event.name == 'test_emittable_with_event_name'

    def test_with_event_name_and_arg(self):
        event = Event('test_event', component='test_component')
        assert event.name == 'test_event'
        assert event.component == 'test_component'

    def test_with_event_name_and_arg_out_of_order(self):
        event = Event('test_event', system='test_system')
        assert event.name == 'test_event'
        assert event.system == 'test_system'

    def test_with_all(self):
        event_1 = dict(system='test_system_1', component='test_component_2')
        event_2 = Event(name='test_event_2', tid='test_tid_2', system='test_system_2')
        event_3 = dict(name='test_event_3', tid='test_tid_3')

        event = Event(
            'test_event_arg', 'test_operation_arg', event_1, event_2, event_3, system='test_system')
        _assert_event(event, dict(
            name='test_event_3',
            operation='test_operation_arg',
            component='test_component_2',
            system='test_system',
            tid='test_tid_3'))

    def test_with_dict(self):
        event_dict = dict(name='test_event', component='test_component')
        event = Event(event_dict)
        assert event.name == 'test_event'
        assert event.component == 'test_component'

    def test_with_dict_out_of_order(self):
        event_dict = dict(name='test_event', system='test_system')
        event = Event(event_dict)
        assert event.name == 'test_event'
        assert event.system == 'test_system'

    def test_with_multi_dict(self):
        expect = dict(name='test_event_1', operation='test_operation_1')
        event_1 = dict(**expect)
        event_2 = dict(system='test_system_2', component='test_component_2')
        expect.update(system='test_system_2', component='test_component_2')
        event_3 = dict(name='test_event_3', tid='test_tid_3')
        expect.update(name='test_event_3', tid='test_tid_3')

        event = Event(event_1, event_2, event_3)
        _assert_event(event, expect)

    def test_with_multi_dict_and_event(self):
        expect = dict(name='test_event_1', operation='test_operation_1')
        event_1 = dict(**expect)
        event_2 = Event(system='test_system_2', component='test_component_2')
        expect.update(system='test_system_2', component='test_component_2')
        event_3 = dict(name='test_event_3', tid='test_tid_3')
        expect.update(name='test_event_3', tid='test_tid_3')

        event = Event(event_1, event_2, event_3)
        _assert_event(event, expect)

    def test_with_multi_dict_and_events(self):
        data = dict(data_key_1='value_1')
        fields = dict(fields_key_1='value_1')
        expect = dict(name='test_event_1', operation='test_operation_1')

        event_1 = dict(**expect)
        event_2 = Event(system='test_system_2', component='test_component_2')
        expect.update(system='test_system_2', component='test_component_2')
        event_3 = dict(name='test_event_3', tid='test_tid_3')
        expect.update(name='test_event_3', tid='test_tid_3')
        event_4 = Event(data=data)
        event_5 = dict(fields=fields)
        expect.update(data=data, fields=fields)
        event = Event(event_1, event_2, event_3, event_4, event_5)
        _assert_event(event, expect)


@pytest.mark.event_defaults
class TestEventDefaults(TestCase):
    def test_default(self):
        event = Event()
        assert event.name is ''
        event.default('name', 'test_default')
        assert event.name == 'test_default'

    def test_default_data(self):
        event = Event()
        assert not len(event.data)
        event.default('data', {'test': 'test_default_data'})
        assert len(event.data)
        assert event.data['test'] == 'test_default_data'

    def test_default_tag(self):
        event = Event()
        assert not len(event.tags)
        event.tags.add('test_default_tags')
        assert len(event.tags)
        assert 'test_default_tags' in event.tags

    def test_default_tag_neg(self):
        event = tevent()
        assert not len(event.tags)
        event.tags.add(False)
        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`tags` must only contain strings' == str(excinfo.value)
        with pytest.raises(ValueError) as excinfo:
            event.tags = ['foo', False, 'bar']
        assert '`tags` must only contain strings' == str(excinfo.value)

    def test_default_tags(self):
        event = Event()
        assert not len(event.tags)
        event.default('tags', ['test_default_tags'])
        assert event.tags is not None
        assert 'test_default_tags' in event.tags

    def test_default_missing(self):
        event = Event()
        assert event.replay == ''
        event.default('replay', 'test_default_missing')
        assert event.replay is 'test_default_missing'

    def test_default_missing_fields(self):
        event = Event()
        assert not len(event.fields)
        event.default('fields', {})
        assert event.fields is not None

    def test_default_missing_fields_value(self):
        event = Event()
        assert not len(event.fields)
        event.default('fields', {'val': 'test_default_missing_fields_value'})
        assert len(event.fields)
        assert event.fields['val'] == 'test_default_missing_fields_value'

    def test_default_missing_data(self):
        event = Event()
        assert not len(event.data)
        event.default('data', {})
        assert event.data is not None

    def test_default_missing_data_value(self):
        event = Event()
        assert not len(event.data)
        event.default('data', {'val': 'test_default_missing_data_value'})
        assert len(event.data)
        assert event.data['val'] == 'test_default_missing_data_value'

    def test_default_missing_tags(self):
        event = Event()
        assert not len(event.tags)
        event.default('tags', [])
        assert event.tags is not None

    def test_default_missing_tags_neg_not_seq(self):
        event = Event()
        assert not len(event.tags)

        with pytest.raises(ValueError) as excinfo:
            event.default('tags', None)
        assert '`tags` must be an Iterable of strings' == str(excinfo.value)
        assert not len(event.tags)

    def test_default_missing_tags_neg_not_seq_of_str(self):
        event = Event()
        assert not len(event.tags)

        with pytest.raises(ValueError) as excinfo:
            event.default('tags', [False])
        assert '`tags` must only contain strings' == str(excinfo.value)
        assert not len(event.tags)

    def test_default_missing_tags_value(self):
        event = Event()
        assert not len(event.tags)
        event.default('tags', ['test_default_missing_tags_value'])
        assert event.tags is not None
        assert 'test_default_missing_tags_value' in event.tags

    def test_default_not_allowed(self):
        event = Event()

        with pytest.raises(LookupError) as excinfo:
            event.default('not_allowed', 'test_default_not_allowed')
        assert '`not_allowed` key is not allowed' == str(excinfo.value)

    def test_default_fields_invalid(self):
        event = Event()

        with pytest.raises(ValueError) as excinfo:
            event.default('fields', None)
        assert '`fields` must be a Mapping' == str(excinfo.value)

    def test_default_data_invalid(self):
        event = Event()

        with pytest.raises(ValueError) as excinfo:
            event.default('data', None)
        assert '`data` must be a Mapping' == str(excinfo.value)

    def test_default_tags_invalid(self):
        event = Event()

        with pytest.raises(ValueError) as excinfo:
            event.default('tags', None)
        assert '`tags` must be an Iterable of strings' == str(excinfo.value)

    def test_default_tags_invalid_sequence(self):
        event = Event()

        with pytest.raises(ValueError) as excinfo:
            event.default('tags', 'badtag')
        assert '`tags` must be an Iterable of strings' == str(excinfo.value)

    def test_defaults(self):
        event = Event()
        event_defaults = dict(system='test_defaults_system')

        assert event.system is ''
        assert event_defaults['system'] is 'test_defaults_system'
        event.defaults(event_defaults)

        assert event.system is 'test_defaults_system'
        assert event_defaults['system'] is 'test_defaults_system'

    def test_defaults_kwargs(self):
        event = Event()
        assert event.system == ''

        event.defaults(system='test_defaults_kwargs')
        assert event.system == 'test_defaults_kwargs'

    def test_defaults_event(self):
        event = Event()
        event_defaults = Event(system='test_defaults_system')

        assert event.system == ''
        assert event_defaults.system == 'test_defaults_system'
        event.defaults(event_defaults)

        assert event.system == 'test_defaults_system'
        assert event_defaults.system == 'test_defaults_system'

    def test_defaults_non_mappings(self):
        event = Event()
        event_expect = dict(**event)

        for bad_value in [False, '', 'bad_value', [], ['bad_value']]:
            with pytest.raises(ValueError) as excinfo:
                event.defaults(bad_value)
            assert '`defaults` must be an Event or Mapping' == str(excinfo.value)
        assert event == event_expect

    def test_defaults_no_override(self):
        event = Event(system='existing_system')
        event_defaults = dict(system='test_defaults_no_override_system')

        assert event.system == 'existing_system'
        assert event_defaults['system'] == 'test_defaults_no_override_system'
        event.defaults(event_defaults)

        assert event.system is 'existing_system'
        assert event_defaults['system'] == 'test_defaults_no_override_system'

    def test_defaults_missing_with_no_override(self):
        event = Event(system='existing_system')
        event_defaults = dict(system='test_system', component='test_component')

        assert event.system is 'existing_system'
        assert event.component == ''
        event.defaults(event_defaults)
        assert event.system is 'existing_system'
        assert event.component == 'test_component'

    def test_defaults_missing_with_no_override_event(self):
        event = Event(system='existing_system')
        event_defaults = Event(system='test_system', component='test_component')

        assert event.system == 'existing_system'
        assert event.component == ''
        assert event_defaults.system == 'test_system'
        assert event_defaults.component == 'test_component'
        event.defaults(event_defaults)

        assert event.system == 'existing_system'
        assert event.component == 'test_component'
        assert event_defaults.system == 'test_system'
        assert event_defaults.component == 'test_component'


@pytest.mark.event_validation
class TestEventValidation(TestCase):
    def test_validation(self):
        event = teventf()
        event.validate()
        assert event.valid is True

    def test_validation_string_fields(self):

        # EventProperty should be set fine
        for key, value in expect_string_keys_dict.iteritems():
            event = Event()
            setattr(event, key, value)
            _assert_event(event, dict(field=value))

        # EventProperty must be strings
        for key in expect_string_keys:
            with pytest.raises(ValueError) as excinfo:
                event = Event()
                setattr(event, key, False)
            assert '`{}` must be a string'.format(key) == str(excinfo.value)

        # EventProperty must not be empty
        for key in ['tid', 'system', 'component', 'operation']:
            with pytest.raises(ValueError) as excinfo:
                event = Event()
                setattr(event, key, '')
                o = getattr(Event, key)
                o.fval(event, getattr(event, key), final=True)
            assert '`{}` must not be empty'.format(key) == str(excinfo.value)

    def test_validation_all_deny_empty_fields(self):
        for key in keys_allowed:
            if key in ('fields', 'data', 'tags', 'time', 'replay', 'operation'):
                continue
            event = teventf()
            event[key] = ''

            assert event.valid is False, '{} was valid while empty'.format(key)
            with pytest.raises(ValueError) as excinfo:
                event.validate()
            assert '`{0}` must not be empty'.format(key) == str(excinfo.value)
            assert event.valid is False

    def test_validation_fields_set_empty(self):
        event = teventf()
        event.fields = {}
        event.validate()

    def test_validation_data_set_empty(self):
        event = teventf()
        event.data = {}
        event.validate()

    def test_validation_tags_set_empty(self):
        event = teventf()
        event.tags = set()
        event.validate()

    def test_validation_replay_non_string(self):
        event = teventf()
        event['replay'] = False

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`replay` must be a string' == str(excinfo.value)
        assert event.valid is False

    def test_validation_data_non_dict(self):
        event = teventf()
        event['data'] = False

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`data` must be a Mapping' == str(excinfo.value)
        assert event.valid is False

    def test_validation_fields_non_dict(self):
        event = teventf()
        event['fields'] = False

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`fields` must be a Mapping' == str(excinfo.value)
        assert event.valid is False

    def test_validation_tags_non_set(self):
        event = teventf()
        event['tags'] = False

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`tags` must be an Iterable of strings' == str(excinfo.value)

        event = tevent()
        event.tags.add('')
        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`tags` must not contain empty strings' == str(excinfo.value)
        assert event.valid is False

    def test_validation_tags_contains_non_str(self):
        event = teventf()
        event['tags'] = set([False])

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`tags` must only contain strings' == str(excinfo.value)
        assert event.valid is False

    def test_validation_tags_contains_empty_str(self):
        event = teventf()
        event['tags'] = set(['foo', ''])

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`tags` must not contain empty strings' == str(excinfo.value)
        assert event.valid is False

    def test_validation_system_deny_non_str(self):
        event = teventf()
        event['system'] = dict(foo='bar')

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`system` must be a string' == str(excinfo.value)
        assert event.valid is False

    def test_validation_deny_empty_str_sys(self):
        event = teventf()
        event['system'] = ''

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`system` must not be empty' == str(excinfo.value)
        assert event.valid is False

    def test_validation_deny_invalid_time(self):
        event = teventf()
        event['time'] = []

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`time` must be an instance of datetime' == str(excinfo.value)
        assert event.valid is False

    def test_validation_neg(self):
        event = tevent(system='mysystem', name='myevent')
        event.component = ''

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`component` must not be empty' == str(excinfo.value)
        assert event.valid is False

    def test_validation_neg_missing(self):
        event = Event(system='mysystem')
        for k in event.keys_required:
            del event[k]
        event.system = 'mysystem'

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`Event` was missing required keys: name, operation, component, tid, time' == str(excinfo.value)

    def test_validation_neg_extra(self):
        event = teventf()
        event['extra_key'] = 'test_validation_neg_extra'

        with pytest.raises(ValueError) as excinfo:
            event.validate()
        assert '`Event` had extraneous keys: extra_key' == str(excinfo.value)


@pytest.mark.event
class TestEventJsonEncoder(TestCase):
    def test_basic(self):
        event_encoder = EventJsonEncoder()
        event, expect = teventf_expect()
        got = json.loads(event_encoder.encode(event.to_event))
        expect = json.loads(json.dumps(expect, cls=EventJsonEncoder))

        for key in expect:
            assert got[key] == expect[key]

    def test_default_datetime(self):
        event_encoder = EventJsonEncoder()
        event_encoder.default(datetime.now())

    def test_default_other(self, logs):
        # Encode something that will throw
        event_encoder = EventJsonEncoder()
        encoded = event_encoder.default(lambda x: x * x)
        assert isinstance(encoded, dict)
        assert 'error' in encoded
        assert 'message' in encoded
        assert len(logs)

        exc_record = logs.pop()
        assert exc_record.getMessage().startswith('<function <lambda>')

        expect_starts_with = 'EventJsonEncoder.default - unable to decode obj of type'
        dbg_record = logs.pop()
        assert dbg_record.getMessage().startswith(expect_starts_with)
        assert encoded['message'].startswith(expect_starts_with)

    def test_default_unknown(self, logs):
        # Encode something that will throw
        event_encoder = EventJsonEncoder()
        encoded = event_encoder.default(None)
        assert isinstance(encoded, dict)
        assert 'error' in encoded
        assert 'message' in encoded
        assert len(logs)

        exc_record = logs.pop()
        assert exc_record.getMessage() == 'None is not JSON serializable'

        expect_starts_with = 'EventJsonEncoder.default - unable to decode obj of type'
        dbg_record = logs.pop()
        assert dbg_record.getMessage().startswith(expect_starts_with)
        assert encoded['message'].startswith(expect_starts_with)


@pytest.mark.event_stack
class TestEventStack(TestCase):

    def test__or__(self):
        event_stack = EventStack([Event('a')]) | Event('b')
        assert event_stack.name == 'a.b'

        event_stack = EventStack([Event('a')]) | EventStack([Event('b')])
        assert event_stack.name == 'a.b'

        event_stack = EventStack([Event('a')]) | EventStack([Event('b')]) | Event('c')
        assert event_stack.name == 'a.b.c'

        event_stack = EventStack([Event(
                'a', system='system_a', component='component_a')]) \
            | EventStack([Event('b', system='system_b')]) \
            | Event('c')
        assert event_stack.name == 'a.b.c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

        event_stack = EventStack([Event(
                'a', system='system_a', component='component_a')]) \
            | Event('b', system='system_b') \
            | Event('c')
        assert event_stack.name == 'a.b.c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

        event_stack = Event(
                'a', system='system_a', component='component_a') \
            | EventStack([Event('b', system='system_b')]) \
            | Event('c')
        assert event_stack.name == 'a.b.c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

    def test__add__(self):
        event_stack = EventStack([Event('a')]) + Event('b')
        assert event_stack.name == 'b'

        event_stack = EventStack([Event('a')]) + EventStack([Event('b')])
        assert event_stack.name == 'b'

        event_stack = EventStack([Event('a')]) + EventStack([Event('b')]) + Event('c')
        assert event_stack.name == 'c'

        event_stack = EventStack([Event(
                'a', system='system_a', component='component_a')]) \
            + EventStack([Event('b', system='system_b')]) \
            + Event('c')
        assert event_stack.name == 'c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

        event_stack = EventStack([Event(
                'a', system='system_a', component='component_a')]) \
            + Event('b', system='system_b') \
            + Event('c')
        assert event_stack.name == 'c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

        event_stack = Event(
                'a', system='system_a', component='component_a') \
            + EventStack([Event('b', system='system_b')]) \
            + Event('c')
        assert event_stack.name == 'c'
        assert event_stack.system == 'system_b'
        assert event_stack.component == 'component_a'

    def test__init__(self):
        event_stack = EventStack()
        assert isinstance(event_stack, EventStack)
        assert isinstance(event_stack, list)
        assert event_stack.bot is None
        assert event_stack.top is None

    def test__getitem__(self):
        expect = Event(replay='foo')
        event_stack = EventStack()
        with pytest.raises(KeyError):
            assert event_stack['replay'] == 'foo'

        event_stack.append(expect)
        assert event_stack['replay'] == 'foo'

    def test__getitem___int_index(self):
        expect = Event()
        event_stack = EventStack()
        with pytest.raises(IndexError):
            assert event_stack[0] == 'foo'

        event_stack.append(expect)
        assert event_stack[0] == expect

    def test___contains__(self):
        expect = Event(replay='foo')
        event_stack = EventStack()
        assert not ('replay' in event_stack)

        event_stack.append(expect)
        assert 'replay' in event_stack

    def test__str__(self):
        event_stack = EventStack()
        assert str(event_stack) == 'EventStack()'
        event_stack = tevent_stack()
        event_stack_str = str(event_stack)
        event_stack_str_lines = event_stack_str.strip().split('\n')
        expect_str_lines = tevent_stack._as_str.strip().split('\n')
        for (index, line) in enumerate(expect_str_lines):
            a = event_stack_str_lines[index]
            b = expect_str_lines[index]
            assert a.startswith(b), '{}.startswith({}) != True'.format(a, b)

    def test_to_event(self):
        event = tevent()
        event_stack = EventStack([event])
        event_dict = event.to_event
        event_stack_dict = event_stack.to_event

        for k, v in event_dict.iteritems():
            assert event_dict[k] == event_stack_dict[k]

    def test_to_event_operation_as_component_override_top(self):
        event_stack = EventStack()
        event_stack.append(Event(operation='testing'))
        event_stack.append(Event(component='event_stack'))

        assert event_stack.to_event.system == ''
        assert event_stack.to_event.component == 'event_stack'
        assert event_stack.to_event.operation == 'testing'
        assert event_stack.to_event.name == ''

    def test_to_event_operation_as_component_override_bot(self):
        event_stack = EventStack()
        event_stack.append(Event(component='event_stack'))
        event_stack.append(Event(operation='testing'))

        assert event_stack.to_event.system == ''
        assert event_stack.to_event.component == 'event_stack'
        assert event_stack.to_event.operation == 'testing'
        assert event_stack.to_event.name == ''

    def test_slice(self):
        event_stack = tevent_stack()

        for x in range(len(event_stack)):
            sliced = event_stack[:x]
            if x == 0:
                assert len(sliced) == 0
            if x > 0:
                assert sliced['system'] == 'test.pyemit'
                assert sliced['tid'] == 'test.event_stack_tid'
            if x > 1:
                assert sliced['component'] == 'event_stack'
            if x > 2:
                assert sliced['operation'] == 'testing'
            if x == 4:
                assert sliced['name'] == 'base'
            if x == 5:
                assert sliced['name'] == 'base.one'
            if x == 6:
                assert sliced['name'] == 'base.one.two'
            if x == 7:
                assert sliced['name'] == 'base.one.two.three'

    def test_to_event_stutter(self):
        event_stack = tevent_stack()
        event_stack.append(Event(name='one'))
        assert event_stack.to_event.name == 'base.one.two.three.one'
        event_stack.append(Event(name='one'))
        assert event_stack.to_event.name == 'base.one.two.three.one'
        event_stack.append(Event(name='two'))
        assert event_stack.to_event.name == 'base.one.two.three.one.two'
        event_stack.append(Event(name='one'))
        assert event_stack.to_event.name == 'base.one.two.three.one.two.one'
        event_stack.append(Event(name='one'))
        assert event_stack.to_event.name == 'base.one.two.three.one.two.one'

    def test_to_event_stutter_deep(self):
        base = 'base.one.two.three'
        events = ['one', 'two', 'three', 'four']
        event_stack = tevent_stack()

        for index, event in enumerate(events):
            expect = '{0}.{1}'.format(base, '.'.join(events[:index+1]))
            for i in range(10):
                event_stack.append(Event(name=event))
                assert event_stack.to_event.name == expect
            assert event_stack.to_event.name == expect
        assert event_stack.to_event.name == 'base.one.two.three.one.two.three.four'

    def test_bot(self):
        event_stack = EventStack()
        event = Event()
        event_stack.append(event)
        assert event_stack.bot == event
        assert event_stack.bot == event_stack.top
        event_stack.append(Event())
        assert event_stack.bot == event
        assert event_stack.bot != event_stack.top
        event_stack = tevent_stack()
        assert event_stack.bot.system == 'test.pyemit'
        assert event_stack.bot.component == ''
        assert event_stack.bot.operation == ''
        assert event_stack.bot.name == ''

    def test_top(self):
        event_stack = EventStack()
        event = Event()
        event_stack.append(event)
        assert event_stack.top == event
        assert event_stack.bot == event_stack.top
        event_top = Event()
        event_stack.append(event_top)
        assert event_stack.top == event_top
        assert event_stack.bot == event
        assert event_stack.bot != event_stack.top
        event_stack = tevent_stack()
        assert event_stack.top.system == ''
        assert event_stack.top.component == ''
        assert event_stack.top.operation == ''
        assert event_stack.top.name == 'three'

    def test__enter__(self):
        event_stack = tevent_stack()
        expect_name = 'test_stack_callable'
        expect = 'base.one.two.three.{}'.format(expect_name)
        event = event_stack(name=expect_name)

        # Event access is normal
        assert event.name == expect_name

        # Access from stack should be top msg
        assert event_stack.name == event_stack.top.name

        # If not within context it should not polute event stack
        assert event_stack.to_event.name == 'base.one.two.three'

        with event as event_stack_obj:

            # Return value should be contextual event
            assert event == event_stack_obj
            assert event.name == event_stack_obj.name

            # Should be top obj
            assert event_stack.top == event_stack_obj
            assert event_stack.name == event_stack_obj.name

            # Access within event_stack should be roll up
            assert event_stack.to_event.name == expect

    def test__enter__nested(self):
        event_stack = EventStack()

        with event_stack(name='1') as event:
            assert event.name == '1'
            assert event_stack.name == '1'
            assert event_stack.to_event.name == '1'

            with event_stack(name='2') as event:
                assert event.name == '2'
                assert event_stack.name == '2'
                assert event_stack.to_event.name == '1.2'
                with event_stack(name='3') as event:
                    assert event.name == '3'
                    assert event_stack.name == '3'
                    assert event_stack.to_event.name == '1.2.3'

    def test__enter__no_call(self):
        event_stack = EventStack()

        # Entering with no __call__ doesn't need an EventContext bridge
        with event_stack as event:
            event.name = '1'
            assert event.name == '1'
            assert event_stack.name == '1'
            assert event_stack.to_event.name == '1'

            with event_stack as event:
                event.name = '2'
                assert event.name == '2'
                assert event_stack.name == '2'
                assert event_stack.to_event.name == '1.2'
                with event_stack as event:
                    event.name = '3'
                    assert event.name == '3'
                    assert event_stack.name == '3'
                    assert event_stack.to_event.name == '1.2.3'
