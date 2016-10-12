import pytest
import random
import time
import itertools
from multiprocessing.pool import ThreadPool
from datetime import timedelta
from emit.globals import Proxy, GuardedProxy, Config, ConfigDescriptor, LoggerProxy, log, conf
from ..helpers import TestCase


@pytest.mark.conf
@pytest.mark.log
@pytest.mark.proxy
class TestProxy(TestCase):
    def test_proxy_init_no_arg(self):
        p = Proxy()
        assert p._resolve() is None

    def test_proxy_init_with_arg(self):
        p_obj = dict()
        p = Proxy(p_obj)
        assert p._resolve() == p_obj

    def test_proxy_dir(self):
        p_obj = dict()
        p = Proxy(p_obj)
        assert p._resolve() == p_obj

        # Dir p should contain at least the objects of dir
        assert set(dir(p)) >= set(dir(p_obj))

    def test_proxy_repr(self):
        p_obj = dict(foo='bar')
        p = Proxy(p_obj)
        assert p._resolve() == p_obj
        assert "{'foo': 'bar'}" in repr(p)

    def test_proxy_str(self):
        p_obj = dict(foo='bar')
        p = Proxy(p_obj)
        assert p._resolve() == p_obj
        assert "{'foo': 'bar'}" in str(p)

    def test_proxy_contains(self):
        p_obj = dict(key='foo')
        p = Proxy(p_obj)
        assert p._resolve() == p_obj
        assert 'key' in p

    def test_proxy_len(self):
        p_obj = dict(key='foo')
        p = Proxy(p_obj)
        assert p._resolve() == p_obj
        assert len(p) == 1

    def test_proxy_iter(self):
        p_iters = 0
        p_obj = dict(key='foo')
        p = Proxy(p_obj)
        for x in p:
            p_iters += 1
            assert x == 'key'
        assert len(p) == 1
        assert p_iters == 1

    def test_proxy_items(self):
        p_obj = dict()
        p = Proxy(p_obj)
        assert p._resolve() == p_obj
        assert not ('key' in p)

        p['key'] = 'foo'
        assert 'key' in p
        assert p._resolve()['key'] == 'foo'
        assert p['key'] == 'foo'

        del p['key']
        assert not ('key' in p)

    def test_proxy_attr(self):
        p = Proxy()
        p._set_resolver_obj(self)
        assert p._resolve() == self
        assert hasattr(p, 'foo') == False
        p.foo = 'foo'
        assert hasattr(p, 'foo')
        assert p.foo == 'foo'
        del p.foo
        assert hasattr(p, 'foo') == False

    def test_proxy_call(self):
        class _test_proxy(object):
            calls = 0

            def __call__(self, *args, **kwargs):
                _test_proxy.calls += 1
                return _test_proxy.calls

        p = Proxy(_test_proxy())
        p_arg_obj = object()
        assert p(p_arg_obj) == 1
        assert p._resolve()() == 2

    def test_proxy_callable(self):
        p_stack = []
        p = Proxy()
        assert p._resolve() is None

        def p_func(arg):
            p_stack.append(arg)
            return arg

        def p_resolve():
            return p_func

        p._set_resolver(p_resolve)
        assert p._resolve() == p_func

        p_arg_obj = object()
        assert p(p_arg_obj) == p_arg_obj
        assert p_stack.pop() == p_arg_obj

    def test_proxy_callable_obj(self):
        p_stack = []
        p = Proxy()
        assert p._resolve() is None

        def p_func(arg):
            p_stack.append(arg)
            return arg

        p._set_resolver_obj(p_func)
        assert p._resolve() == p_func

        p_arg_obj = object()
        assert p(p_arg_obj) == p_arg_obj
        assert p_stack.pop() == p_arg_obj

    def test_proxy_setattr(self):
        class TT(object):
            foo = 'foo'
        tt = Proxy(TT())
        tt.foo = 'bar'
        assert tt.__getattr__('foo') == 'bar'

    def test_proxy_slots_obj_access(self):
        class T(Proxy):
            __slots__ = ('__resolver', 'test_proxy_slots_obj_access')
            test_proxy_slots_obj_access = '...'

        t = T(object())
        assert t.test_proxy_slots_obj_access == '...'
        assert t.__getattr__('test_proxy_slots_obj_access') == '...'

        with pytest.raises(AttributeError):
            t.__setattr__('test_proxy_slots_obj_access', 'bbb')
        assert t.test_proxy_slots_obj_access == '...'
        assert t.__getattr__('test_proxy_slots_obj_access') == '...'


class TestGlobals(TestCase):

    @pytest.mark.conf
    @pytest.mark.config
    def test_global_conf(self):
        assert isinstance(conf, Proxy)
        assert isinstance(conf._resolve(), Config)

    @pytest.mark.log
    def test_global_log(self):
        assert isinstance(log, LoggerProxy)

    @pytest.mark.log
    def test_global_log_call(self, logs):
        msg = 'TestGlobals.test_global_log_call - test'
        assert callable(log)
        log(msg)
        last_record = logs.pop()
        assert msg == last_record.getMessage()

    @pytest.mark.log
    def test_global_logger_call(self, logs):
        msg = 'TestGlobals.test_global_logger_call - test'
        assert callable(log.logger)
        log.logger(msg)
        last_record = logs.pop()
        assert msg == last_record.getMessage()


@pytest.mark.conf
@pytest.mark.config
@pytest.mark.config_value
class TestConfigDescriptor(TestCase):
    def test_init(self):
        with pytest.raises(AttributeError) as excinfo:
            assert conf.TestConfigDescriptor_test_init is None
        assert '\'Config\' object has no attribute \'TestConfigDescriptor_test_init\'' \
            == str(excinfo.value)
        ConfigDescriptor('TestConfigDescriptor_test_init')
        with pytest.raises(AttributeError) as excinfo:
            assert conf.TestConfigDescriptor_test_init is None
        assert '\'Config\' object has no attribute \'TestConfigDescriptor_test_init\'' \
            == str(excinfo.value)

    def test_init_default(self):
        with pytest.raises(AttributeError) as excinfo:
            assert conf.TestConfigDescriptor_test_init is None
        assert '\'Config\' object has no attribute \'TestConfigDescriptor_test_init\'' \
            == str(excinfo.value)
        ConfigDescriptor('TestConfigDescriptor_test_init', False)
        assert conf.TestConfigDescriptor_test_init is False

    def test_init_as_prop(self):
        class T(object):
            TestConfigDescriptor_test_init_as_descriptor = \
                ConfigDescriptor('TestConfigDescriptor_test_init_as_descriptor')

        t_obj = T()
        assert T.TestConfigDescriptor_test_init_as_descriptor is None
        assert t_obj.TestConfigDescriptor_test_init_as_descriptor is None

        with pytest.raises(AttributeError) as excinfo:
            assert conf.TestConfigDescriptor_test_init_as_descriptor is None
        assert '\'Config\' object has no attribute \'TestConfigDescriptor_test_init_as_descriptor\'' \
            == str(excinfo.value)

        conf.TestConfigDescriptor_test_init_as_descriptor = 'set_G'
        assert conf.TestConfigDescriptor_test_init_as_descriptor == 'set_G'
        assert T.TestConfigDescriptor_test_init_as_descriptor == 'set_G'
        assert t_obj.TestConfigDescriptor_test_init_as_descriptor == 'set_G'

        t_obj.TestConfigDescriptor_test_init_as_descriptor = 'set_T_OBJ'
        assert conf.TestConfigDescriptor_test_init_as_descriptor == 'set_G'
        assert T.TestConfigDescriptor_test_init_as_descriptor == 'set_G'
        assert t_obj.TestConfigDescriptor_test_init_as_descriptor == 'set_T_OBJ'

    def test_init_as_descriptor_pass_through(self):
        class A(object):
            TestConfigDescriptor_test_init_as_descriptor_pass_through = \
                ConfigDescriptor(
                    'TestConfigDescriptor_test_init_as_descriptor_pass_through',
                    'set_A')

        a_obj = A()
        assert A.TestConfigDescriptor_test_init_as_descriptor_pass_through == 'set_A'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through == 'set_A'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_pass_through == 'set_A'

        conf.TestConfigDescriptor_test_init_as_descriptor_pass_through = \
            'set_A_CONF'
        assert A.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'

        a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through = 'set_A_obj'
        assert A.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_obj'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'

        del a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through
        assert A.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_pass_through == \
            'set_A_CONF'

    def test_init_as_descriptor_default(self):
        class A(object):
            TestConfigDescriptor_test_init_as_descriptor_default_a = \
                ConfigDescriptor(
                    'TestConfigDescriptor_test_init_as_descriptor_default_a',
                    'set_A')

            def foo(self):
                self.TestConfigDescriptor_test_init_as_descriptor_default_a = 'set_A_foo'

        class B(object):
            TestConfigDescriptor_test_init_as_descriptor_default_b = 'set_B'

            def foo(self):
                self.TestConfigDescriptor_test_init_as_descriptor_default_b = 'set_B_foo'

        a_obj = A()
        b_obj = B()
        assert A.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'
        assert B.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'
        assert b_obj.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'

        a_obj.TestConfigDescriptor_test_init_as_descriptor_default_a = 'set_A_a_obj'
        b_obj.TestConfigDescriptor_test_init_as_descriptor_default_b = 'set_B_a_obj'
        assert A.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'
        assert B.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A_a_obj'
        assert b_obj.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B_a_obj'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'

        a_obj.foo()
        b_obj.foo()
        assert A.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'
        assert B.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A_foo'
        assert b_obj.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B_foo'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'

        A.TestConfigDescriptor_test_init_as_descriptor_default_a = 'set_A_C'
        B.TestConfigDescriptor_test_init_as_descriptor_default_b = 'set_B_C'
        assert A.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A_C'
        assert B.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B_C'
        assert a_obj.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A_foo'
        assert b_obj.TestConfigDescriptor_test_init_as_descriptor_default_b == 'set_B_foo'
        assert conf.TestConfigDescriptor_test_init_as_descriptor_default_a == 'set_A'


@pytest.mark.proxy
@pytest.mark.guarded_proxy
@pytest.mark.config
class TestGuardedProxy(TestCase):

    def test_init(self):
        p = GuardedProxy()
        with p:
            assert p._resolve() is None

    def test_init_with_arg(self):
        p_obj = dict()
        p = GuardedProxy(p_obj)
        with p:
            assert p._resolve() == p_obj

    def test_out_of_context(self):
        p_obj = dict()
        p = GuardedProxy(p_obj)

        with pytest.raises(RuntimeError):
            assert p._resolve() == p_obj

    @pytest.mark.slow
    def test_thread_safe(self):
        def ctx_worker(o):
            for i in range(iterations):
                with o as ctx:
                    for x in range(calls):
                        ctx(zip(enters, exits))

        def worker(o):
            for i in range(iterations):
                for x in range(calls):
                    o(zip(enters, exits))

        # For our CallableStack to append on enter/exit to assert thread safety
        def work(f, ctx):
            enter, exit = ctx.pop(0)
            try:
                items.append(enter)
                sleep_random()
                return f(ctx)
            finally:
                items.append(exit)

        def sleep_random():
            # Contention is created through random sleeps, shouldn't take anymore
            # time than this to trigger a race condition @ 100%
            time.sleep(timedelta(0, .0001).total_seconds() * random.random())

        def test_ctx(ctx_func, ctx):
            pool = ThreadPool(processes=concurrency)
            pool.imap_unordered(ctx_func, list(ctx for i in range(concurrency)))
            pool.close()
            pool.join()

            expect = 2 * depth
            expect *= calls * iterations * concurrency
            assert len(items) == expect

            expect_iter = itertools.cycle(range(depth * 2))
            for item in items:
                assert item == next(expect_iter), 'race condition'

        # how much / long to run
        concurrency = 12
        iterations = 40
        calls = 3
        depth = 10

        # each work() func leaves a call trail counter at enter and exit
        enters = list(range(depth))
        exits = list(reversed(range(depth, depth * 2)))
        cs = CallableStack(*list(work for i in range(depth)))
        cs_guarded = GuardedProxy(cs)

        items = []
        test_ctx(ctx_worker, cs_guarded)

        items = []
        with pytest.raises(AssertionError) as excinfo:
            assert test_ctx(worker, cs)
        assert str(excinfo.value).startswith('race condition')


class CallableStack(list):
    """
    Unit testing utility provides a simple pattern for reentrant ordered call
    recursion through a stack of `funcs`. This is useful for setting up groups
    of context that may expire after a period of time and need support from
    parent calls to rebuild. I.E.:

    ctx = Context()
    cs = CallableStack(initialize, prepare, process, cleanup)
    cs(ctx):
        initialize(cs, ctx):
            additional_ctx = ...
            cs(ctx, additional_ctx) ->
                additional_ctx.transform()
                prepare(cs, ctx, additional_ctx):
                    cs() ->
                        process(cs):
                            cs() ->
                                cleanup()
                            ...
                # prepare exits
            # initialize exits
    """
    def __init__(self, *callables):
        super(CallableStack, self).__init__(callables)

    def __call__(self, *args):
        return self[0](CallableStack(*self[1:]), *args) if len(self) else None
