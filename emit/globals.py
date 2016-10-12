import threading
from functools import partial
from .config import Config
from .logger import getLogger


class Proxy(object):
    __slots__ = ('__resolver')

    def __init__(self, resolver_obj=None, resolver=None):
        if resolver_obj is not None:
            def resolver_func():
                return resolver_obj
            resolver = resolver_func
        object.__setattr__(self, '_Proxy__resolver', resolver)

    def _resolve(self):
        resolver = object.__getattribute__(self, '_Proxy__resolver')

        if resolver is None:
            return None
        return resolver()

    def _set_resolver(self, resolver):
        return object.__setattr__(self, '_Proxy__resolver', resolver)

    def _set_resolver_obj(self, obj):
        def resolver_func():
            return obj
        return object.__setattr__(self, '_Proxy__resolver', resolver_func)

    def __setitem__(self, key, value):
        self._resolve()[key] = value

    def __delitem__(self, key):
        del self._resolve()[key]

    def __getattr__(self, name):
        if name in self.__class__.__slots__:
            return object.__getattribute__(self, name)
        if name == '__members__':
            return dir(self._resolve())
        return getattr(self._resolve(), name)

    def __setattr__(self, name, value):
        if name in self.__slots__:
            object.__setattr__(self, name, value)
        else:
            setattr(self._resolve(), name, value)

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __len__(self):
        return len(self._resolve())

    def __getitem__(self, index):
        return self._resolve()[index]

    def __iter__(self):
        return iter(self._resolve())

    def __contains__(self, index):
        return index in self._resolve()

    def __delattr__(self, attr):
        return delattr(self._resolve(), attr)


class GuardedProxy(Proxy):
    __slots__ = ('__resolver', '__lock', '__locked')

    def __init__(self, resolver_obj=None, resolver=None):
        super(GuardedProxy, self).__init__(resolver_obj, resolver)
        object.__setattr__(self, '_GuardedProxy__lock', threading.RLock())
        object.__setattr__(self, '_GuardedProxy__locked', False)

    def __enter__(self):
        object.__getattribute__(self, '_GuardedProxy__lock').acquire()
        object.__setattr__(self, '_GuardedProxy__locked', True)
        return super(GuardedProxy, self)._resolve()

    def __exit__(self, exc_type, exc_value, tb):
        object.__getattribute__(self, '_GuardedProxy__lock').release()
        object.__setattr__(self, '_GuardedProxy__locked', False)

    def _resolve(self):
        if not object.__getattribute__(self, '_GuardedProxy__locked'):
            raise RuntimeError(
                'attempted access to guarded object outside context manager,'
                ' try `with obj:` instead')
        return super(GuardedProxy, self)._resolve()


class LoggerProxy(Proxy):
    __slots__ = ('__resolver', 'logger')

    @property
    def logger(self):
        return self._resolve()


class ConfigDescriptor(object):
    """Just takes a `name` and sets the default value as `default` in the `conf`
    global. When retrieved it will first check the callers dict, then `conf`
    global."""
    def __init__(self, name, default=None):
        self.__name__ = name
        self.__default__ = default
        if default is not None:
            conf[self.__name__] = default

    def __get__(self, obj, obj_type):
        if self.__name__ in conf.__dict__:
            return conf.__dict__[self.__name__]
        return self.__default__


conf = Proxy(Config())
log = LoggerProxy(resolver=partial(getLogger, 'emit'))
