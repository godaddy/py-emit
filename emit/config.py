import os
import importlib
from datetime import timedelta


def _bool(k, v):
    return v.lower() in ['true', '1', 't', 'y', 'yes']


def _str(k, v):
    return str(v)


def _int(k, v):
    return int(v)


def _timedelta(k, v):
    return timedelta(seconds=float(v))


def _class(k, v):
    lookups = dict(
        adapter_class=importlib.import_module('emit.adapters'),
        event_stack_class=importlib.import_module('emit.event'),
        event_class=importlib.import_module('emit.event'),
        logger_class=importlib.import_module('emit.logger'),
        queue_class=importlib.import_module('emit.queue'),
        transport_class=importlib.import_module('emit.transports'),
        worker_class=importlib.import_module('emit.transports'))
    return getattr(lookups[k], v)


_env_defaults = dict(

    # Default classes
    adapter_class=('Adapter', _class),
    event_stack_class=('EventStack', _class),
    event_class=('Event', _class),
    logger_class=('Logger', _class),
    queue_class=('Queue', _class),
    transport_class=('Transport', _class),
    worker_class=('ThreadedWorker', _class),

    # Default adapter url will be used by Adapter.__call__ when it has len()
    adapter_url=('', _str),

    # Max size of queue before put/get blocks. -1 Means queue forever.
    max_queue_size=('-1', _int),

    # Max time an adapter may spend flushing it's buffers.
    max_flush_time=('10', _timedelta),  # timedelta(seconds=10)

    # The max time a transport worker may spend trying to clear the queue before
    # dropping all messages permanently.
    max_stopping_time=('30', _timedelta),  # timedelta(seconds=30)

    # Worker: max time it may try to process the queue
    # ThreadedWorker: max time it spends between mutex acquires, unimportant as long
    #   as it is greater than zero.
    max_work_time=('.5', _timedelta),  # timedelta(seconds=.5)

    # Debug mode
    debug=('false', _bool),

    # Pretty print user facing data
    pretty=('false', _bool))


class Config(dict):
    ENV_PREFIX = 'EMIT_'

    @classmethod
    def from_env(cls):
        return cls().load_env()

    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def env_name(self, name):
        return '{}{}'.format(self.ENV_PREFIX, name.upper())

    def env_value(self, name):
        key = self.env_name(name)
        if key in os.environ:
            return os.environ[key]
        return ''

    def load_env(self):

        # For *_class -> (Default, Factory)
        for k, v in _env_defaults.iteritems():

            # Look for key i.e. EMIT_DEBUG in env
            value = self.env_value(k)
            if value == '':

                # If it doesn't exist load the default, first tuple value
                value = v[0]
            self[k] = v[1](k, value)
        return self
