from .globals import conf, Config, ConfigDescriptor
from .event import Event
from .adapters import Adapter
from .transports import Transport, Worker, ThreadedWorker
from .emitters import Emitter
from . import (
    adapters, decorators, emitters, logger,
    event, queue, transports, utils)


__all__ = [

    # Modules
    'adapters', 'decorators', 'logger', 'emitters',
    'event', 'queue', 'transports', 'utils',

    # Top level classes
    'Adapter', 'Emitter', 'Event', 'Transport', 'Worker', 'ThreadedWorker',

    # Conf
    'conf', 'Config', 'ConfigDescriptor']


# Loads the configurations default values. Checks the environment first, then
# uses the default in config.py, i.e. the process for conf.adapter_class:
#   1) Check for 'EMIT_ADAPTER_CLASS' in os.environ
#   2) If it exists, look for that class name in the adapters module. For
#        example export EMIT_ADAPTER_CLASS=MultiAdapter will set it to a python
#        class instance of adapters.MultiAdapter.
#   3) If nothing exists in the environment, use the default. Which is usually
#      the bare class name of the module. I.e.:
#         adapter_class -> adapters.Adapter
#         transport_class -> transports.Transport
#         queue_class -> queue.Queue
conf.load_env()

# Default emitter
emit = Emitter()
