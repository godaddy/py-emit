from __future__ import absolute_import
import sys
from logging import (
    StreamHandler, Formatter, captureWarnings,
    getLogger, getLoggerClass, setLoggerClass)


Loggers = ['Logger']


__all__ = Loggers + ['Loggers', 'configure_logger']


class Logger(getLoggerClass()):
    def __call__(self, *args, **kwargs):
        return self.debug(*args, **kwargs)


def configure_logger(name, level=None):
    captureWarnings(True)
    setLoggerClass(Logger)
    logger = getLogger(name)

    if level:
        logger.setLevel(level)
    if len(logger.handlers):
        return
    del logger.handlers[:]

    handler = StreamHandler(sys.stdout)
    handler.setFormatter(Formatter('[%(asctime)s %(threadName)s] %(levelname)s: %(message)s'))
    logger.addHandler(handler)


# Logger adds streamhandler if no logger exists, otherwise leaves default
configure_logger('emit')
