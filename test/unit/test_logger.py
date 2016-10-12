import logging
from emit.globals import log, LoggerProxy
from emit.logger import configure_logger
from ..helpers import TestCase


class TestLogging(TestCase):

    def test_configure(self):
        assert configure_logger('emit') is None
        assert isinstance(log, LoggerProxy)
        assert isinstance(log._resolve(), logging.getLoggerClass())

    def test_configure_level(self):
        configure_logger('emit', level=logging.DEBUG)
        assert isinstance(log, LoggerProxy)
        assert log.getEffectiveLevel() == logging.DEBUG


class TestLogProxy(TestCase):

    def test_logger(self):
        assert isinstance(log._resolve(), logging.getLoggerClass())

    def test_call(self, logs):
        msg = 'TestLogProxy.test_call - test'
        assert callable(log)
        log(msg)
        last_record = logs.pop()
        assert msg == last_record.getMessage()

    def test_getattr(self):
        assert callable(log.debug)
