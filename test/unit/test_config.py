import pytest
import os
from emit.config import Config
from ..helpers import TestCase


def eput(key, val, prefix=Config.ENV_PREFIX):
    _key = '{}{}'.format(prefix, key.upper())
    os.environ[_key] = val


def eget(key, prefix=Config.ENV_PREFIX):
    _key = '{}{}'.format(prefix, key.upper())
    if _key in os.environ:
        return os.environ[_key]
    return ''


def edel(key, val, prefix=Config.ENV_PREFIX):
    _key = '{}{}'.format(prefix, key.upper())
    del os.environ[_key]


@pytest.mark.config
class TestConfig(TestCase):
    @pytest.fixture(autouse=True)
    def env(self, request):
        restore = os.environ.copy()
        try:
            yield
        finally:
            for k, v in os.environ.copy().iteritems():
                if not (k in restore):
                    del os.environ[k]
            for k, v in restore.iteritems():
                os.environ[k] = v

    def test_config_env_key(self):
        c = Config()
        assert c.env_name('debug') == 'EMIT_DEBUG'
        assert c.env_name('dEbUg') == 'EMIT_DEBUG'
        assert c.env_name('adapter_url') == 'EMIT_ADAPTER_URL'
        assert c.env_name('ADAPTER_URL') == 'EMIT_ADAPTER_URL'

    def test_config_env_get(self):
        try:
            restore = eget('debug')
            eput('debug', 'true')
            c = Config()
            assert c.env_value('debug') == 'true'
            assert c.env_value('test_config_env_get') == ''
        finally:
            eput('debug', restore)

    def test_config_from_env(self):
        c = Config.from_env()
        assert c.debug is False

        try:
            restore = eget('debug')
            eput('debug', 'true')
            c.load_env()
            assert c['debug'] is True
        finally:
            eput('debug', restore)
        assert c.debug is True
        c.load_env()
        assert c['debug'] is False
