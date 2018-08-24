import pytest
import py  # needed by pytest, I use it for term width
import logging
import os
from datetime import datetime
from emit.globals import log
from emit import transports, adapters, emitters, event


_root_dir = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + os.sep + '../emit/')
_log_container = dict()


class TContext():
    worker_class = transports.Worker
    adapter_class = adapters.ListAdapter

    def __str__(self):
        return 'TContext({})'.format(str(self.__dict__))

    def __init__(self, obj):
        self.obj = obj

        for name in dir(self):
            if name.endswith('_class') and hasattr(obj, name):
                setattr(self, name, getattr(obj, name))

    def transport(self):
        return transports.Transport(
            adapter=self.adapter_class(), worker_class=self.worker_class)

    def worker(self):
        t = self.transport()
        w = self.worker_class(t)
        return w

    def event(self, *args, **kwargs):
        return event.Event(*args, **kwargs)

    def emitter(self, **kwargs):
        return emitters.Emitter(**kwargs)


@pytest.fixture(autouse=True)
def setup_context(request):
    if not (request.instance is None):
        request.instance.ctx = TContext(request.instance)


@pytest.yield_fixture
def logs(request):
    local_handler = LogHandler()
    # log.addHandler SILENTLY refuses to add same log handler class, dumb.
    log.handlers.append(local_handler)
    yield local_handler
    for (index, handler) in enumerate(log.handlers):
        if handler == local_handler:
            del log.handlers[index:]


@pytest.yield_fixture
def a(request):
    if not hasattr(request.cls, 'adapter_class'):
        setattr(request.cls, 'adapter_class', adapters.ListAdapter)
    yield request.cls.adapter_class()


@pytest.yield_fixture
def t(request, a):
    if not hasattr(request.cls, 'transport_class'):
        setattr(request.cls, 'transport_class', transports.Transport)
    if not hasattr(request.cls, 'worker_class'):
        setattr(request.cls, 'worker_class', transports.Worker)
    yield transports.Transport(
            adapter=a, worker_class=request.cls.worker_class)


@pytest.yield_fixture
def w(request, t):
    if not hasattr(request.cls, 'worker_class'):
        setattr(request.cls, 'worker_class', transports.Worker)
    worker = request.cls.worker_class(t)
    t.worker = worker
    yield worker


def pytest_sessionstart(session):
    log.setLevel(logging.DEBUG)


@pytest.hookimpl(trylast=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    if report.outcome == 'passed':
        return
    if item.nodeid in _log_container and len(_log_container[item.nodeid]):
        report.sections.append(
            ('Captured log calls', str(_log_container[item.nodeid])))


def pytest_runtest_setup(item):
    handler = LogHandler()
    _log_container[item.nodeid] = handler
    del log.handlers[:]
    log.addHandler(handler)


def pytest_addoption(parser):
    parser.addoption("--amqp_url", action="store", default='')
    parser.addoption("--http_url", action="store", default='')


# http://pytest.org/latest/writing_plugins.html#conftest
# http://pytest.org/latest/writing_plugins.html#conftest-py
# http://pytest.org/latest/writing_plugins.html#pytest-hook-reference
# http://pytest.org/latest/writing_plugins.html#_pytest.main.Node
def pytest_collection_modifyitems(items):
    """Pytest will call this to modify tests before they run, gets an array of
    test nodes as an argument. We use it to mark tests. These are the marks used:

    - smoke
        Quick smoke tests that should be run first to see if environment is in
        a healthy state. Should provide feedback to indicate if the currently
        configured environment may serve as a reliable authority for failure.

    - acceptance
        Set of tests to assert a given contract is met to the end user. As a
        useful side effect it may provide an example reference to expected
        library usage patterns.

    - unit
        Unit tests are responsible for isolation and prevention of failure
        at the lowest level in the library. It is the front line defense against
        bugs and regression and usually provides immediate feedback about what
        went wrong in a higher level test.

    - integration
        Integration tests are responsible for isolation and prevention of failure
        points within the exposed public interfaces of individual components.

    - component
        Component tests will be responsible for isolation and prevention of
        failure points between related sets of components. The primary difference
        from the integration tests is the focus on relations that may span across
        multiple components.

    - system
        System tests will be responsible for end to end coverage. It asserts that
        all components within a given system are satisfying the currently agreed
        upon integration points.
    """
    for item in items:
        """Items are arrays of test cases which look like:
            # from pprint import pprint
            # pprint(vars(item), indent=2)
            { '_args': None,
            '_fixtureinfo': <_pytest.python.FuncFixtureInfo instance at 0x7f8ec4c6f5f0>,
            '_name2pseudofixturedef': { },
            '_obj': <unbound method TestApi.test_example_usage>,
            '_report_sections': [],
            '_request': <FixtureRequest for <TestCaseFunction 'test_example_usage'>>,
            'config': <_pytest.config.Config object at 0x7f8ec7b4a690>,
            'extra_keyword_matches': set([]),
            'fixturenames': [],
            'fspath': local('/app/test/patterns/test_api.py'),
            'funcargs': { },
            'keywords': <NodeKeywords for node <TestCaseFunction 'test_example_usage'>>,
            'name': 'test_example_usage',
            'parent': <UnitTestCase 'TestApi'>,
            'session': <Session 'test'>}
        """
        if item.nodeid.startswith("smoke/"):
            item.add_marker(pytest.mark.smoke)

        if item.nodeid.startswith("acceptance/"):
            item.add_marker(pytest.mark.acceptance)

        if item.nodeid.startswith("unit/"):
            item.add_marker(pytest.mark.unit)

        if item.nodeid.startswith("integration/"):
            item.add_marker(pytest.mark.integration)

        if item.nodeid.startswith("component/"):
            item.add_marker(pytest.mark.component)

        if item.nodeid.startswith("system/"):
            item.add_marker(pytest.mark.system)

        if "environment.py::Test" in item.nodeid:
            item.add_marker(pytest.mark.smoke)


class LogHandler(logging.Handler, list):
    def __str__(self):
        return '\n'.join(self.records())

    def __init__(self):
        logging.Handler.__init__(self)
        list.__init__(self)
        self.created = datetime.utcnow()

    def records(self):
        lengths = [0, 0]
        records = []

        for i, record in enumerate(self):
            record = self.normalize(i, record)
            lengths = map(max, map(len, record[:2]), lengths)
            records.append(record)
        for record in records:
            yield self.clean(record, lengths)

    def normalize(self, i, record):
        pathname = record.pathname
        try:
            pathname = pathname.replace(_root_dir, '').lstrip('/')
        except:
            pass
        if i > 0:
            delta = -(self[i-1].created - record.created)
        else:
            delta = 0
        return [record.levelname, pathname + ':' + str(record.lineno), delta, self.format(record)]

    def clean(self, record, lengths):
        def squish(size, data):
            if "\n" in data or len(data) <= size:
                return data
            width = (size / 2) - min(len(data) - size, 2)
            return '{0:.<{2}}{1:.>{2}}'.format(
                data[:width], data[-(width+1):], size / 2)
        return '[+{2:.6f} {0: <{3}} {1: <{4}}] {5}'.format(*(
            record[:3] + lengths + [squish(
                py.io.get_terminal_width() - sum(lengths) - 14, record[3])]))

    def emit(self, record):
        self.append(record)
