# Emit Python Client

  [Emit System] | [Event Specification]

  > ```json
  > {
  >   "tid": "84546fec-0661-41ac-bedb-3dff65903a5d",
  >   "time": "2016-01-01T10:10:10.123456-07:00",
  >   "system": "test.emit",
  >   "component": "emitter",
  >   "operation": "ping",
  >   "event": "open"
  > }
  > ```


## About

Python client library for emitting events. This library has extensive [Unit Test Coverage](blob/master/test/unit) which may serve as a reference for more advanced usage.


## Example usage:

  - Using Emit in your application.

    > Python:
    > ```python
    > import logging
    > from emit.globals import conf
    > from emit.emitters import Emitter
    > from emit.adapters import StdoutAdapter
    > from uuid import uuid4
    >
    > # Emit will print log messages at this level.
    > logging.getLogger().setLevel(logging.DEBUG)
    >
    > # Setting the adapter to stdout for testing
    > conf.adapter_class = StdoutAdapter
    > conf.debug = True
    >
    > # Create emitter, this would probably go in your modules __init__
    > emit_cfg = dict(
    >     system='emitexample',
    >     component='emitexample',
    >     operation='emitexample')
    > emitter = Emitter(**emit_cfg)
    >
    > # TID: Represents the transaction ID, indicative of a new "request"
    > emitter.tid = str(uuid4())
    >
    > # At the start of a transaction if you emit an "open" event it helps you know
    > # the transaction started naturally and can be used as the entry point when
    > # trying to roll up a given TID.
    > emitter(event='open')
    >
    > with emitter(event='applogic'):
    >     # Call out to a database?
    >
    >     # Lets log some interesting part of the db result
    >     emitter('databaseresult', data={'db_result': 'result'})
    >
    >     with emitter(event='callsome') as ctx:
    >
    >         # Context will be added to all events emitted within this scope
    >         # and discarded once you leave it.
    >         ctx.data = {'some_host': 'http://example.com'}
    >
    >         # As you nest your context, an event bread crumb is created for you
    >         # so that you may enter and exit scopes and the event name will
    >         # be representitive of the application flow.
    >         emitter('someresult', data={'some_result': 'result'})
    >
    > # Could print out response data
    > emitter('responding')
    >
    > # At the end of the transaction we emit a "close" to indicate the application
    > # provided its own exit point. Even if the request failed from the
    > # perspective of the end user, a missing close is meant to indicate an
    > # unnatural code path or application crash.
    > emitter(event='close')
    > ```
    >
    > Output:
    > ```json
    > Result:
    > {
    >   "time": "2016-07-29T20:33:15.462740Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "open"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.464674Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.466147Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic.enter"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.468706Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "component": "emitexample",
    >   "operation": "emitexample",
    >   "data": {
    >     "db_result": "result"
    >   },
    >   "system": "emitexample",
    >   "event": "applogic.databaseresult"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.470734Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic.callsome"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.472647Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic.callsome.enter"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.475656Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "component": "emitexample",
    >   "operation": "emitexample",
    >   "data": {
    >     "some_result": "result"
    >   },
    >   "system": "emitexample",
    >   "event": "applogic.callsome.someresult"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.472669Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic.callsome.exit"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.466169Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "applogic.exit"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.483044Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "responding"
    > }
    > {
    >   "time": "2016-07-29T20:33:15.484473Z",
    >   "tid": "29b79925-6ef2-4e52-809e-431733aed9e2",
    >   "operation": "emitexample",
    >   "component": "emitexample",
    >   "system": "emitexample",
    >   "event": "close"
    > }
    > ```

  - Run unit tests.

    > Command:
    > ```bash
    > py.test --cov emit --cov-report term-missing ./test \
    >   | awk '{print "    > " $0}'
    > ```
    >
    > Output:
    > ```
    > =============================================== test session starts ===============================================
    > platform darwin -- Python 2.7.10, pytest-3.0.1, py-1.4.31, pluggy-0.3.1
    > rootdir: py-emit/test, inifile: pytest.ini
    > plugins: cov-2.3.1
    > collected 464 items
    >
    > test/unit/test_adapters.py ..........................................................................................
    > test/unit/test_config.py ...
    > test/unit/test_decorators.py ......
    > test/unit/test_emitter.py .......................................................
    > test/unit/test_event.py ........................................................................................................................................
    > test/unit/test_globals.py ............................
    > test/unit/test_logger.py .....
    > test/unit/test_queue.py .............................
    > test/unit/test_transports.py ..............................................................................
    > test/unit/test_utils.py ..................................
    >
    > ---------- coverage: platform darwin, python 2.7.10-final-0 ----------
    > Name                 Stmts   Miss  Cover   Missing
    > --------------------------------------------------
    > emit/__init__.py         9      0   100%
    > emit/adapters.py       230      5    98%   58, 184-186, 188
    > emit/config.py          36      0   100%
    > emit/decorators.py      58      0   100%
    > emit/emitter.py        127      0   100%
    > emit/event.py          395      0   100%
    > emit/globals.py         86      0   100%
    > emit/logger.py          21      0   100%
    > emit/queue.py          122      0   100%
    > emit/transports.py     274      3    99%   317-319
    > emit/utils.py          113      0   100%
    > --------------------------------------------------
    > TOTAL                 1471      8    99%
    > ```

  - Import emit and send a open, ping and close event to the default transport.

    > Command:
    > ```python
    > from emit import emit
    > print emit.ping()
    > ```
    >
    > Output:
    > ```
    > 'f0ccfa68-7c26-4825-815b-32888cd1ea8f'
    > ```


## Installation

  - System wide install with pip:

    > Command:
    > ```bash
    > pip install git+ssh://git@github.com/godaddy/py-emit.git
    python -c 'import emit; print emit.ping()'
    > ```

  - Install to a virtualenv for local usage as a library:

    > Command:
    > ```bash
    > mkdir myproject && cd myproject
    > virtualenv env
    > . env/bin/activate
    > pip install git+ssh://git@git@github.com/godaddy/py-emit.git
    > python -c 'import emit; emit.ping()'
    > ```

  - Setup for local development of this library:

    > Command:
    > ```bash
    > git clone git+https://git@github.com/godaddy/py-emit.git
    > mkdir myproject && cd myproject
    > virtualenv env
    > . env/bin/activate
    > pip install -r ../py-emit/requirements.txt
    > python -c 'import emit; emit.ping()'
    > echo $(realpath $(pwd)/../py-emit) > env/lib/python2.7/site-packages/usrlocal.pth
    > ```

  - Or local dev with emit directly in your project:

    > Command:
    > ```bash
    > mkdir myproject && cd myproject
    > virtualenv env
    > . env/bin/activate
    > git clone git+https://git@github.com/godaddy/py-emit.git
    > ln -s py-emit/emit .
    > pip install -r emit/requirements.txt
    > python -c 'import emit; emit.ping()'
    > ```

  - Run tests with coverage:

    > Command:
    > ```bash
    > env/bin/py.test --cov=emit --cov-report term-missing ./test/
    > ```


## Bugs and Patches

  Feel free to report bugs and submit pull requests.

  * bugs:
    <https://git@github.com/godaddy/py-emit/issues>
  * patches:
    <https://git@github.com/godaddy/py-emit/pulls>
