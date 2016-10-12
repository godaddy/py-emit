.PHONY: all
all: check build tools test

.PHONY: clean
clean:
	rm -f .coverage
	rm -rf .cache
	find . -regex ".*\.pyc$$" -type f -exec rm {} \;
	find . -name __pycache__ -type d -prune -exec rm -rf {} \;

.PHONY: clean-env
clean-env:
	rm -rf env

.PHONY: tag
tag: env $(wildcard env/lib/*)
	TAG=v$$(python setup.py --version) && git tag -fa $$TAG -m "Release $$TAG"

.PHONY: build
build: env $(wildcard env/lib/*)

.PHONY: tools
tools: isort flake8
	env/bin/py.test -v test/

.PHONY: isort
isort: env/bin/isort
	env/bin/isort -rc ./emit --atomic --diff --verbose

.PHONY: flake8
flake8: env/bin/flake8
	env/bin/flake8 -v --max-line-length 120 emit

.PHONY: test
test: env/bin/py.test
	env/bin/py.test -v test/

.PHONY: testcov
testcov: env/bin/py.test
	env/bin/py.test --cov=emit --cov-report term-missing test/

.PHONY: install
install: test
	pip install $(CURDIR)

.PHONY: uninstall
uninstall:
	pip uninstall emit

.PHONY: check
check:
	command -v virtualenv >/dev/null 2>&1 || \
		{ echo >&2 "virtualenv command not found"; exit 1; }

$(wildcard env/lib/*): requirements.txt
	touch $(@)
	env/bin/pip install -r requirements.txt

env:
	virtualenv env
	env/bin/pip install --upgrade pip
	env/bin/pip install -r requirements.txt

env/bin/py.test: | env
	env/bin/pip install pytest

env/bin/%: | env
	env/bin/pip install $(@F)
