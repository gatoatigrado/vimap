tests: _PHONY
	tox

test: _PHONY tests

test-no-performance: _PHONY
	tox -- --exclude-suite=performance tests

coverage: _PHONY
	tox -e coverage

clean:
	rm -rf $$(find . -iname "*.pyc")
	rm -rf .tox
	rm -rf vimap.egg-info

_PHONY:
