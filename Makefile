tests: _PHONY
	tox

test: _PHONY tests

coverage: _PHONY
	tox -e coverage

clean:
	rm -rf $$(find . -iname "*.pyc")
	rm -rf .tox

_PHONY:
