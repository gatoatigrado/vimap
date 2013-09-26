TESTIFY_OPTIONS=-x disabled --summary

tests: _PHONY
	testify $(TESTIFY_OPTIONS) tests

test: _PHONY tests

run_coverage:
	coverage run `which testify` $(TESTIFY_OPTIONS) tests

coverage: run_coverage
	coverage report -m --include='vimap/*' --omit=''

clean:
	rm -rf $$(find . -iname "*.pyc")

_PHONY:
