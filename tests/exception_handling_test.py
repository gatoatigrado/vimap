# -*- coding: utf-8 -*-
"""
Unit tests for exception handling utilities
"""
from __future__ import absolute_import
from __future__ import print_function

import cPickle

import testify

from vimap import exception_handling
from vimap.testing import unpickleable


class ExceptionContextTest(testify.TestCase):
    err = ValueError("hi")

    def get_ec(self):
        try:
            raise self.err
        except:
            return exception_handling.ExceptionContext.current()
        testify.assert_not_reached()

    def test_current_method(self):
        """The actual method under test is in get_ec()."""
        testify.assert_is(self.get_ec().value, self.err)

    def test_formatted_traceback(self):
        ec = self.get_ec()
        testify.assert_in("exception_handling_test.py", ec.formatted_traceback)
        testify.assert_in("in get_ec", ec.formatted_traceback)

    def test_formatted_exception(self):
        testify.assert_equal("ValueError: hi", self.get_ec().formatted_exception)

    def test_reraise(self):
        testify.assert_raises_and_contains(
            exception_handling.WorkerException,
            ("ValueError: hi", "Traceback:\n", "in get_ec"),
            lambda: self.get_ec().reraise()
        )

    def test_unpickleable(self):
        err = Exception(unpickleable)
        try:
            raise err
        except:
            ec = exception_handling.ExceptionContext.current()
        cPickle.dumps(ec)  # make sure it can indeed be serialized
        testify.assert_isinstance(ec, exception_handling.ExceptionContext)
        testify.assert_isinstance(ec.value, Exception)
        testify.assert_equal(str(ec.value), "UNPICKLEABLE AND UNSERIALIZABLE MESSAGE")

    def test_unpickleable_with_uninitializable_exception(self):
        """
        Tests an exception that can't be pickled (due to CustomException
        not being in global variables) and one that can't be reinitialized
        with a single string argument.
        """
        class CustomException(Exception):
            def __init__(self, a, b):
                self.a, self.b = a, b

            def __str__(self):
                return "{0}, {1}".format(self.a, self.b)

        err = CustomException(3, 4)
        try:
            raise err
        except:
            ec = exception_handling.ExceptionContext.current()
        cPickle.dumps(ec)  # make sure it can indeed be serialized
        testify.assert_isinstance(ec.value, exception_handling.UnpickleableException)
        testify.assert_equal(str(ec.value), "CustomException: 3, 4")
