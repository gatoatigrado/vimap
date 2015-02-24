'''
Provides exception handling routines. The goal is to print useful tracebacks.

Note: `logging` sometimes plays bad with scribe streams, so we're not using
it riight now ....
'''
from __future__ import absolute_import
from __future__ import print_function

import cPickle
import sys
import traceback
from collections import namedtuple


# Lightweight terminal colorizing, don't depend on the [better] blessings package
_red = (lambda s: '\x1b[31m{0}\x1b[0m'.format(s)) if sys.stderr.isatty() else (lambda s: s)


_ExceptionContext = namedtuple('ExceptionContext', ('value', 'formatted_traceback'))


class WorkerException(Exception):
    """Represents an exception that was raised from a worker process."""


class UnpickleableException(Exception):
    """Represents an exception from the worker that was both unpickleable
    and unserializable to a string."""


def try_serialize_exception(exception):
    """
    Serializes an exception that was unpickleable.

    :type exception: Exception
    """
    exception_name = type(exception).__name__

    # Try to extract a string representation of the exception ...
    try:
        string_exc = str(exception)
    except TypeError:
        try:
            string_exc = repr(exception)
        except TypeError:
            string_exc = 'UNPICKLEABLE AND UNSERIALIZABLE MESSAGE'

    try:
        return type(exception)(string_exc)
    except TypeError:
        return UnpickleableException('{0}: {1}'.format(exception_name, string_exc))


class ExceptionContext(_ExceptionContext):
    '''Pickleable representation of an exception from a process.

    It contains the exception value and the formatted traceback string.
    '''
    __slots__ = ()

    @property
    def formatted_exception(self):
        return "{0}: {1}".format(type(self.value).__name__, self.value)

    def reraise(self):
        raise WorkerException(
            self.formatted_exception + "\nTraceback:\n" + self.formatted_traceback
        )

    @classmethod
    def current(cls):
        _, value, tb = sys.exc_info()
        if value is None:
            raise TypeError('no exception in current context')

        try:
            cPickle.dumps(value)
        except (TypeError, cPickle.PicklingError):
            value = try_serialize_exception(value)

        formatted_traceback = ''.join(traceback.format_tb(tb) + [repr(value)])
        return cls(value, formatted_traceback)


def clean_print(msg, fd=None, end='\n'):
    """Prints a message to stderr (or another fd), flushing it before and after.
    """
    fd = sys.stderr if fd is None else fd
    msg = msg + end
    fd.write(msg)
    fd.flush()


def print_exception(ec, traceback_, worker_):
    '''Prints an exception when the user hasn't explicitly handled it. Use of
    sys.stderr.write is an attempt to avoid multiple threads munging log lines.
    '''
    clean_print(
        _red("[Worker Exception] {ec.value.__class__.__name__}: {ec.value}".format(ec=ec)) +
        "\n" +
        ec.formatted_traceback
    )


def print_warning(message, **kwargs):
    '''Prints a warning message
    '''
    clean_print('[Warning] {message}{fmt_cond}{args}\n'.format(
        message=message, fmt_cond=(': ' if kwargs else ''), args=kwargs))
