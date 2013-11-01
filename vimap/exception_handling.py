'''
Provides exception handling routines. The goal is to print useful tracebacks.

Note: `logging` sometimes plays bad with scribe streams, so we're not using
it riight now ....
'''
from __future__ import absolute_import
from __future__ import print_function

import sys
import traceback
from collections import namedtuple


_ExceptionContext = namedtuple('ExceptionContext', ('value', 'formatted_traceback'))


class ExceptionContext(_ExceptionContext):
    '''Pickleable representation of an exception from a process.

    It contains the exception value and the formatted traceback string.
    '''
    __slots__ = ()

    @classmethod
    def current(cls):
        _, value, tb = sys.exc_info()
        if value is None:
            raise TypeError('no exception in current context')

        formatted_traceback = ''.join(traceback.format_tb(tb) + [repr(value)])
        return cls(value, formatted_traceback)


def print_exception(ec, traceback_, worker_):
    '''Prints an exception when the user hasn't explicitly handled it. Use of
    sys.stderr.write is an attempt to avoid multiple threads munging log lines.
    '''
    exception_str = '[Worker Exception] {typ}: {exception}\n'.format(
        typ=ec.value.__class__.__name__, exception=ec.value)
    sys.stderr.flush()
    sys.stderr.write(exception_str)
    sys.stderr.flush()


def print_warning(message, **kwargs):
    '''Prints a warning message
    '''
    warning_str = '[Warning] {message}{fmt_cond}{args}\n'.format(
        message=message, fmt_cond=(': ' if kwargs else ''), args=kwargs)
    sys.stderr.flush()
    sys.stderr.write(warning_str)
    sys.stderr.flush()
