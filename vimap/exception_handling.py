'''
Provides exception handling routines. The goal is to print useful tracebacks.

Note: `logging` sometimes plays bad with scribe streams, so we're not using
it riight now ....
'''
from __future__ import absolute_import
from __future__ import print_function

import sys


def print_exception(exception, traceback_, worker_):
    '''Prints an exception when the user hasn't explicitly handled it. Use of
    sys.stderr.write is an attempt to avoid multiple threads munging log lines.
    '''
    exception_str = '[Worker Exception] {typ}: {exception}\n'.format(
        typ=exception.__class__.__name__, exception=exception)
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
