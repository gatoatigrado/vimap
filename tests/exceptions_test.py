import mock
import testify as T

import vimap.exception_handling
import vimap.pool
import vimap.worker_process


@vimap.worker_process.worker
def worker_raise_exc_immediately(seq, init=0):
    raise ValueError("hello")


@vimap.worker_process.worker
def worker_raise_exc_with_curleys(seq, init=0):
    for _ in seq:
        raise ValueError("{0} curley braces!")


def serialize_error(error):
    return (type(error), str(error))


class ExceptionsTest(T.TestCase):
    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_basic_exceptions(self, print_exc_mock):
        processes = vimap.pool.fork(worker_raise_exc_immediately.init_args(init=i)
            for i in [1, 1, 1])
        res = list(processes.imap(list(range(1, 10))).zip_in_out())
        T.assert_equal(res, [])

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call[0][0]) for call in calls]
        T.assert_equal(errors, [serialize_error(ValueError("hello"))] * 3)

    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_exception_with_curleys(self, print_exc_mock):
        '''Dumb test ... I aim to write tests for most every bug that had existed,
        but this is kinda 1-off ... (.format() got a curley brace).
        '''
        processes = vimap.pool.fork(worker_raise_exc_with_curleys.init_args(init='{a}')
            for _ in [1, 1, 1])
        res = list(processes.imap(list(range(1, 10))).zip_in_out())
        T.assert_equal(res, [])

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call[0][0]) for call in calls]
        T.assert_equal(errors, [serialize_error(ValueError("{0} curley braces!"))] * 3)

    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_unconsumed_exceptions(self, print_exc_mock):
        processes = vimap.pool.fork(worker_raise_exc_immediately.init_args(init=i)
            for i in [1, 1, 1])
        del processes

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call[0][0]) for call in calls]
        T.assert_equal(errors, [serialize_error(ValueError("hello"))] * 3)
