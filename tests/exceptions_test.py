import mock
import testify as T

import vimap.exception_handling
from vimap.exception_handling import ExceptionContext
import vimap.pool
import vimap.worker_process
import vimap.testing


@vimap.worker_process.worker
def worker_raise_exc_immediately(seq, init=0):
    raise ValueError("hello")
    return seq


@vimap.worker_process.worker
def worker_raise_exc_with_curleys(seq, init=0):
    for x in seq:
        if x >= 0:
            raise ValueError("{0} curley braces!")
        yield


def serialize_error(error):
    return (type(error), str(error))


class ExceptionsTest(T.TestCase):
    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_basic_exceptions(self, print_exc_mock):
        processes = vimap.pool.fork(worker_raise_exc_immediately.init_args(init=i)
            for i in [1, 1, 1])
        res = list(processes.imap(list(range(1, 10))).zip_in_out())
        T.assert_equal(res, [])
        T.assert_equal(processes.finished_workers, True)

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

    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_a_few_error(self, print_exc_mock):
        processes = vimap.pool.fork((worker_raise_exc_with_curleys.init_args(init=i)
            for i in xrange(2)), in_queue_size_factor=2)
        processes.imap([1]).block_ignore_output()
        del processes

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call[0][0]) for call in calls]
        T.assert_equal(errors, [serialize_error(ValueError("{0} curley braces!"))])

    @mock.patch.object(vimap.exception_handling, 'print_warning')
    @mock.patch.object(vimap.exception_handling, 'print_exception')
    def test_fail_after_a_while(self, print_exc_mock, print_warning_mock):
        processes = vimap.pool.fork((worker_raise_exc_with_curleys.init_args(init=i)
            for i in xrange(100)), in_queue_size_factor=2)
        processes.imap([-1] * 3000 + list(range(50)))
        del processes

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call[0][0]) for call in calls]
        T.assert_equal(errors, [serialize_error(ValueError("{0} curley braces!"))] * 50)

        # NOTE: Sometimes, the weakref in the pool is deleted, so 'has_exceptions' is
        # not set, and the pool prints warnings we don't actually care about. Make
        # sure that this is the only warning printed.
        if print_warning_mock.call_args_list:
            T.assert_equal(len(print_warning_mock.call_args_list), 1)
            [warning] = print_warning_mock.call_args_list
            T.assert_in('Pool disposed before input was consumed', warning[0][0])


class ExceptionContextTest(T.TestCase):
    def test_no_exception_currently(self):
        with T.assert_raises(TypeError):
            ExceptionContext.current()

    def test_formats_correctly(self):
        expected_formatted_tb = ['Traceback:\n', 'stuff\n', 'more stuff\n']
        expected_ex = ValueError('test')
        expected_formatted_tb_str = """Traceback:\nstuff\nmore stuff\nValueError('test',)"""
        expected_ec = ExceptionContext(value=expected_ex, formatted_traceback=expected_formatted_tb_str)
        expected_tb = mock.Mock()
        mock_exc_info = (None, expected_ex, expected_tb)

        with mock.patch.object(
            vimap.exception_handling.sys,
            'exc_info',
            autospec=True,
            return_value=mock_exc_info,
        ):
            with mock.patch.object(
                vimap.exception_handling.traceback,
                'format_tb',
                autospec=True,
                return_value=expected_formatted_tb,
            ):
                found_ec = ExceptionContext.current()
                T.assert_equal(found_ec, expected_ec)
