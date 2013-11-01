import mock
import testify as T

import vimap.exception_handling
import vimap.pool
import vimap.testing
import vimap.worker_process
from vimap.exception_handling import ExceptionContext


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


@vimap.worker_process.worker
def worker_raise_exc_after_iteration(seq, init=0):
    for x in seq:
        yield x
    raise ValueError("goodbye")


def serialize_error(error):
    return (type(error), str(error))


def run_test_pool(worker_f):
    '''Start a 3 worker pool of worker_f and pass [1, 10) into
    it.'''
    num_workers = 3
    processes = vimap.pool.fork(
        worker_f.init_args(init=i)
        for i
        in range(num_workers)
    )
    # Give every worker something to chew on.
    res = list(processes.imap(list(range(1, 10))).zip_in_out_typ())
    # They should all be done by this point.
    T.assert_equal(processes.__runonce__['finish_workers'], True)
    return res


class ExceptionsTest(T.TestCase):
    def check_died_prematurely_warning(self, print_warning_mock):
        T.assert_gte(print_warning_mock.call_args_list, 1)
        for (args, kwargs) in print_warning_mock.call_args_list:
            T.assert_equal(args, ('All processes died prematurely!',))

    @mock.patch.object(vimap.exception_handling, 'print_warning', autospec=True)
    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_exception_before_iteration(self, print_exc_mock, print_warning_mock):
        res_to_compare = [
            (inp, serialize_error(ec.value), typ)
            for inp, ec, typ
            in run_test_pool(worker_raise_exc_immediately)
        ]
        # Each worker will stop processing once an exception makes it to
        # the top so we only get that number of exceptions back out.
        expected_res_to_compare = [
            (vimap.pool.NO_INPUT, serialize_error(ValueError("hello")), 'exception'),
        ] * 3
        T.assert_sorted_equal(res_to_compare, expected_res_to_compare)
        self.check_died_prematurely_warning(print_warning_mock)

    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_exception_after_iteration_not_returned(self, print_exc_mock):
        res_to_compare = [
            (inp, out, typ)
            for inp, out, typ
            in run_test_pool(worker_raise_exc_after_iteration)
        ]
        # The pool notices that all output has been returned, so doesn't
        # wait for any more responses. We shouldn't see exceptions.
        expected_res_to_compare = [
            (inp, inp, 'output')
            for inp
            in range(1, 10)
        ]
        T.assert_sorted_equal(res_to_compare, expected_res_to_compare)

    @mock.patch.object(vimap.exception_handling, 'print_warning', autospec=True)
    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_exception_with_curleys(self, print_exc_mock, print_warning_mock):
        '''Dumb test ... I aim to write tests for most every bug that had existed,
        but this is kinda 1-off ... (.format() got a curley brace).
        '''
        res_to_compare = [
            (serialize_error(ec.value), typ)
            for _, ec, typ
            in run_test_pool(worker_raise_exc_with_curleys)
        ]
        # Each worker will stop processing once an exception makes it to
        # the top so we only get that number of exceptions back out.
        expected_res_to_compare = [
            # We're not sure which inputs will get picked, but all
            # should return this exception.
            (serialize_error(ValueError("{0} curley braces!")), 'exception'),
        ] * 3
        T.assert_sorted_equal(res_to_compare, expected_res_to_compare)
        self.check_died_prematurely_warning(print_warning_mock)

    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_unconsumed_exceptions(self, print_exc_mock):
        '''Unconsumed exceptions should only be printed.
        '''
        processes = vimap.pool.fork(
            worker_raise_exc_immediately.init_args(init=i) for i in [1, 1, 1])
        del processes

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call_args[0].value) for call_args, _ in calls]
        T.assert_equal(errors, [serialize_error(ValueError("hello"))] * 3)

    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_a_few_error(self, print_exc_mock):
        processes = vimap.pool.fork(
            (worker_raise_exc_with_curleys.init_args(init=i) for i in xrange(2)),
            in_queue_size_factor=2)
        processes.imap([1]).block_ignore_output()
        del processes

        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call_args[0].value) for call_args, _ in calls]
        T.assert_equal(errors, [serialize_error(ValueError("{0} curley braces!"))])

    @mock.patch.object(vimap.exception_handling, 'print_warning', autospec=True)
    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_fail_after_a_while(self, print_exc_mock, print_warning_mock):
        processes = vimap.pool.fork(
            (worker_raise_exc_with_curleys.init_args(init=i) for i in xrange(100)),
            in_queue_size_factor=2)
        processes.imap([-1] * 3000 + list(range(50)))

        # Check yielded output.
        res_to_compare = []
        for inp, out, typ in processes.zip_in_out_typ():
            if typ == 'exception':
                res_to_compare.append((inp, serialize_error(out.value), typ))
            else:
                res_to_compare.append((inp, out, typ))
        # All the -1s will produce None output.
        expected_res_to_compare = [
            (-1, None, 'output')
        ] * 3000
        # Once we get to the positive numbers, we start causing 50 of
        # the 100 workers to throw exceptions.
        expected_res_to_compare.extend([
            (i, serialize_error(ValueError("{0} curley braces!")), 'exception')
            for i
            in range(50)
        ])
        T.assert_sorted_equal(res_to_compare, expected_res_to_compare)

        # Check out exception logging.
        calls = print_exc_mock.call_args_list
        errors = [serialize_error(call_args[0].value) for call_args, _ in calls]
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
        expected_ec = ExceptionContext(
            value=expected_ex,
            formatted_traceback=expected_formatted_tb_str)
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
