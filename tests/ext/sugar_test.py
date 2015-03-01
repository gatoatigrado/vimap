import mock
import testify as T

import vimap.ext.sugar
import vimap.exception_handling


class BasicTest(T.TestCase):
    def test_basic(self):
        def fcn(i, to_add):
            return i + to_add
        T.assert_equal(
            set(vimap.ext.sugar.imap_unordered(fcn, [1, 2, 3], to_add=1)),
            set([2, 3, 4])
        )

    @mock.patch.object(vimap.exception_handling, 'print_exception', autospec=True)
    def test_exceptions(self, mock_print_exception):
        def fcn(x):
            if x:
                raise ValueError("Bad value: {0}".format(x))
            return x
        T.assert_raises_and_contains(
            vimap.exception_handling.WorkerException,
            ("ValueError: Bad value: 3",),
            lambda: tuple(vimap.ext.sugar.imap_unordered(fcn, [False, 3, 0]))
        )
        T.assert_equal(mock_print_exception.called, True)
