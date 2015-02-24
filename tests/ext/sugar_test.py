import testify as T

import vimap.ext.sugar


class BasicTest(T.TestCase):
    def test_basic(self):
        def fcn(i, to_add):
            return i + to_add
        T.assert_equal(
            set(vimap.ext.sugar.imap_unordered(fcn, [1, 2, 3], to_add=1)),
            set([2, 3, 4])
        )
