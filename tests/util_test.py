import testify as T

import vimap.util


class RunonceDebugObject(object):
    def __init__(self):
        self.num_x, self.num_z = 0, 0

    @vimap.util.instancemethod_runonce(depends=['z'])
    def x(self):
        self.num_x += 1

    @vimap.util.instancemethod_runonce(depends=[])
    def z(self):
        self.num_z += 1


class MiscTest(T.TestCase):
    def test_runonce_diff_objects(self):
        a = RunonceDebugObject()
        b = RunonceDebugObject()
        a.z()
        b.z()
        a.x()
        b.x()

    def test_runonce_dependency_fail(self):
        a = RunonceDebugObject()
        T.assert_raises(AssertionError, a.x)

    def test_runonce_really_once(self):
        a = RunonceDebugObject()
        a.z()
        a.z()
        a.x()
        a.x()
        T.assert_equal(a.num_x, 1)
        T.assert_equal(a.num_z, 1)
