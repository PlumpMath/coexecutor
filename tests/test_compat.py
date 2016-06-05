from tests.test_utils import *
import unittest


class TestCompat1(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test1(1))

    def test2(self):
        self.assertTrue(do_test1(2))

    def test3(self):
        self.assertTrue(do_test1(3))

    def test4(self):
        self.assertTrue(do_test1(4))

    def test5(self):
        self.assertTrue(do_test1(5))

    def test6(self):
        self.assertTrue(do_test1(6))

    def test7(self):
        self.assertTrue(do_test1(7))


class TestCompat2(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test2(1))

    def test2(self):
        self.assertTrue(do_test2(2))

    def test3(self):
        self.assertTrue(do_test2(3))

    def test4(self):
        self.assertTrue(do_test2(4))

    def test5(self):
        self.assertTrue(do_test2(5))

    def test6(self):
        self.assertTrue(do_test2(6))

    def test7(self):
        self.assertTrue(do_test2(7))

class TestCompat3(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test3(1))

    def test3(self):
        self.assertTrue(do_test3(2))

    def test3(self):
        self.assertTrue(do_test3(3))

    def test4(self):
        self.assertTrue(do_test3(4))

    def test5(self):
        self.assertTrue(do_test3(5))

    def test6(self):
        self.assertTrue(do_test3(6))

    def test7(self):
        self.assertTrue(do_test3(7))

if __name__ == '__main__':
    unittest.main()
