from tests.test_utils import *
import unittest


class TestCompat1(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test1(1))

    def test2(self):
        self.assertTrue(do_test1(2))

    def test3(self):
        self.assertTrue(do_test1(3))


class TestCompat2(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test2(1))

    def test2(self):
        self.assertTrue(do_test2(2))

    def test3(self):
        self.assertTrue(do_test2(3))


class TestCompat3(unittest.TestCase):
    def test1(self):
        self.assertTrue(do_test3(1))

    def test3(self):
        self.assertTrue(do_test3(2))

    def test3(self):
        self.assertTrue(do_test3(3))
