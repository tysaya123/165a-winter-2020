import unittest
from unittest import TestCase

from page import BasePage
from config import *


class TestBasePageMethods(TestCase):
    def setUp(self):
        self.test_page = BasePage()

    def testHasCapacity(self):
        self.assertTrue(self.test_page.has_capacity())

    def testReadWrite(self):
        rand_int = 242444
        rand_rid = 44
        max_int = 9223372036854775807  # 2^63 - 1
        max_rid = 4294967295  # 2^32
        min_int = -max_int
        min_rid = 0
        self.test_page.write(rand_rid, rand_int)
        self.test_page.write(max_rid, max_int)
        self.test_page.write(min_rid, min_int)

        self.assertEqual(self.test_page.read(rand_rid), rand_int)
        self.assertEqual(self.test_page.read(max_rid), max_int)
        self.assertEqual(self.test_page.read(min_rid), min_int)

        self.assertEqual(self.test_page.getDirty(rand_rid), SchemaEncoding.CLEAN)
        self.assertEqual(self.test_page.getDirty(max_rid), SchemaEncoding.CLEAN)
        self.assertEqual(self.test_page.getDirty(min_rid), SchemaEncoding.CLEAN)


if __name__ == '__main__':
    unittest.main()
