import unittest
from unittest import TestCase


from page import Page


class TestPageMethods(TestCase):
    def testHasCapacity(self):
        test_page = Page()
        self.assertTrue(test_page.has_capacity())


if __name__ == '__main__':
    unittest.main()
