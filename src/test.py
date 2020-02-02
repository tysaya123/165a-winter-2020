import unittest
import logging
from unittest import TestCase
from random import randrange


from page import BasePage, TailPage
from table import Table
from bufferpool import BufferPool
from config import *


class TestPageMethods(TestCase):
    def setUp(self):
        self.base_page = BasePage()
        self.num_tail_cols = 5
        self.tail_page = TailPage(self.num_tail_cols)
        self.max_int = 9223372036854775807  # 2^63 - 1
        self.max_rid = 4294967295  # 2^32
        self.min_int = -self.max_int
        self.min_rid = 0

    def testMaxMinValues(self):
        self.base_page.new_record(self.max_rid, self.max_int, 0)
        tail_values = self.num_tail_cols * [self.max_int]
        self.tail_page.new_record(self.max_rid, tail_values)

        self.assertEqual((self.max_rid, self.max_int, 0), self.base_page.read(self.max_rid))
        self.assertEqual(tuple(tail_values), self.tail_page.read(self.max_rid)[1:self.num_tail_cols+1])

        self.base_page.new_record(self.min_rid, self.min_int, 0)
        tail_values = self.num_tail_cols * [self.min_int]
        self.tail_page.new_record(self.min_rid, tail_values)

        self.assertEqual((self.max_rid, self.max_int, 0), self.base_page.read(self.max_rid))
        self.assertEqual(tuple(tail_values), self.tail_page.read(self.min_rid)[1:self.num_tail_cols+1])

    def testBaseCapacity(self):
        for i in range(self.base_page.get_max()):
            self.assertEqual(True, self.base_page.has_capacity())
            self.base_page.new_record(randrange(self.max_rid+1), randrange(self.min_int, self.max_int+1), randrange(2))
        self.assertEqual(False, self.base_page.has_capacity())

    def testTailCapacity(self):
        for i in range(self.tail_page.get_max()):
            self.assertEqual(True, self.tail_page.has_capacity())
            self.tail_page.new_record(randrange(self.max_rid+1), 5*[randrange(self.min_int, self.max_int+1)])
        self.assertEqual(False, self.tail_page.has_capacity())

    def testDeleteRecord(self):
        self.base_page.new_record(self.max_rid, self.max_int, 0)
        tail_values = self.num_tail_cols * [self.max_int]
        self.tail_page.new_record(self.max_rid, tail_values)

        self.base_page.mark_record_deleted(self.max_rid)
        self.tail_page.mark_record_deleted(self.max_rid)

        self.assertEqual(None, self.base_page.read(self.max_rid))
        self.assertEqual(None, self.tail_page.read(self.max_rid))


class TestTableMethods(TestCase):
    def setUp(self):
        self.bufferpool = BufferPool()
        self.table = Table('test', 3, 0, self.bufferpool)

    def testInsert1(self):
        self.table.insert(10,20,30)
        vals = self.table.select(10, None)
        self.assertEqual([10,20,30], vals)

if __name__ == '__main__':
    unittest.main()
