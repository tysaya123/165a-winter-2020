import unittest
import logging
from unittest import TestCase
from random import randrange

import pdb

from page import BasePage, TailPage
from table import Table, Record
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

        self.assertEqual([self.max_int, 0], self.base_page.read(self.max_rid))
        self.assertEqual(tail_values, self.tail_page.read(self.max_rid))

        self.base_page.new_record(self.min_rid, self.min_int, 0)
        tail_values = self.num_tail_cols * [self.min_int]
        self.tail_page.new_record(self.min_rid, tail_values)

        self.assertEqual([self.max_int, 0], self.base_page.read(self.max_rid))
        self.assertEqual(tail_values, self.tail_page.read(self.min_rid))

    def testBaseCapacity(self):
        for i in range(self.base_page.get_max()):
            self.assertEqual(True, self.base_page.has_capacity())
            self.base_page.new_record(randrange(self.max_rid + 1), randrange(self.min_int, self.max_int + 1),
                                      randrange(2))
        self.assertEqual(False, self.base_page.has_capacity())

    def testTailCapacity(self):
        for i in range(self.tail_page.get_max()):
            self.assertEqual(True, self.tail_page.has_capacity())
            self.tail_page.new_record(randrange(self.max_rid + 1), 5 * [randrange(self.min_int, self.max_int + 1)])
        self.assertEqual(False, self.tail_page.has_capacity())

    def testDeleteRecord(self):
        self.base_page.new_record(self.max_rid, self.max_int, 0)
        tail_values = self.num_tail_cols * [self.max_int]
        self.tail_page.new_record(self.max_rid, tail_values)

        self.base_page.delete_record(self.max_rid)
        self.tail_page.delete_record(self.max_rid)

        self.assertEqual(None, self.base_page.read(self.max_rid))
        self.assertEqual(None, self.tail_page.read(self.max_rid))


class TestTableMethods(TestCase):
    def setUp(self):
        self.bufferpool = BufferPool()
        self.table = Table('test', 3, 0, self.bufferpool)

    def testInsert1(self):
        self.table.insert(10, 20, 30)
        recs = self.table.select(10, 3 * [1])
        self.assertEqual([[10, 20, 30]], [x.columns for x in recs])

    def testInsert2(self):
        self.table.insert(10, 20, 30)
        self.table.insert(15, 25, 35)
        recs1 = self.table.select(10, 3 * [1])
        recs2 = self.table.select(15, 3 * [1])

        self.assertEqual([[10, 20, 30]], [x.columns for x in recs1])
        self.assertEqual([[15, 25, 35]], [x.columns for x in recs2])

    def testDelete(self):
        self.table.insert(1, 2, 3)
        self.table.insert(15, 25, 35)

        self.table.update(1, 5, 3, 4)

        self.table.delete(5)

        self.assertEqual(None, self.table.select(1, 3 * [1]))
        self.assertEqual(None, self.table.select(5, 3 * [1]))

    def testInsertBig(self):
        NUM_RECORDS = 10000
        for i in range(NUM_RECORDS):
            self.table.insert(i, i * 2, i * 3)

        for i in range(NUM_RECORDS):
            recs = self.table.select(i, 3 * [1])
            self.assertEqual([[i, i * 2, i * 3]], [x.columns for x in recs])

    def testUpdate(self):
        self.table.insert(1, 2, 3)
        self.table.update(1, 5, 3, 4)
        recs = self.table.select(5, 3 * [1])
        self.assertEqual([[5, 3, 4]], [x.columns for x in recs])

    def testUpdateBig(self):
        NUM_RECORDS = 10000
        for i in range(1, NUM_RECORDS):
            self.table.insert(i, i * 10, i * 20)

        for i in range(1, NUM_RECORDS):
            self.table.update(i, i + NUM_RECORDS, i * 20, i * 30)

        for i in range(1, NUM_RECORDS):
            recs = self.table.select(i + NUM_RECORDS, 3 * [1])
            self.assertEqual([[i + NUM_RECORDS, i * 20, i * 30]], [x.columns for x in recs])

    def testKeyColumn(self):
        bufferpool = BufferPool()
        table = Table('test', 3, 1, self.bufferpool)
        table.insert(2, 1, 3)
        table.insert(2, 2, 12)

        recs1 = table.select(1, 3 * [1])
        recs2 = table.select(2, 3 * [1])

        self.assertEqual([[2, 1, 3]], [x.columns for x in recs1])
        self.assertEqual([[2, 2, 12]], [x.columns for x in recs2])

    def testSum(self):
        self.table.insert(1, 2, 3)
        self.table.insert(2, 2, 3)
        self.table.insert(3, 2, 4)

        self.table.update(3, 4, 4, 4)

        val = self.table.sum(1, 4, 1)

        self.assertEqual(val, 8)


if __name__ == '__main__':
    unittest.main()
