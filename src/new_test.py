import unittest
import logging
from unittest import TestCase
from random import randrange

import pdb

from page import BasePage, TailPage
from table import Table, Record
from bufferpool import BufferPool
from query import Query
from db import Database
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
        self.table = Table('test', 3, 1, self.bufferpool)

    def test_insert1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

    def test_select1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        vals1 = self.table.select(2, 1, [1]*3)
        truth1 = [Record(0, 0, [2, 2, 3])]

        vals2 = self.table.select(2, 0, [1]*3)
        truth2 = [Record(0, 0, [2,2,3]), Record(0, 0, [2,3,4]), Record(0, 0, [2, 4, 4])]

        self.assertEqual(vals1, truth1)
        self.assertEqual(vals2, truth2)

    def test_update1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        self.table.update(2, None, 2, 5)
        self.table.update(4, 2, 5, None)

        vals2 = self.table.select(2, 0, [1]*3)
        truth2 = [Record(0, 0, [2,2,5]), Record(0, 0, [2,3,4]), Record(0, 0, [2, 5, 4])]

        self.assertEqual(vals2, truth2)

    def test_delete1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        self.table.delete(2)

        vals2 = self.table.select(2, 0, [1]*3)
        truth2 = [Record(0, 0, [2,3,4]), Record(0, 0, [2, 4, 4])]

        self.assertEqual(vals2, truth2)

    def test_sum1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        vals1 = self.table.sum(1, 5, 0)
        vals2 = self.table.sum(1, 5, 1)
        vals3 = self.table.sum(1, 5, 2)

        self.assertEqual(vals1, 6)
        self.assertEqual(vals2, 9)
        self.assertEqual(vals3, 11)

class TestDbMethods(TestCase):

    def testTwoTables(self):
        db = Database()
        table1 = db.create_table('table1', 5, 2)
        table2 = db.create_table('table2', 7, 4)

        query1 = Query(table1)
        query2 = Query(table2)

        query1.insert(1, 2, 3, 4, 5)
        query2.insert(11,12,13,14,15,16,17)

        vals1 = query1.select(3, [1]*5)[0].columns
        vals2 = query2.select(15, [1]*7)[0].columns

        self.assertEqual(vals1, [1,2,3,4,5])
        self.assertEqual(vals2, [11,12,13,14,15,16,17])


if __name__ == '__main__':
    unittest.main()
