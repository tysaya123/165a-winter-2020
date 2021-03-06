import logging
import pickle
import struct
import unittest
import shutil

from os import path, remove
from random import randrange, randint
from unittest import TestCase
from multiprocessing import Queue

import pdb

from page import BasePage, TailPage
from table import Table, Record
from bufferpool import BufferPool
from query import Query
from db import Database
from config import *


# class TestNewBuff(TestCase):
#     def setUp(self):
#         self.folder = path.expanduser("~")
#
#     def tearDown(self):
#         remove(path.join(self.folder, 'memory_file.txt'))
#
#     def testNewPage(self):
#         q = Queue()
#         bp = BufferPool([q], self.folder)
#         q.put([BufferPoolCalls.WRITE_NEW_BASE_PAGE, 10])
#         bp.serveNextInQueue()
#         out = q.get()
#         self.assertIsInstance(out, BasePage)
#         self.assertEqual(out.num_records, 0)
#
#         q.put([BufferPoolCalls.WRITE_NEW_TAIL_PAGE, 10, 4])
#         bp.serveNextInQueue()
#         out = q.get()
#         self.assertIsInstance(out, TailPage)
#         self.assertEqual(out.num_records, 0)
#         self.assertEqual(out.num_cols, 4)
#
#     def testWritePage(self):
#         q = Queue()
#         bp = BufferPool([q], self.folder)
#
#         # Make a new base page
#         q.put([BufferPoolCalls.WRITE_NEW_BASE_PAGE, 10])
#         bp.serveNextInQueue()
#         out: BasePage = q.get()
#         out.new_record(11, 13, 1)
#
#         # Close the base page
#         q.put([BufferPoolCalls.CLOSE_WRITE_PAGE, 10, out])
#         bp.serveNextInQueue()
#
#         # Open it again
#         q.put([BufferPoolCalls.WRITE_BASE_PAGE, 10])
#         bp.serveNextInQueue()
#         out: BasePage = q.get()
#         self.assertTrue(out.read(11), [13, 1])
#
#         # Change a value
#         out.set_dirty(11, 0)
#         q.put([BufferPoolCalls.CLOSE_WRITE_PAGE, 10, out])
#         bp.serveNextInQueue()
#
#         q.put([BufferPoolCalls.WRITE_BASE_PAGE, 10])
#         bp.serveNextInQueue()
#         out: BasePage = q.get()
#         self.assertTrue(out.read(11), [13, 0])
#
#
#
#
#
#     def testClosePage(self):
#         folder = path.expanduser("~")
#         q = Queue()
#         bp = BufferPool([q], folder)
#         q.put([BufferPoolCalls.WRITE_NEW_BASE_PAGE, 10])
#         bp.serveNextInQueue()
#         out = q.get()
#         q.put([BufferPoolCalls.CLOSE_WRITE_PAGE, 10, out])
#         bp.serveNextInQueue()
#         page_rep = bp.getRidInPool(10)
#         self.assertEqual(page_rep.get_write(), 0)
#         self.assertEqual(page_rep.get_read(), 0)


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

        self.assertRaises(KeyError, self.base_page.read, self.max_rid)
        self.assertRaises(KeyError, self.tail_page.read, self.max_rid)

    def testPack(self):
        self.base_page.new_record(1, 10, 0)
        self.base_page.new_record(2, 50, 1)

        byte_rep = self.base_page.pack()
        val = struct.pack(self.base_page.record_format, 1, 10, 0) + struct.pack(self.base_page.record_format, 2, 50, 1)
        val = val.ljust(4096, b'\00')

        self.assertEqual(byte_rep, val)

    def testUnpack(self):
        self.base_page.new_record(1, 10, 0)
        self.base_page.new_record(2, 50, 1)

        data = bytes()
        data += struct.pack('>Lqb', 5, 50, 1)
        data += struct.pack('>Lqb', 10, 100, 0)
        data = data.ljust(4096, b'\0')

        self.base_page.unpack(data)

        self.assertEqual(self.base_page.read(5), [50, 1])
        self.assertEqual(self.base_page.read(10), [100, 0])

    def testPackUnpack(self):
        self.base_page.new_record(1, 10, 0)
        self.base_page.new_record(2, 50, 1)

        page = BasePage()
        page.unpack(self.base_page.pack())

        self.assertEqual(self.base_page, page)


class TestTableMethods(TestCase):
    def setUp(self):

        folder = path.expanduser("~")
        self.bufferpool = BufferPool(folder)
        self.table = Table(self.bufferpool, 'test', 3, 1)

    def tearDown(self):
        self.bufferpool.close_file()
        folder = path.expanduser("~")
        remove(path.join(folder, 'memory_file.txt'))

    def testInsert1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

    def testSelect1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        vals1 = self.table.select(2, 1, [1] * 3)
        truth1 = [Record(0, 0, [2, 2, 3])]

        vals2 = self.table.select(2, 0, [1] * 3)
        truth2 = [Record(0, 0, [2, 2, 3]), Record(0, 0, [2, 3, 4]), Record(0, 0, [2, 4, 4])]

        self.assertEqual(vals1, truth1)
        self.assertEqual(vals2, truth2)

    def testUpdate1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        self.table.update(2, None, 2, 5)
        self.table.update(4, 2, 5, None)

        vals2 = self.table.select(2, 0, [1] * 3)
        truth2 = [Record(0, 0, [2, 2, 5]), Record(0, 0, [2, 3, 4]), Record(0, 0, [2, 5, 4])]

        self.assertEqual(vals2, truth2)

    def testDelete1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        self.table.delete(2)

        vals2 = self.table.select(2, 0, [1] * 3)
        truth2 = [Record(0, 0, [2, 3, 4]), Record(0, 0, [2, 4, 4])]

        self.assertEqual(vals2, truth2)

    def testSum1(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        vals1 = self.table.sum(1, 5, 0)
        vals2 = self.table.sum(1, 5, 1)
        vals3 = self.table.sum(1, 5, 2)

        self.assertEqual(vals1, 6)
        self.assertEqual(vals2, 9)
        self.assertEqual(vals3, 11)

    def testMerge1(self):
        self.table.insert(1, 1, 1)
        for i in range(1000):
            self.table.update(1, i, None, i + 1)
        self.table.start_merge_once()

        vals = self.table.select(1, 1, [1] * 3)
        truth = [Record(0, 0, [999, 1, 1000])]

        self.assertEqual(vals, truth)

    def testMerge2(self):
        NUM_INSERTS = 300
        for i in range(NUM_INSERTS):
            self.table.insert(i, i, i)

        for i in range(NUM_INSERTS):
            self.table.update(i, i + 1, i + (NUM_INSERTS * 10), i + 2)

        for i in range(100):
            self.table.start_merge_once()

        vals = []
        for i in range(NUM_INSERTS):
            vals += self.table.select(i + (NUM_INSERTS * 10), 1, [1] * 3)

        truths = []
        for i in range(NUM_INSERTS):
            truths.append(Record(0, 0, [i + 1, i + (NUM_INSERTS * 10), i + 2]))

        self.assertEqual(vals, truths)

    def testPickle(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        vals1 = self.table.sum(1, 5, 0)
        vals2 = self.table.sum(1, 5, 1)
        vals3 = self.table.sum(1, 5, 2)

        data = self.table.dump()

        table2 = Table(bufferpool=self.bufferpool)
        table2.load(data)

        vals11 = table2.sum(1, 5, 0)
        vals12 = table2.sum(1, 5, 1)
        vals13 = table2.sum(1, 5, 2)

        self.assertEqual(self.table, table2)
        self.assertEqual(vals1, vals11)
        self.assertEqual(vals2, vals12)
        self.assertEqual(vals3, vals13)

    def testPickle2(self):
        NUM_INSERTS = 300
        for i in range(NUM_INSERTS):
            self.table.insert(i, i, i)

        for i in range(NUM_INSERTS):
            self.table.update(i, i + 1, i + (NUM_INSERTS * 10), i + 2)

        data = self.table.dump()

        table2 = Table(bufferpool=self.bufferpool)
        table2.load(data)

        self.assertEqual(self.table, table2)

        for i in range(100):
            self.table.start_merge_once()
            table2.start_merge_once()

        vals = []
        vals2 = []
        for i in range(NUM_INSERTS):
            vals += self.table.select(i + (NUM_INSERTS * 10), 1, [1] * 3)
            vals2 += table2.select(i + (NUM_INSERTS * 10), 1, [1] * 3)

        truths = []
        for i in range(NUM_INSERTS):
            truths.append(Record(0, 0, [i + 1, i + (NUM_INSERTS * 10), i + 2]))

        self.assertEqual(vals, truths)
        self.assertEqual(vals2, truths)

    def testInsertLocks(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        locked1 = {}
        locked2 = {}
        self.assertTrue(self.table.select_lock(4, 2, locked1))
        self.assertTrue(self.table.select_lock(3, 1, locked2))

        self.assertEqual(self.table.num_pins(), 3)

        self.table.release_locks(locked1)

        self.assertEqual(self.table.num_pins(), 1)

        self.table.release_locks(locked2)

        self.assertEqual(self.table.num_pins(), 0)

    def testDeleteLocks(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        locked = {}
        self.assertTrue(self.table.delete_lock(3, locked))

        self.assertEqual(self.table.num_pins(), 1)

        self.assertFalse(self.table.select_lock(2, 0, {}))
        self.assertFalse(self.table.delete_lock(3, {}))

        self.assertEqual(self.table.num_pins(), 1)

        self.table.release_locks(locked)

        self.assertEqual(self.table.num_pins(), 0)

    def testUpdateLocks(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        locked = {}
        self.assertTrue(self.table.update_lock(3, locked))

        self.assertEqual(self.table.num_pins(), 1)

        self.assertFalse(self.table.select_lock(2, 0, {}))
        self.assertFalse(self.table.update_lock(3, {}))

        self.assertEqual(self.table.num_pins(), 1)

        self.table.release_locks(locked)

        self.assertEqual(self.table.num_pins(), 0)

    def testUpdateLocks(self):
        self.table.insert(2, 2, 3)
        self.table.insert(2, 3, 4)
        self.table.insert(2, 4, 4)

        locked = {}
        self.assertTrue(self.table.sum_lock(1, 4, 1, locked))

        self.assertEqual(self.table.num_pins(), 3)

        self.table.release_locks(locked)

        self.assertEqual(self.table.num_pins(), 0)


class TestBufferPoolMethods(TestCase):
    def setUp(self):
        folder = path.expanduser("~")
        self.bufferpool = BufferPool(folder)

    def tearDown(self):
        self.bufferpool.close_file()
        folder = path.expanduser("~")
        remove(path.join(folder, 'memory_file.txt'))

    def testNewBasePage(self):
        pid = self.bufferpool.new_base_page()
        page = self.bufferpool.get_base_page(pid)
        page.new_record(0, 1, 0)
        self.bufferpool.close_page(pid)

        self.assertEqual(page.read(0), [1, 0])

    def testNewTailPage(self):
        pid = self.bufferpool.new_tail_page(3)
        page = self.bufferpool.get_tail_page(pid, 3)
        page.new_record(0, [1, 2, 3])
        self.bufferpool.close_page(pid)

        self.assertEqual(page.read(0), [1, 2, 3])

    def testManyBaseRecords(self):
        vals = []
        page_pids = []
        for i in range(1, 10000):
            vals.append([i, randint(0, 1000), randint(0, 1)])

        pid = self.bufferpool.new_base_page()
        page = self.bufferpool.get_base_page(pid)

        for val in vals:
            if not page.has_capacity():
                self.bufferpool.close_page(pid)
                pid = self.bufferpool.new_base_page()
                page = self.bufferpool.get_base_page(pid)
            page.new_record(*val)
            page_pids.append(pid)

        self.bufferpool.close_page(pid)

        for i, val in enumerate(vals):
            page = self.bufferpool.get_base_page(page_pids[i])
            page_vals = page.read(val[0])
            self.bufferpool.close_page(page_pids[i])
            self.assertEqual(page_vals, val[1:])

    def testManyTailRecords(self):
        vals = []
        page_pids = []
        for i in range(1, 10000):
            vals.append([i, randint(0, 1000), randint(0, 50), randint(0, 100)])

        pid = self.bufferpool.new_tail_page(3)
        page = self.bufferpool.get_tail_page(pid, 3)

        for val in vals:
            if not page.has_capacity():
                self.bufferpool.close_page(pid)
                pid = self.bufferpool.new_tail_page(3)
                page = self.bufferpool.get_tail_page(pid, 3)
            page.new_record(val[0], val[1:])
            page_pids.append(pid)

        self.bufferpool.close_page(pid)

        for i, val in enumerate(vals):
            page = self.bufferpool.get_tail_page(page_pids[i], 3)
            page_vals = page.read(val[0])
            self.bufferpool.close_page(page_pids[i])
            self.assertEqual(page_vals, val[1:])


class TestDbMethods(TestCase):
    def setUp(self):
        self.db = Database()
        folder = path.expanduser("~/test")
        shutil.rmtree(folder, ignore_errors=True)
        self.db.open("~/test")

    def tearDown(self):
        self.db.close()
        folder = path.expanduser("~/test")
        shutil.rmtree(folder, ignore_errors=True)

    def testTwoTables(self):
        table1 = self.db.create_table('table1', 5, 2)
        table2 = self.db.create_table('table2', 7, 4)

        query1 = Query(table1)
        query2 = Query(table2)

        query1.insert(1, 2, 3, 4, 5)
        query2.insert(11, 12, 13, 14, 15, 16, 17)

        vals1 = query1.select(3, 2, [1] * 5)[0].columns
        vals2 = query2.select(15, 4, [1] * 7)[0].columns

        self.assertEqual(vals1, [1, 2, 3, 4, 5])
        self.assertEqual(vals2, [11, 12, 13, 14, 15, 16, 17])


class TestDbOpenClose(TestCase):
    def testTwoTables(self):
        # Open the db and insert a few rows
        folder = path.expanduser("~/test")
        shutil.rmtree(folder, ignore_errors=True)
        db = Database()
        db.open("~/test")

        table1 = db.create_table('table1', 5, 2)
        table2 = db.create_table('table2', 7, 4)

        query1 = Query(table1)
        query2 = Query(table2)

        query1.insert(1, 2, 3, 4, 5)
        query2.insert(11, 12, 13, 14, 15, 16, 17)

        vals1 = query1.select(3, 2, [1] * 5)[0].columns
        vals2 = query2.select(15, 4, [1] * 7)[0].columns

        self.assertEqual(vals1, [1, 2, 3, 4, 5])
        self.assertEqual(vals2, [11, 12, 13, 14, 15, 16, 17])

        db.close()

        db = None

        # Open the db again and perform the same asserts as before to check they are still there
        db = Database()
        folder = path.expanduser("~/test")
        db.open("~/test")

        table1 = db.get_table('table1')
        table2 = db.get_table('table2')

        query1 = Query(table1)
        query2 = Query(table2)

        vals1 = query1.select(3, 2, [1] * 5)[0].columns
        vals2 = query2.select(15, 4, [1] * 7)[0].columns

        self.assertEqual(vals1, [1, 2, 3, 4, 5])
        self.assertEqual(vals2, [11, 12, 13, 14, 15, 16, 17])

        db.close()
        shutil.rmtree(folder, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
