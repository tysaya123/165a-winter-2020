INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

from page import BasePage

import pdb

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string             #Table name
    :param num_columns: int         #Number of Columns: all columns are integer
    :param key: int                 #Index of table key in columns
    :param bufferpool: BufferPool   #Reference to the global bufferpool
    """
    def __init__(self, name, num_columns, key_index, bufferpool):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns
        self.bufferpool = bufferpool

        # Maps keys -> rids
        self.index = {}

        # Maps rids -> pids
        self.rid_directory = {}
        # Maps base records to the tail records, rid -> rid
        self.indirection = {}

        # A reference to the current base pages and the tail page, stores pids
        self.base_pages = [None] * num_columns
        self.tail_page = None

        self.rid_counter = 1

        # Initialize base pages
        for i in range(self.num_columns):
            self.base_pages[i] = bufferpool.new_base_page()

    def __merge(self):
        pass

    def insert(self, *columns):
        # TODO: Check if record already exists
        rid = self.rid_counter
        self.rid_counter += 1

        self.rid_directory[rid] = [None] * self.num_columns
        rids = []

        for index, base_page in enumerate(self.base_pages):

            page = self.bufferpool.get(base_page)

            # Check to see if base page for column has space. If not, allocate
            # new page, and update references to it.
            if not page.has_capacity():
                new_pid = self.bufferpool.new_base_page()
                self.base_pages[index] = new_pid

            # Write to the base page
            page_id = self.base_pages[index]
            base_page = self.bufferpool.get(page_id)
            base_page.new_record(rid, columns[index], 0)

            # Create reference from the record id to the page for it
            rids.append(page_id)

        # Update page, rid_directory, indirection, index directories
        self.index[columns[0]] = rid
        self.rid_directory[rid] = rids


    def select(self, key, query_columns):
        rid = self.index[key]
        pids = self.rid_directory[rid]

        # Result of the select
        vals = []

        # First, check to see if any of the columns have the dirty bit set
        # for this particular record.
        has_dirty_bit = False
        for pid in pids:
            page = self.bufferpool.get(pid)

            # Check the record to see if the dirty bit is 1.
            # TODO: Throw this back in
            if False:#page.get_dirty(rid):
                has_dirty_bit = True
                break
            vals.append(page.read(rid)[1])

        # If record has a dirty bit, pull the tail page, and get the values
        # from there. If not, then pull the values from the base pages.
        if has_dirty_bit:
            # TODO: Check whether this actually gets proper values or not.
            tail_page = self.bufferpool.get(self.rid_directory[self.indirection[rid]])
            vals = tail_page.read(rid)[1]

        return vals


    def delete(self, key):
        rid = index.get(key)
        pids = rid_directory[rid]

        # TODO: Rest of delete

    def update(self, key, *columns):
        values = self.select(key, *columns)

        rid = index.get(key)
        pids = rid_directory[rid]

        for pid in pids:
            page = bufferpool.get(pid)
            # Write dirty bit to page
            # page.write(/* */)

        if not tail_page.has_capacity():
            pass
            # Allocate new tail page, update references

        # Write to tail page
