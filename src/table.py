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
        self.base_page_pids = [None] * num_columns
        self.tail_page_pid = None

        self.rid_counter = 1

        # Initialize the tail page
        self.tail_page_pid = bufferpool.new_tail_page(self.num_columns)

        # Initialize base pages
        for i in range(self.num_columns):
            self.base_page_pids[i] = bufferpool.new_base_page()

    def new_rid(self):
        self.rid_counter += 1
        return self.rid_counter - 1

    def __merge(self):
        pass

    def insert(self, *columns):
        # TODO: Check if record already exists
        rid = self.new_rid()

        self.rid_directory[rid] = [None] * self.num_columns
        rids = []

        for i, base_page in enumerate(self.base_page_pids):

            page = self.bufferpool.get(base_page)

            # Check to see if base page for column has space. If not, allocate
            # new page, and update references to it.
            if not page.has_capacity():
                new_pid = self.bufferpool.new_base_page()
                self.base_page_pids[i] = new_pid

            # Write to the base page
            page_id = self.base_page_pids[i]
            base_page = self.bufferpool.get(page_id)
            base_page.new_record(rid, columns[i], 0)

            # Create reference from the record id to the page for it
            rids.append(page_id)

        # Update page, rid_directory, indirection, index directories
        self.index[columns[self.key_index]] = rid
        self.rid_directory[rid] = rids
        self.indirection[rid] = rid


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
            if page.get_dirty(rid):
                has_dirty_bit = True
                break
            vals.append(page.read(rid)[0])

        # If record has a dirty bit, pull the tail page, and get the values
        # from there. If not, then pull the values from the base pages.
        if has_dirty_bit:
            # TODO: Check whether this actually gets proper values or not.
            tail_rid = self.indirection[rid]
            tail_page = self.bufferpool.get(self.rid_directory[tail_rid])
            vals = list(tail_page.read(tail_rid))

        return vals


    def delete(self, key):
        rid = self.index.get(key)
        pids = rid_directory[rid]

        # TODO: Rest of delete

    def update(self, key, *columns):
        # TODO: Make sure that right columns are selected
        values = self.select(key, None)

        rid = self.index[key]
        pids = self.rid_directory[rid]

        # Set the dirty bits to 1 for the entire record.
        # TODO: Set the dirty bits only to the columns we're changing.
        for pid in pids:
            page = self.bufferpool.get(pid)
            page.set_dirty(rid, 1)

        tail_page = self.bufferpool.get(self.tail_page_pid)

        # Allocate new tail page, update references
        if not tail_page.has_capacity():
            new_pid = self.bufferpool.new_tail_page(self.num_columns)
            self.tail_page_pid = new_pid
            tail_page = self.bufferpool.get(new_pid)

        # Create new record in tail page.
        tail_rid = self.new_rid()
        tail_page.new_record(tail_rid, columns)

        # TODO: Delete old key in index
        # TODO: Optionally update key, if the key is changed
        self.index[columns[self.key_index]] = rid

        # Update references for the indirection column, and rid_directory.
        self.indirection[tail_rid] = self.indirection[rid]
        self.indirection[rid] = tail_rid
        self.rid_directory[tail_rid] = self.tail_page_pid

    def sum(self, start_range, end_range, aggregate_column):
        pass
