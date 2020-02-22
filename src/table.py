INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

from index import Index
from page import BasePage

import pdb


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return str(self.columns)

    def __repr__(self):
        return 'Record{}'.format(self.columns)

    def __eq__(self, other):
        return self.columns == other.columns


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
        self.indexes = []
        for i in range(num_columns):
            self.indexes.append(Index())

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
        for i in range(self.num_columns):
            self.indexes[i].set(columns[i], rid)

        self.rid_directory[rid] = rids
        self.indirection[rid] = rid

    def select(self, key, column, query_columns):
        rids = self.indexes[column].get(key)
        if rids is None:
            return None

        # Stores all the records for all the rids given.
        records = []

        for i, rid in enumerate(rids):
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

            records.append(Record(rid, i, vals))

        return records

    def delete(self, key):
        base_rid = self.indexes[self.key_index].get(key)[0]
        if base_rid is None:
            return

        # Delete the key -> rid mappings for all of the indexes.
        values = self.select(key, self.key_index, [1] * self.num_columns)[0].columns
        for i, val in enumerate(values):
            self.indexes[i].delete(values[i], base_rid)
        pids = self.rid_directory[base_rid]

        # Mark all base records as deleted
        for pid in pids:
            curr_page = self.bufferpool.get(pid)
            curr_page.delete_record(base_rid)

        # Loop through the cycle of records
        curr_rid = base_rid
        while True:
            # Get the next rid and break if we are back at the start
            next_rid = self.indirection[curr_rid]
            #   Delete the indirection after using it
            self.indirection.pop(curr_rid)
            if next_rid == base_rid:
                break
            curr_rid = next_rid

            # Get the page of tail record
            curr_pid = self.rid_directory[curr_rid]
            curr_page = self.bufferpool.get(curr_pid)

            curr_page.delete_record(curr_rid)

    def update(self, key, *columns):
        rid = self.indexes[self.key_index].get(key)[0]
        if rid is None:
            return
        pids = self.rid_directory[rid]

        to_select = [1 if x is None else 0 for x in columns]
        values = self.select(key, self.key_index, to_select)[0].columns

        # Get all of the updated values into result, from either the passed
        # in variable columns or from the tail page.
        result = []
        for i in range(self.num_columns):
            if columns[i] is not None:
                result.append(columns[i])
            else:
                result.append(values[i])


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
        tail_page.new_record(tail_rid, result)

        # Delete old keys for the indexes.
        for i in range(self.num_columns):
            if columns[i] is not None and columns[i] != values[i]:
                self.indexes[i].set(columns[i], rid)
                self.indexes[i].delete(values[i], rid)

        # Update references for the indirection column, and rid_directory.
        self.indirection[tail_rid] = self.indirection[rid]
        self.indirection[rid] = tail_rid
        self.rid_directory[tail_rid] = self.tail_page_pid

    def sum(self, start_range, end_range, aggregate_column):
        result = 0

        for i in range(start_range, end_range + 1):
            compr = [1 if x == aggregate_column else 0 for x in range(self.num_columns)]
            vals = self.select(i, self.key_index, compr)
            if vals is not None:
                result += vals[0].columns[aggregate_column]

        return result
