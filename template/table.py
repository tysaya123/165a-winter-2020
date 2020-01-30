from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, index, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = index
        self.bufferpool = bufferpool

    def __merge(self):
        pass

    def insert(self, *columns):
        # TODO: Check if record already exists
        rid = rid_counter
        rid_counter += 1


    def select(self, key, query_columns):
        rid = index.get(column[0])
        pids = page_directory[rid]

        has_dirty_bit = False
        for pid in pids:
            # If dirty bit, set has_dirty_bit to True, break
            pass

        if has_dirty_bit:
            # Pull tail page


    def delete(self, key):
        pass

    def update(self, key, *columns):
 
