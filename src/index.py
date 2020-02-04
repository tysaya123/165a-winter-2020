from src.table import Table, Record
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table, num_columns, primary_key_index):
        self.table = table
        self.num_columns = num_columns
        self.prime_key = primary_key_index
        self.indices = [None] * self.num_columns
        self.indices_used = [0] * self.num_columns
        self.indices[self.prime_key] = {}
        self.indices_used[self.prime_key] = 1

    def prime_put(self, key, value):
        self.indices[self.prime_key][key] = value

    def prime_get(self, key):
        return self.indices[self.prime_key].get(key)

    def prime_delete(self, key):
        del(self.indices[self.prime_key][key])

    def append(self, col, key, value):
        if self.indices[col][key] is None: self.indices[col][key] = []
        self.indices[col][key].append(value)

    def get(self, col, key):
        return self.indices[col].get(key)

    def remove(self, col, key, value):
        self.indices[col][key].delete(value)
        if self.indices[col][key] == 0:
            del (self.indices[col][key])

    def delete(self, col, key, value):
        del (self.indices[col][key])
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if column_number == self.prime_key: return

        self.indices_used[column_number] = 1
        self.indices[column_number] = {}

        query_cols = [1 if x == column_number for x in range(self.num_columns)]
        keys = list(self.indices[self.prime_key].values())
        for key in keys:
            recs = self.table.select(key, query_cols)
            for rec in recs:
                self.append(column_number, rec.rid, rec.columns[column_number])



    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices_used[column_number] = 0
        self.indices[column_number] = None
