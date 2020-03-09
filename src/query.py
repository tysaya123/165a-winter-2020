from table import Table, Record
from index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        self.table.delete(key)

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        self.table.insert(*columns)

    """
    # Read a record with specified key
    """

    def select(self, key, column, query_columns):
        return self.table.select(key, column, query_columns)

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        self.table.update(key, *columns)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum(start_range, end_range, aggregate_column_index)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
