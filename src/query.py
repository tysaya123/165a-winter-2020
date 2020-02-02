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
        #schema_encoding = '0' * self.table.num_columns
        self.table.insert(*columns)

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        self.table.select(key, query_columns)

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
        self.table.sum(start_range, end_range, aggregate_column_index)
