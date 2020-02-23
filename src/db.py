from table import Table
from bufferpool import BufferPool

class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = BufferPool()

    def open(self):
        pass

    def close(self):
        self.bufferpool.close_file()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferpool)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name]
