from table import Table
from bufferpool import BufferPool
from os import listdir, path, mkdir
import pickle
import glob

class Database():

    def __init__(self):
        self.tables = {}
        self.bufferpool = None
        self.folder = None

    def open(self, folder):
        self.folder = path.expanduser(folder)
        if not path.isdir(self.folder):
            mkdir(self.folder, 0o777)
            self.bufferpool = BufferPool(self.folder)
            return
        self.open_bufferpool()
        self.open_tables()

    def open_bufferpool(self):
        file = path.join(self.folder, 'bufferpool.pkl')
        with open( file, 'r') as f:
            bufferpool = BufferPool(self.folder)
            pkl = pickle.load(f)
            bufferpool.dump(pkl)
            self.bufferpool = bufferpool

    def open_tables(self):
        files = glob(path.join(self.folder, '*_table.pkl'))
        for file in files:
            with open(path.join(folder, file), 'r') as f:
                table = Table(self.bufferpool)
                pkl = pickle.load(f)
                table.dump(pkl)
                self.tables[table.name] = table

    def close(self):
        self.close_bufferpool()
        self.close_tables()

    def close_bufferpool(self):
        with open(path.join(self.folder, 'bufferpool.pkl'), 'wb') as f:
            bufferpool.flush_all()
            pkl = self.bufferpool.dump()
            f.write(pkl)

    def close_tables(self):
        for name, table in self.tables.items():
            with open(path.join(self.folder, name + '_table.pkl'), 'wb') as f:
                pkl = table.dump()
                f.write(pkl)
    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(self.bufferpool, name, num_columns, key)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name]
