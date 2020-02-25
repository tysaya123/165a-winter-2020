from table import Table
from bufferpool import BufferPool
from os import listdir, path, mkdir
from glob import glob
import pickle


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
        bufferpool = BufferPool(self.folder)
        file = self.folder + 'bufferpool.pkl'
        if path.isfile(file):
            with open(file, 'r') as f:
                pkl = pickle.load(f)
                bufferpool.dump(pkl)
        self.bufferpool = bufferpool

    def open_tables(self):
        files = glob(path.join(self.folder, '*_table.pkl'))
        for file in files:
            with open(file, 'rb') as f:
                table = Table(self.bufferpool)
                pkl = f.read()
                table.load(pkl)
                self.tables[table.name] = table

    def close(self):
        # TODO join merges
        self.close_bufferpool()
        self.close_tables()

    def close_bufferpool(self):
        with open(self.folder + '/' + 'bufferpool.pkl', 'wb') as f:
            self.bufferpool.flush_all()
            pkl = self.bufferpool.dump()
            f.write(pkl)

    def close_tables(self):
        for name, table in self.tables.items():
            with open(self.folder + '/' + name + '_table.pkl', 'wb') as f:
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


    """
    # Return an existing table
    :param name: string         #Table name
    """

    def get_table(self, name):
        return self.tables[name]
