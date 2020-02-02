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
    :param name: string             #Table name
    :param num_columns: int         #Number of Columns: all columns are integer
    :param key: int                 #Index of table key in columns
    :param index: int               #???
    :param bufferpool: BufferPool   #Reference to the global bufferpool
    """
    def __init__(self, name, num_columns, key_index, index, bufferpool):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns
        self.index = index
        self.bufferpool = bufferpool

        # Maps rids -> pids
        self.rid_directory = {}
        # Maps base records to the tail records, rid -> rid
        self.indirection = {}

        # A reference to the current base pages and the tail page
        self.base_pages = [None] * num_columns
        self.tail_page = None

    def __merge(self):
        pass

    def insert(self, *columns):
        # TODO: Check if record already exists
        rid = rid_counter
        rid_counter += 1

        for index, base_page in enumerate(base_pages):

            # Check to see if base page for column has space. If not, allocate
            # new page, and update references to it.
            if not base_page.has_capacity():
                new_base_page = bufferpool.new_base_page()
                self.base_pages[index] = new_base_page

            # Write to the base page
            base_page.write(rid, columns)

        # Update page, indirection, index directories


    def select(self, key, query_columns):
        rid = index.get(column[0])
        pids = rid_directory[rid]

        # Result of the select
        vals = []

        # First, check to see if any of the columns have the dirty bit set
        # for this particular record.
        has_dirty_bit = False
        for pid in pids:
            page = bufferpool.get(pid)
            if page.has_dirty():
                has_dirty_bit = True
                break
            vals.append(page.read(rid))

        # If record has a dirty bit, pull the tail page, and get the values
        # from there. If not, then pull the values from the base pages.
        if has_dirty_bit:
            tail_page = rid_directory[indirection[rid]]
            vals = tail_page.read(rid)

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
