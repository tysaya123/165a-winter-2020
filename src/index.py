from table import Table

class Index:

    def __init__(self, table):
        #Maps key to rid
        self.index = {}
    """
    # get function finds key in index dictionary, returns list of rids
    """
    def get(self, key):
        return self.index[key]

    """
        # set function appends the rid to a list of rids from given key
    """
    def set(self, key, rid):
        self.index[key].append(rid)

    """
        # delete function finds given key and removes using pop function
    """
    def delete(self, key):
        for i in self.index[key]:
            if i == key:
                self.index.pop(key)

"""
    def locate(self, value):

        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):

        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass
"""
