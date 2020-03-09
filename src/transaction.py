from table import Table, Record
from index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.rids = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        locked_rids = {}
        for query, args in self.queries:
            if query.__name__ == "select":
                # new_args = args.append(locked_rids)
                query.__self__.table.select_lock(args[0], args[1], locked_rids)
            elif query.__name__ == "increment":
                # new_args = args.append(locked_rids)
                query.__self__.table.increment_lock(args[0], locked_rids)
            elif query.__name__ == "update":
                # new_args = args.append(locked_rids)
                query.__self__.table.update_lock(args[0], locked_rids)
            else:
                print("NONE WERE CALLED")
                print(query.__name__ )
            # If the query has failed the transaction should abort
        for query, args in self.queries:
            # print(query.__name__)
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(query, locked_rids)
        return self.commit(self.queries[0][0], locked_rids)

    def abort(self, query, locked_rids):
        query.__self__.table.release_locks(locked_rids)
        return False

    def commit(self, query, locked_rids):
        query.__self__.table.release_locks(locked_rids)
        return True
