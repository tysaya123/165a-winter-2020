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
        rids_to_lock = {}
        for query, args in self.queries:
            if query.__name__ == "select":
                # new_args = args.append(locked_rids)
                query.__self__.table.select_lock(args[0], args[1], rids_to_lock)
            elif query.__name__ == "increment":
                # new_args = args.append(locked_rids)
                query.__self__.table.increment_lock(args[0], rids_to_lock)
            elif query.__name__ == "update":
                # new_args = args.append(locked_rids)
                query.__self__.table.update_lock(args[0], rids_to_lock)
            elif query.__name__ == "sum":
                query.__self__.table.sum_lock(args[0], args[1], args[2], rids_to_lock)
            elif query.__name__ == "insert":
                # Should not need to lock anything for any locks
                pass
            else:
                print("NONE WERE CALLED")
                print(query.__name__ )
        if not self.queries[0][0].__self__.table.grab_locks(rids_to_lock):
            self.abort()
        for query, args in self.queries:
            # print(query.__name__)
            query(*args)
        return self.commit(self.queries[0][0], rids_to_lock)

    def abort(self):
        return False

    def commit(self, query, locked_rids):
        query.__self__.table.release_locks(locked_rids)
        return True
