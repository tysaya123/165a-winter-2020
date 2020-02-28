import shutil
import timeit
from os import path

from db import Database
from query import Query

folder = path.expanduser('~/ECS165')
shutil.rmtree(folder, ignore_errors=True)
    
db = None
grades_table = None

def reset():
    global db, grades_table
    folder = path.expanduser('~/ECS165')
    shutil.rmtree(folder, ignore_errors=True)
    
    db = Database()
    db.open('~/ECS165')
    
    grades_table = db.create_table('Grades', 5, 0)


def insert_many():
    global db, grades_table
    reset()
    for i in range(10000):
        grades_table.insert(i, 1, 2, 3, 4)
    db.close()

def select_many():
    global db, grades_table
    for i in range(10000):
        grades_table.insert(i, 1, 2, 3, 4)


def update_many():
    global db, grades_table
    for i in range(1000):
        grades_table.update(i, None, None, None, None, 5)


if __name__=='__main__':
    setup = 'from __main__ import insert_many, select_many, update_many'
    print(timeit.timeit('insert_many()', setup=setup, number=1))

    reset()
    for i in range(10000):
        grades_table.insert(i, 1, 2, 3, 4)
    print(timeit.timeit('select_many()', setup=setup, number=10))
    print(timeit.timeit('update_many()', setup=setup, number=1))
    print(timeit.timeit('select_many()', setup=setup, number=10))
