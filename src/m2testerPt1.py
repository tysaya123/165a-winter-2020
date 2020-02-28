from db import Database
from query import Query
import pickle
from time import process_time

from random import choice, randint, sample, seed

from os import path
import shutil
folder = path.expanduser("~/ECS165")
shutil.rmtree(folder, ignore_errors=True)

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)
first_key = 92106429
insert_time_0 = process_time()
for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
insert_time_1 = process_time()
print("Insert finished")
print("Inserting 1000 records took:  \t\t\t", insert_time_1 - insert_time_0)

select_time_0 = process_time()
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    # else
    #     print('select on', key, ':', record)
select_time_1 = process_time()
print("Select finished")
print("Selecting records took:  \t\t\t", select_time_1 - select_time_0)


update_time_0 = process_time()
for _ in range(10):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
            # else:
            #     print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
update_time_1 = process_time()
print("Update finished")
print("Updating records took:  \t\t\t", update_time_1 - update_time_0)

agg_time_0 = process_time()
for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    # else:
    #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
agg_time_1 = process_time()
print("Aggregate finished")
print("Aggregate took:\t", agg_time_1 - agg_time_0)

recs = {}
for key in keys:
    recs[key] = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns

with open("m2t1.pkl", "w+b") as f:
    pickle.dump(recs, f)

db.close()
