from db import Database
from query import Query

from random import choice, randint, sample, seed

import pickle

# Student Id and 4 grades
db = Database()
db.open('~/ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)

# repopulate with random data
# records2 = {}
# seed(3562901)
# for i in range(0, 1000):
#     key = 92106429 + i
#     records2[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
#     print(records2[key])
# keys = sorted(list(records2.keys()))
# for _ in range(10):
#     for key in keys:
#         for j in range(1, grades_table.num_columns):
#             value = randint(0, 20)
#             records2[key][j] = value
# keys = sorted(list(records2.keys()))
# for key in keys:
#     print(records2[key])
#     print(records2[key])

with open('m2t1.pkl', "r+b") as f:
    records = pickle.load(f)

for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    if record.columns != records[key]:
        error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
print("Select finished")

keys = list(records.keys())
deleted_keys = sample(keys, 100)
for key in deleted_keys:
    query.delete(key)
    records.pop(key, None)

for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
print("Aggregate finished")

db.close()
