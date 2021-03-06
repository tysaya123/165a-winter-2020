import logging
import struct

from config import *

logging.basicConfig(level=logging.DEBUG)


# https://stackoverflow.com/questions/9940859/fastest-way-to-pack-a-list-of-floats-into-bytes-in-python

class Page:
    def __init__(self):
        # add modified bit
        self.num_records = 0
        self.records = {}
        self.record_size = None
        self.record_format = None
        self.max_records = None
        self.dirty = False

    def has_capacity(self):
        return self.num_records < self.max_records

    def get_max(self):
        return self.max_records

    def read(self, rid):
        if rid not in self.records:
            raise KeyError('rid not a valid key')

        return self.records[rid]

    def delete_record(self, rid):
        self.dirty = True
        return self.records.pop(rid, NULL_RID)

    def pack(self):
        data = bytearray(PAGE_SIZE)

        for i, key in enumerate(self.records.keys()):
            record_data = struct.pack(self.record_format, key, *self.records[key])
            data[i * self.record_size:i * self.record_size + self.record_size] = record_data

        data.ljust(4096, b'0')

        return data

    def unpack(self, data):
        self.dirty = True
        self.records.clear()
        self.num_records = 0

        format_size = struct.Struct(self.record_format).size

        i = 0
        while i <= 4096 - format_size:
            try:
                record = list(struct.unpack(self.record_format, data[i: i + format_size]))
            except:
                print(" here")
            if record[0] == 0:
                break
            self._unpack(record)
            i += format_size

    def update_record(self, key, data):
        if not self.records[key]:
            raise KeyError('rid not a valid key')

        self.dirty = True
        self.records[key] = data

    def __eq__(self, other):
        return (self.num_records == other.num_records and self.records == other.records
            and self.record_size == other.record_size and self.record_format == other.record_format
            and self.max_records == other.max_records and self.dirty == other.dirty)

class BasePage(Page):
    def __init__(self):
        super().__init__()
        self.record_size = RID_SIZE + VALUE_SIZE + SCHEMA_SIZE
        self.record_format = ENDIAN_FORMAT + RID_FORMAT + VALUES_FORMAT + SCHEMA_FORMAT
        self.max_records = int(PAGE_SIZE / self.record_size)

    def new_record(self, rid, value, dirty):
        if not self.has_capacity():
            raise MemoryError('No more space in Page')

        if dirty not in [0, 1]:
            raise ValueError('Dirty bit not a valid value')

        self.dirty = True
        self.records[rid] = [value] + [dirty]
        self.num_records += 1
        return rid

    def _unpack(self, values):
        self.new_record(*values)

    def get_dirty(self, rid):
        if rid not in self.records:
            raise KeyError('rid not valid key')

        return self.records[rid][1]

    def set_dirty(self, rid, dirty):
        if rid not in self.records:
            raise KeyError('rid not valid key')

        if dirty not in [0,1]:
            raise ValueError('Dirty bit not valid value')

        self.dirty = True
        self.records[rid][1] = dirty
        return 1


class TailPage(Page):
    def __init__(self, num_cols):
        super().__init__()

        if num_cols <= 0:
            raise ValueError('Number of columns cannot be <= 0')

        self.num_cols = num_cols
        self.record_size = RID_SIZE + num_cols * VALUE_SIZE
        self.record_format = ENDIAN_FORMAT + RID_FORMAT + num_cols * VALUES_FORMAT
        self.max_records = int(PAGE_SIZE / self.record_size)

    def new_record(self, rid, values):
        if not self.has_capacity():
            raise MemoryError('No more space in Page')

        if len(values) != self.num_cols:
            raise ValueError('Number of values not equal to number of columns')

        self.dirty = True
        self.records[rid] = values
        self.num_records += 1
        return rid

    def _unpack(self, values):
        self.new_record(values[0], values[1:])
