import logging
import struct

from config import *

logging.basicConfig(level=logging.DEBUG)


# https://stackoverflow.com/questions/9940859/fastest-way-to-pack-a-list-of-floats-into-bytes-in-python

class Page:
    def __init__(self):
        # add modified bit
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.records = {}
        self.record_size = None
        self.record_format = None
        self.max_records = None

    def has_capacity(self):
        if self.num_records < self.max_records:
            return True
        else:
            return False

    def get_max(self):
        return self.max_records

    def read(self, rid):
        if rid not in self.records: return None
        return self.records[rid]

    def delete_record(self, rid):
        return self.records.pop(rid, NULL_RID)

    def pack(self):
        i = 0
        for key in self.records.keys():
            record_data = struct.pack(self.record_format, key + self.records[key])
            self.set_record(i, record_data)
            i += 1
        record_data = struct.pack(ENDIAN_FORMAT + RID_FORMAT, NULL_RID)
        self.set_record(i, record_data)

    def unpack(self, data):
        i = 0
        while True:
            record = self.get_record(i)
            if record[0] == NULL_RID: break
            self.records[record[0]] = record[1:]
            i += 1

    def get_record(self, i):
        return struct.unpack(self.record_format,
                             self.data[i * self.record_size:i * self.record_size + self.record_size])

    def set_record(self, i, data):
        self.data[i * self.record_size:i * self.record_size + self.record_size] = data


class BasePage(Page):
    def __init__(self):
        super().__init__()
        self.record_size = RID_SIZE + VALUE_SIZE + SCHEMA_SIZE
        self.record_format = ENDIAN_FORMAT + RID_FORMAT + VALUES_FORMAT + SCHEMA_FORMAT
        self.max_records = int(PAGE_SIZE / self.record_size)

    def new_record(self, rid, value, dirty):
        if not self.has_capacity(): return NULL_RID
        self.records[rid] = [value] + [dirty]
        self.num_records += 1
        return rid

    def get_dirty(self, rid):
        if rid not in self.records: return None
        return self.records[rid][1]

    def set_dirty(self, rid, dirty):
        if rid not in self.records: return 0
        self.records[rid][1] = dirty
        return 1


class TailPage(Page):
    def __init__(self, num_cols):
        super().__init__()
        self.num_cols = num_cols
        self.record_size = RID_SIZE + num_cols * VALUE_SIZE
        self.record_format = ENDIAN_FORMAT + RID_FORMAT + num_cols * VALUES_FORMAT
        self.max_records = int(PAGE_SIZE / self.record_size)

    def new_record(self, rid, values):
        if not self.has_capacity(): return NULL_RID
        self.records[rid] = values
        self.num_records += 1
        return rid
