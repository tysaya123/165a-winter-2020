import logging
import struct

from config import *

logging.basicConfig(level=logging.DEBUG)


# https://stackoverflow.com/questions/9940859/fastest-way-to-pack-a-list-of-floats-into-bytes-in-python


class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.record_size = None
        self.record_format = None
        self.max_records = None

    def has_capacity(self):
        if self.num_records < self.max_records:
            return True
        else:
            return False

    def get_max(self): return self.max_records

    def read(self, rid):
        for i in range(self.num_records):
            curr_record = self.get_record(i)
            if curr_record[0] == rid:
                return curr_record
        return None

    def append(self, data):
        if not self.has_capacity(): return 0
        self.set_record(self.num_records, data)
        self.num_records += 1
        return 1

    def update(self, rid, data, offset=0, length=0):
        if length == 0: length = self.record_size

        for i in range(self.num_records):
            curr_record = self.get_record(i)
            if curr_record[0] == rid:
                self.data[i * self.record_size + offset:i * self.record_size + offset + length] = data
                return 1
        return 0

    def mark_record_deleted(self, rid):
        return self.update(rid, struct.pack(ENDIAN_FORMAT + RID_FORMAT, 0), 0, RID_SIZE)

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
        # logging.debug(str(self.num_records) + "/" + str(self.max_records))
        # logging.debug(self.record_format + ":" + str(rid) + ":" + str(value) + ":" + str(dirty))
        record_data = struct.pack(self.record_format, rid, value, dirty)
        self.append(record_data)

    def get_dirty(self, rid):
        record = self.read(rid)
        if record is None: return record
        return record[2]

    def set_dirty(self, rid, dirty):
        dirty = struct.pack(ENDIAN_FORMAT + SCHEMA_FORMAT, dirty)
        self.update(rid, dirty, RID_SIZE + VALUE_SIZE, SCHEMA_SIZE)


class TailPage(Page):
    def __init__(self, num_cols):
        super().__init__()
        self.num_cols = num_cols
        self.record_size = RID_SIZE + num_cols * VALUE_SIZE
        self.record_format = ENDIAN_FORMAT + RID_FORMAT + num_cols * VALUES_FORMAT
        self.max_records = int(PAGE_SIZE / self.record_size)

    def new_record(self, rid, values):
        record_data = struct.pack(self.record_format, rid, *values)
        self.append(record_data)
