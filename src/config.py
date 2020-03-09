# Global Setting for the Database
# PageSize, StartRID, etc..
from enum import Enum
from mmap import PAGESIZE

PAGE_SIZE = PAGESIZE
ENDIAN_FORMAT = '>'
RID_FORMAT = 'L'
VALUES_FORMAT = 'q'
SCHEMA_FORMAT = 'b'
RID_SIZE = 4
VALUE_SIZE = 8
SCHEMA_SIZE = 1

NULL_RID = 0

# Number of pages possible in the buffer pool 128000 is 1/2 a gb
BUFFERPOOL_SIZE = 1000


class SchemaEncoding(Enum):
    CLEAN = 0
    DIRTY = 1


class RWPins(Enum):
    READ = 0
    WRITE = 1


class BufferPoolCalls(Enum):
    READ_TAIL_PAGE = 0
    READ_BASE_PAGE = 1
    WRITE_TAIL_PAGE = 2
    WRITE_BASE_PAGE = 3
    WRITE_NEW_TAIL_PAGE = 4
    WRITE_NEW_BASE_PAGE = 5
    CLOSE_READ_PAGE = 6
    CLOSE_WRITE_PAGE = 7


def init():
    pass
