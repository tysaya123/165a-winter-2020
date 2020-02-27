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
BUFFERPOOL_SIZE = 100


class SchemaEncoding(Enum):
    CLEAN = 0
    DIRTY = 1

def init():
    pass
