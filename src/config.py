# Global Setting for the Database
# PageSize, StartRID, etc..
from enum import Enum

PAGE_SIZE = 4096
ENDIAN_FORMAT = '>'
RID_FORMAT = 'L'
VALUES_FORMAT = 'q'
SCHEMA_FORMAT = 'b'
RID_SIZE = 4
VALUE_SIZE = 8
SCHEMA_SIZE = 1

NULL_RID = 0


class SchemaEncoding(Enum):
    CLEAN = 0
    DIRTY = 1

def init():
    pass
