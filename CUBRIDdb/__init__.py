"""
CUBRIDdb - A DB API v2.0 compatible interface to CUBRID.

This package is a wrapper around _cubrid.

connect() -- connects to server

"""
from datetime import date, datetime, time
from time import localtime

from _cubrid import *

from CUBRIDdb import field_type
from CUBRIDdb.connections import Connection

# pylint: disable=invalid-name

threadsafety = 2
apilevel = "2.0"
paramstyle = 'qmark'


Binary = bytes
Date = date
Time = time
Timestamp = datetime

def DateFromTicks(ticks):
    """
    This function constructs an object holding a date value from the given
    ticks value (number of seconds since the epoch).
    """
    return date(*localtime(ticks)[:3])

def TimeFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given
    ticks value (number of seconds since the epoch).
    """
    return time(*localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    """
    This function constructs an object holding a time stamp value from the
    given ticks value (number of seconds since the epoch).
    """
    return datetime(*localtime(ticks)[:6])

class DBAPISet(frozenset):
    """
    A special type of set for which A == x is true if A is a
    DBAPISet and x is a member of that set.
    """

    def __eq__(self, other):
        if isinstance(other, DBAPISet):
            return not self.difference(other)
        return other in self

STRING = DBAPISet([field_type.CHAR, field_type.STRING, field_type.NCHAR, field_type.VARCHAR])
BINARY = DBAPISet([field_type.BIT, field_type.VARBIT])
NUMBER = DBAPISet([field_type.NUMERIC, field_type.INT, field_type.SMALLINT, field_type.BIGINT])
DATETIME = DBAPISet([field_type.DATE, field_type.TIME, field_type.TIMESTAMP])
FLOAT = DBAPISet([field_type.FLOAT, field_type.DOUBLE])
SET = DBAPISet([field_type.SET, field_type.MULTISET, field_type.SEQUENCE])
BLOB = DBAPISet([field_type.BLOB])
CLOB = DBAPISet([field_type.CLOB])
ROWID = DBAPISet()

def _connect(*args, **kwargs):
    """Factory function for connections.Connection."""
    return Connection(*args, **kwargs)

connect = _connect

__all__ = [
    'connect',
    'apilevel',
    'paramstyle',
    'threadsafety',
    'STRING',
    'BINARY',
    'NUMBER',
    'DATETIME',
    'FLOAT',
    'SET',
    'BLOB',
    'CLOB',
    'ROWID',
]
