"""
This module provides exception classes for the CUBRID database driver.
"""
# pylint: disable=unused-import

from _cubrid import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)
