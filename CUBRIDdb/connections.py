"""
This module implements connections for CUBRIDdb. Presently there is
only one class: Connection. Others are unlikely. However, you might
want to make your own subclasses.
"""
from _cubrid import connect as cubrid_connect

from CUBRIDdb.cursors import DictCursor, Cursor


class Connection:
    """CUBRID Database Connection Object"""

    def __init__(self, *args, **kwargs):
        """Create a connecton to the database."""
        self.charset = ''
        kwargs2 = kwargs.copy()
        self.charset = kwargs2.pop('charset', 'utf8')

        self.connection = cubrid_connect(*args, **kwargs2)

    def __del__(self):
        pass

    def cursor(self, dict_cursor = None):
        """Return a new Cursor Object using the connection."""
        cursor_class = DictCursor if dict_cursor else Cursor
        return cursor_class(self)

    def set_autocommit(self, value):
        """
        Set the autocommit attribute of the connection.
        value -- True or False
        """
        if not isinstance(value, bool):
            raise ValueError("Parameter should be a boolean value")
        self.connection.set_autocommit(value)

    def get_autocommit(self):
        """
        Get the autocommit attribute of the connection.
        Return bool
        """
        return self.connection.autocommit

    autocommit = property(get_autocommit, set_autocommit,
                          doc = "autocommit value for current Cubrid session")

    def commit(self):
        """
        Commit any pending transaction to the database.
        Note that if the database supports an auto-commit feature,
        this must be initially off. An interface method may be provided
        to turn it back on.
        Database modules that do not support transactions should implement
        this method with void functionality.
        """
        self.connection.commit()

    def rollback(self):
        """
        This method causes the database to roll back to the start of any
        pending transaction.
        Closing a connection without committing the changes first will cause
        an implicit rollback to be performed.
        """
        self.connection.rollback()

    def set(self):
        """
        Create a LIST/SET/MULTISET object.
        """
        return self.connection.set()

    def ping(self):
        """
        Checks whether or not the connection to the server is working.
        """
        return self.connection.ping()

    def close(self):
        """
        Close the connection now
        """
        self.connection.close()

    def escape_string(self, buf):
        """
        Escape special characters in a string for use in an SQL statement
        """
        return self.connection.escape_string(buf)

    def server_version(self):
        """
        Returns a string that represents the CUBRID server version.
        """
        return self.connection.server_version()

    def batch_execute(self, sql):
        """
        Executes more than one sql statement at the same time.
        """
        return self.connection.batch_execute(sql)
