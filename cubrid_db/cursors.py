"""
Module: cursors.py

This module is part of the Python API for interacting with CUBRID databases. It provides
implementations of cursor objects, which are essential for executing SQL queries and
managing the interactions with the database. These cursor implementations are tailored
specifically to work with CUBRID's features and conventions, ensuring a smooth
interaction between Python applications and CUBRID databases.

Key Features:
- SQL Query Execution: Cursors in this module enable the execution of SQL queries
  against CUBRID databases, allowing for both data manipulation and retrieval.
- Data Fetching: Implemented cursors provide functionalities to fetch data from the
  database, supporting both single row and batch fetching for efficiency.
- Parameter Binding: The module includes features for parameter binding in SQL queries,
  enhancing security by preventing SQL injection attacks.
- Transaction Management: Cursors play a crucial role in managing database transactions,
  thereby maintaining data integrity and consistency within applications.

This module is designed to be used as part of a broader Python application that interacts
with CUBRID databases. It abstracts the lower-level details of database communication,
enabling developers to focus on higher-level application logic without deep knowledge of
the underlying database protocols.

Note:
- While this module is tailored for the CUBRID database, it follows general Python
  database API standards where applicable.
- Understanding of basic SQL and database interaction principles is assumed for effective
  use of this module.

For more detailed documentation on the use of this module and the Python CUBRID API,
refer to the official CUBRID documentation and Python API guidelines.
"""
from datetime import date, time, datetime
from decimal import Decimal

from cubrid_db import field_type
from cubrid_db.exceptions import InterfaceError


INT_MIN = -2147483648
INT_MAX = +2147483647


def bytes_to_binary_string(bytes_value):
    binary_string = ''
    for byte in bytes_value:
        binary_string += bin(byte)[2:].zfill(8)
    return binary_string


def get_set_element_type(iterable):
    """
    Determine the homogeneous data type of elements in an iterable.

    This function iterates over each element in the provided iterable and
    determines its data type based on predefined type categories. The categories
    include INT for integers, FLOAT for floating point numbers, MONETARY for Decimal,
    DATE for date objects, TIME for time objects, DATETIME for datetime objects,
    VARBIT for bytes, and VARCHAR for strings. These categories are represented by
    field_type attributes.

    The function checks the type of each element and assigns it to one of the
    predefined categories. If all elements are of the same type, it returns
    that type. If the iterable contains elements of different types, the function
    raises a TypeError.

    Parameters:
    iterable (iterable): The iterable to check the data types of its elements.

    Returns:
    field_type: The type category of the elements in the iterable if they are homogeneous.

    Raises:
    TypeError: If the iterable contains elements of different types.

    Example:
    >>> get_set_element_type([1, 2, 3])
    field_type.INT

    >>> get_set_element_type([1, 2.5, 'text'])
    TypeError: Iterable contains elements of different types: field_type.VARCHAR != field_type.INT
    """
    chosen_type = None
    for obj in iterable:
        if isinstance(obj, int):
            t = field_type.INT
        elif isinstance(obj, float):
            t = field_type.FLOAT
        elif isinstance(obj, Decimal):
            t = field_type.NUMERIC
        elif isinstance(obj, date):
            t = field_type.DATE
        elif isinstance(obj, time):
            t = field_type.TIME
        elif isinstance(obj, datetime):
            t = field_type.DATETIME
        elif isinstance(obj, bytes):
            t = field_type.VARBIT
        elif isinstance(obj, str):
            t = field_type.VARCHAR

        if chosen_type is None:
            chosen_type = t
        elif t is not chosen_type:
            raise TypeError(f"Iterable contains elements of different types: {t} != {chosen_type}")

    return chosen_type


class BaseCursor:
    """
    A base for Cursor classes. Useful attributes:

    description::
        A tuple of DB API 7-tuples describing the columns in
        the last executed query; see PEP-249 for details.

    arraysize::
        default number of rows fetchmany() will fetch
    """

    def __init__(self, conn):
        self.con = conn

        self._cs = conn.connection.cursor()
        if self._cs is None:
            raise InterfaceError("Bad connection, invalid cursor")

        self.arraysize = 1
        self.rowcount = -1
        self.description = None

        self.charset = conn.charset
        self._cs.set_charset(conn.charset)

    def __del__(self):
        try:
            if self._cs is not None:
                self.close()
        except AttributeError:   # self._cs not exists
            pass

    def __check_state(self):
        if self._cs is None:
            raise InterfaceError("The cursor has been closed. No operation is allowed any more.")

    def close(self):
        """Close the cursor, and no further queries will be possible."""
        if self._cs is None:
            return

        self._cs.close()
        self._cs = None

    def _prepare(self, query):
        if isinstance(query, (bytes, bytearray)):
            query = query.decode()

        self._cs.prepare(query)

    def _bind_params(self, args):
        """
        Bind parameters to a command statement in a database cursor.

        This method processes the provided arguments (args) and binds them to a command
        statement associated with the database cursor. It handles different types of
        arguments including booleans, iterables, bytes, and other data types by converting
        or processing them appropriately before binding.

        For each argument in 'args':
        - If the argument is None, it is skipped.
        - If the argument is a boolean, it is converted to '1' or '0' string.
        - If the argument is an iterable (except strings and bytes), its element type
        is determined using 'get_set_element_type', and then it's bound as a set.
        - If the argument is a bytes object, it is converted to a binary string using
        'bytes_to_binstr' and bound with type 'field_type.VARBIT'.
        - For strings and other data types, the argument is bound directly or after
        converting to string, respectively.

        The method uses 'self.__check_state()' to ensure that the cursor is in an appropriate
        state for binding parameters.

        Parameters:
        args (any): The argument or a sequence of arguments to be bound to the command statement.
                    If 'args' is not an iterable, it is wrapped in a list.

        Raises:
        TypeError: If the iterable 'args' contains elements of different types when binding
                an iterable argument.
        """

        self.__check_state()

        def is_iterable(obj):
            if isinstance(obj, (bytes, str)):
                return False
            try:
                iter(obj)
                return True
            except TypeError:
                return False

        if not is_iterable(args):
            args = [args,]

        for i, arg in enumerate(args, start=1):
            if arg is None:
                continue

            if isinstance(arg, bool):
                self._cs.bind_param(i, 1 if arg else 0)
            elif isinstance(arg, int):
                if arg < INT_MIN or arg > INT_MAX:
                    self._cs.bind_param(i, arg, field_type.BIGINT)
                else:
                    self._cs.bind_param(i, arg)
            elif isinstance(arg, (float, str, date, time, datetime, Decimal)):
                self._cs.bind_param(i, arg)
            elif isinstance(arg, bytes):
                self._cs.bind_param(i, arg, field_type.VARBIT)
            elif is_iterable(arg):
                element_type = get_set_element_type(arg)
                s = self.con.connection.set()

                if element_type == field_type.VARBIT:
                    adapt = bytes_to_binary_string
                else:
                    adapt = str

                s.imports(tuple(map(adapt, arg)), element_type)
                self._cs.bind_set(i, s)
            else:
                arg = str(arg)
                self._cs.bind_param(i, arg)

    def execute(self, query, args=None):
        """
        Execute a query.

        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.

        Returns long integer rows affected, if any
        """
        self.__check_state()

        self._prepare(query)

        if args is not None:
            self._bind_params(args)

        r = self._cs.execute()
        self.rowcount = self._cs.rowcount
        self.description = self._cs.description
        return r

    def executemany(self, query, args_list):
        """
        Execute a multi-row query.

        query -- string, query to execute on server

        args_list -- Sequence of sequences or mappings, parameters to use with query

        This method improves performance on multiple-row INSERT and REPLACE.
        Otherwise it is equivalent to looping over args with execute().
        """
        self.__check_state()

        self._prepare(query)

        for args in args_list:
            self._bind_params(args)
            self._cs.execute()

        self.rowcount = self._cs.rowcount
        self.description = self._cs.description

    @classmethod
    def _get_fetch_type(cls):
        """
        Return the type of fetch to be passed to fetch_row.
        To be implemented in the subclasses.
        """
        raise NotImplementedError

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence, or
        None when no more data is available.
        """
        self.__check_state()
        return self._cs.fetch_row(self._get_fetch_type())

    def _fetch_many(self, size):
        self.__check_state()
        rlist = []
        i = 0
        while size < 0 or i < size:
            r = self.fetchone()
            if not r:
                break
            rlist.append(r)
            i = i+1
        return rlist

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's arraysize determines the number of rows
        to be fetched.
        The method should try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number of rows
        not being available, fewer rows may be returned.
        """
        self.__check_state()
        if size is None:
            size = self.arraysize
        if size <= 0:
            return []
        return self._fetch_many(size)

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples).
        Note that the cursor's arraysize attribute can affect the performance
        of this operation.
        """
        self.__check_state()
        return self._fetch_many(-1)

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def nextset(self):
        """Advance to the next result set.
        Returns None if there are no more result sets."""
        raise NotImplementedError("Cursor.nextset() not implemented")

    def callproc(self, procname, args=()):
        """
        Execute stored procedure procname with args

        procname -- string, name of procedure to execute on server

        args -- Sequence of parameters to use with procedure

        Returns the original args.

        """
        raise NotImplementedError("Cursor.callproc() not implemented")

    def __iter__(self):
        """
        Iteration over the result set which calls self.fetchone()
        and returns the next row.
        """
        self.__check_state()
        return self  # iter(self.fetchone, None)

    def next(self):
        """
        Return the next row from the currently executing SQL statement
        using the same semantics as fetchone().
        A StopIteration exception is raised when the result set is
        exhausted for Python versions 2.2 and later.
        """
        self.__check_state()
        return next(self)

    def __next__(self):
        self.__check_state()
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Cursor(BaseCursor):
    '''
    This is the standard Cursor class that returns rows as tuples
    and stores the result set in the client.
    '''
    # pylint: disable=abstract-method

    @classmethod
    def _get_fetch_type(cls):
        return 0 # Tuple rows


class DictCursor(BaseCursor):
    '''
    This is a Cursor class that returns rows as dictionaries and
    stores the result set in the client.
    '''
    # pylint: disable=abstract-method

    @classmethod
    def _get_fetch_type(cls):
        return 1 # Dict tuple rows
