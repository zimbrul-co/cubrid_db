"""
This module tests DB API 2.0 compliance for the CUBRID database interface, using pytest for setup
and teardown of test environments. It includes fixtures for database table management and covers
a range of DB API 2.0 features like connection and cursor operations, transaction control, and
data integrity.

Goals:
- Ensure predictable interaction with CUBRID via DB API 2.0 standards.
- Facilitate a consistent programming experience for Python developers.

Fixtures:
- booze_table: Prepares a database table for testing, then cleans up afterwards.

Notes:
- Assumes a running CUBRID database instance is available.

Usage:
- Execute with pytest to run all module tests.
- Verify CUBRID connection details are set correctly.
"""
# pylint: disable=missing-function-docstring

import datetime
import decimal
import random
import time

import pytest

from conftest import (
    BOOZE_SAMPLES,
    TABLE_PREFIX,
)

import cubrid_db


def test_connect(cubrid_db_connection):
    assert cubrid_db_connection is not None, "Connection to cubrid_db failed"


def test_connect_empty_dsn():
    with pytest.raises(cubrid_db.InterfaceError):
        cubrid_db.connect(dsn = "")


def test_connect_no_dsn():
    with pytest.raises(cubrid_db.InterfaceError):
        cubrid_db.connect()


def test_apilevel():
    # Must exist
    assert hasattr(cubrid_db, 'apilevel'), "Driver doesn't define apilevel"

    # Must be a valid value
    apilevel = cubrid_db.apilevel
    assert apilevel == '2.0', f"Expected apilevel to be '2.0', got {apilevel}"


def test_paramstyle():
    # Must exist
    assert hasattr(cubrid_db, 'paramstyle'), "Driver doesn't define paramstyle"

    # Must be a valid value
    paramstyle = cubrid_db.paramstyle
    valid_styles = ('qmark', 'numeric', 'format', 'pyformat')
    assert paramstyle in valid_styles, \
        f"paramstyle must be one of {valid_styles}, got {paramstyle}"


def test_db_api_exceptions_hierarchy():
    # Base exceptions
    assert hasattr(cubrid_db, 'Error'), "'Error' exception is missing"

    # Subclasses of Error
    for exc in ['InterfaceError', 'DatabaseError']:
        assert hasattr(cubrid_db, exc), f"'{exc}' exception is missing"
        assert issubclass(getattr(cubrid_db, exc), cubrid_db.Error), \
            f"'{exc}' does not subclass 'Error'"

    # Subclasses of DatabaseError
    for exc in ['DataError', 'OperationalError', 'IntegrityError', 'InternalError',
                'ProgrammingError', 'NotSupportedError']:
        assert hasattr(cubrid_db, exc), f"'{exc}' exception is missing"
        assert issubclass(getattr(cubrid_db, exc), cubrid_db.DatabaseError), \
            f"'{exc}' does not subclass 'DatabaseError'"


def test_invalid_sql_insert_raises_dberror(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    table_name = f'{TABLE_PREFIX}booze'
    try:
        with pytest.raises(cubrid_db.DatabaseError):
            cur.execute(f"insert into {TABLE_PREFIX}booze values error_sql ('Hello')")
    finally:
        cur.execute(f'drop table if exists {table_name}')


def test_invalid_sql_insert_raises_error(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    table_name = f'{TABLE_PREFIX}booze'
    try:
        with pytest.raises(cubrid_db.Error):
            cur.execute(f"insert into {TABLE_PREFIX}booze values ('Hello', 'hello2')")
    finally:
        cur.execute(f'drop table if exists {table_name}')


def test_commit(cubrid_db_connection):
    # The commit operation is tested to ensure it can be called without raising an exception.
    cubrid_db_connection.commit()


def test_rollback(cubrid_db_connection):
    # Test to ensure the rollback operation can be called without raising an exception.
    cubrid_db_connection.rollback()


def test_cursor(cubrid_db_cursor):
    # Since the cubrid_db_cursor fixture handles cursor creation, this test implicitly verifies
    # that a cursor can be successfully obtained and closed without errors.
    # Additional operations or assertions to test the cursor's functionality can be added here.
    pass


def test_cursor_isolation(cubrid_db_connection):
    con = cubrid_db_connection
    table_name = f'{TABLE_PREFIX}booze'

    try:
        # Make sure cursors created from the same connection have
        # the documented transaction isolation level
        cur1 = con.cursor()
        cur2 = con.cursor()

        cur1.execute(f'create table {table_name} (name varchar(20))')
        cur1.execute(f"insert into {table_name} values ('Victoria Bitter')")
        cur2.execute(f"select name from {table_name}")
        booze = cur2.fetchall()

        assert len(booze) == 1, "Expected to fetch one row"
        assert len(booze[0]) == 1, "Expected row to have one column"
        assert booze[0][0] == 'Victoria Bitter', "Expected to find 'Victoria Bitter'"
    finally:
        # Clean up: close cursors and clean the test data
        if cur1:
            cur1.execute(f'drop table if exists {table_name}')
            cur1.close()
        if cur2:
            cur2.close()


def test_description(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    assert cur.description is None, \
        'cursor.descripton should be none after executing a'\
        'statement that can return no rows (such as DDL)'

    cur.execute(f'select name from {booze_table}')
    nc = len(cur.description)
    assert nc == 1, f'cursor.description describes {nc} columns'

    assert len(cur.description[0]) == 7, 'cursor.description[x] tuples must have 7 elements'
    assert cur.description[0][0].lower() == 'name',\
        'cursor.description[x][0] must return column name'
    assert cur.description[0][1] == cubrid_db.STRING,\
        f'cursor.description[x][1] must return column type. Got {cur.description[0][1]:r}'

    table_name = f'{TABLE_PREFIX}barflys'
    try:
        # Make sure self.description gets reset
        cur.execute(f'create table {table_name} (name varchar(20))')
        assert cur.description is None,\
            'cursor.description not being set to None when executing '\
            'no-result statments (eg. DDL)'
    finally:
        cur.execute(f'drop table if exists {table_name}')


def _test_description(cubrid_db_cursor, description_table, expected):
    cur, _ = cubrid_db_cursor
    column_name = expected[0]
    cur.execute(f"SELECT {column_name} from {description_table}")
    assert cur.description[0] == expected


def test_description_int(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_int', 8, 0, 0, 10, 0, 1))


def test_description_short(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_short', 9, 0, 0, 5, 0, 1))


def test_description_numeric(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_numeric', 7, 0, 0, 15, 0, 1))


def test_description_float(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_float', 11, 0, 0, 7, 0, 1))


def test_description_double(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_double', 12, 0, 0, 15, 0, 1))


def test_description_monetary(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_monetary', 10, 0, 0, 15, 0, 1))


def test_description_date(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_date', 13, 0, 0, 10, 0, 1))


def test_description_time(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_time', 14, 0, 0, 8, 0, 1))


def test_description_datetime(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_datetime', 22, 0, 0, 23, 3, 1))


def test_description_timestamp(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_timestamp', 15, 0, 0, 19, 0, 1))


def test_description_bit(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_bit', 5, 0, 0, 8, 0, 1))


def test_description_varbit(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_varbit', 6, 0, 0, 8, 0, 1))


def test_description_char(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_char', 1, 0, 0, 4, 0, 1))


def test_description_varchar(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_varchar', 2, 0, 0, 4, 0, 1))


def test_description_string(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_string', 2, 0, 0, 1073741823, 0, 1))


def test_description_set(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_set', 32, 0, 0, 0, 0, 1))


def test_description_multiset(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_multiset', 64, 0, 0, 0, 0, 1))


def test_description_sequence(cubrid_db_cursor, desc_table):
    _test_description(cubrid_db_cursor, desc_table, ('c_sequence', 96, 0, 0, 0, 0, 1))


def test_rowcount(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    assert cur.rowcount == -1, \
        'cursor.rowcount should be -1 after executing no-result statements'

    cur.execute(f"insert into {booze_table} value ('Victoria Bitter')")
    assert cur.rowcount in (-1, 1),\
        'cursor.rowcount should == number or rows inserted, or '\
        'set to -1 after executing an insert statment'

    cur.execute(f'select name from {booze_table}')
    assert cur.rowcount in (-1, 1),\
        'cursor.rowcount should == number or rows inserted, or '\
        'set to -1 after executing an insert statment'

    table_name = f'{TABLE_PREFIX}barflys'
    try:
        # Make sure self.description gets reset
        cur.execute(f'create table {table_name} (name varchar(20))')
        assert cur.rowcount == -1, \
            'cursor.rowcount should be -1 after executing no-result statements'
    finally:
        cur.execute(f'drop table if exists {table_name}')


def test_close(cubrid_db_connection):
    con = cubrid_db_connection
    cur = con.cursor()
    con.close()

    table_name = f'{TABLE_PREFIX}booze'
    with pytest.raises(cubrid_db.InterfaceError):
        cur.execute(f'create table {table_name} (name varchar(20))')

    with pytest.raises(cubrid_db.InterfaceError):
        con.commit()


def test_insert_utf8(cubrid_db_cursor, barflys_table):
    cur, _ = cubrid_db_cursor

    rc = cur.execute(f"insert into {barflys_table} (name) values (?)", ['Tom',])
    assert rc == 1
    rc = cur.execute(f"insert into {barflys_table} (name) values (?)", [b'Jenny',])
    assert rc == 1
    rc = cur.execute(f"insert into {barflys_table} (name) values (?)", ['小王',])
    assert rc == 1


def test_executemany(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    largs = [("Cooper's",), ("Boag's",)]
    cur.executemany(f'insert into {booze_table} values (?)', largs)

    cur.execute(f'select name from {booze_table}')
    res = cur.fetchall()
    assert len(res) == 2, 'cursor.fetchall returned incorrect number of rows'
    beers = [res[0][0], res[1][0]]
    assert beers == [a[0] for a in largs], \
        'cursor.fetchall retrieved incorrect data, or data inserted incorrectly'


def test_autocommit(cubrid_db_cursor, booze_table):
    cur, con = cubrid_db_cursor

    assert con.get_autocommit() is True, "autocommit must be on by default"

    con.set_autocommit(False)
    assert con.get_autocommit() is False, "autocommit must be set to off"

    cur.execute(f"insert into {booze_table} values ('Hello')")
    con.rollback()
    cur.execute(f"select * from {booze_table}")
    rows = cur.fetchall()

    # No rows affected
    assert len(rows) == 0

    con.set_autocommit(True)
    assert con.get_autocommit() is True, "autocommit must be set to on"

    cur.execute(f"insert into {booze_table} values ('Hello')")
    cur.execute(f"select * from {booze_table}")
    rows = cur.fetchall()

    # One row affected
    assert len(rows) == 1


def test_datatype(cubrid_db_cursor, datatype_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"insert into {datatype_table} values "
        "(2012, 2012.345, 20.12345, time'11:21:30 am',"
        "date'2012-10-26', datetime'2012-10-26 11:21:30 am',"
        "timestamp'11:21:30 am 2012-10-26',"
        "B'101100111', 'TESTSTR', {'a', 'b', 'c'},"
        "{'c', 'c', 'c', 'b', 'b', 'a'},"
        '\'{"a": 1, "b": 2}\''
        ")"
    )

    cur.execute(f"select * from {datatype_table}")
    row = cur.fetchone()

    datatypes = [int, float, decimal.Decimal, datetime.time,
        datetime.date, datetime.datetime, datetime.datetime,
        bytes, str, set, list, str,
    ]

    for i, t in enumerate(datatypes):
        assert isinstance(row[i], t),\
            f'incorrect data type converted from CUBRID to Python (index {i} - {t})'
        if issubclass(t, set) or issubclass(t, list):
            assert isinstance(row[i].pop(), str) # see python_cubrid.c
            # _cubrid_CursorObject_dbset_to_pyvalue returns str for all elements


def test_fetchone_error_before_select(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor

    # cursor.fetchone should raise an Error if called before
    # executing a select-type query
    with pytest.raises(cubrid_db.Error):
        cur.fetchone()


@pytest.mark.xfail(reason="CCI does not return error when fetchone cannot return rows")
def test_fetchone_error_no_rows(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    # cursor.fetchone should raise an Error if called after
    # executing a query that cannnot return rows
    # like the create table query performed by booze_table
    with pytest.raises(cubrid_db.Error):
        cur.fetchone()

    # or the insert query
    cur.execute(f"insert into {booze_table} values ('Victoria Bitter')")
    with pytest.raises(cubrid_db.Error):
        cur.fetchone()


def test_fetchone_return_none_no_rows(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor
    cur.execute(f'select name from {booze_table}')
    assert cur.fetchone() is None,\
        'cursor.fetchone should return None if a query retrieves no rows'
    assert cur.rowcount in (-1, 0)


def test_fetchone(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"insert into {booze_table} values ('Victoria Bitter')")
    cur.execute(f"select name from {booze_table}")
    r = cur.fetchone()
    assert len(r) == 1, 'cursor.fetchone should have retrieved a single column'

    assert r[0] == 'Victoria Bitter', 'cursor.fetchone retrieved incorrect data'

    assert cur.fetchone() is None,\
        'cursor.fetchone should return None if no more rows available'
    assert cur.rowcount in (-1, 1)


def test_fetchone_multi(cubrid_db_cursor, populated_booze_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select name from {populated_booze_table}")
    r = cur.fetchone()
    l = [r[0]]
    while r is not None:
        r = cur.fetchone()
        if r is None:
            break
        assert len(r) == 1, 'cursor.fetchone should have retrieved a single column'
        l.append(r[0])

    assert l == BOOZE_SAMPLES


def test_fetchmany_error_no_query(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor

    # cursor.fetchmany should raise an Error if called without issuing a query
    with pytest.raises(cubrid_db.Error):
        cur.fetchmany(4)


def test_fetchmany_default_array_size(cubrid_db_cursor, populated_booze_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f'select name from {populated_booze_table}')
    r = cur.fetchmany() # should get 1 row
    assert len(r) == 1,\
        'cursor.fetchmany retrieved incorrect number of rows, '\
        'default of array is one.'


def _gen_random_n_numbers_with_given_sum(n, s):
    if n <= 0 or s < n:
        raise ValueError("Impossible to distribute S among N numbers "
            "with each number being at least 1")

    if n == 1:
        return [s]

    if s == n:
        return [1] * n

    # Ensure there's at least 1 for each number
    remaining_sum = s - n

    r = random.Random(0)

    # Generate N-1 random partition points within the remaining sum
    partitions = sorted(r.sample(range(1, remaining_sum + 1), n - 1))

    # Calculate numbers based on differences between partitions (plus the ends)
    numbers = [partitions[0]] + [partitions[i] - partitions[i-1]
        for i in range(1, n-1)] + [remaining_sum - partitions[-1]]

    # Add 1 back to each number to ensure the minimum value is 1
    numbers = [x + 1 for x in numbers]

    return numbers


@pytest.mark.parametrize("array_size", list(range(1, 11, 2)))
@pytest.mark.parametrize("fetch_count", list(range(1, 4)))
@pytest.mark.parametrize("attempted_row_count", [3, 6, 7, 9])
def test_fetchmany(cubrid_db_cursor, populated_booze_table,
                   array_size, fetch_count, attempted_row_count):
    cur, _ = cubrid_db_cursor

    if fetch_count > attempted_row_count:
        return

    cur.execute(f'select name from {populated_booze_table}')

    total_count = remaining_count = len(BOOZE_SAMPLES)
    row_count_list = _gen_random_n_numbers_with_given_sum(
        fetch_count, attempted_row_count)

    def check_fetch(max_count, expected_count):
        r = cur.fetchmany(max_count)
        assert len(r) == expected_count,\
            'cursor.fetchmany retrieved incorrect number of rows, '\
            f'expected {expected_count}, returned {len(r)}'

    cur.arraysize = array_size

    for row_count in row_count_list:
        expected_count = min(remaining_count, row_count)
        check_fetch(row_count, expected_count)
        remaining_count = max(remaining_count - row_count, 0)

    assert cur.rowcount in (-1, total_count)


def test_fetchmany_empty_table(cubrid_db_cursor, populated_booze_table,
                               barflys_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f'select name from {barflys_table}')
    r = cur.fetchmany()
    assert not r,\
            'cursor.fetchmany should return an empty sequence '\
            'if query retrieved no rows'
    assert cur.rowcount in (-1, 0)


def test_fetchmany_nosize(cubrid_db_cursor, fetchmany_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select * from {fetchmany_table}")
    data = cur.fetchmany()
    assert len(data) == 1
    assert data == [(1, 21, 'myName-1')]


def test_fetchmany_negativeone(cubrid_db_cursor, fetchmany_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select * from {fetchmany_table}")
    data = cur.fetchmany(-1)
    assert not data


def test_fetchmany_zero(cubrid_db_cursor, fetchmany_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select * from {fetchmany_table}")
    data = cur.fetchmany(0)
    assert not data


def test_fetchmany_all(cubrid_db_cursor, fetchmany_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select * from {fetchmany_table}")
    data = cur.fetchmany(cur.rowcount)
    assert len(data) == cur.rowcount


def test_fetchmany_overflow(cubrid_db_cursor, fetchmany_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f"select * from {fetchmany_table}")
    data = cur.fetchmany(cur.rowcount + 10)
    assert len(data) == cur.rowcount


@pytest.mark.xfail(reason="CCI does not return error when fetchall cannot return rows")
def test_fetchall_error_no_rows(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    # cursor.fetchall should raise an Error if called without
    # executing a query that may return rows (such as a select)
    # like the create table query performed by booze_table
    with pytest.raises(cubrid_db.Error):
        cur.fetchall()

    # or the insert query
    cur.execute(f"insert into {booze_table} values ('Victoria Bitter')")
    with pytest.raises(cubrid_db.Error):
        cur.fetchall()


def test_fetchall_error_no_query(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor

    # cursor.fetchall should raise an Error if called without issuing a query
    with pytest.raises(cubrid_db.Error):
        cur.fetchall()


def test_fetchall(cubrid_db_cursor, populated_booze_table):
    cur, _ = cubrid_db_cursor

    row_count = len(BOOZE_SAMPLES)

    cur.execute(f'select name from {populated_booze_table}')
    rows = cur.fetchall()

    assert cur.rowcount in (-1, row_count)
    assert len(rows) == row_count, 'cursor.fetchall did not retrieve all rows'

    rows = [r[0] for r in rows]
    rows.sort()
    assert rows == BOOZE_SAMPLES, 'cursor.fetchall retrieved incorrect rows'

    rows = cur.fetchall()
    assert not rows,\
        'cursor.fetchall should return an empty list if called '\
        'after the whole result set has been fetched'
    assert cur.rowcount in (-1, row_count)


def test_fetchall_empty_table(cubrid_db_cursor, populated_booze_table,
                               barflys_table):
    cur, _ = cubrid_db_cursor

    cur.execute(f'select name from {barflys_table}')
    rows = cur.fetchall()

    assert cur.rowcount in (-1, 0)
    assert not rows,\
            'cursor.fetchall should return an empty list '\
            'if a select query returns no rows'


def _test_fetchall_datatype(cur, columns_sql, rows, expected_rows = None):
    table_name = f'{TABLE_PREFIX}fetchall'
    placeholders = ','.join(['?'] * len(rows[0]))
    cur.execute(f'drop table if exists {table_name}')
    try:
        cur.execute(f"create table if not exists {table_name} ({columns_sql})")
        cur.executemany(f"insert into {table_name} values ({placeholders})", rows)
        assert cur.rowcount == 1

        cur.execute(f"select * from {table_name}")
        fetched_rows = cur.fetchall()
        expected_rows = expected_rows or rows
        assert fetched_rows == expected_rows
    finally:
        cur.execute(f'drop table if exists {table_name}')


def test_fetchall_int(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [1,0,-1,2147483647,-2147483648]]
    _test_fetchall_datatype(cur, 'c_int int', rows)


def test_fetchall_short(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [1,0,-1,32767,-32768]]
    _test_fetchall_datatype(cur, 'c_int short', rows)


def test_fetchall_numeric(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(decimal.Decimal(x),) for x in ['12345.6789','0.12345678','-0.123456789']]
    expected_rows = [(decimal.Decimal(x),) for x in ['12345.6789','0.1235','-0.1235']]
    _test_fetchall_datatype(cur, 'c_num numeric(10,4)', rows, expected_rows)


def test_fetchall_float(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [1.1,0.0,-1.1]]
    _test_fetchall_datatype(cur, 'c_float float', rows)


def test_fetchall_double(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [1.1,0.0,-1.1]]
    _test_fetchall_datatype(cur, 'c_double double', rows)


def test_fetchall_char(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in ['a','abcd','zzz']]
    expected_rows = [(x,) for x in ['a   ','abcd','zzz ']]
    _test_fetchall_datatype(cur, 'c_char char(4)', rows, expected_rows)


def test_fetchall_varchar(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in ['a','abcd','abc']]
    _test_fetchall_datatype(cur, 'c_varchar varchar(4)', rows)


def test_fetchall_string(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in ['a','abcd','abcdefgh']]
    _test_fetchall_datatype(cur, 'c_string string', rows)


def test_fetchall_date(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [datetime.date.min, datetime.date.today(), datetime.date.max]]
    _test_fetchall_datatype(cur, 'c_date date', rows)


def test_fetchall_time(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(t,) for t in [datetime.time.min, datetime.time.max]]
    expected_rows = [(datetime.time(t.hour, t.minute, t.second),)
                     for t in [datetime.time.min, datetime.time.max]]
    _test_fetchall_datatype(cur, 'c_time time', rows, expected_rows)


def test_fetchall_datetime(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(t,) for t in [datetime.datetime.now(), datetime.datetime.max]]
    expected_rows = [(datetime.datetime(t.year, t.month, t.day,
        t.hour, t.minute, t.second, t.microsecond // 1000 * 1000),)
        for t in [datetime.datetime.now(), datetime.datetime.max]]
    _test_fetchall_datatype(cur, 'c_datetime datetime', rows, expected_rows)


def test_fetchall_bit(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [b'\xde\xad', b'\xbe\xaf', b'\x55']]
    expected_rows = [(x,) for x in [b'\xde\xad', b'\xbe\xaf', b'\x55\x00']]
    _test_fetchall_datatype(cur, 'c_bit bit(16)', rows, expected_rows)


def test_fetchall_varbit(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [(x,) for x in [b'\xde\xad\xbe\xef', b'\xbe\xaf', b'\x55']]
    _test_fetchall_datatype(cur, 'c_varbit bit varying', rows)


def test_fetchall_multiple_columns(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    rows = [
        (1400, 0.987654321, "ana has apples", datetime.date.today()),
        (1500, 1.987654321, "bonny has oranges", datetime.date.today()),
        (1600, 2.987654321, "chris has bananas", datetime.date.today()),
        (2000, 3.987654321, "dora has pies", datetime.date.today()),
    ]
    _test_fetchall_datatype(cur, 'c_int int, c_double double, c_varchar varchar, c_date date', rows)


def test_mixdfetch(cubrid_db_cursor, populated_booze_table):
    cur, _ = cubrid_db_cursor

    row_count = len(BOOZE_SAMPLES)

    cur.execute(f'select name from {populated_booze_table}')
    row1 = cur.fetchone()
    rows23 = cur.fetchmany(2)
    row4 = cur.fetchone()
    rows_last = cur.fetchall()
    assert cur.rowcount in (-1, row_count)
    assert len(rows23)== 2, 'fetchmany returned incorrect number of rows'
    assert len(rows_last) == max(row_count - 4, 0),\
            'fetchall returned incorrect number of rows'

    rows = [row1] + rows23 + [row4] + rows_last
    rows = [r[0] for r in rows]
    rows.sort()
    assert rows == BOOZE_SAMPLES, 'incorrect data retrieved or inserted'


def test_threadsafety():
    assert hasattr(cubrid_db, 'threadsafety')
    threadsafety = cubrid_db.threadsafety
    assert threadsafety in (0,1,2,3)


def test_binary():
    cubrid_db.Binary([0x10, 0x20, 0x30])


def test_date():
    cubrid_db.Date(2011,3,17)
    cubrid_db.DateFromTicks(time.mktime((2011,3,17,0,0,0,0,0,0)))


def test_time():
    cubrid_db.Time(10, 30, 45)
    cubrid_db.TimeFromTicks(time.mktime((2011,3,17,17,13,30,0,0,0)))


def test_timestamp():
    cubrid_db.Timestamp(2002,12,25,13,45,30)
    cubrid_db.TimestampFromTicks(time.mktime((2002,12,25,13,45,30,0,0,0)))


def test_attr_string():
    assert hasattr(cubrid_db, 'STRING'), 'module.STRING must be defined'


def test_attr_binary():
    assert hasattr(cubrid_db, 'BINARY'), 'module.BINARY must be defined'


def test_attr_number():
    assert hasattr(cubrid_db, 'NUMBER'), 'module.NUMBER must be defined'


def test_attr_datetime():
    assert hasattr(cubrid_db, 'DATETIME'), 'module.DATETIME must be defined'


def test_attr_rowid():
    assert hasattr(cubrid_db, 'ROWID'), 'module.ROWID must be defined'


def _test_binding(cur, columns_sql, samples):
    table_name = f'{TABLE_PREFIX}bindings'
    cur.execute(f'drop table if exists {table_name}')
    try:
        cur.execute(f"create table if not exists {table_name} ({columns_sql})")
        cur.executemany(f"insert into {table_name} values (?)",
                        [(sample,) for sample in samples])
        assert cur.rowcount == 1

        cur.execute(f"select * from {table_name}")
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.execute(f'drop table if exists {table_name}')


def test_bind_int(cubrid_db_cursor):
    numbers = [100, 200, 300, 400]
    inserted = _test_binding(cubrid_db_cursor[0], 'x int', numbers)
    assert inserted == numbers


def test_bind_bigint(cubrid_db_cursor):
    numbers_bigint = [-9223372036854775808, +9223372036854775807, 567890987654321012]
    inserted = _test_binding(cubrid_db_cursor[0], 'x bigint', numbers_bigint)
    assert inserted == numbers_bigint


def test_bind_float(cubrid_db_cursor):
    numbers = [1.234, 3.14, -10.441875, 5.]
    inserted = _test_binding(cubrid_db_cursor[0], 'x float', numbers)
    assert inserted == numbers


def test_bind_str(cubrid_db_cursor):
    samples = [
        'Carlton Cold', 'Carlton Draft', 'Mountain Goat',
        'Redback', 'Victoria Bitter', 'XXXX'
    ]
    inserted = _test_binding(cubrid_db_cursor[0], 'name varchar(20)', samples)
    assert inserted == samples


def test_bind_date(cubrid_db_cursor):
    dates = ["1987-10-29", "1989-07-14", "2024-02-29"]
    date_objects = [datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    for date_str in dates]

    inserted = _test_binding(cubrid_db_cursor[0], 'xdate date', dates)
    assert inserted == date_objects

    inserted = _test_binding(cubrid_db_cursor[0], 'xdate date', date_objects)
    assert inserted == date_objects


def test_bind_time(cubrid_db_cursor):
    times = ["11:30:29", "10:00:00", "23:59:59", "05:30:01"]
    time_objects = [datetime.datetime.strptime(time_str, "%H:%M:%S").time()
                    for time_str in times]

    inserted = _test_binding(cubrid_db_cursor[0], 'xtime time', times)
    assert inserted == time_objects

    inserted = _test_binding(cubrid_db_cursor[0], 'xtime time', time_objects)
    assert inserted == time_objects


def test_bind_timestamp(cubrid_db_cursor):
    times = ["2011-5-3 11:30:29", "2024-2-6 14:01:20"]
    ts_objects = [datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                    for time_str in times]

    inserted = _test_binding(cubrid_db_cursor[0], 'xts timestamp', times)
    assert inserted == ts_objects

    inserted = _test_binding(cubrid_db_cursor[0], 'xts timestamp', ts_objects)
    assert inserted == ts_objects


def test_bind_binary(cubrid_db_cursor):
    samples_bin = ['0100', '01010101010101', '111111111', '1111100000010101010110111111']

    # Function to convert a binary string to bytes
    def binary_str_to_bytes(binary_str):
        # Convert to integer
        integer_representation = int(binary_str, 2)

        # Convert integer to bytes
        # Calculate the length of the bytes object needed
        bytes_length = (len(binary_str) + 7) // 8  # Round up division
        return integer_representation.to_bytes(bytes_length, 'big')

    samples_bytes = [binary_str_to_bytes(b) for b in samples_bin]

    inserted = _test_binding(cubrid_db_cursor[0], 'xbit BIT VARYING(256)', samples_bytes)
    assert inserted == samples_bytes
