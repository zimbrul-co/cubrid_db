"""
This module provides pytest fixtures for establishing a connection and creating a cursor
for interacting with a CUBRID database in a testing environment. It encapsulates the
database connection parameters and setup logic, offering a simplified interface for
database interactions within test cases.
"""
# pylint: disable=missing-function-docstring

import pytest

import _cubrid
import cubrid_db


def _get_connect_args():
    ip = "localhost"
    port = "33000"
    dbname = "demodb"

    return {
        'dsn': f"CUBRID:{ip}:{port}:{dbname}:::",
    }


@pytest.fixture
def cubrid_connection():
    conn = _cubrid.connect(_get_connect_args()['dsn'])
    yield conn

    conn.close()


@pytest.fixture
def cubrid_cursor(cubrid_connection):
    # Obtain a cursor from the database connection provided by the cubrid_connection fixture
    cursor = cubrid_connection.cursor()
    yield cursor, cubrid_connection

    # Ensure the cursor is closed after the test
    cursor.close()


@pytest.fixture
def cubrid_db_connection():
    conn = cubrid_db.connect(**_get_connect_args())
    yield conn
    conn.close()


@pytest.fixture
def cubrid_db_cursor(cubrid_db_connection):
    # Obtain a cursor from the database connection provided by the cubrid_connection fixture
    cursor = cubrid_db_connection.cursor()
    yield cursor, cubrid_db_connection

    # Ensure the cursor is closed after the test
    cursor.close()


TABLE_PREFIX = 'dbapi20test_'


def _create_table(cdb_cur, name_suffix, columns_sql):
    cur, _ = cdb_cur
    table_name = f'{TABLE_PREFIX}{name_suffix}'
    cur.execute(f'drop table if exists {table_name}')
    cur.execute(f'create table {table_name} ({columns_sql})')
    return table_name

def _drop_table(cdb_cur, table_name):
    cur, _ = cdb_cur
    cur.execute(f'drop table if exists {table_name}')


@pytest.fixture
def booze_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'booze', 'name varchar(20)')
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def barflys_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'barflys', 'name varchar(20)')
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def datatype_table(cubrid_db_cursor):
    columns_sql = ('col1 int, col2 float, col3 numeric(12,3), '
        'col4 time, col5 date, col6 datetime, col7 timestamp, '
        'col8 bit varying(100), col9 varchar(100), col10 set(char(1)), '
        'col11 list(char(1)), col12 json')
    table_name = _create_table(cubrid_db_cursor, 'datatype', columns_sql)
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


BOOZE_SAMPLES = [
    'Carlton Cold',
    'Carlton Draft',
    'Mountain Goat',
    'Redback',
    'Victoria Bitter',
    'XXXX'
]


@pytest.fixture
def populated_booze_table(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor
    cur.executemany(f'insert into {booze_table} values (?)', BOOZE_SAMPLES)
    yield booze_table


@pytest.fixture
def fetchmany_table(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    table_name = _create_table(cubrid_db_cursor, 'fetchmany',
        "id NUMERIC AUTO_INCREMENT(1, 1), age int, name varchar(50)")
    cur.executemany(f"insert into {table_name} values (?, ?, ?)",
        [(None, 20 + i % 30, f'myName-{i}') for i in range(1, 100)])
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def desc_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'description',
        "c_int int, c_short short,c_numeric numeric,c_float float,"
        "c_double double,c_monetary monetary,"
        "c_date date, c_time time, c_datetime datetime, c_timestamp timestamp,"
        "c_bit bit(8),c_varbit bit varying(8),"
        "c_char char(4),c_varchar varchar(4),c_string string,"
        "c_set set,c_multiset multiset, c_sequence sequence"
        )
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def exc_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'execute',
        "a int primary key, b varchar(20), c timestamp")
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def exc_issue_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'execute_issue',
        "nameid int primary key ,age int,name VARCHAR(40)")
    cur, _ = cubrid_db_cursor
    cur.execute(f"INSERT INTO {table_name} (name,nameid,age) "
                "VALUES('Mike',1,30),('John',2,28),('Bill',3,45)")
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def exc_index_tables(cubrid_db_cursor):
    t = _create_table(cubrid_db_cursor, 'index_t', "id int, val int, fk int")
    u = _create_table(cubrid_db_cursor, 'index_u', "id int, val int,text string")

    cur, _ = cubrid_db_cursor

    cur.execute(f"create index _t_id on {t}(id)")
    cur.execute(f"create index _t_val on {t}(val)")
    cur.execute(f"create index _u_id on {u}(id)")
    cur.execute(f"create index _u_val on {u}(val)")
    cur.execute(f"create index _u_r_text on {u}(text)")
    cur.execute(f"insert into {t} values (1, 100, 1),(2, 200, 1),(3, 300, 2),(4, 300, 3)")
    cur.execute(f"insert into {u} values (1, 1000, '1000 text'),(2, 2000, '2000 text'),"
                "(3, 3000, '3000 text'),(3, 3001, '3001 text')")

    yield t, u

    _drop_table(cubrid_db_cursor, t)
    _drop_table(cubrid_db_cursor, u)


@pytest.fixture
def exc_part_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'partition',
        "id int not null, test_char char(50),test_varchar varchar(2000), test_bit bit(16),"
        "test_varbit bit varying(20),test_nchar nchar(50),test_nvarchar nchar varying(2000),"
        "test_string string,test_datetime timestamp, primary key (id, test_char)")
    yield table_name
    _drop_table(cubrid_db_cursor, table_name)


@pytest.fixture
def exc_primary_tables(cubrid_db_cursor):
    # Avoid drop error if primary_table is dropped first
    ftb = f'{TABLE_PREFIX}foreign_table'
    _drop_table(cubrid_db_cursor, ftb)

    # Create tables
    ptb = _create_table(cubrid_db_cursor, 'primary_table',
        "id CHAR(10) PRIMARY KEY, title VARCHAR(100), artist VARCHAR(100)")
    ftb = _create_table(cubrid_db_cursor, 'foreign_table',
        "album CHAR(10),dsk INTEGER,posn INTEGER,song VARCHAR(255),"
        f"FOREIGN KEY (album) REFERENCES {ptb}(id) ON UPDATE RESTRICT")

    yield ptb, ftb

    _drop_table(cubrid_db_cursor, ftb)
    _drop_table(cubrid_db_cursor, ptb)


@pytest.fixture
def exc_rollback_table(cubrid_db_cursor):
    table_name = _create_table(cubrid_db_cursor, 'rollback',
        "nameid int primary key ,age int,name VARCHAR(40)")

    cur, con = cubrid_db_cursor
    cur.execute(f"INSERT INTO {table_name} (name,nameid,age) VALUES('Mike',1,30),"
        "('John',2,28),('Bill',3,45)")

    con.set_autocommit(False)

    yield table_name

    con.set_autocommit(True)

    _drop_table(cubrid_db_cursor, table_name)
