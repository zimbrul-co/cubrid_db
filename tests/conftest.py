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
    #;pylint: disable=protected-access
    # Obtain a cursor from the database connection provided by the cubrid_connection fixture
    cursor = cubrid_connection.cursor()
    cursor._set_charset_name('utf8')
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
