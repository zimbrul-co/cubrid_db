# pylint: disable=missing-function-docstring,missing-module-docstring
import re

def test_execute(cubrid_db_cursor, booze_table):
    cur, _ = cubrid_db_cursor

    res = cur.execute(f'select name from {booze_table}')
    assert res == 0, 'cur.execute should return 0 if a query retrieves no rows'

    cur.execute(f"insert into {booze_table} values ('Victoria Bitter')")
    assert cur.rowcount in (-1, 1)

    # qmark param style
    cur.execute(f'insert into {booze_table} values (?)', ("Cooper's",))

    assert cur.rowcount in (-1, 1)

    cur.execute(f'select name from {booze_table}')
    res = cur.fetchall()
    assert len(res) == 2, 'cursor.fetchall returned incorrect number of rows'
    beers = [res[0][0], res[1][0]]
    beers.sort()
    assert beers[0] == "Cooper's",\
        'cursor.fetchall retrieved incorrect data, or data inserted incorrectly'
    assert beers[1] == "Victoria Bitter",\
        'cursor.fetchall retrieved incorrect data, or data inserted incorrectly'


def test_execute_select_version(cubrid_db_cursor):
    cur, _ = cubrid_db_cursor
    cur.execute("SELECT VERSION()")
    row = cur.fetchone()
    assert re.match(r'[0-9]+\.[0-9]+\.[0-9]+\.[0-9]{4}', row[0])


def test_execute_insert(cubrid_db_cursor, exc_table):
    cur, _ = cubrid_db_cursor
    rc = cur.execute(f"insert into {exc_table}(a,b,c) values (1,'foo_data', systimestamp)")
    assert rc == 1


def test_execute_select(cubrid_db_cursor, exc_table):
    cur, _ = cubrid_db_cursor
    for n in range(100):
        rc = cur.execute(f"insert into {exc_table}(a,b,c) values ({n} ,'foo_data', systimestamp)")
        assert rc == 1

    cur.execute(f"select count(*) from {exc_table} where a >= 0")
    row = cur.fetchone()
    assert row[0] == 100


def test_execute_update(cubrid_db_cursor, exc_table):
    cur, _ = cubrid_db_cursor
    rc = cur.execute(f"insert into {exc_table}(a,b,c) values (1,'foo_data', systimestamp)")
    assert rc == 1

    rc = cur.execute(f"update {exc_table} set b = 'foo_data_new' where a = 1")
    assert rc == 1

    cur.execute(f"select * from {exc_table} where a = 1")
    row = cur.fetchone()
    assert row[1] == 'foo_data_new'


def test_execute_delete(cubrid_db_cursor, exc_table):
    cur, _ = cubrid_db_cursor
    rc = cur.execute(f"insert into {exc_table}(a) values (1),(2),(3),(4),(5)")
    assert rc == 5

    rc = cur.execute(f"delete from {exc_table} where a = 1")
    assert rc == 1

    cur.execute(f"select count(*) from {exc_table} where a >= 0")
    row = cur.fetchone()
    assert row[0] == 4
