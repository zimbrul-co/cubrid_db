"""
Sample Python script for using the _cubrid module.

The _cubrid module is an extension module written in C which interacts directly
with the CUBRID CCI API.

For a module compliant with Python DB API, use cubrid_db.
"""

import _cubrid

print('Establishing connection...')
print()

# Establish connection
con = _cubrid.connect(
    url = 'CUBRID:localhost:33000:demodb:::',
    user = 'public',
)

# Server and client version retrieval
print('server version:', con.server_version())
print('client version:', con.client_version())
print()

# Cursor initialization for executing SQL commands
cur = con.cursor()

# Creating a table
print('create a table - test_cubrid')
print()
cur.prepare('DROP TABLE IF EXISTS test_cubrid')
cur.execute()
cur.prepare('CREATE TABLE test_cubrid (id NUMERIC AUTO_INCREMENT(2009122350, 1), name VARCHAR(50))')
cur.execute()

# Inserting data into the table
print('insert some data...')
print()
cur.prepare("insert into test_cubrid (name) values ('Zhang San'), ('Li Si'), ('Wang Wu')")
cur.execute()
print('insertion rowcount:', cur.rowcount)
cur.prepare("insert into test_cubrid (name) values ('Zimbrul CO')")
cur.execute()
print('insertion rowcount:', cur.rowcount)

# Displaying the last insert id and schema information
print('last insert id:', con.insert_id())
print('schema info:', con.schema_info(_cubrid.CUBRID_SCH_TABLE, 'test_cubrid'))
print()

# Inserting more data with parameters
cur.prepare('insert into test_cubrid (name) values (?),(?)')
cur.bind_param(1, 'Ma Liu')
cur.bind_param(2, 'Niu Qi')
cur.execute()

# Selecting and displaying data from the table
print('select data from test_cubrid')
print()
cur.prepare('select * from test_cubrid')
cur.execute()

# Displaying column descriptions
print('description:')
for item in cur.description:
    print(item)
print()

# Fetching and displaying rows
row = cur.fetch_row()
while row:
    print(row)
    row = cur.fetch_row()
print()

# Cursor movement operations
print('beginning to move cursor...')

print('data_seek(1)')
cur.data_seek(1)
print('row_tell():', cur.row_tell())
print('fetch the first row:', cur.fetch_row())
print('data_seek(3)')
cur.data_seek(3)
print('row_tell():', cur.row_tell())
print('row_seek(-1)')
cur.row_seek(-1)
print('row_tell():', cur.row_tell())
print('row_seek(2)')
cur.row_seek(2)
print('row_tell():', cur.row_tell())
print()

# Displaying result info
print('result info:')
result_infos = cur.result_info()
for item in result_infos:
    print(item)

# Closing cursor and connection
cur.close()
con.close()
