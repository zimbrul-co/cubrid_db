"""
Sample Python script for using the _cubrid module for LOB objects.

The _cubrid module is an extension module written in C which interacts directly
with the CUBRID CCI API.

For a module compliant with Python DB API, use cubrid_db.
"""

import _cubrid  # Import the CUBRID Python database module.

# Establish a connection to a CUBRID database.
con = _cubrid.connect(url='CUBRID:localhost:33000:demodb:::', user='public')

# Create a cursor object from the connection for executing SQL commands.
cur = con.cursor()

# Prepare and execute SQL to create a new table with a BLOB column for storing binary data.
cur.prepare('drop table if exists test_lob')
cur.execute()
cur.prepare('create table test_lob (image BLOB)')
cur.execute()

# Create a LOB object for managing large binary objects within the database.
lob_bind = con.lob()
# Import a binary file into the LOB object from the file system.
lob_bind.imports('tests/cubrid_logo.png')

# Prepare and execute SQL to insert a row into the table, binding the LOB to the BLOB column.
cur.prepare("insert into test_lob values (?)")
cur.bind_lob(1, lob_bind)  # Bind the LOB object to the SQL command's placeholder.
cur.execute()

# Prepare and execute SQL to retrieve all rows from the table.
cur.prepare('select * from test_lob')
cur.execute()

# Create a LOB object for fetching binary data from the database.
lob_fetch = con.lob()
# Fetch the binary data from the first column of the first row into the LOB object.
cur.fetch_lob(1, lob_fetch)
# Export the binary data from the LOB object to a file in the file system.
lob_fetch.export('samples/cubrid_logo_out.png')

# Close the LOB objects to free up resources.
lob_bind.close()
lob_fetch.close()

# Close the cursor and connection to release database resources.
cur.close()
con.close()
