cubrid_db
=========

Overview
-----------
cubrid_db is a Python package that implements Python Database API 2.0,
for the CUBRID DBMS.

In addition to the minimal feature set of the standard Python DB API,
cubrid_db API also exposes nearly the entire native client API of the
database engine in the _cubrid package.

Project URL
-----------
  * Project Home: https://github.com/zimbrul-co/cubrid_db

Dependencies
-------------------------
  * CUBRID >=10.1 (prepared for 11.2 by default)
  * OS
    - Linux (64bit)
    - Other Unix and Unix-like os
    - Windows (x86 and x86_64) (not tested yet)
  * Python: Python >=3.9
  * Compiler:
    - GNU Developer Toolset 6 or higher
    - CMake
    - Bash
    - Visual Studio 2015 (for Windows)

Build instructions
--------------------

Clone the repository:

```
$ git clone --recursive git@github.com:zimbrul-co/cubrid_db.git
```

Enter the package root directory:

```
$ cd cubrid_db
```

If your CUBRID version is different from 11.2, enter the cci-src directory
and change the CCI version to match your CUBRID version:

```
$ cd cci-src
$ git tag -l
v10.1
v10.2
v10.2.1
v10.2.3
v11.0
v11.0.1
v11.1.0.0013
v11.2.0.0029
$ git checkout v11.2.0.0029 # replace this with the required version
$ cd ..
```

Note: As of 23 February 2024, CUBRID 11.3 is not present in cci-src tags.

Build:

```
$ python setup.py build
```

Note: CCI is built first, from cci-src.
Note: Older CCI versions use automake/autoconf, newer version use CMake.

Install:

```
$ sudo python setup.py install   # (Windows: python setup.py install)
```

Sample usage
------------

Prepare virtual environment:

```
$ python -m venv venv
$ source venv/bin/activate
(venv) $ pip install -e .
```

Start demodb from the CUBRID distribution:
```
$ cubrid service start
$ cubrid server start demodb
```

Sample Python script:

```
import cubrid_db

con = cubrid_db.connect(
    dsn = 'CUBRID:localhost:33000:demodb:::',
    user = 'public',
)
cur = con.cursor()

cur.execute('CREATE TABLE test_cubrid (id NUMERIC AUTO_INCREMENT, name STRING)')
cur.execute("insert into test_cubrid (name) values (?)", ['Tom',])
cur.execute('select * from test_cubrid')
cur.fetchall()

cur.close()
con.close()
```

Testing
-------

Make sure you are in the virtual environment. Install the testing requirements
and run pytest.

```
(venv) $ pip install -r tests/requirements.txt
(venv) $ pytest
```

Django CUBRID backend - django_cubrid
-------------------------------------
 * django_cubrid is the Django backend for CUBRID Database.
 * Moved to https://github.com/zimbrul-co/django_cubrid

 Documents
---------
  * See Python DB API 2.0 Spec (http://www.python.org/dev/peps/pep-0249/)
