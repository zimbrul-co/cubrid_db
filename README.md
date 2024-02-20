cubrid_db Python driver for the CUBRID DBMS
------------------------------------------------------------------------------------------

Overview
========
cubrid_db is a Python package that implements Python Database API 2.0.
In additional to the minimal feature set of the standard Python DB API,
cubrid_db API also exposes nearly the entire native client API of the
database engine in _cubrid.


Project URL
-----------
  * Project Home: https://github.com/zimbrul-co/cubrid_db

Dependencies for cubrid_db
-------------------------
```
  * CUBRID: 10.1 or higher
  * OS    : Windows (x86 and x86_64)
            Linux (64bit)
            Other Unix and Unix-like os
  * Python: Python 3.9+
  * Compiler: to build from Source
            Visual Studio 2015 (Windows)
            GNU Developer Toolset 6 or higher
  * Bash: to build from Source (Linux)
```

Install for cubrid_db
--------------------
  To build and install from source, you should move into the top-level directory
  of the cubrid_db distribution and issue the following commands.
 ```
  $ git clone --recursive git@github.com:zimbrul-co/cubrid_db.git
  $ cd cubrid_db
  $ python setup.py build
  $ sudo python setup.py install   (Windows: python setup.py install)
```
Documents
---------
  * See Python DB API 2.0 Spec (http://www.python.org/dev/peps/pep-0249/)

Samples
-------
  * See directory "samples".

Django CUBRID backend - django_cubrid
-------------------------------------
 * django_cubrid is the Django backend for CUBRID Database.
 * Moved to https://github.com/zimbrul-co/django_cubrid
