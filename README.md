CUBRID-Python driver for CUBRID
------------------------------------------------------------------------------------------

Overview
========
cubrid_db driver: CUBRID Module for Python DB API 2.0
                 Python driver for CUBRID Database

Abstract
========
  cubrid_db is a Python extension package that implements Python Database API 2.0.
  In additional to the minimal feature set of the standard Python DB API,
  CUBRID Python API also exposes nearly the entire native client API of the
  database engine in _cubrid.


Project URL
-----------
  * Project Home: https://github.com/zimbrul-co/cubrid-python

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
  $ git clone --recursive git@github.com:zimbrul-co/cubrid-python.git
  $ cd cubrid-python
  $ python setup.py build
  $ sudo python setup.py install   (Windows: python setup.py install)
```
Documents
---------
  * See Python DB API 2.0 Spec (http://www.python.org/dev/peps/pep-0249/)

Samples
-------
  * See directory "samples".

Notes
-----
 * django_cubrid is the Django backend for CUBRID Database.
 * Please refer to the django_cubrid README.md for more information.
