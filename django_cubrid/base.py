"""
Cubrid database backend for Django.

Requires CUBRIDdb: http://www.cubrid.org/wiki_apis
"""

import re
import django

try:
    import CUBRIDdb as Database
    from CUBRIDdb import FIELD_TYPE
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading CUBRIDdb module: %s" % e)

import django.db.utils

from django.conf import settings
from django.db.backends import *
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.signals import connection_created
from django.utils.encoding import force_text

from django_cubrid.client import DatabaseClient
from django_cubrid.creation import DatabaseCreation
from django_cubrid.features import DatabaseFeatures
from django_cubrid.introspection import DatabaseIntrospection
from django_cubrid.operations import DatabaseOperations
from django_cubrid.schema import DatabaseSchemaEditor
from django_cubrid.validation import DatabaseValidation


"""
Takes a CUBRID exception and raises the Django equivalent.
"""
def raise_django_exception(e):
    cubrid_exc_type = type(e)
    django_exc_type = getattr(django.db.utils,
        cubrid_exc_type.__name__, django.db.utils.Error)
    raise django_exc_type(*tuple(e.args))


class CursorWrapper(object):
    """
    A thin wrapper around CUBRID's normal curosr class.

    """

    def __init__(self, cursor):
        self.cursor = cursor

    def execute(self, query, args=None):
        try:
            query = re.sub('([^%])%s', '\\1?', query)
            query = re.sub('%%', '%', query)
            return self.cursor.execute(query, args)

        except Exception as e:
            raise_django_exception(e)

    def executemany(self, query, args):
        try:
            query = re.sub('([^%])%s', '\\1?', query)
            query = re.sub('%%', '%', query)

            return self.cursor.executemany(query, args)
        except Exception as e:
            raise_django_exception(e)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'cubrid'
    # Operators taken from PosgreSQL implementation.
    # Check for differences between this syntax and CUBRID's.
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': 'LIKE %s',
        'icontains': 'LIKE UPPER(%s)',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
        'regex': 'REGEXP BINARY %s',
        'iregex': 'REGEXP %s',
    }
    # Patterns taken from other backend implementations.
    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\\', '\\\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }
    class BitFieldFmt:
        def __mod__(self, field_dict):
            assert isinstance(field_dict, dict)
            assert 'max_length' in field_dict

            s = 'BIT VARYING'
            if field_dict['max_length'] is not None:
                s += '(%i)' % (8 * field_dict['max_length'])
            return s

    data_types = {
        'AutoField': 'integer AUTO_INCREMENT',
        'BigAutoField': 'bigint AUTO_INCREMENT',
        'BinaryField': BitFieldFmt(),
        'BooleanField': 'short',
        'CharField': 'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'datetime',
        'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'bigint',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'char(15)',
        'GenericIPAddressField': 'char(39)',
        'JSONField': 'json',
        'NullBooleanField': 'short',
        'OneToOneField': 'integer',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField': 'string',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }

    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    validation_class = DatabaseValidation

    Database = Database


    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self.server_version = None

    def get_connection_params(self):
        # Backend-specific parameters
        return None

    def get_new_connection(self, conn_params):
        settings_dict = self.settings_dict

        # Connection to CUBRID database is made through connect() method.
        # Syntax:
        # connect (url[, user[password]])
        #    url - CUBRID:host:port:db_name:db_user:db_password:::
        #    user - Authorized username.
        #    password - Password associated with the username.
        url = "CUBRID"
        user = "public"
        passwd = ""

        if settings_dict['HOST'].startswith('/'):
            url += ':' + settings_dict['HOST']
        elif settings_dict['HOST']:
            url += ':' + settings_dict['HOST']
        else:
            url += ':localhost'
        if settings_dict['PORT']:
            url += ':' + settings_dict['PORT']
        if settings_dict['NAME']:
            url += ':' + settings_dict['NAME']
        if settings_dict['USER']:
            user = settings_dict['USER']
        if settings_dict['PASSWORD']:
            passwd = settings_dict['PASSWORD']

        url += ':::'

        con = Database.connect(url, user, passwd, charset='utf8')

        return con

    def _valid_connection(self):
        if self.connection is not None:
            return True
        return False

    def init_connection_state(self):
        pass

    def create_cursor(self, name=None):
        if not self._valid_connection():
            self.connection = self.get_new_connection(None)
            connection_created.send(sender=self.__class__, connection=self)

        cursor = CursorWrapper(self.connection.cursor())
        return cursor

    def _set_autocommit(self, autocommit):
        self.connection.autocommit = autocommit

    def is_usable(self):
        try:
            self.connection.ping()
        except Database.Error:
            return False
        else:
            return True

    def get_server_version(self):
        if not self.server_version:
            if not self._valid_connection():
                self.connection = self.get_new_connection(None)
            m = self.connection.server_version()
            if not m:
                raise Exception('Unable to determine CUBRID version')
            self.server_version = m
        return self.server_version

    def _savepoint_commit(self, sid):
        # CUBRID does not support "RELEASE SAVEPOINT xxx"
        pass
