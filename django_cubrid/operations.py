"""
This module, operations.py, is part of a Django backend customized for CUBRID database
integration. It defines the `DatabaseOperations` class, which extends or modifies
Django's base database operations to ensure compatibility and optimized performance
with the CUBRID database system.

Key Features and Functionalities:
- `DatabaseOperations`: A class that inherits from Django's `BaseDatabaseOperations`.
  This class overrides and adds methods specific to the CUBRID database, handling various
  database operations such as data formatting, query construction, and schema management.
- CUBRID-specific Implementations: Includes custom methods and properties tailored for
  the CUBRID database, ensuring that Django's ORM can efficiently interact with CUBRID.
- Compatibility Layer: Acts as a compatibility layer between Django's ORM system and the
  CUBRID database, translating Django's standard operations into CUBRID-compatible queries
  and commands.
- Utility Functions: Provides utility functions for date and time formatting, string
  encoding, and regular expression operations, which are crucial for database operations
  in Django.

This module is a critical component of the Django-CUBRID backend, enabling seamless integration
and operation of Django applications with the CUBRID database.
"""
import uuid
import sys
import warnings

from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.encoding import force_str
from django.utils.regex_helper import _lazy_re_compile


class DatabaseOperations(BaseDatabaseOperations):
    """
    The DatabaseOperations class extends Django's BaseDatabaseOperations, providing
    support for the CUBRID database within the Django-CUBRID backend. This class
    facilitates interaction between Django applications and CUBRID databases.

    Attributes:
        compiler_module (str): Module for SQL compilation in CUBRID context.

    Key Functionalities:
    - Custom Database Operations: Adapts BaseDatabaseOperations methods for CUBRID.
    - SQL Query Formatting: Adjusts SQL syntax for CUBRID compatibility.
    - Data Type Handling: Manages data types as per CUBRID standards.
    - Schema Management: Tailors schema operations for CUBRID databases.
    - Performance Optimizations: Enhances efficiency for CUBRID interactions.

    Note:
    This class is part of the Django-CUBRID backend. It's optimized for CUBRID
    databases and works best with other components of this backend.
    """
    # pylint: disable=too-many-public-methods

    compiler_module = "django_cubrid.compiler"

    _extract_format_re = _lazy_re_compile(r"[A-Z_]+")

    def format_for_duration_arithmetic(self, sql):
        """Do nothing since formatting is handled in the custom function."""
        return sql

    def date_extract_sql(self, lookup_type, sql, params):
        # https://www.cubrid.org/manual/en/10.1/sql/function/datetime_fn.html
        if lookup_type == "week_day":
            # DAYOFWEEK() returns an integer, 1-7, Sunday=1.
            return f"DAYOFWEEK({sql})", params
        if lookup_type == "iso_week_day":
            # WEEKDAY() returns an integer, 0-6, Monday=0.
            return f"WEEKDAY({sql}) + 1", params
        if lookup_type == "week":
            # Mode 3: Monday, 1-53, with 4 or more days this year.
            return f"WEEK({sql}, 3)", params
        if lookup_type == "iso_year":
            return f"YEAR({sql})", params

        # EXTRACT returns 1-53 based on ISO-8601 for the week number.
        lookup_type = lookup_type.upper()
        if not self._extract_format_re.fullmatch(lookup_type):
            raise ValueError(f"Invalid loookup type: {lookup_type!r}")
        return f"EXTRACT({lookup_type} FROM {sql})", params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        fields = {
            "year": "%Y-01-01",
            "month": "%Y-%m-01",
        }
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return f"CAST(DATE_FORMAT({sql}, %s) AS DATE)", (*params, format_str)
        if lookup_type == "quarter":
            return (
                f"MAKEDATE(YEAR({sql}), 1) + "
                f"INTERVAL QUARTER({sql}) QUARTER - INTERVAL 1 QUARTER",
                (*params, *params),
            )
        if lookup_type == "week":
            return f"DATE_SUB({sql}, INTERVAL WEEKDAY({sql}) DAY)", (*params, *params)
        return f"DATE({sql})", params

    def _convert_sql_to_tz(self, sql, params, tzname):
        if tzname and settings.USE_TZ:
            warnings.warn("CUBRID does not support timezone conversion", RuntimeWarning)
        return sql, params

    def datetime_cast_date_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"DATE({sql})", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"TIME({sql})", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return self.date_extract_sql(lookup_type, sql, params)

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        fields = ['year', 'month', 'day', 'hour', 'minute', 'second', 'milisecond']
        dt_format = ('%%Y-', '%%m', '-%%d', ' %%H:', '%%i', ':%%s', '.%%ms')
        dt_format_defaults = ('0000-', '01', '-01', ' 00:', '00', ':00', '.00')
        if lookup_type == "quarter":
            return (
                f"CAST(DATE_FORMAT(MAKEDATE(YEAR({sql}), 1) + "
                f"INTERVAL QUARTER({sql}) QUARTER - "
                f"INTERVAL 1 QUARTER, %s) AS DATETIME)"
            ), (*params, *params, "%Y-%m-01 00:00:00.00")
        if lookup_type == "week":
            return (
                f"CAST(DATE_FORMAT("
                f"DATE_SUB({sql}, INTERVAL WEEKDAY({sql}) DAY), %s) AS DATETIME)"
            ), (*params, *params, "%Y-%m-%d 00:00:00.00")
        try:
            i = fields.index(lookup_type) + 1
        except ValueError:
            pass
        else:
            format_str = "".join(dt_format[:i] + dt_format_defaults[i:])
            return f"CAST(DATE_FORMAT({sql}, %s) AS DATETIME)", (*params, format_str)
        return sql, params

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        fields = {
            "hour": "%H:00:00",
            "minute": "%H:%i:00",
            "second": "%H:%i:%s",
        }
        if lookup_type in fields:
            format_str = fields[lookup_type]
            return f"CAST(DATE_FORMAT({sql}, %s) AS TIME)", (*params, format_str)

        return f"TIME({sql})", params

    def force_no_ordering(self):
        return [(None, ("NULL", [], False))]

    def quote_name(self, name):
        if name.startswith("`") and name.endswith("`"):
            # Quoting once is enough.
            return name
        return f"`{name}`"

    def regex_lookup(self, lookup_type):
        if lookup_type == "regex":
            return "%s REGEXP BINARY %s"
        return "%s REGEXP %s"

    def no_limit_value(self):
        # 2**63 - 1
        return 9223372036854775807

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute("SELECT LAST_INSERT_ID()")
        result = cursor.fetchone()

        # LAST_INSERT_ID() returns Decimal type value.
        # This causes problem in django.contrib.auth test,
        # because Decimal is not JSON serializable.
        # So convert it to int if possible.
        # I think LAST_INSERT_ID should be modified
        # to return appropriate column type value.
        if result[0] < sys.maxsize:
            return int(result[0])

        return result[0]

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        # pylint: disable=consider-using-f-string
        if not tables:
            return []

        # TODO: If there are FK constraints, the sqlflush command in Django may fail.

        if reset_sequences:
            # It's faster to TRUNCATE tables that require a sequence reset
            # since ALTER TABLE AUTO_INCREMENT is slower than TRUNCATE.
            return [
                "%s %s;"
                % (
                    style.SQL_KEYWORD("TRUNCATE"),
                    style.SQL_FIELD(self.quote_name(table_name)),
                )
                for table_name in tables
            ]

        # Otherwise issue a simple DELETE since it's faster than TRUNCATE
        # and preserves sequences.
        return [
            "%s %s %s;"
            % (
                style.SQL_KEYWORD("DELETE"),
                style.SQL_KEYWORD("FROM"),
                style.SQL_FIELD(self.quote_name(table_name)),
            )
            for table_name in tables
        ]

    def sequence_reset_by_name_sql(self, style, sequences):
        # pylint: disable=consider-using-f-string
        return [
            "%s %s %s %s = 1;"
            % (
                style.SQL_KEYWORD('ALTER'),
                style.SQL_KEYWORD('TABLE'),
                style.SQL_TABLE(self.quote_name(sequence_info['table'])),
                style.SQL_KEYWORD('AUTO_INCREMENT'),
            )
            for sequence_info in sequences
        ]

    def year_lookup_bounds(self, value):
        """
        Returns the start and end timestamps for a given year.

        This method is used to generate the bounds for filtering database records
        based on a year. It creates timestamps for the start and end of the year,
        excluding microseconds.

        Args:
            value (str): The year as a string.

        Returns:
            list: A list containing two strings, the start and end timestamps of the year.
        """
        first = '%s-01-01 00:00:00.00'
        second = '%s-12-31 23:59:59.99'
        return [first % value, second % value]

    def lookup_cast(self, lookup_type, internal_type=None):
        """
        Returns the SQL lookup cast for a given lookup type.

        This method adjusts the SQL query based on the lookup type. For case-insensitive
        lookups, it wraps the lookup in an UPPER function.

        Args:
            lookup_type (str): The type of lookup to perform.
            internal_type (str, optional): The internal type of the field. Defaults to None.

        Returns:
            str: The SQL lookup cast.
        """
        lookup = '%s'

        # Use UPPER(x) for case-insensitive lookups.
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            lookup = f'UPPER({lookup})'

        return lookup

    def max_name_length(self):
        """
        Returns the maximum length of database object names.

        This method specifies the maximum allowed length for names of database objects
        (like tables, columns, etc.) in the CUBRID database.

        Returns:
            int: The maximum name length.
        """
        return 64

    def bulk_insert_sql(self, fields, placeholder_rows):
        """
        Generates SQL for bulk insert operations.

        This method creates an SQL string for performing bulk insert operations. It formats
        the provided data into a suitable SQL 'VALUES' clause.

        Args:
            fields (list): The list of fields for the insert.
            placeholder_rows (list): A list of placeholder rows for the values to be inserted.

        Returns:
            str: The SQL string for the bulk insert operation.
        """
        # pylint: disable=unused-argument
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join(f"({sql})" for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def get_db_converters(self, expression):
        """
        Returns a list of database value converters for a given field type.

        Based on the type of the field in a database expression, this method appends
        appropriate converter functions to the list of converters. These converters
        are used to adapt Python values from database-specific formats.

        Args:
            expression (Expression): The database field expression.

        Returns:
            list: A list of converter functions.
        """
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'BinaryField':
            converters.append(self.convert_binaryfield_value)
        elif internal_type == 'TextField':
            converters.append(self.convert_textfield_value)
        elif internal_type in ['BooleanField', 'NullBooleanField']:
            converters.append(self.convert_booleanfield_value)
        elif internal_type == 'DateTimeField':
            if settings.USE_TZ:
                converters.append(self.convert_datetimefield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif internal_type in ['IPAddressField', 'GenericIPAddressField']:
            converters.append(self.convert_ipaddress_value)
        return converters

    # The following methods (convert_binaryfield_value, convert_textfield_value,
    # convert_booleanfield_value, convert_datetimefield_value, convert_uuidfield_value,
    # and convert_ipaddress_value) are converters used in get_db_converters. They
    # take a value, an expression, and a connection as arguments, and return a converted value.

    # pylint: disable=unused-argument

    def convert_binaryfield_value(self, value, expression, connection):
        """
        Converts a binary field value from a database-specific format to a Python bytes object.

        Args:
            value (str): The binary value as a string.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            bytes: The converted binary value.
        """
        if not value.startswith('0B'):
            raise ValueError(f'Unexpected value: {value}')
        value = value[2:]
        def gen_bytes():
            for i in range(0, len(value), 8):
                yield int(value[i:i + 8], 2)
        value = bytes(gen_bytes())
        return value

    def convert_textfield_value(self, value, expression, connection):
        """
        Converts a text field value to a Python string.

        Args:
            value (str): The text value.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            str: The converted text value.
        """
        if value is not None:
            value = force_str(value)
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        """
        Converts a boolean field value to a Python boolean.

        Args:
            value (int): The boolean value (0 or 1).
            expression, connection: Unused, but required for interface consistency.

        Returns:
            bool: The converted boolean value.
        """
        if value in (0, 1):
            value = bool(value)
        return value

    def convert_datetimefield_value(self, value, expression, connection):
        """
        Converts a datetime field value to a timezone-aware Python datetime object.

        Args:
            value (datetime): The datetime value.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            datetime: The timezone-aware datetime object.
        """
        if value is not None:
            value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        """
        Converts a UUID field value to a Python UUID object.

        Args:
            value (str): The UUID value as a string.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            UUID: The converted UUID value.
        """
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_ipaddress_value(self, value, expression, connection):
        """
        Converts an IP address field value to a Python string.

        Args:
            value (str): The IP address value.
            expression, connection: Unused, but required for interface consistency.

        Returns:
            str: The converted IP address value.
        """
        if value is not None:
            value = value.strip()
        return value

    # pylint: enable=unused-argument
