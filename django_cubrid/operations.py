import uuid
import sys
import warnings

from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.utils import timezone
from django.utils.encoding import force_str
from django.utils.regex_helper import _lazy_re_compile


class DatabaseOperations(BaseDatabaseOperations):
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
        elif lookup_type == "iso_week_day":
            # WEEKDAY() returns an integer, 0-6, Monday=0.
            return f"WEEKDAY({sql}) + 1", params
        elif lookup_type == "week":
            # Mode 3: Monday, 1-53, with 4 or more days this year.
            return f"WEEK({sql}, 3)", params
        elif lookup_type == "iso_year":
            return f"YEAR({sql})", params
        else:
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
        elif lookup_type == "quarter":
            return (
                f"MAKEDATE(YEAR({sql}), 1) + "
                f"INTERVAL QUARTER({sql}) QUARTER - INTERVAL 1 QUARTER",
                (*params, *params),
            )
        elif lookup_type == "week":
            return f"DATE_SUB({sql}, INTERVAL WEEKDAY({sql}) DAY)", (*params, *params)
        else:
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
        else:
            return f"TIME({sql})", params

    def force_no_ordering(self):
        return [(None, ("NULL", [], False))]

    def quote_name(self, name):
        if name.startswith("`") and name.endswith("`"):
            # Quoting once is enough.
            return name
        return "`%s`" % name

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

    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        # 'TRUNCATE x;', 'TRUNCATE y;', 'TRUNCATE z;'... style SQL statements
        # to clear all tables of all data
        # TODO: when there are FK constraints, the sqlflush command in django may be failed.
        if tables:
            sql = []
            for table in tables:
                sql.append('%s %s;' % (style.SQL_KEYWORD('TRUNCATE'), style.SQL_FIELD(self.quote_name(table))))

            # 'ALTER TABLE table AUTO_INCREMENT = 1;'... style SQL statements
            # to reset sequence indices
            sql.extend(
                ["%s %s %s %s %s;" % (style.SQL_KEYWORD('ALTER'),
                 style.SQL_KEYWORD('TABLE'),
                 style.SQL_TABLE(self.quote_name(sequence['table'])),
                 style.SQL_KEYWORD('AUTO_INCREMENT'),
                 style.SQL_FIELD('= 1'),) for sequence in sequences])
            return sql
        else:
            return []

    def year_lookup_bounds(self, value):
        # Again, no microseconds
        first = '%s-01-01 00:00:00.00'
        second = '%s-12-31 23:59:59.99'
        return [first % value, second % value]

    def lookup_cast(self, lookup_type, internal_type=None):
        lookup = '%s'

        # Use UPPER(x) for case-insensitive lookups.
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            lookup = 'UPPER(%s)' % lookup

        return lookup

    def max_name_length(self):
        return 64

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("({0})".format(sql) for sql in placeholder_rows_sql)
        return "VALUES " + values_sql

    def get_db_converters(self, expression):
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

    # pylint: disable=unused-argument

    def convert_binaryfield_value(self, value, expression, connection):
        if not value.startswith('0B'):
            raise ValueError('Unexpected value: %s' % value)
        value = value[2:]
        def gen_bytes():
            for i in range(0, len(value), 8):
                yield int(value[i:i + 8], 2)
        value = bytes(gen_bytes())
        return value

    def convert_textfield_value(self, value, expression, connection):
        if value is not None:
            value = force_str(value)
        return value

    def convert_booleanfield_value(self, value, expression, connection):
        if value in (0, 1):
            value = bool(value)
        return value

    def convert_datetimefield_value(self, value, expression, connection):
        if value is not None:
            value = timezone.make_aware(value, self.connection.timezone)
        return value

    def convert_uuidfield_value(self, value, expression, connection):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_ipaddress_value(self, value, expression, connection):
        if value is not None:
            value = value.strip()
        return value

    # pylint: enable=unused-argument
