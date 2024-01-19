"""
This module provides Django backend support for CUBRID database operations,
particularly focusing on schema editing functionalities. It defines the
`DatabaseSchemaEditor` class, an extension of Django's `BaseDatabaseSchemaEditor`,
tailored to generate CUBRID-specific SQL statements for schema modifications.
These modifications include operations like table deletion, column modification,
and alteration of column constraints. The custom implementation ensures compatibility
and efficient interaction between Django models and the CUBRID database schema.
"""
import datetime

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.models.fields.related import ManyToManyField


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    """
    A Django database schema editor for CUBRID databases.

    This class extends Django's `BaseDatabaseSchemaEditor` to provide
    specialized schema editing capabilities for CUBRID databases. It overrides
    and specifies SQL statement templates for various schema operations such as
    deleting tables, dropping columns, and modifying column types and constraints.
    The customization is necessary to accommodate the SQL syntax and features
    specific to CUBRID, ensuring seamless schema manipulations within Django's ORM.
    """

    sql_delete_table = "DROP TABLE %(table)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_alter_column_type = "MODIFY %(column)s %(type)s"
    sql_alter_column_null = "MODIFY %(column)s %(type)s"
    sql_alter_column_not_null = "MODIFY %(column)s %(type)s NOT NULL"

    sql_rename_column = "ALTER TABLE %(table)s CHANGE %(old_column)s %(new_column)s %(type)s"
    sql_delete_unique = "ALTER TABLE %(table)s DROP INDEX %(name)s"
    sql_create_fk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY "
        "(%(column)s) REFERENCES %(to_table)s (%(to_column)s)"
    )
    sql_create_column_inline_fk = (
        ', ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) '
        'REFERENCES %(to_table)s(%(to_column)s)'
    )
    sql_delete_fk = "ALTER TABLE %(table)s DROP FOREIGN KEY %(name)s"
    sql_delete_index = "DROP INDEX %(name)s ON %(table)s"
    alter_string_set_null = 'MODIFY %(column)s %(type)s;'
    alter_string_drop_null = 'MODIFY %(column)s %(type)s NOT NULL;'
    sql_create_pk = "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s)"
    sql_delete_pk = "ALTER TABLE %(table)s DROP PRIMARY KEY"

    def quote_value(self, value):
        """
        Quotes a value for use in a SQL statement, adapting it to the CUBRID database format.

        This method takes a Python data type and converts it into a string representation
        suitable for inclusion in a SQL query, ensuring proper formatting and escaping
        as needed for the CUBRID database. It handles various data types like dates, times,
        strings, bytes, booleans, and other basic types.

        Parameters:
            value: The value to be quoted. Can be an instance of `datetime.date`,
                `datetime.time`, `datetime.datetime`, `str`, `bytes`, `bytearray`,
                `memoryview`, `bool`, or other basic data types.

        Returns:
            str: A string representation of the input value formatted as a literal
                suitable for SQL queries. Dates and times are returned in single quotes,
                strings are escaped and quoted, bytes are converted to hexadecimal format,
                booleans are represented as '1' or '0', and other types are converted
                to their string representation.

        Note:
            For string values, this method uses the `escape_string` method of the
            CUBRID database connection to handle escaping, ensuring that the value
            is safe to include in SQL queries.
        """
        if isinstance(value, (datetime.date, datetime.time, datetime.datetime)):
            return f"'{value}'"
        if isinstance(value, str):
            return f"'{self.connection.connection.escape_string(value)}'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            return f"'{value.hex()}'"
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def prepare_default(self, value):
        return self.quote_value(value)

    def column_sql(self, model, field, include_default=False):
        """
        Takes a field and returns its column definition.
        The field must already have had set_attributes_from_name called.
        """
        # Get the column's type and use that as the basis of the SQL
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params['type']
        params = []
        # Check for fields that aren't actually columns (e.g. M2M)
        if sql is None:
            return None, None
        # Work out nullability
        null = field.null
        # If we were told to include a default value, do so
        include_default = include_default and not self.skip_default(field)
        if include_default:
            default_value = self.effective_default(field)
            if default_value is not None:
                sql += f" DEFAULT {self.prepare_default(default_value)}"
        if not field.get_internal_type() in ("BinaryField",):
            if null:
                sql += " NULL"
            elif not null:
                sql += " NOT NULL"
            # Primary key/unique outputs
            if field.primary_key:
                sql += " PRIMARY KEY"
            elif field.unique:
                sql += " UNIQUE"
        # Return the sql
        return sql, params

    def add_field(self, model, field):
        """
        Creates a field on a model.
        Usually involves adding a column, but may involve adding a
        table instead (for M2M fields)
        """
        # pylint: disable=protected-access

        # Special-case implicit M2M tables
        if ((isinstance(field, ManyToManyField) or
                field.get_internal_type() == 'ManyToManyField') and
                field.remote_field.through._meta.auto_created):
            self.create_model(field.remote_field.through)
            return

        # Get the column's definition
        definition, params = self.column_sql(model, field, include_default=True)
        # It might not actually have a column behind it
        if definition is None:
            return
        # Check constraints can go on the column SQL here
        db_params = field.db_parameters(connection=self.connection)
        if db_params['check']:
            definition += f" CHECK ({db_params['check']})"
        # Build the SQL and run it
        sql = self.sql_create_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "definition": definition,
        }
        self.execute(sql, params)

        # Add an index, if required
        if field.db_index and not field.unique:
            self.deferred_sql.append(self._create_index_sql(model, fields=[field]))
        # Add any FK constraints later
        if field.is_relation and field.db_constraint:
            self.deferred_sql.append(self._create_fk_sql(
                model, field, "_fk_%(to_table)s_%(to_column)s"))
