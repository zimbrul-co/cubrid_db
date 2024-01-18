import datetime

from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.models.fields.related import ManyToManyField


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):

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
        # Special-case implicit M2M tables
        if ((isinstance(field, ManyToManyField) or field.get_internal_type() == 'ManyToManyField') and
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
            self.deferred_sql.append(self._create_fk_sql(model, field, "_fk_%(to_table)s_%(to_column)s"))
