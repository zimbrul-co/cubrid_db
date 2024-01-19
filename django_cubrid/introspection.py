import re

from collections import namedtuple

from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection,
    FieldInfo,
    TableInfo,
)
from django.db.models.indexes import Index
from django.utils.encoding import force_str

from CUBRIDdb import field_type


InfoLine = namedtuple('InfoLine', [
    'col_name', 'attr_type', 'data_type', 'prec scale', 'is_nullable',
    'default_value', 'def_order', 'is_system_class', 'class_type', 'partitioned',
    'owner_name', 'is_reuse_old_class',
])


class DatabaseIntrospection(BaseDatabaseIntrospection):
    data_types_reverse = {
        field_type.BIT: 'BinaryField',
        field_type.VARBIT: 'BinaryField',
        field_type.CHAR: 'CharField',
        field_type.VARCHAR: 'CharField',
        field_type.NCHAR: 'CharField',
        field_type.VARNCHAR: 'CharField',
        field_type.NUMERIC: 'DecimalField',
        field_type.INT: 'IntegerField',
        field_type.SMALLINT: 'SmallIntegerField',
        field_type.BIGINT: 'BigIntegerField',
        field_type.FLOAT: 'FloatField',
        field_type.DOUBLE: 'FloatField',
        field_type.DATE: 'DateField',
        field_type.TIME: 'TimeField',
        field_type.TIMESTAMP: 'DateTimeField',
        field_type.DATETIME: 'DateTimeField',
        field_type.STRING: 'CharField',
        field_type.SET: 'TextField',
        field_type.MULTISET: 'TextField',
        field_type.SEQUENCE: 'TextField',
    }

    def get_table_list(self, cursor):
        """Returns a list of table names in the current database."""
        cursor.execute("SHOW FULL TABLES")
        return [TableInfo(row[0], {'BASE TABLE': 't', 'VIEW': 'v'}.get(row[1]))
                for row in cursor.fetchall()]

    def identifier_converter(self, name):
        """Identifiers are case insensitive under CUBRID"""
        return name.lower()

    def get_table_description(self, cursor, table_name):
        """Returns a description of the table, with the DB-API cursor.description interface."""

        # Get accurate information with this query (taken from cubridmanager)
        cursor.execute("""
            SELECT a.attr_name, a.attr_type, a.data_type, a.prec, a.scale, a.is_nullable,
            a.default_value, a.def_order, c.is_system_class, c.class_type, c.partitioned,
            c.owner_name, c.is_reuse_oid_class
            FROM db_attribute a, db_class c
            WHERE c.class_name=a.class_name AND c.class_name = ?
            ORDER BY a.class_name, a.def_order;""", [table_name])
        field_info = {line[0]: InfoLine(*line) for line in cursor.fetchall()}

        cursor.execute(f"SELECT * FROM {self.connection.ops.quote_name(table_name)} LIMIT 1")

        fields = []
        for line in cursor.description:
            info = field_info[line[0]]
            fields.append(FieldInfo(
                force_str(line[0]),         # name
                line[1],                    # type
                line[2],                    # display_size
                info.prec,                  # internal size - use precision value
                info.prec,                  # precision
                info.scale,                 # scale
                info.is_nullable == "YES",  # null_ok
                info.default_value,         # default
            ))
        return fields

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """

        raise NotImplementedError

    def get_sequences(self, cursor, table_name, table_fields=()):
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        _, stmt = cursor.fetchone()

        # Only one auto increment possible
        m = re.search(r'\[([\w]+)\][\w\s]*AUTO_INCREMENT', stmt)
        if m is None:
            return []

        return [{'table': table_name, 'column': m.group(1)}]

    def get_indexes(self, cursor, table_name):
        cursor.execute("""
            SELECT db_index_key.key_attr_name, db_index.is_primary_key, db_index.is_unique
            FROM db_index_key, db_index
            WHERE db_index_key.class_name = ?
              AND db_index.class_name = ?
              AND db_index_key.key_order = 0
              AND db_index_key.index_name = db_index.index_name
              AND db_index.key_count = 1;""", [table_name, table_name])
        rows = list(cursor.fetchall())
        indexes = {}
        for row in rows:
            indexes[row[0]] = {'primary_key': (row[1] == 'YES'), 'unique': (row[2] == 'YES')}

        return indexes

    def get_constraints(self, cursor, table_name):
        # pylint: disable=too-many-statements

        def parse_create_table_stmt(stmt):
            i = 0
            while i < len(stmt):
                i_constraint = stmt.find('CONSTRAINT', i + 1)
                i_index = stmt.find('INDEX', i + 1)

                if i_constraint == -1 and i_index == -1:
                    yield stmt[i:]
                    return

                if i_constraint == -1:
                    i1 = i_index
                elif i_index == -1:
                    i1 = i_constraint
                else:
                    i1 = min(i_constraint, i_index)

                yield stmt[i:i1]
                i = i1

        def parse_columns_sql(columns_sql):
            return [column[1:-1] for column in columns_sql.split(", ")]

        def parse_constraint_sql(sql):
            name_i0 = sql.index('[')
            name_i1 = sql.index(']', name_i0)
            name = sql[name_i0 + 1 : name_i1]

            sql = sql[name_i1 + 1:]
            kind_i1 = sql.index('(')
            kind = sql[:kind_i1].strip()

            sql = sql[kind_i1 + 1:]
            columns_i1 = sql.index(')')

            attrs = {
                'columns': parse_columns_sql(sql[:columns_i1].strip()),
                'primary_key': kind == "PRIMARY KEY",
                'unique': kind == "UNIQUE KEY",
                'foreign_key': None,
                'check': False,
                'index': False,
            }

            if kind == "FOREIGN KEY":
                sql = sql[columns_i1 + 1:]
                assert sql.strip().startswith("REFERENCES")
                ref_name_i0 = sql.index('[')
                ref_name_i1 = sql.index(']')
                ref_name = sql[ref_name_i0 + 1 : ref_name_i1]

                sql = sql[ref_name_i1 + 1:]
                ref_columns_i0 = sql.index('(')
                ref_columns_i1 = sql.index(')')
                ref_columns_sql = sql[ref_columns_i0 + 1 : ref_columns_i1]
                ref_columns = parse_columns_sql(ref_columns_sql)
                assert len(ref_columns) == 1

                attrs['foreign_key'] = (ref_name, ref_columns[0])

            return name, attrs

        def parse_index_sql(sql):
            name = sql.split('[')[1].split(']')[0]
            columns_sql = sql.split('(')[1].split(')')[0]
            columns = parse_columns_sql(columns_sql)
            orders = ['DESC' if column.find('DESC') > -1 else 'ASC' for column in columns]

            attrs = {
                'columns': columns,
                'primary_key': False,
                'unique': False,
                'foreign_key': None,
                'check': False,
                'index': True,
                'orders': orders,
                'type': Index.suffix,
            }

            return name, attrs

        query = f"SHOW CREATE TABLE {table_name}"
        cursor.execute(query)
        _, stmt = cursor.fetchone()

        constraints = {}
        l = list(parse_create_table_stmt(stmt))
        for sql in l:
            if sql.startswith("CONSTRAINT"):
                name, attrs = parse_constraint_sql(sql)
            elif sql.startswith("INDEX"):
                name, attrs = parse_index_sql(sql)
            else:
                name = None

            if name is not None:
                constraints[name] = attrs

        return constraints
