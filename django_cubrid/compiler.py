"""
SQL Compiler Module for CUBRID Database Backend in Django.

This module contains classes and functions responsible for compiling Django ORM
queries into SQL queries that are compatible with the CUBRID database. It extends
and customizes Django's default SQL compiler to accommodate the SQL syntax,
functions, and conventions specific to CUBRID.

The module includes classes like SQLCompiler, SQLInsertCompiler, SQLDeleteCompiler,
and SQLUpdateCompiler, each tailored to handle different aspects of SQL query
compilation. These compilers translate Django's QuerySet methods and expressions
into corresponding SQL statements, ensuring that complex queries, including
aggregations, joins, and subqueries, are executed correctly on a CUBRID database.

Key Features:
- Custom SQL query compilation: Adapts Django's query construction to CUBRID's SQL syntax.
- Support for complex queries: Handles advanced ORM features including joins, subqueries,
  and aggregation.
- Performance optimizations: Includes CUBRID-specific optimizations to enhance query
  performance.

This module plays a crucial role in the integration between Django's ORM and the CUBRID
database, acting as a bridge that translates high-level ORM queries into raw SQL queries
understood by CUBRID.

Note:
- This module is part of the Django-CUBRID database backend and is not intended to be
  used directly by application developers.
- Understanding of Django's ORM internals and CUBRID's SQL syntax is essential for
  modifying or extending this module.
"""
from django.db.models.sql.compiler import (
    SQLCompiler as BaseSQLCompiler,
    SQLInsertCompiler as BaseSQLInsertCompiler,
    SQLDeleteCompiler as BaseSQLDeleteCompiler,
    SQLUpdateCompiler as BaseSQLUpdateCompiler,
    SQLAggregateCompiler as BaseSQLAggregateCompiler,
)
from django.db.models.fields.json import (
    compile_json_path,
    ContainedBy,
    DataContains,
    HasKeyLookup,
    KeyTransform,
    KeyTransformIn,
)


def json_data_contains_as_cubrid(self, compiler, connection):
    """For json data_contains, need to compare the result of JSON_CONTAINS with 1"""
    sql, params = self.as_sql(compiler, connection)
    sql = f"{sql}=1"
    return sql, params

setattr(DataContains, 'as_cubrid', json_data_contains_as_cubrid)


def json_contained_by_as_cubrid(self, compiler, connection):
    """For json contained_by, need to compare the result of JSON_CONTAINS with 1"""
    sql, params = self.as_sql(compiler, connection)
    sql = f"{sql}=1"
    return sql, params

setattr(ContainedBy, 'as_cubrid', json_contained_by_as_cubrid)


def json_key_transform_as_cubrid(self, compiler, connection):
    """For json key transform, set usage of JSON_EXTRACT function"""
    lhs, params, key_transforms = self.preprocess_lhs(compiler, connection)
    json_path = compile_json_path(key_transforms)
    return f"JSON_EXTRACT({lhs}, '{json_path}')", params

setattr(KeyTransform, 'as_cubrid', json_key_transform_as_cubrid)


def json_has_key_lookup_as_cubrid(self, compiler, connection):
    """For json has_key, set usage of JSON_CONTAINS_PATH function"""
    sql, params = self.as_sql(compiler, connection,
        template="JSON_CONTAINS_PATH(%s, 'one', %%s)=1",
    )
    return sql, params

setattr(HasKeyLookup, 'as_cubrid', json_has_key_lookup_as_cubrid)


def json_key_transform_in_resolve_expression_parameter(self, compiler, connection, sql, param):
    """Use the JSON_EXTRACT function, like the MySQL backend"""
    sql, params = super(KeyTransformIn, self).resolve_expression_parameter(
        compiler,
        connection,
        sql,
        param,
    )
    if not hasattr(param, "as_sql"):
        sql = "JSON_EXTRACT(%s, '$')"
    return sql, params

setattr(KeyTransformIn, 'resolve_expression_parameter',
        json_key_transform_in_resolve_expression_parameter)



class SQLCompiler(BaseSQLCompiler):
    """
    Custom SQL Compiler for the CUBRID Database Backend in Django.

    This class extends Django's standard SQL compiler to generate SQL queries
    specifically formatted for the CUBRID database. It primarily overrides the
    'as_sql' method to customize how SQL queries are constructed, particularly
    in handling limit and offset clauses which might differ in syntax from other
    databases.

    Note:
    - This class should only be modified with a thorough understanding of both Django's
      query compilation process and CUBRID's SQL syntax, particularly as it pertains
      to query limits and offsets.
    """
    def as_sql(self, with_limits=True, with_col_aliases=False):
        """
        Creates and returns the SQL query string and its parameters for this query.

        This method constructs the SQL query based on the current query context,
        optionally including column aliases and limit/offset clauses. It first
        calls the parent class's implementation of `as_sql` to generate the base
        query string and parameters. Then, if 'with_limits' is True, it modifies
        the query to include CUBRID-specific limit and offset clauses.

        Parameters:
        with_limits (bool): If True, includes limit/offset information in the query.
                            If False, these clauses are omitted. Defaults to True.
        with_col_aliases (bool): If True, includes column aliases in the query.
                                Defaults to False.

        Returns:
        tuple: A tuple containing the SQL query string and a list of parameters.
            The query string is adapted to include limit/offset clauses as per
            CUBRID's syntax if 'with_limits' is True.

        The limit/offset handling is specifically adapted for CUBRID's SQL syntax.
        If 'high_mark' (upper limit) is set in the query, it calculates the number
        of rows to fetch. If 'low_mark' (offset) is also set, it includes both
        offset and limit in the SQL query. Otherwise, only the limit is included.

        Example Usage:
        # Example for internal usage within Django's ORM framework
        compiler = SQLCompiler(...)
        sql, params = compiler.as_sql(with_limits=True, with_col_aliases=False)
        """
        sql, params = super().as_sql(
            with_limits=False,
            with_col_aliases=with_col_aliases,
        )

        if with_limits:
            if self.query.high_mark is not None:
                row_count = self.query.high_mark - self.query.low_mark
                if self.query.low_mark:
                    sql = sql + f' LIMIT {self.query.low_mark},{row_count}'
                else:
                    sql = sql + f' LIMIT {row_count}'
            else:
                val = self.connection.ops.no_limit_value()
                if val:
                    if self.query.low_mark:
                        sql = sql + f' LIMIT {self.query.low_mark},{val}'

        return sql, params


class SQLInsertCompiler(BaseSQLInsertCompiler, BaseSQLCompiler):
    """
    SQL Insert Compiler for the CUBRID Database Backend in Django.

    This class is a specialized version of Django's SQL insert compiler, tailored
    for use with the CUBRID database. It inherits from Django's standard
    SQLInsertCompiler as well as the custom SQLCompiler specifically designed
    for CUBRID. This inheritance structure allows the class to leverage the
    general insert compilation logic of Django while also applying any CUBRID-specific
    adaptations defined in SQLCompiler.

    The SQLInsertCompiler class is used by Django's ORM to compile insert statements
    into SQL queries that are compatible with the CUBRID database. This includes
    handling of batch inserts, handling of primary key values, and any other
    CUBRID-specific considerations for insert operations.

    As this class does not define additional methods or properties and solely relies
    on its parent classes, it serves as a bridge that combines the general insert
    compilation logic with CUBRID-specific customizations.
    """
    def execute_sql(self, returning_fields=None):
        """
        Custom implementation for the insert execute_sql.
        last_insert_id() does not work if the pk value is provided, and returns None.
        In that case, we need to use the pre-save value of the pk and put in in the
        returned tuple, so the model instance does not have the value set to None
        after saving.
        """
        rows = super().execute_sql(returning_fields)
        if not self.returning_fields:
            return rows

        assert len(rows) == 1

        pre_save_values = [self.pre_save_val(field, self.query.objs[0])
            for field in self.returning_fields]
        returning_values = list(rows[0])
        returning_values = [psv if rv is None else rv for psv, rv in
            zip(pre_save_values, returning_values)]
        return [tuple(returning_values)]



class SQLDeleteCompiler(BaseSQLDeleteCompiler, BaseSQLCompiler):
    """
    SQL Delete Compiler for the CUBRID Database Backend in Django.

    This class extends Django's standard SQL delete compiler, integrating it with
    the custom SQLCompiler tailored for the CUBRID database. It inherits the
    functionality of Django's SQLDeleteCompiler and the CUBRID-specific adaptations
    from the SQLCompiler. This setup ensures that delete operations generated by
    Django's ORM are compatible with the CUBRID database's SQL syntax and behavior.

    The SQLDeleteCompiler is responsible for compiling Django ORM delete queries
    into SQL commands that can be executed on a CUBRID database. It handles
    the translation of Django's QuerySet delete operations into appropriate
    SQL DELETE statements, taking into account any CUBRID-specific requirements
    or optimizations.

    Since this class does not override or add any methods to those inherited from
    its parent classes, it primarily functions to ensure that delete operations
    in Django are executed with a full understanding of CUBRID's SQL capabilities
    and constraints.
    """


class SQLUpdateCompiler(BaseSQLUpdateCompiler, BaseSQLCompiler):
    """
    SQL Update Compiler for the CUBRID Database Backend in Django.

    This class is a specialized compiler that extends Django's built-in SQL update
    compiler with adaptations for the CUBRID database. It inherits from Django's
    standard SQLUpdateCompiler as well as the custom SQLCompiler designed for CUBRID.
    The dual inheritance allows the class to utilize Django's update compilation logic
    while integrating any CUBRID-specific customizations defined in SQLCompiler.

    The primary function of the SQLUpdateCompiler is to compile Django ORM update
    queries into SQL statements that are executable on the CUBRID database. It ensures
    that the update operations, including bulk updates and field-specific updates, are
    translated into SQL queries that adhere to CUBRID's SQL syntax and performance
    considerations.

    Since this class is a straightforward subclass that does not override or introduce
    new methods, it mainly serves as a conduit to ensure that update operations
    are compatible with the nuances of the CUBRID database.
    """


class SQLAggregateCompiler(BaseSQLAggregateCompiler, BaseSQLCompiler):
    """
    SQL Aggregate Compiler for the CUBRID Database Backend in Django.

    This class extends Django's standard SQL aggregate compiler to cater to the
    specific requirements of the CUBRID database. By inheriting from both Django's
    SQLAggregateCompiler and the custom SQLCompiler for CUBRID, it ensures that
    aggregate queries generated by Django's ORM are compatible with the SQL syntax
    and nuances of the CUBRID database.

    The SQLAggregateCompiler class is responsible for compiling Django ORM aggregate
    queries, such as COUNT, SUM, AVG, MIN, and MAX, into SQL statements that can be
    executed on a CUBRID database. It handles the translation of Django's QuerySet
    aggregate methods into the corresponding SQL aggregate functions, considering any
    CUBRID-specific syntax or behavior.

    As this class does not introduce any new methods or properties and relies solely
    on the functionality provided by its parent classes, it primarily serves to combine
    Django's general aggregate compilation logic with CUBRID-specific adaptations.
    """
