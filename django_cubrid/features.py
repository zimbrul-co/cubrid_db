"""
Database Features Module for the CUBRID Database Backend in Django

This module defines the DatabaseFeatures class, which specifies the capabilities
and limitations of the CUBRID database in the context of Django's ORM.

The DatabaseFeatures class is a critical component of the Django database backend
architecture. It informs Django's ORM about the specific behaviors and
characteristics of the CUBRID database, allowing the ORM to adapt its operations
accordingly. This adaptation covers various aspects of database interactions,
including transaction handling, schema management, query formation, and more.

This module is intended for internal use by Django's ORM and database backend
system. It helps maintain the database-agnostic nature of Django by providing a
clear definition of the CUBRID database's capabilities.
"""
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.utils import InterfaceError
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    """
    Database feature flags for CUBRID Database Backend in Django.

    This class extends Django's BaseDatabaseFeatures to specify the database
    capabilities and features supported by the CUBRID database backend. Each attribute
    in this class represents a specific feature or capability of the CUBRID database,
    allowing Django to appropriately adapt its behavior and queries.

    This class is used internally by Django to determine how to implement certain
    ORM features based on the capabilities of the CUBRID database.
    """

    minimum_database_version = (10, 1)

    allow_sliced_subqueries_with_in = False

    allows_group_by_selected_pks = True

    atomic_transactions = False

    can_introspect_check_constraints = False

    can_rollback_ddl = True

    closed_cursor_error_class = InterfaceError

    has_bulk_insert = True

    has_native_json_field = True

    has_select_for_update = True

    has_select_for_update_nowait = False

    has_zoneinfo_database = False

    related_fields_match_type = True

    requires_literal_defaults = True

    supports_column_check_constraints = False

    supports_comments = True

    supports_comments_inline = True

    supports_date_lookup_using_string = False

    supports_expression_indexes = False

    supports_forward_references = False

    supports_ignore_conflicts = False

    supports_paramstyle_pyformat = False

    supports_partial_indexes = False

    supports_regex_backreferencing = False

    supports_table_check_constraints = False

    supports_timezones = False

    supports_unspecified_pk = True

    @cached_property
    def introspected_field_types(self):
        """Specify how the field types are introspected with CUBRID"""
        return {
            **super().introspected_field_types,
            "BooleanField": "SmallIntegerField",
            "DurationField": "BigIntegerField",
        }

    django_test_skips = {
        "CUBRID does not support disabling constraint checks": {
            "backends.base.test_creation.TestDeserializeDbFromString.test_circular_reference",
            "backends.base.test_creation.TestDeserializeDbFromString.test_self_reference",
            "backends.base.test_creation.TestDeserializeDbFromString."
            "test_circular_reference_with_natural_key",
            "backends.tests.FkConstraintsTests.test_disable_constraint_checks_manually",
            "backends.tests.FkConstraintsTests.test_disable_constraint_checks_context_manager",
            "backends.tests.FkConstraintsTests.test_check_constraints",
            "backends.tests.FkConstraintsTests.test_check_constraints_sql_keywords",
        },
        "CUBRID does not allow duplicate indexes": {
            "schema.tests.SchemaTests.test_add_inline_fk_index_update_data",
            "schema.tests.SchemaTests.test_remove_index_together_does_not_remove_meta_indexes",
        },
        "CUBRID does not allow auto increment on char field": {
            "schema.tests.SchemaTests.test_alter_auto_field_to_char_field",
        },
        "CUBRID does not support removing the primary key": {
            "schema.tests.SchemaTests.test_alter_not_unique_field_to_primary_key",
            "schema.tests.SchemaTests.test_primary_key",
        },
        "CUBRID does not allow foreign key to reference non-primary key": {
            "schema.tests.SchemaTests.test_rename_referenced_field",
        },
        "CUBRID cannot change attributes used in foreign keys": {
            "schema.tests.SchemaTests.test_alter_pk_with_self_referential_field"
        },
    }
