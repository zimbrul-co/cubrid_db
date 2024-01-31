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

    can_rollback_ddl = True

    has_bulk_insert = True

    has_native_json_field = True

    has_select_for_update = True

    has_select_for_update_nowait = False

    has_zoneinfo_database = False

    related_fields_match_type = True

    requires_literal_defaults = True

    supports_date_lookup_using_string = False

    supports_expression_indexes = False

    supports_forward_references = False

    supports_ignore_conflicts = False

    supports_paramstyle_pyformat = False

    supports_partial_indexes = False

    supports_regex_backreferencing = False

    supports_table_check_constraints = False

    supports_timezones = False
