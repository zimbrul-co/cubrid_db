import operator

from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):

    allows_group_by_pk = True

    # Can an object have a primary key of 0? MySQL says No.
    allows_primary_key_0 = True

    allow_sliced_subqueries = False

    # Does the backend prevent running SQL queries in broken transactions?
    atomic_transactions = False

    can_defer_constraint_checks = False

    # Support for the DISTINCT ON clause
    can_distinct_on_fields = False

    can_introspect_duration_field = False

    # CUBRID 9.3 can't retrieve foreign key info from catalog tables.
    can_introspect_foreign_keys = False

    can_introspect_json_field = True

    can_introspect_small_integer_field = True

    can_return_id_from_insert = False

    can_rollback_ddl = True

    # insert into ... values(), (), ()
    has_bulk_insert = True

    has_native_json_field = True

    # This feature is supported after 9.3
    has_select_for_update = True

    has_select_for_update_nowait = False

    # Does the database have a copy of the zoneinfo database?
    has_zoneinfo_database = False

    ignores_nulls_in_unique_constraints = False

    related_fields_match_type = True

    # When performing a GROUP BY, is an ORDER BY NULL required
    # to remove any ordering?
    requires_explicit_null_ordering_when_grouping = False

    # Can't take defaults as parameter
    requires_literal_defaults = True

    supports_date_lookup_using_string = False

    supports_expression_indexes = False

    # Can a fixture contain forward references? i.e., are
    # FK constraints checked at the end of transaction, or
    # at the end of each save operation?
    supports_forward_references = False

    supports_ignore_conflicts = False

    supports_json_field_contains = True

    supports_paramstyle_pyformat = False

    supports_partial_indexes = False

    supports_regex_backreferencing = False

    supports_table_check_constraints = False

    supports_timezones = False

    uses_autocommit = True
    uses_savepoints = True
