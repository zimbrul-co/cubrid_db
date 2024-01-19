from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = (10, 1)

    allow_sliced_subqueries_with_in = False

    allows_group_by_selected_pks = True

    atomic_transactions = False

    can_introspect_foreign_keys = False

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
