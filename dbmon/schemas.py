"""Schema of every output dataset, written by the recipes on every run, even when a result has
no row (Oracle role_settings, a run where no database returns objects...). The schema never
depends on the data: it stays the same from one day to the next.

Adding a column to a SQL file or a result: add it here too, at the same place. A column that is not
declared here stops the recipe with its name.

Types are DSS types: string, bigint, double, date. A string column has a maximum length,
written in the schema (maxLength) so that DSS creates the column with it; a longer value is
cut, ending with "...", so that a write never fails. Lists of values are never stored in
one string: they are datasets of their own (dss_dataset_shares, dss_flow_links,
dss_project_tags, dss_connection_permissions, dss_group_members).
"""
import pandas as pd

# Maximum length of a string column, in characters
STRING_LENGTH = 1000
# Free text: errors, notes, JSON of connection settings, paths, lists of tablespaces.
# 4000 fits an Oracle VARCHAR2. If DSS creates NVARCHAR2 columns on your connection
# (2000 characters at most), set 2000.
LONG_TEXT_LENGTH = 4000
LONG_TEXT_COLUMNS = (
    "error", "note", "jdbc_url", "jdbc_properties", "driver_properties", "post_connect_statements", "naming_rule",
    "description", "location", "tablespaces", "compression", "search_path",
    "path", "resolved_path", "partition_dimensions",
)

# In front of every row of storage_units, role_settings, objects and tables
FRONT = [
    ("snapshot_day", "string"),
    ("collected_utc", "date"),
    ("label", "string"),
    ("environment", "string"),
    ("dss_connection", "string"),
    ("engine", "string"),
    ("server_name", "string"),
    ("database_name", "string"),
    ("unique_name", "string"),
    ("role", "string"),
    ("version", "string"),
]

SCHEMAS = {
    # --- step 1: collect_storage ---------------------------------------------------------------
    "storage_units": FRONT + [
        ("tablespace_name", "string"),
        ("contents", "string"),
        ("location", "string"),
        ("n_files", "bigint"),
        ("allocated_mb", "double"),
        ("used_mb", "double"),
        ("recyclebin_mb", "double"),
        ("free_mb", "double"),
        ("max_mb", "double"),
        ("raw_allocated_mb", "double"),
    ],
    "role_settings": FRONT + [
        ("role_name", "string"),
        ("setting_database", "string"),
        ("search_path", "string"),
    ],
    "objects": FRONT + [
        ("schema_name", "string"),
        ("object_name", "string"),
        ("object_type", "string"),
        ("family", "string"),
        ("parent_schema", "string"),
        ("parent_table", "string"),
        ("tablespace_name", "string"),
        ("partitioned", "string"),
        ("n_segments", "bigint"),
        ("size_mb", "double"),
        ("compression", "string"),
    ],
    "tables": FRONT + [
        ("schema_name", "string"),
        ("table_name", "string"),
        ("standalone", "string"),
        ("total_mb", "double"),
        ("table_mb", "double"),
        ("index_mb", "double"),
        ("lob_mb", "double"),
        ("other_mb", "double"),
        ("n_indexes", "bigint"),
        ("partitioned", "string"),
        ("n_segments", "bigint"),
        ("tablespaces", "string"),
        ("compression", "string"),
    ],
    "collect_log": [
        ("snapshot_day", "string"),
        ("started_utc", "date"),
        ("label", "string"),
        ("dss_connection", "string"),
        ("engine", "string"),
        ("objects", "string"),
        ("status", "string"),
        ("error", "string"),
        ("storage_units_rows", "bigint"),
        ("role_settings_rows", "bigint"),
        ("objects_rows", "bigint"),
        ("tables_rows", "bigint"),
        ("identity_seconds", "double"),
        ("storage_units_seconds", "double"),
        ("role_settings_seconds", "double"),
        ("objects_seconds", "double"),
        ("seconds", "double"),
    ],
    # --- step 2: extract_dss (raw DSS extraction) ---------------------------------------------
    "dss_projects": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("project_name", "string"),
        ("owner_login", "string"),
    ],
    "dss_project_tags": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("tag", "string"),
        ("tag_category", "string"),
        ("tag_value", "string"),
    ],
    "dss_datasets": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("dataset_name", "string"),
        ("dataset_type", "string"),
        ("managed", "string"),
        ("partitioned", "string"),
        ("partition_dimensions", "string"),
        ("connection", "string"),
        ("mode", "string"),
        ("schema", "string"),
        ("table", "string"),
        ("catalog", "string"),
        ("path", "string"),
        ("resolved_schema", "string"),
        ("resolved_table", "string"),
        ("resolved_path", "string"),
        ("shared", "string"),
        ("n_shared_with", "bigint"),
        ("metric_rows", "bigint"),
        ("metric_size", "double"),
        ("metric_files", "bigint"),
        ("is_populated", "string"),
    ],
    "dss_managed_folders": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("folder_id", "string"),
        ("folder_name", "string"),
        ("folder_type", "string"),
        ("connection", "string"),
        ("path", "string"),
        ("resolved_path", "string"),
        ("metric_size", "double"),
        ("metric_files", "bigint"),
        ("is_populated", "string"),
    ],
    "dss_dataset_shares": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("dataset_name", "string"),
        ("target_project", "string"),
    ],
    "dss_flow_links": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("recipe_name", "string"),
        ("recipe_type", "string"),
        ("direction", "string"),
        ("object_type", "string"),
        ("object_project", "string"),
        ("object_name", "string"),
        ("foreign", "string"),
    ],
    "dss_connections": [
        ("snapshot_day", "string"),
        ("connection", "string"),
        ("connection_type", "string"),
        ("credentials_mode", "string"),
        ("connection_mode", "string"),
        ("field_host", "string"),
        ("field_port", "string"),
        ("field_database_or_service", "string"),
        ("field_user", "string"),
        ("jdbc_url", "string"),
        ("jdbc_vendor", "string"),
        ("jdbc_host", "string"),
        ("jdbc_port", "string"),
        ("jdbc_database_or_service", "string"),
        ("jdbc_user", "string"),
        ("jdbc_properties", "string"),
        ("effective_host", "string"),
        ("effective_port", "string"),
        ("effective_database_or_service", "string"),
        ("effective_user", "string"),
        ("stale_fields", "string"),
        ("default_schema", "string"),
        ("driver_properties", "string"),
        ("post_connect_statements", "string"),
        ("naming_rule", "string"),
        ("error", "string"),
    ],
    "dss_connection_permissions": [
        ("snapshot_day", "string"),
        ("connection", "string"),
        ("permission", "string"),
        ("scope", "string"),
        ("group_name", "string"),
    ],
    "dss_groups": [
        ("snapshot_day", "string"),
        ("group_name", "string"),
        ("description", "string"),
        ("source_type", "string"),
        ("admin", "string"),
    ],
    "dss_group_members": [
        ("snapshot_day", "string"),
        ("group_name", "string"),
        ("login", "string"),
    ],
    "dss_users": [
        ("snapshot_day", "string"),
        ("login", "string"),
        ("display_name", "string"),
        ("source_type", "string"),
        ("enabled", "string"),
        ("user_profile", "string"),
    ],
    "inventory_log": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("status", "string"),
        ("error", "string"),
        ("datasets", "bigint"),
        ("managed_folders", "bigint"),
        ("links", "bigint"),
        ("seconds", "double"),
    ],
    # --- step 3: refine_inventory -------------------------------------------------------------
    "dataset_roles": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("object_type", "string"),
        ("object_name", "string"),
        ("managed", "string"),
        ("n_producers", "bigint"),
        ("n_consumers", "bigint"),
        ("n_consuming_projects", "bigint"),
        ("dataset_nature", "string"),
    ],
    "dataset_locations": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("dataset_name", "string"),
        ("candidate_rank", "bigint"),
        ("candidate_status", "string"),
        ("connection_name", "string"),
        ("audit_connection_name", "string"),
        ("database_label", "string"),
        ("schema_name", "string"),
        ("table_name", "string"),
        ("schema_source", "string"),
        ("note", "string"),
    ],
    "dataset_tables": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("dataset_name", "string"),
        ("match_status", "string"),
        ("connection_name", "string"),
        ("audit_connection_name", "string"),
        ("database_label", "string"),
        ("schema_name", "string"),
        ("table_name", "string"),
        ("candidate_rank", "bigint"),
        ("schema_source", "string"),
        ("note", "string"),
    ],
    "table_ownership": [
        ("snapshot_day", "string"),
        ("label", "string"),
        ("engine", "string"),
        ("server_name", "string"),
        ("database_name", "string"),
        ("schema_name", "string"),
        ("table_name", "string"),
        ("standalone", "string"),
        ("total_mb", "double"),
        ("table_mb", "double"),
        ("index_mb", "double"),
        ("lob_mb", "double"),
        ("other_mb", "double"),
        ("status", "string"),
        ("owner_project", "string"),
        ("owner_dataset", "string"),
        ("owner_nature", "string"),
        ("multiple_owners", "string"),
        ("n_owning_datasets", "bigint"),
        ("n_source_datasets", "bigint"),
        ("n_unused_datasets", "bigint"),
        ("n_referencing_projects", "bigint"),
        ("schema_use_case", "string"),
        ("schema_layer", "string"),
    ],
    "dataset_storage": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("object_type", "string"),
        ("object_name", "string"),
        ("display_name", "string"),
        ("storage_type", "string"),
        ("connection", "string"),
        ("dataset_nature", "string"),
        ("storage_kind", "string"),
        ("size_mb", "double"),
        ("size_source", "string"),
        ("counted", "string"),
        ("note", "string"),
    ],
    "project_consumption": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("project_name", "string"),
        ("use_case", "string"),
        ("use_case_source", "string"),
        ("n_datasets", "bigint"),
        ("n_managed_folders", "bigint"),
        ("n_produced", "bigint"),
        ("n_managed_idle", "bigint"),
        ("n_external_source", "bigint"),
        ("n_external_unused", "bigint"),
        ("n_datasets_found", "bigint"),
        ("n_datasets_not_found", "bigint"),
        ("n_owned_tables", "bigint"),
        ("database_mb", "double"),
        ("database_table_mb", "double"),
        ("database_index_mb", "double"),
        ("database_lob_mb", "double"),
        ("local_storage_mb", "double"),
        ("external_storage_mb", "double"),
        ("total_mb", "double"),
        ("idle_mb", "double"),
        ("database_read_mb", "double"),
        ("database_unused_mb", "double"),
        ("external_read_mb", "double"),
        ("n_size_unknown", "bigint"),
    ],
}


def string_length(column_name):
    """Maximum length of a string column."""
    if column_name in LONG_TEXT_COLUMNS:
        return LONG_TEXT_LENGTH
    return STRING_LENGTH


def dss_schema(dataset_name):
    """The schema of an output dataset, as dataiku.Dataset.write_schema expects it."""
    columns = []
    for column_name, column_type in SCHEMAS[dataset_name]:
        column = {"name": column_name, "type": column_type}
        if column_type == "string":
            column["maxLength"] = string_length(column_name)
        columns.append(column)
    return columns


def conform(rows, dataset_name):
    """Rows with exactly the declared columns of the dataset, in order and with their types.
    A declared column missing from rows is added empty. Works on rows without any row or
    column. Raises an error when rows has a column that is not declared."""
    declared = []
    for column_name, column_type in SCHEMAS[dataset_name]:
        declared.append(column_name)

    unknown = []
    for column_name in rows.columns:
        if column_name not in declared:
            unknown.append(column_name)
    if unknown:
        raise ValueError("%s: columns not declared in dbmon/schemas.py: %s" % (dataset_name, ", ".join(unknown)))

    result = pd.DataFrame(index=rows.index)
    for column_name, column_type in SCHEMAS[dataset_name]:
        if column_name in rows.columns:
            values = rows[column_name]
        else:
            values = pd.Series([None] * len(rows), index=rows.index, dtype=object)
        result[column_name] = typed(values, column_type, string_length(column_name))
    return result.reset_index(drop=True)


def typed(values, column_type, max_length):
    """values converted to the pandas type that DSS writes as column_type. Strings longer
    than max_length are cut, ending with "..."."""
    if column_type == "bigint":
        numbers = pd.to_numeric(values, errors="coerce")
        return numbers.round().astype("Int64")
    if column_type == "double":
        return pd.to_numeric(values, errors="coerce").astype("float64")
    if column_type == "date":
        return pd.to_datetime(values, errors="coerce")

    # string: text, or None when empty
    texts = []
    for value in values:
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            texts.append(None)
            continue
        text = str(value)
        if len(text) > max_length:
            text = text[:max_length - 3] + "..."
        texts.append(text)
    return pd.Series(texts, index=values.index, dtype=object)


def write(dataset, rows, dataset_name):
    """Write rows to a dataiku.Dataset with the declared schema, even when rows is empty."""
    conformed = conform(rows, dataset_name)
    dataset.write_schema(dss_schema(dataset_name))
    dataset.write_dataframe(conformed)
    return conformed
