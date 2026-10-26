"""Schema of every output dataset, written by the recipes on every run, even when a result has
no row (Oracle role_settings, a run where no database returns objects...). The schema never
depends on the data: it stays the same from one day to the next.

Adding a column to a SQL file: add it here too, at the same place. A column that is not
declared here stops the recipe with its name.

Types are DSS types: string, bigint, double, date.
"""
import pandas as pd

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
    # --- step 2: inventory_dss -----------------------------------------------------------------
    "dss_datasets": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("project_tags", "string"),
        ("use_case", "string"),
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
        ("flow_role", "string"),
        ("n_producers", "bigint"),
        ("n_consumers", "bigint"),
        ("shared", "string"),
        ("shared_with", "string"),
        ("used_by_projects", "string"),
        ("metric_rows", "bigint"),
        ("metric_size", "double"),
        ("is_populated", "string"),
        ("database_label", "string"),
        ("schema_source", "string"),
        ("schema_candidates", "string"),
        ("location_status", "string"),
        ("location_note", "string"),
    ],
    "dss_flow_links": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("recipe_name", "string"),
        ("recipe_type", "string"),
        ("direction", "string"),
        ("dataset_project", "string"),
        ("dataset_name", "string"),
        ("foreign", "string"),
    ],
    "dss_connections": [
        ("snapshot_day", "string"),
        ("connection", "string"),
        ("connection_type", "string"),
        ("credentials_mode", "string"),
        ("host", "string"),
        ("port", "string"),
        ("database_or_service", "string"),
        ("login_user", "string"),
        ("default_schema", "string"),
        ("jdbc_properties", "string"),
        ("post_connect_statements", "string"),
        ("naming_rule", "string"),
        ("error", "string"),
    ],
    "inventory_log": [
        ("snapshot_day", "string"),
        ("project_key", "string"),
        ("status", "string"),
        ("error", "string"),
        ("datasets", "bigint"),
        ("links", "bigint"),
    ],
}


def dss_schema(dataset_name):
    """The schema of an output dataset, as dataiku.Dataset.write_schema expects it."""
    columns = []
    for column_name, column_type in SCHEMAS[dataset_name]:
        columns.append({"name": column_name, "type": column_type})
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
        result[column_name] = typed(values, column_type)
    return result.reset_index(drop=True)


def typed(values, column_type):
    """values converted to the pandas type that DSS writes as column_type."""
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
        else:
            texts.append(str(value))
    return pd.Series(texts, index=values.index, dtype=object)


def write(dataset, rows, dataset_name):
    """Write rows to a dataiku.Dataset with the declared schema, even when rows is empty."""
    conformed = conform(rows, dataset_name)
    dataset.write_schema(dss_schema(dataset_name))
    dataset.write_dataframe(conformed)
    return conformed
