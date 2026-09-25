"""Results computed from the facts, written once for every engine.

Inputs are the facts of one database, already checked against the contract
(dbmon.model): contract columns plus engine columns.
"""
import numpy as np
import pandas as pd

STALE_PERCENT = 10      # Oracle's default STALE_PERCENT, used for every engine
FAMILIES = ("TABLE", "INDEX", "LOB", "LOBINDEX")


def derive(name, facts):
    """Derived result `name` from its source facts ({fact name: DataFrame})."""
    return {"tables": tables}[name](**facts)


def post(name, df):
    """Computed columns of a fact (see Column.computed in the contract)."""
    return lifecycle(df) if name == "lifecycle" else df


def tables(objects, table_attributes):
    """One row per table: its objects rolled up, joined to its attributes.

    An object without a table (Oracle cluster or in-flight TEMPORARY segment,
    PostgreSQL sequence) gets a row of its own, named after it, so that the sizes
    of the tables add up to the sizes of the objects.
    """
    obj = objects.copy()
    resolved = obj["parent_table"].notna()
    obj["_schema"] = obj["parent_schema"].where(resolved, obj["schema_name"])
    obj["_table"] = obj["parent_table"].where(resolved, obj["object_name"])
    obj["_unresolved"] = obj["object_type"].where(~resolved)
    size = pd.to_numeric(obj["size_mb"]).fillna(0.0)
    family, kind = obj["family"], obj["ts_kind"]
    obj = obj.assign(
        _total=size,
        _table_mb=size.where(family == "TABLE", 0.0),
        _index_mb=size.where(family == "INDEX", 0.0),
        _lob_mb=size.where(family == "LOB", 0.0),
        _lobindex_mb=size.where(family == "LOBINDEX", 0.0),
        _other_mb=size.where(~family.isin(FAMILIES), 0.0),
        _default=size.where(kind == "DEFAULT", 0.0),
        _quota=size.where(kind == "QUOTA", 0.0),
        _elsewhere=size.where(~kind.isin(("DEFAULT", "QUOTA")), 0.0),
        _segments=pd.to_numeric(obj["n_segments"]).fillna(0),
    )
    obj["_partitions"] = obj["_segments"].where((family == "TABLE") & (obj["partitioned"] == "YES"), 0)
    keys = ["_schema", "_table"]
    grouped = obj.groupby(keys, sort=False)
    roll = grouped.agg(unresolved_type=("_unresolved", "first"), n_segments=("_segments", "sum"),
                       total_mb=("_total", "sum"), table_mb=("_table_mb", "sum"),
                       index_mb=("_index_mb", "sum"), lob_mb=("_lob_mb", "sum"),
                       lobindex_mb=("_lobindex_mb", "sum"), other_mb=("_other_mb", "sum"),
                       in_default_ts_mb=("_default", "sum"), in_quota_ts_mb=("_quota", "sum"),
                       in_other_ts_mb=("_elsewhere", "sum"), n_tablespaces=("tablespace_name", "nunique"),
                       n_table_partitions=("_partitions", "sum"))
    roll["tablespaces"] = grouped["tablespace_name"].agg(_join)
    roll["n_indexes"] = obj[family == "INDEX"].groupby(keys)["object_name"].nunique()
    roll["n_lob_segments"] = obj[family == "LOB"].groupby(keys)["object_name"].nunique()
    roll["table_compression"] = obj[family == "TABLE"].groupby(keys)["compression"].agg(_join)
    roll = roll.reset_index().rename(columns={"_schema": "schema_name", "_table": "table_name"})

    attrs = table_attributes[["schema_name", "table_name", "table_kind", "storage_created",
                              "row_estimate", "avg_row_len", "last_analyzed"]]
    out = attrs.merge(roll, on=["schema_name", "table_name"], how="outer")
    out["table_kind"] = out["table_kind"].fillna(out["unresolved_type"]).fillna("UNRESOLVED")
    for column in ("n_segments", "n_tablespaces", "n_indexes", "n_lob_segments", "n_table_partitions"):
        out[column] = out[column].fillna(0).astype("int64")
    for column in ("total_mb", "table_mb", "index_mb", "lob_mb", "lobindex_mb", "other_mb",
                   "in_default_ts_mb", "in_quota_ts_mb", "in_other_ts_mb"):
        out[column] = out[column].fillna(0.0).round(3)
    rows, length = pd.to_numeric(out["row_estimate"]), pd.to_numeric(out["avg_row_len"])
    out["est_row_data_mb"] = (rows * length / 1048576).round(3)
    return out.sort_values(["schema_name", "total_mb"], ascending=[True, False], kind="stable")


def lifecycle(df):
    """dml_pct_since_stats and stale_est, the same way for every engine."""
    out = df.copy()
    dml = pd.to_numeric(out["dml_since_stats"])
    rows = pd.to_numeric(out["row_estimate"])
    out["dml_pct_since_stats"] = (100 * dml / rows.where(rows != 0)).round(2)
    stale = (out["truncated_since_stats"] == "YES").to_numpy(dtype=bool) | \
            (dml.fillna(0) > rows.fillna(0) * STALE_PERCENT / 100).to_numpy(dtype=bool)
    out["stale_est"] = np.where(out["last_analyzed"].isna(), None, np.where(stale, "YES", "NO"))
    return out


def _join(values):
    """Sorted distinct values, comma-separated lists flattened; None when empty."""
    tokens = {t.strip() for v in values.dropna() for t in str(v).split(",") if t.strip()}
    return ",".join(sorted(tokens)) or None
