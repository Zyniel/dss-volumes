"""dbmon: database storage monitoring for Dataiku, as a project library.

Engines: Oracle and PostgreSQL, through adapters (dbmon.adapters) that all fill the
same contract (dbmon.model). Results stack across engines and join on their keys.

    import dbmon
    conns = ["MON_ORA_PRD", {"connection": "MON_PG_UAT", "env": "UAT"}]
    dbmon.tablespaces(conns)                                   # one row per tablespace
    dbmon.tables(conns, schemas=["SALES_DSSWORK"])             # one schema
    dbmon.objects("MON_ORA_PRD", table="SALES_orders")         # one table, schema looked up
    dbmon.write({"objects": "db_objects", "tables": "db_tables"}, conns, runlog="db_runlog")

connections: a Dataiku connection name, a list of names or of dicts {"connection":
name, "engine": optional, other keys become label columns}, or a DataFrame with a
"connection" column. Choosing and labelling them is the caller's job.
schemas: exact names, chosen by the caller; by default every non-system schema.
The use-case convention is optional: see dbmon.uc.
"""
from . import model, uc
from .core import collect, uid, write

__all__ = ["tablespaces", "schemas", "objects", "table_attributes", "tables", "lifecycle",
           "collect", "write", "expand_details", "uid", "uc", "model"]


def tablespaces(connections, names=None, with_log=False):
    """One row per tablespace: all of them, or the ones named."""
    return collect("tablespaces", connections, tablespaces=names, with_log=with_log)


def schemas(connections, schemas=None, with_log=False):
    """One row per schema and tablespace it is tied to."""
    return collect("schemas", connections, schemas=schemas, with_log=with_log)


def objects(connections, schemas=None, table=None, with_log=False):
    """One row per object and tablespace; table keeps the objects of one table."""
    return collect("objects", connections, schemas=schemas, table=table, with_log=with_log)


def table_attributes(connections, schemas=None, table=None, with_log=False):
    """One row per table: kind and optimizer statistics."""
    return collect("table_attributes", connections, schemas=schemas, table=table, with_log=with_log)


def tables(connections, schemas=None, table=None, with_log=False):
    """One row per table: objects rolled up (indexes, LOB / TOAST, partitions) plus attributes."""
    return collect("tables", connections, schemas=schemas, table=table, with_log=with_log)


def lifecycle(connections, schemas=None, table=None, with_log=False):
    """One row per table: creation, last DDL, changes since the last statistics."""
    return collect("lifecycle", connections, schemas=schemas, table=table, with_log=with_log)


def expand_details(df):
    """Copy of a result with its "details" JSON spread into columns (for one engine at a time)."""
    import json
    import pandas as pd
    if "details" not in df.columns:
        return df.copy()
    parsed = pd.DataFrame([json.loads(v) if isinstance(v, str) else {} for v in df["details"]],
                          index=df.index)
    parsed = parsed[[c for c in parsed.columns if c not in df.columns]]
    return pd.concat([df.drop(columns=["details"]), parsed], axis=1)
