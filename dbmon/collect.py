"""Collect the storage of each database through its admin connection.

The SQL lives in sql/<engine>/, one file per result. Each file returns its final columns
and runs as-is in SQL*Plus, SQLcl or psql. This module only runs the files and puts in
front of every row: the collection time, the DSS connection, and the identity of the
database (identity.sql).
"""
import datetime
import os
import time

import pandas as pd
from dataiku import SQLExecutor2

ENGINES = ("oracle", "postgresql")
QUERIES = ("storage_units", "objects")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def read_sql(engine, name):
    """Text of sql/<engine>/<name>.sql as sent to the database: comment lines and the
    final semicolon removed."""
    path = os.path.join(SQL_DIR, engine, name + ".sql")
    with open(path, encoding="utf-8") as f:
        lines = [line for line in f if not line.lstrip().startswith("--")]
    return "".join(lines).strip().rstrip(";")


def run(connection, engine, name):
    """Result of one SQL file, with lower-case column names."""
    df = SQLExecutor2(connection=connection).query_to_df(read_sql(engine, name), infer_from_schema=True)
    df.columns = [c.lower() for c in df.columns]
    return df


def collect(connection, engine):
    """Every result of one database, as {result name: DataFrame}."""
    if engine not in ENGINES:
        raise ValueError("unknown engine %r, expected one of: %s" % (engine, ", ".join(ENGINES)))
    collected_utc = _utc_now()
    identity = run(connection, engine, "identity").iloc[0].to_dict()
    results = {}
    for name in QUERIES:
        df = run(connection, engine, name)
        front = {"collected_utc": collected_utc, "dss_connection": connection}
        front.update(identity)
        if engine == "postgresql" and name == "storage_units":
            front["database_name"] = None  # PostgreSQL tablespaces belong to the whole cluster
        for position, (column, value) in enumerate(front.items()):
            df.insert(position, column, value)
        results[name] = _naive_utc(df)
    return results


def collect_all(databases):
    """Collect every database listed in a DataFrame with the columns connection and engine.

    Returns ({result name: DataFrame}, log DataFrame). A database that fails is written in
    the log and skipped. Raises an error when no database could be collected.
    """
    frames = {name: [] for name in QUERIES}
    log = []
    for db in databases.itertuples(index=False):
        started = time.time()
        entry = {"started_utc": _utc_now(), "dss_connection": db.connection, "engine": db.engine,
                 "status": "OK", "error": None}
        try:
            results = collect(db.connection, db.engine)
        except Exception as exc:
            entry.update(status="FAILED", error=str(exc)[:2000])
            print("FAILED %s: %s" % (db.connection, exc))
        else:
            for name, df in results.items():
                frames[name].append(df)
                entry[name + "_rows"] = len(df)
        entry["seconds"] = round(time.time() - started, 1)
        log.append(entry)
    if not any(frames[name] for name in QUERIES):
        raise RuntimeError("no database could be collected:\n%s" % pd.DataFrame(log).to_string())
    stacked = {name: pd.concat(dfs, ignore_index=True) for name, dfs in frames.items()}
    # every database of a PostgreSQL cluster returns the cluster's tablespaces: keep them once
    stacked["storage_units"] = stacked["storage_units"].drop_duplicates(
        subset=["engine", "server_name", "database_name", "tablespace_name"])
    return stacked, pd.DataFrame(log)


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None, microsecond=0)


def _naive_utc(df):
    """Dates with a time zone converted to UTC without time zone, so that both engines stack."""
    for column in df.columns:
        if isinstance(df[column].dtype, pd.DatetimeTZDtype):
            df[column] = df[column].dt.tz_convert("UTC").dt.tz_localize(None)
    return df
