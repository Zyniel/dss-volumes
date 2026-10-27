"""Collect the storage of each database through its admin connection.

The SQL lives in sql/<engine>/, one file per result. Each file returns its final columns
and runs as-is in SQL*Plus, SQLcl or psql.

This module:
- runs the SQL files through the admin connection of each database,
- puts in front of every row: the collection time, the label and environment of the
  database, the DSS connection, and the identity of the database (identity.sql),
- copies the use case and layer that naming_rules.sql detects from the names: by schema
  onto objects and tables, by tablespace onto storage_units,
- sums the objects of each table (tables).

Variants: the engine of a database can be written <engine>-<variant> in databases, e.g.
oracle-legacy. Each SQL file is then read from sql/oracle-legacy/ when it is there, else
from sql/oracle/. A variant usually holds only naming_rules.sql, with its own rules.
"""
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from dataiku import SQLExecutor2

from dbmon import progress

ENGINES = ("oracle", "postgresql")

# SQL files run for each database, after identity.sql
QUERIES = ("naming_rules", "storage_units", "role_settings", "objects")

# DataFrames returned for each database
RESULTS = ("naming_rules", "storage_units", "role_settings", "objects", "tables")

# Columns copied from naming_rules
DETECTED_COLUMNS = ["detected_use_case", "detected_layer"]

# Families of objects (column "family" of objects.sql)
FAMILIES = ("TABLE", "INDEX", "LOB", "OTHER")

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

# Run before each query, so that DBAs recognise the sessions:
# - Oracle: module DBMON, action = the query (V$SESSION)
# - PostgreSQL: application_name "dbmon <query>" (pg_stat_activity)
SESSION_TAG = {
    "oracle": "BEGIN DBMS_APPLICATION_INFO.SET_MODULE(module_name => 'DBMON', action_name => '%s'); END;",
    "postgresql": "SET application_name = 'dbmon %s'",
}


# ---------------------------------------------------------------------------------------
# One query
# ---------------------------------------------------------------------------------------

def split_engine(engine):
    """(base engine, variant) of an engine written <engine> or <engine>-<variant>.
    The variant is None when there is none. Raises an error for an unknown base engine,
    or a variant without its folder sql/<engine>-<variant>/."""
    base = engine
    variant = None
    if "-" in engine:
        base, variant = engine.split("-", 1)
    if base not in ENGINES:
        raise ValueError("unknown engine %r, expected one of: %s, or <engine>-<variant>"
                         % (engine, ", ".join(ENGINES)))
    if variant is not None and not os.path.isdir(os.path.join(SQL_DIR, engine)):
        raise ValueError("engine %r: no folder sql/%s/ for its variant" % (engine, engine))
    return base, variant


def sql_file_path(engine, name):
    """sql/<engine>/<name>.sql. For a variant (oracle-legacy), its own file when the
    variant folder has one, else the file of the base engine (oracle)."""
    base, variant = split_engine(engine)
    if variant is not None:
        variant_path = os.path.join(SQL_DIR, engine, name + ".sql")
        if os.path.isfile(variant_path):
            return variant_path
    return os.path.join(SQL_DIR, base, name + ".sql")


def read_sql(engine, name):
    """Text of the SQL file of a query, as sent to the database: comment lines and the
    final semicolon removed. engine can be a variant (see sql_file_path)."""
    path = sql_file_path(engine, name)

    kept_lines = []
    with open(path, encoding="utf-8") as sql_file:
        for line in sql_file:
            if line.lstrip().startswith("--"):
                continue
            kept_lines.append(line)

    sql = "".join(kept_lines)
    sql = sql.strip()
    sql = sql.rstrip(";")
    return sql


def run(connection, engine, name):
    """Result of one SQL file, run in a tagged session. Column names are in lower case."""
    sql = read_sql(engine, name)
    base, variant = split_engine(engine)
    session_tag = SESSION_TAG[base] % name

    executor = SQLExecutor2(connection=connection)
    result = executor.query_to_df(sql, pre_queries=[session_tag], infer_from_schema=True)

    # Oracle returns upper-case column names, PostgreSQL lower-case ones
    result = result.rename(columns=str.lower)
    return result


def run_and_time(connection, engine, name, timings):
    """Same as run(), and records in timings how many seconds the query took, even when
    it fails."""
    started = time.time()
    progress.log("%s: %s query started" % (connection, name))
    try:
        rows = run(connection, engine, name)
    finally:
        timings[name] = seconds_since(started)
    progress.log("%s: %s query done, %d rows in %s s" % (connection, name, len(rows), timings[name]))
    return rows


# ---------------------------------------------------------------------------------------
# One database
# ---------------------------------------------------------------------------------------

def collect(connection, engine, with_objects=True, label=None, environment=None, timings=None):
    """Read one database. Returns a dict {result name: DataFrame}: storage_units and
    role_settings, plus objects and tables when with_objects is True.

    connection    DSS connection with the admin account
    engine        oracle or postgresql, or a variant: oracle-<variant>, postgresql-<variant>
    with_objects  False: tablespaces only
    label         name shown in the reports (default: the connection name)
    environment   PROD, DEV...
    timings       optional dict, filled with the seconds each query took
    """
    base, variant = split_engine(engine)
    if label is None:
        label = connection
    if timings is None:
        timings = {}

    # Columns put in front of every row: when, which database, through which connection
    front_columns = {
        "collected_utc": utc_now(),
        "label": label,
        "environment": environment,
        "dss_connection": connection,
    }
    identity = run_and_time(connection, engine, "identity", timings)
    first_row = identity.iloc[0]
    for column in identity.columns:
        front_columns[column] = first_row[column]
    # identity.sql gives the base engine (oracle); the variant (legacy) gets its own column
    front_columns["sql_variant"] = variant

    results = {}

    # Use case and layer detected from the names of the schemas and tablespaces
    naming_rules = run_and_time(connection, engine, "naming_rules", timings)
    add_front_columns(naming_rules, front_columns)
    if base == "postgresql":
        # PostgreSQL tablespaces belong to the whole cluster, not to one database
        is_tablespace = naming_rules["name_kind"] == "TABLESPACE"
        naming_rules.loc[is_tablespace, "database_name"] = None
    results["naming_rules"] = naming_rules

    # Tablespaces
    storage_units = run_and_time(connection, engine, "storage_units", timings)
    add_front_columns(storage_units, front_columns)
    if base == "postgresql":
        # PostgreSQL tablespaces belong to the whole cluster, not to one database
        storage_units["database_name"] = None
    results["storage_units"] = add_detected(storage_units, naming_rules, "TABLESPACE", "tablespace_name")

    # search_path settings of roles and databases (PostgreSQL only, always empty on Oracle)
    role_settings = run_and_time(connection, engine, "role_settings", timings)
    add_front_columns(role_settings, front_columns)
    if base == "postgresql":
        # these settings belong to the whole cluster, not to one database
        role_settings["database_name"] = None
    results["role_settings"] = role_settings

    # Objects, and the tables they belong to. An object gets the rules of its own schema,
    # a table those of the table's schema.
    if with_objects:
        objects = run_and_time(connection, engine, "objects", timings)
        add_front_columns(objects, front_columns)
        table_rows = tables(objects, list(front_columns))
        results["objects"] = add_detected(objects, naming_rules, "SCHEMA", "schema_name")
        results["tables"] = add_detected(table_rows, naming_rules, "SCHEMA", "schema_name")

    return results


def add_detected(rows, naming_rules, name_kind, name_column):
    """rows with detected_use_case and detected_layer at the end: those naming_rules gives
    to the schema or tablespace (name_kind) named in name_column. Empty when it gives none."""
    rules = naming_rules[naming_rules["name_kind"] == name_kind]
    rules = rules[["name"] + DETECTED_COLUMNS].drop_duplicates("name")
    rules = rules.rename(columns={"name": name_column})
    detected = rows.merge(rules, how="left", on=name_column)
    return detected


def tables(objects, front_columns):
    """One row per table: all its objects summed (data with every partition, indexes, LOB
    or TOAST storage).

    An object that belongs to no table (sequence, cluster, temporary segment) keeps a row
    of its own, with standalone = YES. The total of total_mb is the total of
    objects.size_mb.

    front_columns  names of the columns in front of every row, kept as they are
    """
    rows = objects.copy()

    # The table of each object: its parent table, or the object itself when it has none
    has_no_parent = rows["parent_table"].isna()
    rows["standalone"] = "NO"
    rows.loc[has_no_parent, "standalone"] = "YES"
    rows["schema_name"] = rows["parent_schema"].fillna(rows["schema_name"])
    rows["table_name"] = rows["parent_table"].fillna(rows["object_name"])

    # One size column per family: the size of the object in its family's column, 0 in the others
    for family in FAMILIES:
        size_column = family.lower() + "_mb"
        in_family = rows["family"] == family
        rows[size_column] = 0.0
        rows.loc[in_family, size_column] = rows.loc[in_family, "size_mb"]

    # Index names, to count the indexes of each table
    is_index = rows["family"] == "INDEX"
    rows["index_name"] = None
    rows.loc[is_index, "index_name"] = rows.loc[is_index, "object_name"]

    # Compression of the table data, not of its indexes or LOBs
    is_data = rows["family"] == "TABLE"
    rows["data_compression"] = None
    rows.loc[is_data, "data_compression"] = rows.loc[is_data, "compression"]

    # One row per table
    group_columns = front_columns + ["schema_name", "table_name", "standalone"]
    grouped = rows.groupby(group_columns, dropna=False, sort=False)
    result = grouped.agg(
        total_mb=("size_mb", "sum"),
        table_mb=("table_mb", "sum"),
        index_mb=("index_mb", "sum"),
        lob_mb=("lob_mb", "sum"),
        other_mb=("other_mb", "sum"),
        n_indexes=("index_name", "nunique"),
        partitioned=("partitioned", "max"),
        n_segments=("n_segments", "sum"),
        tablespaces=("tablespace_name", distinct_list),
        compression=("data_compression", distinct_list),
    )
    result = result.reset_index()

    # Sums of decimal numbers: keep 3 decimals, like the sizes of the objects
    for size_column in ["total_mb", "table_mb", "index_mb", "lob_mb", "other_mb"]:
        result[size_column] = result[size_column].round(3)

    return result


# ---------------------------------------------------------------------------------------
# All databases
# ---------------------------------------------------------------------------------------

def collect_all(databases, parallel=4):
    """Collect every database listed in the databases DataFrame, with the columns:

    connection   DSS connection with the admin account
    engine       oracle or postgresql, or a variant: oracle-<variant>, postgresql-<variant>
    objects      optional. N: tablespaces only, no objects nor tables (default Y)
    label        optional. Name shown in the reports (default: the connection)
    environment  optional. PROD, DEV...

    Up to `parallel` databases are read at the same time. Returns (results, log):
    - results: {result name: DataFrame} with the rows of every database
    - log: one row per database, OK or FAILED
    A database that fails is written in the log and skipped. Raises an error when no
    database could be collected.
    """
    started = time.time()
    database_rows = list(databases.itertuples(index=False))

    workers = int(parallel)
    if workers < 1:
        workers = 1
    progress.log("Database extraction: %d databases, %d at the same time" % (len(database_rows), workers))
    counter = progress.Counter(len(database_rows))

    def collect_one(database):
        return collect_one_database(database, counter)

    # Read the databases, up to `workers` at the same time. map() keeps their order.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(collect_one, database_rows))

    # Gather the log entries, and the results of each database by result name
    log_entries = []
    collected = 0
    frames = {}
    for name in RESULTS:
        frames[name] = []

    for log_entry, results in outcomes:
        log_entries.append(log_entry)
        if results is None:
            continue  # this database failed
        collected = collected + 1
        for name in results:
            frames[name].append(results[name])

    log = pd.DataFrame(log_entries)
    progress.log("Database extraction done: %d of %d databases collected in %s s"
                 % (collected, len(database_rows), seconds_since(started)))
    if collected == 0:
        raise RuntimeError("no database could be collected:\n%s" % log.to_string())

    # Stack the rows of all the databases
    stacked = {}
    for name in RESULTS:
        if frames[name]:
            stacked[name] = pd.concat(frames[name], ignore_index=True)
        else:
            stacked[name] = pd.DataFrame()

    # Every database of a PostgreSQL cluster returns the cluster's tablespaces: keep them once
    stacked["storage_units"] = stacked["storage_units"].drop_duplicates(
        subset=["engine", "server_name", "database_name", "tablespace_name"])
    stacked["naming_rules"] = stacked["naming_rules"].drop_duplicates(
        subset=["engine", "server_name", "database_name", "name_kind", "name"])
    # Same for the role settings
    if not stacked["role_settings"].empty:
        stacked["role_settings"] = stacked["role_settings"].drop_duplicates(
            subset=["engine", "server_name", "role_name", "setting_database"])

    return stacked, log


def collect_one_database(database, counter=None):
    """Collect one row of the databases DataFrame.
    Returns (log entry, results), results being None when the database failed.
    counter: progress.Counter of the databases done, for the progress traces."""
    started = time.time()
    connection = database.connection
    engine = database.engine
    label = optional_value(database, "label", connection)
    environment = optional_value(database, "environment", None)
    objects_flag = optional_value(database, "objects", "Y").upper()
    if objects_flag != "N":
        objects_flag = "Y"

    log_entry = {
        "started_utc": utc_now(),
        "label": label,
        "dss_connection": connection,
        "engine": engine,
        "objects": objects_flag,
        "status": "OK",
        "error": None,
    }

    progress.log("database %s (%s, label %s): started" % (connection, engine, label))
    timings = {}
    results = None
    try:
        results = collect(connection, engine, objects_flag == "Y", label, environment, timings)
    except Exception as error:
        log_entry["status"] = "FAILED"
        log_entry["error"] = str(error)[:2000]  # first 2000 characters

    if results is not None:
        for name in results:
            log_entry[name + "_rows"] = len(results[name])
    for query_name in timings:
        log_entry[query_name + "_seconds"] = timings[query_name]
    log_entry["seconds"] = seconds_since(started)

    done = ""
    if counter is not None:
        done = " " + counter.next()
    if results is None:
        progress.log("database%s %s: FAILED in %s s: %s" % (done, connection, log_entry["seconds"], log_entry["error"]))
    else:
        progress.log("database%s %s: OK in %s s" % (done, connection, log_entry["seconds"]))
    return log_entry, results


def optional_value(database, column, default):
    """Value of an optional column of the databases dataset, as stripped text.
    default when the column does not exist or the cell is empty."""
    if not hasattr(database, column):
        return default

    value = getattr(database, column)
    if value is None or pd.isna(value):
        return default

    text = str(value).strip()
    if text == "":
        return default
    return text


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

def add_front_columns(rows, front_columns):
    """Insert the front columns at the start of rows (in place), then convert the dates
    with a time zone to UTC without time zone, so that the rows of both engines stack."""
    position = 0
    for column, value in front_columns.items():
        rows.insert(position, column, value)
        position = position + 1

    for column in rows.columns:
        if isinstance(rows[column].dtype, pd.DatetimeTZDtype):
            utc_dates = rows[column].dt.tz_convert("UTC")
            rows[column] = utc_dates.dt.tz_localize(None)


def distinct_list(values):
    """Distinct values, sorted and comma separated. None when there is none."""
    distinct = set()
    for value in values.dropna():
        distinct.add(str(value))

    if not distinct:
        return None
    return ",".join(sorted(distinct))


def utc_now():
    """Current UTC time, without time zone nor microseconds."""
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.replace(tzinfo=None, microsecond=0)


def seconds_since(started):
    """Seconds elapsed since started (a time.time() value), one decimal."""
    return round(time.time() - started, 1)
