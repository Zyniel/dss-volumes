"""Step 1: the SQL files and dbmon.collect, on the PostgreSQL and Oracle test containers.

    python tests/test_collect.py
"""
import os
import subprocess
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import (ORA_CONTAINER, REPO, check, finish, install_fake_dataiku, psql, sqlplus,
                     OracleExecutor, PostgresExecutor)

install_fake_dataiku()
from dbmon import collect, use_case  # noqa: E402

SQL_FILES = ("identity",) + collect.QUERIES


def sql_path(engine, name):
    return os.path.join(REPO, "dbmon", "sql", engine, name + ".sql")


# --- the SQL files run as-is -----------------------------------------------------------------
for name in SQL_FILES:
    with open(sql_path("postgresql", name), encoding="utf-8") as sql_file:
        run = psql("appdb", sql_file.read())
    check("postgresql/%s.sql runs as-is in psql" % name, run.returncode == 0, run.stderr)

# docker cp writes as root: clean up as root too
subprocess.run(["docker", "exec", "-u", "root", ORA_CONTAINER, "rm", "-rf", "/tmp/dbmon_sql"])
subprocess.run(["docker", "cp", os.path.join(REPO, "dbmon", "sql", "oracle"), ORA_CONTAINER + ":/tmp/dbmon_sql"],
               check=True)
for name in SQL_FILES:
    run = sqlplus("WHENEVER SQLERROR EXIT FAILURE\nSET MARKUP CSV ON\nSET PAGESIZE 50000 FEEDBACK OFF\n"
                  "@/tmp/dbmon_sql/%s.sql\nEXIT\n" % name)
    check("oracle/%s.sql runs as-is in SQL*Plus with SELECT_CATALOG_ROLE only" % name,
          run.returncode == 0 and "ORA-" not in run.stdout and "SP2-" not in run.stdout, run.stdout[-1000:])

    with open(sql_path("oracle", name), encoding="utf-8") as sql_file:
        text = sql_file.read()
    statement_lines = []
    for line in text.splitlines():
        if not line.startswith("--"):
            statement_lines.append(line)
    statement = "\n".join(statement_lines).strip()
    check("oracle/%s.sql keeps SQL*Plus rules: no blank line in the statement, no &" % name,
          "\n\n" not in statement and "&" not in text)

for engine in collect.ENGINES:
    for name in SQL_FILES:
        sent = collect.read_sql(engine, name)
        check("%s/%s: text sent without comments nor final ;" % (engine, name),
              sent.split(None, 1)[0] in ("SELECT", "WITH") and not sent.endswith(";") and "\n--" not in sent)

# --- collect_all on both engines -------------------------------------------------------------
databases = pd.DataFrame([
    ("ADM_PG_APP", "postgresql", "Y", "PG app", "TEST"),
    ("ADM_PG_OTHER", "postgresql", "n", "PG other", " "),
    ("ADM_PG_DOWN", "postgresql", "Y", "PG down", "TEST"),
    ("ADM_ORA", "oracle", None, None, "PROD"),
], columns=["connection", "engine", "objects", "label", "environment"])
results, log = collect.collect_all(databases, parallel=4)
print(log.to_string())
by_connection = log.set_index("dss_connection")

check("log: one row per database, in the order of the databases dataset",
      log.dss_connection.tolist() == databases.connection.tolist())
check("log: the unreachable database FAILED with its error, the others OK",
      by_connection.status.to_dict() == {"ADM_PG_APP": "OK", "ADM_PG_OTHER": "OK", "ADM_PG_DOWN": "FAILED", "ADM_ORA": "OK"}
      and "no_such_db" in by_connection.loc["ADM_PG_DOWN", "error"])
check("log: seconds per query", by_connection.loc["ADM_ORA", ["identity_seconds", "storage_units_seconds",
                                                               "role_settings_seconds", "objects_seconds"]].notna().all())
check("objects = N: tablespaces only", by_connection.loc["ADM_PG_OTHER", "objects"] == "N"
      and pd.isna(by_connection.loc["ADM_PG_OTHER", "objects_rows"])
      and "ADM_PG_OTHER" not in set(results["objects"].dss_connection))
check("objects empty: Y", by_connection.loc["ADM_ORA", "objects"] == "Y")

front = ["collected_utc", "label", "environment", "dss_connection", "engine", "server_name", "database_name",
         "unique_name", "role", "version"]
check("results: storage_units, role_settings, objects, tables", list(results) == list(collect.RESULTS))
for name in results:
    check("%s: identity columns in front" % name, list(results[name].columns[:len(front)]) == front)
for name in collect.QUERIES:
    pg_columns = list(collect.run("ADM_PG_APP", "postgresql", name).columns)
    ora_columns = list(collect.run("ADM_ORA", "oracle", name).columns)
    check("%s: same columns on both engines, same order" % name, pg_columns == ora_columns,
          "%s vs %s" % (pg_columns, ora_columns))

labels = results["storage_units"].drop_duplicates("dss_connection").set_index("dss_connection")
check("label given kept, empty label = the connection, environment copied",
      labels.loc["ADM_PG_APP", "label"] == "PG app" and labels.loc["ADM_PG_APP", "environment"] == "TEST"
      and labels.loc["ADM_ORA", "label"] == "ADM_ORA" and labels.loc["ADM_ORA", "environment"] == "PROD")

oracle_tag = OracleExecutor("ADM_ORA").query_to_df(
    "SELECT SYS_CONTEXT('USERENV', 'MODULE') AS module_name, SYS_CONTEXT('USERENV', 'ACTION') AS action_name FROM dual",
    pre_queries=[collect.SESSION_TAG["oracle"] % "objects"])
check("session tag on Oracle: module DBMON, action objects", oracle_tag.values.tolist() == [["DBMON", "objects"]])
pg_tag = PostgresExecutor("ADM_PG_APP").query_to_df("SELECT current_setting('application_name') AS app",
                                                    pre_queries=[collect.SESSION_TAG["postgresql"] % "objects"])
check("session tag on PostgreSQL: application_name dbmon objects", pg_tag.app.iloc[0] == "dbmon objects")

# --- storage_units ---------------------------------------------------------------------------
storage = results["storage_units"]
pg_storage = storage[storage.engine == "postgresql"].set_index("tablespace_name")
check("PostgreSQL tablespaces kept once per cluster, without database_name",
      sorted(pg_storage.index) == ["pg_default", "pg_global", "sales_dsswork_index"]
      and pg_storage.database_name.isna().all())
check("PostgreSQL tablespace locations are directories",
      pg_storage.loc["sales_dsswork_index", "location"] == "/var/lib/postgresql/ts_index"
      and pg_storage.loc["pg_default", "location"].endswith("/base"))
ora_storage = storage[storage.engine == "oracle"].set_index("tablespace_name")
work = ora_storage.loc["SALES_DSSWORK_DATA"]
check("Oracle: the dropped table counted in recyclebin_mb", work.recyclebin_mb > 0)
check("Oracle: allocated = used + recyclebin + free",
      abs(work.allocated_mb - work.used_mb - work.recyclebin_mb - work.free_mb) < 0.01)
check("Oracle: max_mb is the autoextend ceiling, or the size without autoextend",
      work.max_mb == 200 and ora_storage.loc["SALES_DSSOUT_DATA", "max_mb"] == ora_storage.loc["SALES_DSSOUT_DATA", "allocated_mb"])

# --- objects -----------------------------------------------------------------------------------
objects = results["objects"]
pg_objects = objects[objects.dss_connection == "ADM_PG_APP"]
pg_orders = pg_objects[(pg_objects.parent_schema == "sales_dsswork") & (pg_objects.parent_table == "SALES_orders")]
check("PostgreSQL: TOAST table and index are family LOB of their table",
      set(pg_orders[pg_orders.family == "LOB"].object_type) == {"TOAST TABLE", "TOAST INDEX"})
pg_ledger = pg_objects[pg_objects.parent_table == "ledger"].set_index("object_name")
check("PostgreSQL: partitions summed under the root table and index",
      pg_ledger.loc["ledger", "partitioned"] == "YES" and pg_ledger.loc["ledger", "n_segments"] == 2
      and pg_ledger.loc["ledger_d", "partitioned"] == "YES")

ora_objects = objects[objects.engine == "oracle"]
ora_orders = ora_objects[ora_objects.parent_table == "ORDERS"]
check("Oracle: ORDERS = data, LOB segment and LOB index, 2 indexes",
      sorted(ora_orders.family) == ["INDEX", "INDEX", "LOB", "LOB", "TABLE"])
check("Oracle: the index created in the INDEX tablespace is reported there",
      ora_orders[ora_orders.object_name == "ORDERS_AMOUNT_IX"].tablespace_name.tolist() == ["SALES_DSSWORK_INDEX"])
ora_ledger = ora_objects[ora_objects.parent_table == "LEDGER"].set_index("object_name")
check("Oracle: partitions summed, compression per partition",
      ora_ledger.loc["LEDGER", "n_segments"] == 2 and ora_ledger.loc["LEDGER", "compression"] == "BASIC,NONE")
iot = ora_objects[ora_objects.parent_table == "IOT_T"]
check("Oracle: IOT top index and overflow are both table data", len(iot) == 2 and set(iot.family) == {"TABLE"})
check("Oracle: the dropped table is left out", "OLD_STUFF" not in set(ora_objects.parent_table.dropna()))
restored = ora_objects[ora_objects.parent_table == "T_RESTORE"]
check("Oracle: the restored table keeps its BIN$-named index", restored.object_name.str.startswith("BIN$").sum() == 1)
check("Oracle: uncompressed indexes are NONE", "DISABLED" not in set(ora_objects.compression.dropna()))

# --- tables ------------------------------------------------------------------------------------
tables = results["tables"]
for engine in collect.ENGINES:
    engine_tables = tables[tables.engine == engine]
    engine_objects = objects[objects.engine == engine]
    check("%s: total of tables = total of objects" % engine,
          abs(engine_tables.total_mb.sum() - engine_objects.size_mb.sum()) < 0.01)
ora_tables = tables[tables.engine == "oracle"].set_index("table_name")
check("Oracle: ORDERS with its LOB and indexes, in DATA and INDEX tablespaces",
      ora_tables.loc["ORDERS", "lob_mb"] > 24 and ora_tables.loc["ORDERS", "n_indexes"] == 2
      and ora_tables.loc["ORDERS", "tablespaces"] == "SALES_DSSWORK_DATA,SALES_DSSWORK_INDEX")
pg_tables = tables[tables.dss_connection == "ADM_PG_APP"].set_index(["schema_name", "table_name"])
check("PostgreSQL: the sequence is a standalone row",
      pg_tables.loc[("appx", "seq1"), "standalone"] == "YES" and pg_tables.loc[("appx", "seq1"), "other_mb"] > 0)

# --- role_settings ---------------------------------------------------------------------------
settings = results["role_settings"]
check("role_settings: PostgreSQL settings once per cluster, none on Oracle",
      len(settings) == 2 and set(settings.engine) == {"postgresql"})

# --- one database at a time gives the same data ----------------------------------------------
sequential, sequential_log = collect.collect_all(databases, parallel=1)
for name in collect.RESULTS:
    parallel_rows = results[name].drop(columns="collected_utc").fillna("-").astype(str)
    sequential_rows = sequential[name].drop(columns="collected_utc").fillna("-").astype(str)
    parallel_rows = parallel_rows.sort_values(list(parallel_rows.columns)).reset_index(drop=True)
    sequential_rows = sequential_rows.sort_values(list(sequential_rows.columns)).reset_index(drop=True)
    check("parallel = 4 and parallel = 1 give the same %s" % name, parallel_rows.equals(sequential_rows))

# --- errors and use case ---------------------------------------------------------------------
try:
    collect.collect("X", "mysql")
    check("unknown engine rejected", False)
except ValueError:
    check("unknown engine rejected", True)

check("use case: CDP-UseCase tag read", use_case.project_use_case("P1", ["finance", "CDP-UseCase:SALES"]) == "SALES")
check("use case: category name case does not matter", use_case.project_use_case("P1", ["cdp-usecase:SALES"]) == "SALES")
check("use case: several tags shown together",
      use_case.project_use_case("P1", ["CDP-UseCase:SALES", "CDP-UseCase:FIN"]) == "FIN,SALES")
check("use case: no tag, current rule (None for now)", use_case.project_use_case("P1", ["finance"]) is None)

finish()
