"""Step 3: the Oracle SQL recipes of recipes/sql, run in the Oracle test container on the data
of steps 1 (collected for real) and 2 (stand-in DSS client).

The inputs are loaded into a scratch schema DBMON_STORE, with tables and columns named as DSS
creates them on an Oracle connection (quoted, lower case). Each recipe runs as a CREATE TABLE
... AS of its query, with $DKU_DST_snapshot_day replaced, in the order of the Flow.

    python tests/test_analysis.py
"""
import io
import os
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import REPO, check, finish, install_fake_dataiku, sqlplus
from fake_dss import DATABASES, FakeClient

install_fake_dataiku()
from dbmon import collect, inventory, schemas  # noqa: E402

DAY = "2026-10-26"
STORE = "DBMON_STORE"
SYSDBA = "/ as sysdba"
RECIPES = ["dataset_roles", "dataset_tables", "table_ownership", "project_consumption"]
ORACLE_TYPES = {"string": "VARCHAR2(4000)", "bigint": "NUMBER(19)", "double": "NUMBER", "date": "TIMESTAMP"}


def run_in_store(script):
    """Run a SQL*Plus script as SYSDBA in the scratch schema; stop the test on any error."""
    header = ("WHENEVER SQLERROR EXIT FAILURE\nSET DEFINE OFF\nSET FEEDBACK OFF\n"
              "ALTER SESSION SET CONTAINER = FREEPDB1;\nALTER SESSION SET CURRENT_SCHEMA = %s;\n" % STORE)
    run = sqlplus(header + script + "\nEXIT\n", logon=SYSDBA)
    if run.returncode != 0:
        raise SystemExit(run.stdout[-3000:])
    return run.stdout


def sql_literal(value, column_type):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return "NULL"
    if column_type in ("bigint", "double"):
        return repr(float(value)) if column_type == "double" else str(int(value))
    if column_type == "date":
        return "TIMESTAMP '%s'" % pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    return "'" + str(value).replace("'", "''") + "'"


def load(dataset_name, rows):
    """Create the table of a dataset as DSS would, and insert its rows."""
    rows = rows.copy()
    rows["snapshot_day"] = DAY
    rows = schemas.conform(rows, dataset_name)

    columns = []
    for column_name, column_type in schemas.SCHEMAS[dataset_name]:
        columns.append('"%s" %s' % (column_name, ORACLE_TYPES[column_type]))
    script = 'CREATE TABLE "%s" (%s);\n' % (dataset_name, ", ".join(columns))

    names = []
    for column_name, column_type in schemas.SCHEMAS[dataset_name]:
        names.append('"%s"' % column_name)
    for row in rows.to_dict("records"):
        values = []
        for column_name, column_type in schemas.SCHEMAS[dataset_name]:
            values.append(sql_literal(row[column_name], column_type))
        script = script + 'INSERT INTO "%s" (%s) VALUES (%s);\n' % (dataset_name, ", ".join(names), ", ".join(values))
    run_in_store(script + "COMMIT;")


def run_recipe(name):
    path = os.path.join(REPO, "recipes", "sql", name + ".sql")
    with open(path, encoding="utf-8") as sql_file:
        lines = []
        for line in sql_file:
            if not line.lstrip().startswith("--"):
                lines.append(line)
    query = "".join(lines).strip().replace("$DKU_DST_snapshot_day", DAY)
    run_in_store('CREATE TABLE "%s" AS\n%s;' % (name, query))


def read(name):
    output = run_in_store('SET MARKUP CSV ON\nSET PAGESIZE 50000\nSELECT * FROM "%s";' % name)
    if output.strip() == "":
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(output))


# --- steps 1 and 2 ---------------------------------------------------------------------------
step1, step1_log = collect.collect_all(DATABASES[["connection", "engine", "label"]], parallel=2)
check("step 1 collected on both engines", (step1_log.status == "OK").all(), step1_log.to_string())
step2 = inventory.collect_inventory(FakeClient(), DATABASES, step1["role_settings"])

# --- scratch schema with the inputs ----------------------------------------------------------
sqlplus("ALTER SESSION SET CONTAINER = FREEPDB1;\nDROP USER %s CASCADE;\nEXIT\n" % STORE, logon=SYSDBA)
created = sqlplus("WHENEVER SQLERROR EXIT FAILURE\nALTER SESSION SET CONTAINER = FREEPDB1;\n"
                  "CREATE USER %s NO AUTHENTICATION DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;\nEXIT\n" % STORE,
                  logon=SYSDBA)
check("scratch schema created", created.returncode == 0, created.stdout[-500:])

load("tables", step1["tables"])
for name in ("dss_projects", "dss_datasets", "dss_flow_links", "dss_dataset_locations"):
    load(name, step2[name])

for name in RECIPES:
    run_recipe(name)
check("the four recipes run in Oracle", True)

roles = read("dataset_roles").set_index(["project_key", "dataset_name"])
dataset_tables = read("dataset_tables").set_index(["project_key", "dataset_name"])
ownership = read("table_ownership")
consumption = read("project_consumption").set_index("project_key")
pd.set_option("display.width", 250)
print(dataset_tables[["match_status", "database_label", "schema_name", "table_name", "schema_source"]].to_string())

# --- dataset_roles -----------------------------------------------------------------------------
role = roles.flow_role.to_dict()
check("roles: source, targets, orphans",
      role[("SALES", "raw_orders")] == "source" and role[("SALES", "orders_clean")] == "target"
      and role[("SALES", "orders_ora")] == "target" and role[("FIN", "fin_summary")] == "target"
      and role[("FIN", "dup_writer")] == "target" and role[("SALES", "old_copy")] == "orphan", role)
check("roles: orders_clean read by FIN, 1 producer, 1 consumer",
      roles.loc[("SALES", "orders_clean"), ["n_producers", "n_consumers", "n_consuming_projects"]].tolist() == [1, 1, 1])


# --- dataset_tables ----------------------------------------------------------------------------
def found(key):
    row = dataset_tables.loc[key]
    return (row.match_status, row.schema_name, row.table_name)


check("search_path candidate found", found(("SALES", "raw_orders")) == ("FOUND", "sales_dsswork", "SALES_orders"))
check("post-connect candidate found", found(("SALES", "orders_clean")) == ("FOUND", "sales_dssout", "SALES_orders"))
check("Oracle login user found", found(("SALES", "orders_ora")) == ("FOUND", "SALES_DSSWORK", "ORDERS"))
check("user of a JDBC URL connection found", found(("SALES", "via_url")) == ("FOUND", "SALES_DSSOUT", "LEDGER"))
check("found ignoring case, names as in the database", found(("SALES", "case_test")) == ("FOUND", "fin_dssout", "ledger"))
check("missing table: NOT_FOUND", found(("FIN", "missing"))[0] == "NOT_FOUND")
statuses = dataset_tables.match_status.to_dict()
check("no candidate: the reason kept",
      statuses[("SALES", "per_user")] == "PER_USER" and statuses[("SALES", "query_ds")] == "NO_TABLE"
      and statuses[("SALES", "notes_file")] == "NOT_SQL" and statuses[("SALES", "elsewhere")] == "UNMAPPED")
check("one row per dataset", len(dataset_tables) == len(step2["dss_datasets"]))

# --- table_ownership ---------------------------------------------------------------------------
owned = ownership.set_index(["label", "schema_name", "table_name"])
check("one row per step 1 table", len(ownership) == len(step1["tables"]))
same_totals = True
for label in step1["tables"].label.unique():
    step1_total = step1["tables"][step1["tables"].label == label].total_mb.sum()
    if abs(ownership[ownership.label == label].total_mb.sum() - step1_total) > 0.01:
        same_totals = False
check("space counted once: per database, the total of table_ownership = the total of step 1", same_totals)

two_owners = owned.loc[("PG app", "sales_dssout", "SALES_orders")]
check("two target datasets: owned by the smallest project key, multiple_owners = YES",
      (two_owners.status, two_owners.owner_project, two_owners.owner_dataset, two_owners.multiple_owners)
      == ("OWNED", "FIN", "dup_writer", "YES"), two_owners.to_string())
check("read by a source dataset only: READ_ONLY",
      owned.loc[("PG app", "sales_dsswork", "SALES_orders"), "status"] == "READ_ONLY")
ledger = owned.loc[("PG app", "fin_dssout", "ledger")]
check("owned by FIN, also pointed to by a SALES orphan dataset",
      ledger.status == "OWNED" and ledger.owner_project == "FIN" and ledger.n_orphan_datasets == 1
      and ledger.n_referencing_projects == 2)
check("only an out-of-pipeline dataset points to it: NO_FLOW",
      owned.loc[("PG app", "appx", "mv_totals"), "status"] == "NO_FLOW")
check("Oracle table owned by SALES", owned.loc[("ORA", "SALES_DSSWORK", "ORDERS"), ["status", "owner_project"]].tolist()
      == ["OWNED", "SALES"])
check("no dataset: UNREFERENCED", owned.loc[("ORA", "SALES_DSSOUT", "IOT_T"), "status"] == "UNREFERENCED")
check("use case and layer from the schema name",
      owned.loc[("PG app", "sales_dsswork", "SALES_orders"), ["schema_use_case", "schema_layer"]].tolist() == ["SALES", "DSSWORK"]
      and pd.isna(owned.loc[("PG app", "appx", "mv_totals"), "schema_use_case"]))

# --- project_consumption -------------------------------------------------------------------------
check("one row per project read", sorted(consumption.index) == ["FIN", "SALES"])
check("use case of the project", consumption.loc["SALES", ["use_case", "use_case_source"]].tolist() == ["SALES", "TAG"]
      and consumption.loc["FIN", "use_case_source"] == "NONE")
fin_owned = ownership[(ownership.status == "OWNED") & (ownership.owner_project == "FIN")].total_mb.sum()
check("owned_mb = the tables the project owns", abs(consumption.loc["FIN", "owned_mb"] - fin_owned) < 0.01
      and consumption.loc["FIN", "n_owned_tables"] == 2)
sales_read = (owned.loc[("PG app", "sales_dsswork", "SALES_orders"), "total_mb"]
              + owned.loc[("PG app", "sales_dssout", "SALES_orders"), "total_mb"]
              + owned.loc[("PG app", "fin_dssout", "ledger"), "total_mb"])
check("read_mb = tables SALES points to without owning them, not added to owned",
      abs(consumption.loc["SALES", "read_mb"] - sales_read) < 0.01, (consumption.loc["SALES", "read_mb"], sales_read))
sales_no_flow = (owned.loc[("PG app", "appx", "mv_totals"), "total_mb"]
                 + owned.loc[("ORA", "SALES_DSSOUT", "LEDGER"), "total_mb"])
check("no_flow_mb = tables only its out-of-pipeline datasets point to (old_copy, via_url)",
      abs(consumption.loc["SALES", "no_flow_mb"] - sales_no_flow) < 0.01
      and owned.loc[("ORA", "SALES_DSSOUT", "LEDGER"), "status"] == "NO_FLOW")
check("datasets by role and match",
      consumption.loc["SALES", ["n_datasets", "n_target_datasets", "n_source_datasets", "n_orphan_datasets"]].tolist()
      == [10, 2, 1, 7] and consumption.loc["FIN", "n_datasets_not_found"] == 1)

total_owned = consumption.owned_mb.sum()
not_owned = ownership[ownership.status != "OWNED"].total_mb.sum()
check("owned by projects + not owned = all the space of step 1 tables",
      abs(total_owned + not_owned - step1["tables"].total_mb.sum()) < 0.01)

finish()
