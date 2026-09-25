"""End-to-end tests of dbmon: real PostgreSQL (psql in the dbmon-pg container) and a
canned Oracle, through a stand-in for the dataiku module."""
import io
import json
import os
import re
import subprocess
import sys
import types

sys.dont_write_bytecode = True
import pandas as pd

REPO = sys.argv[1]
sys.path.insert(0, os.path.join(REPO, "dss", "lib", "python"))
DATASETS, CALLS = {}, []
PG_DBS = {"PG_APP": "appdb", "PG_APP_2": "appdb", "PG_OTHER": "otherdb", "PG_DOWN": "no_such_db"}
ORA_FAIL = set()


class PgExecutor(object):
    def __init__(self, connection):
        self.connection, self.db = connection, PG_DBS[connection]

    def query_to_df(self, sql, pre_queries=None, infer_from_schema=False):
        CALLS.append((self.connection, sql))
        script = "".join(q.rstrip().rstrip(";") + ";\n" for q in (pre_queries or [])) + sql + ";\n"
        run = subprocess.run(["docker", "exec", "-i", "dbmon-pg", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1",
                              "--csv", "-U", "dbmon", "-d", self.db],
                             input=script, capture_output=True, text=True, encoding="utf-8")
        if run.returncode:
            raise Exception(run.stderr.strip())
        return pd.read_csv(io.StringIO(run.stdout)) if run.stdout.strip() else pd.DataFrame()


def ora(rows, columns):
    return pd.DataFrame(rows, columns=[c.upper() for c in columns])


ORA = {
    "tablespaces": ora([
        ("SALES_DSSWORK_DATA", "PERMANENT", 1024.0, 300.0, 50.0, 674.0, 32768.0, "YES", "DATAC1"),
        ("SALES_DSSWORK_INDEX", "PERMANENT", 512.0, 12.0, 0.0, 500.0, 32768.0, "YES", "DATAC1")],
        ["tablespace_name", "contents", "allocated_mb", "used_mb", "reclaimable_mb", "free_mb", "max_mb",
         "bigfile", "asm_diskgroups"]),
    "schemas": ora([
        ("SALES_DSSWORK", "SALES_DSSWORK", "SALES_DSSWORK_DATA", "Y", "UNLIMITED", None, 290.0, 50.0, "OPEN"),
        ("SALES_DSSWORK", "SALES_DSSWORK", "SALES_DSSWORK_INDEX", "N", "LIMITED", 1024.0, 12.0, 0.0, "OPEN")],
        ["schema_name", "owner", "tablespace_name", "is_default_ts", "quota_status", "quota_max_mb",
         "quota_used_mb", "reclaimable_mb", "account_status"]),
    "objects": ora([
        ("SALES_DSSWORK", "SALES_orders", "TABLE", "TABLE", "SALES_DSSWORK", "SALES_orders",
         "SALES_DSSWORK_DATA", "DEFAULT", "NO", 1, 100.0, "QUERY HIGH", None, 12800),
        ("SALES_DSSWORK", "SALES_orders_IX", "INDEX", "INDEX", "SALES_DSSWORK", "SALES_orders",
         "SALES_DSSWORK_DATA", "DEFAULT", "NO", 1, 10.0, "DISABLED", "NORMAL", 1280),
        ("SALES_DSSWORK", "SYS_LOB001", "LOBSEGMENT", "LOB", "SALES_DSSWORK", "SALES_orders",
         "SALES_DSSWORK_INDEX", "QUOTA", "NO", 1, 12.0, "SECUREFILE NONE", None, 1536),
        ("SALES_DSSWORK", "SYS_IOT_TOP_1", "INDEX", "TABLE", "SALES_DSSWORK", "IOT_T",
         "SALES_DSSWORK_DATA", "DEFAULT", "YES", 4, 40.0, "DISABLED", "IOT - TOP", 5120),
        ("SALES_DSSWORK", "C_CLUSTER", "CLUSTER", "TABLE", "SALES_DSSWORK", None,
         "SALES_DSSWORK_DATA", "DEFAULT", "NO", 1, 8.0, None, None, 1024),
        ("SALES_DSSWORK", "12.3456", "TEMPORARY", "OTHER", "SALES_DSSWORK", None,
         "USERS", "OTHER", "NO", 1, 0.5, None, None, 64)],
        ["schema_name", "object_name", "object_type", "family", "parent_schema", "parent_table",
         "tablespace_name", "ts_kind", "partitioned", "n_segments", "size_mb", "compression",
         "index_type", "blocks"]),
    "table_attributes": ora([
        ("SALES_DSSWORK", "SALES_orders", "HEAP", "YES", 1000000, 200, pd.Timestamp("2026-09-20 01:00"), "YES"),
        ("SALES_DSSWORK", "IOT_T", "IOT", "N/A", 5000, 50, pd.Timestamp("2026-09-20 01:00"), "YES"),
        ("SALES_DSSWORK", "EMPTY_T", "HEAP", "NO", 0, 0, None, "YES")],
        ["schema_name", "table_name", "table_kind", "storage_created", "row_estimate", "avg_row_len",
         "last_analyzed", "logging"]),
    "lifecycle": ora([
        ("SALES_DSSWORK", "SALES_orders", pd.Timestamp("2026-01-01"), pd.Timestamp("2026-09-24"), 1000000,
         pd.Timestamp("2026-09-20 01:00"), 150000, "NO", 150000),
        ("SALES_DSSWORK", "IOT_T", pd.Timestamp("2026-01-01"), pd.Timestamp("2026-09-24"), 5000,
         pd.Timestamp("2026-09-20 01:00"), 0, "YES", 0),
        ("SALES_DSSWORK", "EMPTY_T", pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-01"), 0, None, 0, "NO", 0)],
        ["schema_name", "table_name", "created", "last_ddl_time", "row_estimate", "last_analyzed",
         "dml_since_stats", "truncated_since_stats", "dml_inserts"]),
}


class OracleExecutor(object):
    def __init__(self, connection):
        self.connection = connection

    def query_to_df(self, sql, pre_queries=None, infer_from_schema=False):
        CALLS.append((self.connection, sql))
        if "current_setting" in sql:
            raise Exception("ORA-00904: invalid identifier")
        if "v$version" in sql:
            return pd.DataFrame({"V": ["Oracle Database 19c"]})
        if "INSTANCE_NAME" in sql.upper() and "v$database" in sql:
            return pd.DataFrame([{"DB_NAME": "ORCLCDB", "DBID": 1000, "DB_UNIQUE_NAME": "ORCLCDB_A", "CON_NAME": "PDB1",
                                  "CON_DBID": 2000, "DATABASE_ROLE": "PRIMARY", "VERSION": "19.0.0.0.0",
                                  "INSTANCE_NAME": "orcl1"}])
        if sql.startswith("SELECT t.owner FROM dba_tables"):
            return pd.DataFrame({"OWNER": ["SALES_DSSWORK"]})
        fact = re.match(r'-- dbmon Oracle adapter, fact "(\w+)"', sql).group(1)
        assert pre_queries and "DBMS_APPLICATION_INFO" in pre_queries[0] and infer_from_schema
        if fact in ORA_FAIL:
            return ORA[fact].drop(columns=["SIZE_MB"])
        return ORA[fact].copy()


class FakeWriter(object):
    def __init__(self, ds):
        self.ds, self.closed = ds, False

    def write_dataframe(self, df):
        assert list(df.columns) == self.ds.schema
        self.ds.frames.append(df.copy())

    def close(self):
        self.closed = True


class FakeDataset(object):
    def __init__(self, name):
        self.name, self.frames, self.schema, self.writers = name, [], None, []
        DATASETS[name] = self

    def write_schema_from_dataframe(self, df):
        self.schema = list(df.columns)

    def get_writer(self):
        self.writers.append(FakeWriter(self))
        return self.writers[-1]

    def write_with_schema(self, df):
        self.schema, self.frames = list(df.columns), [df.copy()]

    def rows(self):
        return pd.concat(self.frames, ignore_index=True)


def executor(connection=None):
    return PgExecutor(connection) if connection in PG_DBS else OracleExecutor(connection)


def api_client():
    raise Exception("no Dataiku API in tests")


mod = types.ModuleType("dataiku")
mod.Dataset = lambda name: DATASETS.get(name) or FakeDataset(name)
mod.SQLExecutor2 = executor
mod.api_client = api_client
sys.modules["dataiku"] = mod
import dbmon                                                             # noqa: E402
from dbmon import adapters, model, rollups, uc                           # noqa: E402

failures = []


def check(label, condition):
    print(("PASS  " if condition else "FAIL  ") + label)
    if not condition:
        failures.append(label)


def raises(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type as exc:
        return str(exc) or exc_type.__name__
    return None


def balanced(sql):
    code = re.sub(r"--[^\n]*", "", sql)
    if code.count("'") % 2:
        return False
    depth = 0
    for ch in re.sub(r"'[^']*'", "''", code):
        depth += (ch == "(") - (ch == ")")
        if depth < 0:
            return False
    return depth == 0


def calls(connection=None, fragment=""):
    return [s for c, s in CALLS if (connection is None or c == connection) and fragment in s]


def reset():
    DATASETS.clear()
    del CALLS[:]


# ============================================================ contract and adapters
check("contract: 5 facts and 1 derived result", model.FACTS == ("tablespaces", "schemas", "objects",
      "table_attributes", "lifecycle") and model.DERIVED == ("tables",))
check("contract: documentation generated", "## tables" in model.markdown() and "`table_uid` | key" in model.markdown())
for engine in ("oracle", "postgresql"):
    a = adapters.get(engine)
    for fact in model.FACTS:
        variants = [{}, {"tablespaces": ["A"]}, {"tablespaces": []}] if fact == "tablespaces" else \
                   [{}, {"schemas": ["A"]}, {"schemas": []}, {"schemas": ["A"], "table": "O'Brien"}]
        for v in variants:
            sql = a.render(fact, **v)
            ok = "{{" not in sql and balanced(sql) and all(re.search(r"\b%s\b" % c, sql, re.I)
                                                           for c in model.RESULTS[fact].required)
            check("%-10s %-16s %-45s renders, balanced, contract columns present" % (engine, fact, v), ok)
check("engine aliases", adapters.get("postgres") is adapters.get("postgresql") and adapters.get("ORACLE").engine == "oracle")

# ============================================================ PostgreSQL, for real
reset()
objs, log = dbmon.objects(["PG_APP", "PG_APP_2", "PG_OTHER", "PG_DOWN"], with_log=True)
st = {(r.dss_connection, r.status) for r in log.itertuples()}
check("pg: engine found by probing when the Dataiku API is unavailable", set(objs["engine"]) == {"postgresql"})
check("pg: second connection to the same database skipped", ("PG_APP_2", "DUPLICATE_DB") in st)
check("pg: missing database logged, others collected", ("PG_DOWN", "ERROR") in st
      and set(objs["dss_connection"]) == {"PG_APP", "PG_OTHER"})
srv = objs.groupby("dss_connection")["server_uid"].first()
dbs = objs.groupby("dss_connection")["db_uid"].first()
check("pg: two databases, one server", srv["PG_APP"] == srv["PG_OTHER"] and dbs["PG_APP"] != dbs["PG_OTHER"])
app = objs[objs["dss_connection"] == "PG_APP"]
toast = app[(app["parent_table"] == "SALES_orders") & (app["schema_name"] == "sales_dsswork") & (app["family"] == "LOB")]
check("pg: TOAST storage attributed to its table, in the table's schema", len(toast) == 1 and toast["size_mb"].iloc[0] > 10)
ix = app[app["object_name"] == "SALES_orders_amount"]
check("pg: index in another tablespace flagged OTHER", ix["ts_kind"].iloc[0] == "OTHER"
      and ix["tablespace_name"].iloc[0] == "sales_dsswork_index")
led = app[(app["object_name"] == "ledger") & (app["family"] == "TABLE")]
check("pg: partitions summed under the root", led["partitioned"].iloc[0] == "YES" and led["n_segments"].iloc[0] == 2)
check("pg: sequence has no table", app[app["object_type"] == "SEQUENCE"]["table_uid"].isna().all())
check("pg: default scope leaves system schemas out", not app["schema_name"].str.startswith("pg_").any()
      and "information_schema" not in set(app["schema_name"]))

reset()
tsp, log = dbmon.tablespaces(["PG_APP", "PG_OTHER"], with_log=True)
check("pg: tablespaces collected once per cluster", sorted(tsp["tablespace_name"]) == ["pg_default", "pg_global",
      "sales_dsswork_index"] and ("PG_OTHER", "SAME_SERVER") in {(r.dss_connection, r.status) for r in log.itertuples()})
check("pg: server rows have no database", tsp["db_uid"].isna().all() and tsp["database_name"].isna().all())
check("pg: tablespace keys match between objects and tablespaces",
      set(ix["tablespace_uid"]) <= set(tsp["tablespace_uid"]))
loc = json.loads(tsp[tsp["tablespace_name"] == "sales_dsswork_index"]["details"].iloc[0])
check("pg: engine columns kept in details", loc.get("location") == "/var/lib/postgresql/ts_index")
check("expand_details spreads them", "location" in dbmon.expand_details(tsp).columns)

reset()
tabs = dbmon.tables(["PG_APP", "PG_OTHER"])
objs = dbmon.objects(["PG_APP", "PG_OTHER"])
t = tabs[(tabs["dss_connection"] == "PG_APP") & (tabs["schema_name"] == "sales_dsswork") & (tabs["table_name"] == "SALES_orders")].iloc[0]
check("pg tables: table + indexes + TOAST rolled up", abs(t["total_mb"] - (t["table_mb"] + t["index_mb"] + t["lob_mb"] + t["lobindex_mb"])) < 0.01
      and t["lob_mb"] > 10 and t["n_indexes"] == 2 and t["table_kind"] == "HEAP")
check("pg tables: split by location", t["in_other_ts_mb"] > 0 and t["n_tablespaces"] == 2
      and t["tablespaces"] == "pg_default,sales_dsswork_index")
l = tabs[(tabs["dss_connection"] == "PG_APP") & (tabs["table_name"] == "ledger")].iloc[0]
check("pg tables: partitioned table", l["table_kind"] == "PARTITIONED" and l["n_table_partitions"] == 2 and l["row_estimate"] > 0)
check("pg tables: object without a table keeps its own row", "SEQUENCE" in set(tabs["table_kind"]))
for conn in ("PG_APP", "PG_OTHER"):
    check("pg tables: sizes add up to the objects on %s" % conn,
          abs(tabs[tabs["dss_connection"] == conn]["total_mb"].sum() - objs[objs["dss_connection"] == conn]["size_mb"].sum()) < 0.05)
check("pg tables: keys join to objects", set(objs["table_uid"].dropna()) <= set(tabs["table_uid"]))

reset()
lc = dbmon.lifecycle("PG_APP")
check("pg lifecycle: computed columns present, missing concepts NULL",
      {"dml_pct_since_stats", "stale_est"} <= set(lc.columns) and lc["created"].isna().all()
      and set(lc["stale_est"].dropna()) <= {"YES", "NO"})

reset()
one, log = dbmon.objects(["PG_APP", "PG_OTHER"], table="SALES_orders", with_log=True)
check("pg single table: schemas looked up, only that table",
      set(one["parent_table"]) == {"SALES_orders"}
      and set(one[one["dss_connection"] == "PG_OTHER"]["schema_name"]) == {"sales_dsswork"}
      and len(calls(fragment="SELECT n.nspname FROM pg_class c")) == 2)

reset()
pg = uc.Convention(case="lower", tablespaces=False)
e = pg.enrich(dbmon.objects("PG_APP", schemas=pg.schemas("sales")))
check("uc on PostgreSQL: lower-case convention", pg.schemas("SALES") == ["sales_dsswork", "sales_dssout"]
      and set(e["schema_name"]) == {"sales_dsswork", "sales_dssout"} and set(e["uc"]) == {"sales"}
      and set(e["schema_layer"]) == {"DSSWORK", "DSSOUT"} and "placement" not in e.columns)

reset()
log = dbmon.write({"objects": "db_objects", "tables": "db_tables"}, ["PG_APP", "PG_OTHER"], runlog="db_runlog")
check("write: objects fetched once per database for objects and tables",
      all(len(calls(c, 'fact "objects"')) == 1 for c in ("PG_APP", "PG_OTHER")))
check("write: one closed writer per output, both databases",
      all(len(DATASETS[n].writers) == 1 and DATASETS[n].writers[0].closed
          and set(DATASETS[n].rows()["dss_connection"]) == {"PG_APP", "PG_OTHER"} for n in ("db_objects", "db_tables")))

# ============================================================ Oracle (canned) and mixed engines
reset()
ot = dbmon.tables({"connection": "ORA_PRD", "env": "PRD"})
row = {r.table_name: r for r in ot.itertuples()}
check("oracle: engine found by probing", set(ot["engine"]) == {"oracle"})
check("oracle tables: rollup by family and location", row["SALES_orders"].total_mb == 122.0
      and row["SALES_orders"].index_mb == 10.0 and row["SALES_orders"].lob_mb == 12.0
      and row["SALES_orders"].in_quota_ts_mb == 12.0 and row["SALES_orders"].table_compression == "QUERY HIGH")
check("oracle tables: IOT top index counted as table data", row["IOT_T"].table_mb == 40.0 and row["IOT_T"].n_table_partitions == 4)
check("oracle tables: cluster and TEMPORARY segments keep their own row",
      row["C_CLUSTER"].table_kind == "CLUSTER" and row["12.3456"].table_kind == "TEMPORARY")
check("oracle tables: table without segment listed with 0 MB", row["EMPTY_T"].total_mb == 0 and row["EMPTY_T"].n_segments == 0)
check("oracle tables: est_row_data_mb from the statistics", abs(row["SALES_orders"].est_row_data_mb - 190.735) < 0.01)
check("oracle: label column from the caller", set(ot["env"]) == {"PRD"})

reset()
oo = dbmon.objects("ORA_PRD")
ots = dbmon.tablespaces("ORA_PRD")
check("oracle: tablespace keys scoped to the database (PDB)", set(ots["db_uid"].dropna()) == set(oo["db_uid"])
      and set(oo[oo["tablespace_name"] == "SALES_DSSWORK_DATA"]["tablespace_uid"]) <= set(ots["tablespace_uid"]))
check("oracle: engine columns in details", json.loads(oo[oo["object_name"] == "SALES_orders_IX"]["details"].iloc[0]) ==
      {"blocks": 1280, "index_type": "NORMAL"})
issues = dict(uc.table_placement(oo).values)
t_uid = oo[oo["object_name"] == "SALES_orders"]["table_uid"].iloc[0]
check("uc on Oracle: table placement from the objects", issues[t_uid] == "DATA_IN_INDEX_TS,INDEX_IN_DATA_TS")
olc = dbmon.lifecycle("ORA_PRD").set_index("table_name")
check("oracle lifecycle: stale_est and dml_pct computed the same way",
      olc.loc["SALES_orders", "stale_est"] == "YES" and olc.loc["SALES_orders", "dml_pct_since_stats"] == 15.0
      and olc.loc["IOT_T", "stale_est"] == "YES" and pd.isna(olc.loc["EMPTY_T", "stale_est"]))

reset()
mixed = dbmon.tables([{"connection": "ORA_PRD", "env": "PRD"}, {"connection": "PG_APP", "env": "UAT"}])
check("mixed engines stack into one result", set(mixed["engine"]) == {"oracle", "postgresql"}
      and set(mixed["env"]) == {"PRD", "UAT"} and mixed["table_uid"].is_unique)
check("same columns whatever the engine", list(mixed.columns) == list(ot.columns))

reset()
ORA_FAIL.add("objects")
check("contract violation stops the call", "lacks contract column" in (raises(ValueError, dbmon.objects, "ORA_PRD") or ""))
ORA_FAIL.clear()
check("unknown result refused", raises(ValueError, dbmon.collect, "awr", "ORA_PRD") is not None)
check("filter no result accepts refused", raises(ValueError, dbmon.write, {"tablespaces": "t"}, "ORA_PRD", schemas=["A"]) is not None)
check("reserved label refused", raises(ValueError, dbmon.tables, [{"connection": "ORA_PRD", "db_uid": "x"}]) is not None)

print("\n%d failure(s)" % len(failures))
sys.exit(1 if failures else 0)
