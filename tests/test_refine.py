"""Step 3: dbmon.refine on the data of steps 1 (collected for real from the test containers)
and 2 (stand-in DSS client).

    python tests/test_refine.py
"""
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import check, finish, install_fake_dataiku
from fake_dss import CONNECTION_MAPPING, DATABASES, FakeClient

install_fake_dataiku()
from dbmon import collect, dss_extract, refine, schemas  # noqa: E402

# --- steps 1 and 2 ---------------------------------------------------------------------------
step1, step1_log = collect.collect_all(DATABASES[["connection", "engine", "label"]], parallel=2)
check("step 1 collected on both engines", (step1_log.status == "OK").all(), step1_log.to_string())
step2 = dss_extract.extract_dss(FakeClient(), parallel=2)

results = refine.refine(step2, step1["tables"], step1["role_settings"], DATABASES, CONNECTION_MAPPING,
                        step1["naming_rules"])
check("outputs", sorted(results) == ["database_usage", "dataset_locations", "dataset_roles", "dataset_storage",
                                    "dataset_tables", "project_consumption", "schema_usage", "table_ownership"],
      sorted(results))
all_declared = True
for name in results:
    try:
        schemas.conform(results[name], name)
    except ValueError as error:
        print(error)
        all_declared = False
check("every column of every output is declared in schemas.py", all_declared)

roles = results["dataset_roles"].set_index(["object_type", "project_key", "object_name"])
locations = results["dataset_locations"]
dataset_tables = results["dataset_tables"].set_index(["project_key", "dataset_name"])
ownership = results["table_ownership"]
storage = results["dataset_storage"].set_index(["object_type", "project_key", "object_name"])
consumption = results["project_consumption"].set_index("project_key")
pd.set_option("display.width", 250)
print(dataset_tables[["match_status", "database_label", "schema_name", "table_name", "schema_source"]].to_string())

# --- dataset_roles -------------------------------------------------------------------------------
nature = roles.dataset_nature.to_dict()
check("nature: produced, external source, external unused, managed idle",
      nature[("DATASET", "SALES", "orders_clean")] == "PRODUCED"
      and nature[("DATASET", "SALES", "orders_ora")] == "PRODUCED"
      and nature[("FOLDER", "SALES", "folder1")] == "PRODUCED"
      and nature[("DATASET", "SALES", "raw_orders")] == "EXTERNAL_SOURCE"
      and nature[("DATASET", "FIN", "lake_input")] == "EXTERNAL_SOURCE"
      and nature[("DATASET", "SALES", "old_copy")] == "EXTERNAL_UNUSED"
      and nature[("FOLDER", "FIN", "idle1")] == "MANAGED_IDLE", nature)
check("roles: orders_clean written once, read by FIN",
      roles.loc[("DATASET", "SALES", "orders_clean"), ["n_producers", "n_consumers", "n_consuming_projects"]].tolist()
      == [1, 1, 1])
check("a managed dataset written by no recipe: MANAGED_IDLE",
      refine.nature_of(0, 0, True) == "MANAGED_IDLE" and refine.nature_of(0, 2, True) == "MANAGED_IDLE")


# --- dataset_locations ---------------------------------------------------------------------------
def candidates(project_key, dataset_name):
    rows = locations[(locations.project_key == project_key) & (locations.dataset_name == dataset_name)]
    rows = rows.sort_values("candidate_rank", na_position="last")
    result = []
    for row in rows.itertuples(index=False):
        result.append((row.candidate_status, row.schema_source, row.schema_name, row.table_name))
    return result


check("empty schema, PostgreSQL: search_path of dbmon in appdb, in order",
      candidates("SALES", "raw_orders") == [("CANDIDATE", "SEARCH_PATH", "sales_dsswork", "SALES_orders"),
                                            ("CANDIDATE", "SEARCH_PATH", "public", "SALES_orders")],
      candidates("SALES", "raw_orders"))
check("empty schema: post-connect SET search_path wins over the role setting",
      candidates("SALES", "orders_clean")[0] == ("CANDIDATE", "POST_CONNECT", "sales_dssout", "SALES_orders"))
check("empty schema, Oracle: the login user, upper case",
      candidates("SALES", "orders_ora") == [("CANDIDATE", "LOGIN_USER", "SALES_DSSWORK", "ORDERS")])
check("Oracle JDBC URL connection: the URL user as login user",
      candidates("SALES", "via_url") == [("CANDIDATE", "LOGIN_USER", "SALES_DSSOUT", "LEDGER")])
check("schema from the dataset settings", candidates("FIN", "fin_summary") == [("CANDIDATE", "DATASET", "fin_dssout", "ledger")])
check("per-user connection: one row, PER_USER", [c[0] for c in candidates("SALES", "per_user")] == ["PER_USER"])
check("SQL query dataset: NO_TABLE", [c[0] for c in candidates("SALES", "query_ds")] == ["NO_TABLE"])
check("file dataset: NOT_SQL", [c[0] for c in candidates("SALES", "notes_file")] == ["NOT_SQL"])
check("connection missing from the mapping: UNMAPPED with its candidate",
      candidates("SALES", "elsewhere") == [("UNMAPPED", "DATASET", "public", "t")])
names = locations.drop_duplicates(["project_key", "dataset_name"]).set_index(["project_key", "dataset_name"])
check("connection_name = the dataset's own connection, audit_connection_name = the mapped admin connection",
      names.loc[("SALES", "raw_orders"), ["connection_name", "audit_connection_name", "database_label"]].tolist()
      == ["PG_SALES", "ADM_PG_APP", "PG app"]
      and names.loc[("SALES", "orders_clean"), "database_label"] == "PG app"
      and pd.isna(names.loc[("SALES", "elsewhere"), "audit_connection_name"]))

audits = refine.audit_connections(DATABASES, CONNECTION_MAPPING)
check("admin connections are their own audit connection",
      audits["ADM_PG_APP"] == [("ADM_PG_APP", "PG app")] and audits["ADM_ORA"] == [("ADM_ORA", "ORA")])
check("mapped to an admin connection databases does not list: nothing", "ORA_LOST" not in audits)
two = refine.audit_connections(DATABASES, pd.DataFrame([("SHARED", "ADM_PG_APP"), ("SHARED", "ADM_ORA")],
                                                       columns=["dss_connection", "audit_connection"]))
check("a connection mapped to two admin connections leads to both",
      two["SHARED"] == [("ADM_PG_APP", "PG app"), ("ADM_ORA", "ORA")])
without_mapping = refine.refine(step2, step1["tables"], step1["role_settings"], DATABASES, None)["dataset_locations"]
check("without the mapping: datasets of non-admin connections are UNMAPPED",
      set(without_mapping[without_mapping.dataset_name == "raw_orders"].candidate_status) == {"UNMAPPED"})
default_path = refine.refine(step2, step1["tables"], schemas.conform(pd.DataFrame(), "role_settings"), DATABASES,
                             CONNECTION_MAPPING)["dataset_locations"]
check("empty role_settings: PostgreSQL default search_path \"$user\", public",
      default_path[default_path.dataset_name == "raw_orders"].schema_name.tolist() == ["dbmon", "public"])


# --- dataset_tables ------------------------------------------------------------------------------
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

# --- table_ownership -----------------------------------------------------------------------------
owned = ownership.set_index(["label", "schema_name", "table_name"])
check("one row per step 1 table", len(ownership) == len(step1["tables"]))
same_totals = True
for label in step1["tables"].label.unique():
    step1_total = step1["tables"][step1["tables"].label == label].total_mb.sum()
    if abs(ownership[ownership.label == label].total_mb.sum() - step1_total) > 0.01:
        same_totals = False
check("space counted once: per database, the total of table_ownership = the total of step 1", same_totals)

two_owners = owned.loc[("PG app", "sales_dssout", "SALES_orders")]
check("two produced datasets: owned by the smallest project key, multiple_owners = YES",
      (two_owners.status, two_owners.owner_project, two_owners.owner_dataset, two_owners.owner_nature,
       two_owners.multiple_owners) == ("OWNED", "FIN", "dup_writer", "PRODUCED", "YES"), two_owners.to_string())
check("read by an external source only: READ_ONLY",
      owned.loc[("PG app", "sales_dsswork", "SALES_orders"), "status"] == "READ_ONLY")
ledger = owned.loc[("PG app", "fin_dssout", "ledger")]
check("owned by FIN, also pointed to by an unused SALES dataset",
      ledger.status == "OWNED" and ledger.owner_project == "FIN" and ledger.n_unused_datasets == 1
      and ledger.n_referencing_projects == 2)
check("only unused datasets point to it: UNUSED", owned.loc[("PG app", "appx", "mv_totals"), "status"] == "UNUSED")
check("Oracle table owned by SALES", owned.loc[("ORA", "SALES_DSSWORK", "ORDERS"), ["status", "owner_project"]].tolist()
      == ["OWNED", "SALES"])
check("no dataset: UNREFERENCED", owned.loc[("ORA", "SALES_DSSOUT", "IOT_T"), "status"] == "UNREFERENCED")
check("use case and layer as detected by the naming rules of the database",
      owned.loc[("PG app", "sales_dsswork", "SALES_orders"), ["detected_use_case", "detected_layer"]].tolist()
      == ["SALES", "DSSWORK"] and pd.isna(owned.loc[("PG app", "appx", "mv_totals"), "detected_use_case"]))
metrics_copied = True
for metric in refine.TABLE_METRICS:
    step1_total = step1["tables"][metric].sum()
    if abs(ownership[metric].sum() - step1_total) > 0.01:
        metrics_copied = False
check("every metric of the step 1 tables copied, same totals", metrics_copied)

# --- dss_relation: what DSS has to do with each table -------------------------------------------
relation = owned.dss_relation.to_dict()
check("dss_relation of each case",
      relation[("PG app", "sales_dssout", "SALES_orders")] == "PILOTED"
      and relation[("PG app", "sales_dsswork", "SALES_orders")] == "READ_BY_DSS"
      and relation[("PG app", "appx", "mv_totals")] == "DECLARED_UNUSED"
      and relation[("PG app", "appx", "seq1")] == "STANDALONE"
      and relation[("ORA", "SALES_DSSOUT", "IOT_T")] == "UNRELATED_DSS_SCHEMA"
      and relation[("ORA", "APPX", "NOTES")] == "UNRELATED", relation)
check("DSS schema: named by a rule, or holding a table a dataset points to (PostgreSQL appx), not ORA APPX",
      owned.loc[("ORA", "SALES_DSSOUT", "IOT_T"), "dss_schema"] == "YES"
      and owned.loc[("PG app", "appx", "scratch"), ["dss_schema", "dss_relation"]].tolist()
      == ["YES", "UNRELATED_DSS_SCHEMA"]
      and owned.loc[("ORA", "APPX", "NOTES"), "dss_schema"] == "NO")
check("owned only by idle managed datasets: PILOTED_IDLE",
      refine.dss_relation({"standalone": "NO", "status": "OWNED", "owner_nature": "MANAGED_IDLE",
                           "dss_schema": "YES"}) == "PILOTED_IDLE")
waste = owned[owned.waste_candidate == "YES"].dss_relation
check("waste candidates: idle, declared unused, unrelated in a DSS schema only",
      set(waste) <= {"PILOTED_IDLE", "DECLARED_UNUSED", "UNRELATED_DSS_SCHEMA"}
      and owned[owned.dss_relation == "UNRELATED"].waste_candidate.eq("NO").all() and len(waste) > 0)

# --- database_usage -----------------------------------------------------------------------------
usage = results["database_usage"]
usage_totals_ok = True
for metric in refine.TABLE_METRICS:
    if abs(usage[metric].sum() - step1["tables"][metric].sum()) > 0.01:
        usage_totals_ok = False
check("database_usage: every metric summed, same totals as step 1, every table counted once",
      usage_totals_ok and usage.n_tables.sum() == len(step1["tables"]))
pg_unrelated = usage[(usage.label == "PG app") & (usage.schema_name == "appx")].set_index("dss_relation")
check("database_usage: one row per schema and relation, with its tables",
      pg_unrelated.loc["DECLARED_UNUSED", "n_tables"] == 1
      and pg_unrelated.loc["DECLARED_UNUSED", "waste_candidate"] == "YES"
      and pg_unrelated.loc["STANDALONE", "n_tables"] >= 1)
check("database_usage: reliability, datasets of the database not located (missing, per_user) and failed projects",
      set(usage[usage.label == "PG app"].n_datasets_unlocated) == {2}
      and set(usage[usage.label == "ORA"].n_datasets_unlocated) == {0}
      and set(usage.n_projects_failed) == {1})
by_database = usage.groupby(["label", "waste_candidate"]).total_mb.sum()
check("database_usage: piloted and possible waste per database add up to the database",
      abs(by_database.loc["ORA"].sum() - step1["tables"][step1["tables"].label == "ORA"].total_mb.sum()) < 0.01)
three_decimals = True
for value in usage.total_mb:
    if value != round(value, 3):
        three_decimals = False
check("database_usage: sizes keep 3 decimals", three_decimals)

# PRODUCED wins over MANAGED_IDLE, whatever the project keys
priority = refine.table_ownership(
    [{"label": "L", "schema_name": "s", "table_name": "t", "total_mb": 1.0}],
    [{"project_key": "AAA", "dataset_name": "idle", "match_status": "FOUND", "database_label": "L",
      "schema_name": "s", "table_name": "t"},
     {"project_key": "ZZZ", "dataset_name": "written", "match_status": "FOUND", "database_label": "L",
      "schema_name": "s", "table_name": "t"}],
    [{"object_type": "DATASET", "project_key": "AAA", "object_name": "idle", "dataset_nature": "MANAGED_IDLE"},
     {"object_type": "DATASET", "project_key": "ZZZ", "object_name": "written", "dataset_nature": "PRODUCED"}])[0]
check("a produced dataset owns before a managed idle one",
      (priority["owner_project"], priority["owner_nature"], priority["multiple_owners"]) == ("ZZZ", "PRODUCED", "YES"))

# --- dataset_storage -----------------------------------------------------------------------------
kind = storage.storage_kind.to_dict()
check("storage kind: database, local files, external files, nothing stored",
      kind[("DATASET", "SALES", "orders_ora")] == "DATABASE"
      and kind[("DATASET", "SALES", "local_out")] == "LOCAL_STORAGE"
      and kind[("FOLDER", "SALES", "folder1")] == "LOCAL_STORAGE"
      and kind[("DATASET", "FIN", "lake_input")] == "EXTERNAL_STORAGE"
      and kind[("FOLDER", "FIN", "idle1")] == "EXTERNAL_STORAGE"
      and kind[("DATASET", "SALES", "query_ds")] == "NO_STORAGE", kind)
oracle_orders = storage.loc[("DATASET", "SALES", "orders_ora")]
orders_table = owned.loc[("ORA", "SALES_DSSWORK", "ORDERS")]
check("database dataset owning its table: the table size and every metric, counted",
      oracle_orders.size_source == "TABLE" and oracle_orders.counted == "YES"
      and abs(oracle_orders.size_mb - orders_table.total_mb) < 0.001
      and abs(oracle_orders.lob_mb - orders_table.lob_mb) < 0.001
      and oracle_orders.n_indexes == orders_table.n_indexes and oracle_orders.n_segments == orders_table.n_segments)
not_owner = storage.loc[("DATASET", "SALES", "orders_clean")]
check("database dataset whose table another dataset owns: not counted, owner noted",
      not_owner.counted == "NO" and "FIN.dup_writer" in not_owner.note)
check("folder: size metric, counted", storage.loc[("FOLDER", "SALES", "folder1"), ["size_mb", "size_source", "counted"]]
      .tolist() == [5.0, "METRIC", "YES"])
check("external source and unused files: not counted",
      storage.loc[("DATASET", "FIN", "lake_input"), "counted"] == "NO"
      and storage.loc[("DATASET", "SALES", "notes_file"), "counted"] == "NO")

# --- project_consumption -------------------------------------------------------------------------
check("one row per project read", sorted(consumption.index) == ["FIN", "SALES"])
check("use case from the project tags", consumption.loc["SALES", ["use_case", "use_case_source"]].tolist()
      == ["SALES", "TAG"] and consumption.loc["FIN", "use_case_source"] == "NONE")
fin_owned = ownership[(ownership.status == "OWNED") & (ownership.owner_project == "FIN")].total_mb.sum()
check("database_mb = the tables the project owns", abs(consumption.loc["FIN", "database_mb"] - fin_owned) < 0.01
      and consumption.loc["FIN", "n_owned_tables"] == 2)
fin_tables = ownership[(ownership.status == "OWNED") & (ownership.owner_project == "FIN")]
check("project_consumption: every metric of the owned tables",
      abs(consumption.loc["FIN", "database_other_mb"] - fin_tables.other_mb.sum()) < 0.01
      and consumption.loc["FIN", "database_n_indexes"] == fin_tables.n_indexes.sum()
      and consumption.loc["FIN", "database_n_segments"] == fin_tables.n_segments.sum()
      and consumption.loc["FIN", "database_n_segments"] > 0)
check("local and external files counted: SALES 4 + 5 MB local, FIN 1 MB external (idle folder)",
      consumption.loc["SALES", "local_storage_mb"] == 9.0 and consumption.loc["SALES", "external_storage_mb"] == 0
      and consumption.loc["FIN", "external_storage_mb"] == 1.0 and consumption.loc["FIN", "idle_mb"] == 1.0)
check("total_mb = database + local + external",
      abs(consumption.loc["SALES", "total_mb"] - consumption.loc["SALES", "database_mb"] - 9.0) < 0.001)
check("external_read_mb = files of the external sources read (FIN reads 10 MB from S3)",
      consumption.loc["FIN", "external_read_mb"] == 10.0 and consumption.loc["SALES", "external_read_mb"] == 0)
sales_read = (owned.loc[("PG app", "sales_dsswork", "SALES_orders"), "total_mb"]
              + owned.loc[("PG app", "sales_dssout", "SALES_orders"), "total_mb"]
              + owned.loc[("PG app", "fin_dssout", "ledger"), "total_mb"])
check("database_read_mb = tables SALES points to without owning them, not in total_mb",
      abs(consumption.loc["SALES", "database_read_mb"] - sales_read) < 0.01,
      (consumption.loc["SALES", "database_read_mb"], sales_read))
sales_unused = (owned.loc[("PG app", "appx", "mv_totals"), "total_mb"]
                + owned.loc[("ORA", "SALES_DSSOUT", "LEDGER"), "total_mb"])
check("database_unused_mb = UNUSED tables its datasets point to (old_copy, via_url)",
      abs(consumption.loc["SALES", "database_unused_mb"] - sales_unused) < 0.01)
check("datasets and folders by nature",
      consumption.loc["SALES", ["n_datasets", "n_managed_folders", "n_produced", "n_managed_idle", "n_external_source",
                                "n_external_unused"]].tolist() == [11, 1, 4, 0, 1, 7]
      and consumption.loc["FIN", ["n_produced", "n_managed_idle", "n_external_source", "n_external_unused"]].tolist()
      == [2, 1, 1, 1] and consumption.loc["FIN", "n_datasets_not_found"] == 1)
check("no counted object without size here", consumption.n_size_unknown.sum() == 0)

three_decimals = True
for column in consumption.columns:
    if column.endswith("_mb"):
        for value in consumption[column]:
            if value != round(value, 3):
                three_decimals = False
check("sizes summed in project_consumption keep 3 decimals", three_decimals)

total_owned = consumption.database_mb.sum()
not_owned = ownership[ownership.status != "OWNED"].total_mb.sum()
check("owned by projects + not owned = all the space of step 1 tables",
      abs(total_owned + not_owned - step1["tables"].total_mb.sum()) < 0.01)

# --- schema_usage: one line per schema ----------------------------------------------------------
schema_lines = results["schema_usage"]
lines = schema_lines.set_index(["label", "schema_name"])
rule_schemas = step1["naming_rules"][step1["naming_rules"].name_kind == "SCHEMA"]
check("schema_usage: one line per schema of every database, the empty ones included",
      len(schema_lines) == len(rule_schemas) and not lines.index.duplicated().any()
      and lines.loc[("PG app", "public"), "n_tables"] == 0 and lines.loc[("ORA", "DBMON"), "n_tables"] == 0)
empty = lines.loc[("PG app", "public")]
check("schema without table: 0 MB, % empty",
      empty.total_mb == 0 and pd.isna(empty.piloted_pct) and pd.isna(empty.waste_pct) and empty.dss_schema == "NO")
totals_ok = True
for metric in refine.TABLE_METRICS:
    if abs(schema_lines[metric].sum() - step1["tables"][metric].sum()) > 0.01:
        totals_ok = False
check("schema_usage: every metric of the tables, same totals as step 1", totals_ok
      and schema_lines.n_tables.sum() == len(step1["tables"]))
dssout = lines.loc[("ORA", "SALES_DSSOUT")]
dssout_tables = ownership[(ownership.label == "ORA") & (ownership.schema_name == "SALES_DSSOUT")]
unused_mb = dssout_tables[dssout_tables.dss_relation == "DECLARED_UNUSED"].total_mb.sum()
check("schema_usage: MB and % of a relation, of the schema's space",
      abs(dssout.declared_unused_total_mb - unused_mb) < 0.001
      and abs(dssout.declared_unused_pct - round(unused_mb * 100 / dssout.total_mb, 2)) < 0.001
      and dssout.declared_unused_n_tables == 1 and dssout.detected_use_case == "SALES",
      dssout[["total_mb", "declared_unused_total_mb", "declared_unused_pct"]].to_string())
check("schema_usage: waste summary",
      abs(dssout.waste_total_mb - dssout.total_mb) < 0.001 and dssout.waste_pct == 100)
check("schema_usage: dss_related = every table a dataset points to, whatever its relation",
      lines.loc[("PG app", "sales_dsswork"), "dss_related_pct"] == 100
      and lines.loc[("PG app", "sales_dsswork"), "read_by_dss_pct"] == 100
      and lines.loc[("PG app", "appx"), "dss_related_pct"] == 50
      and abs(dssout.dss_related_total_mb - dssout.declared_unused_total_mb) < 0.001
      and lines.loc[("ORA", "SALES_DSSWORK"), "dss_related_pct"] == lines.loc[("ORA", "SALES_DSSWORK"), "piloted_pct"])
pct_columns = []
mb_columns = []
for relation in refine.DSS_RELATIONS:
    pct_columns.append(relation.lower() + "_pct")
    mb_columns.append(relation.lower() + "_total_mb")
with_space = schema_lines[schema_lines.total_mb > 0]
check("schema_usage: the % of a schema add up to 100, the MB to its total",
      ((with_space[pct_columns].sum(axis=1) - 100).abs() < 0.05).all()
      and ((with_space[mb_columns].sum(axis=1) - with_space.total_mb).abs() < 0.002).all())
group_metrics_ok = True
for group_name, relations in (("declared_unused", ["DECLARED_UNUSED"]),
                              ("dss_related", ["PILOTED", "PILOTED_IDLE", "READ_BY_DSS", "DECLARED_UNUSED"]),
                              ("waste", ["PILOTED_IDLE", "DECLARED_UNUSED", "UNRELATED_DSS_SCHEMA"])):
    group_tables = dssout_tables[dssout_tables.dss_relation.isin(relations)]
    if dssout[group_name + "_n_tables"] != len(group_tables):
        group_metrics_ok = False
    for metric in refine.TABLE_METRICS:
        if abs(dssout[group_name + "_" + metric] - group_tables[metric].sum()) > 0.001:
            group_metrics_ok = False
check("schema_usage: every metric of the tables per relation and per cumulated group, with its table count",
      group_metrics_ok and dssout.waste_n_tables == 4 and dssout.dss_related_n_tables == 1)
for metric in refine.TABLE_METRICS:
    per_relation = []
    for relation in refine.DSS_RELATIONS:
        per_relation.append(relation.lower() + "_" + metric)
    if ((with_space[per_relation].sum(axis=1) - with_space[metric]).abs() > 0.002).any():
        group_metrics_ok = False
check("schema_usage: the relations of a schema add up to the schema, metric by metric", group_metrics_ok)
check("schema_usage: every relation has its columns, in the declared schema",
      list(schema_lines.columns) == refine.SCHEMA_USAGE_COLUMNS
      and len(schemas.conform(schema_lines, "schema_usage")) == len(schema_lines))
check("schema_usage: reliability counts of the database",
      lines.loc[("PG app", "public"), "n_datasets_unlocated"] == 2 and set(schema_lines.n_projects_failed) == {1})
without_rules = refine.refine(step2, step1["tables"], step1["role_settings"], DATABASES, CONNECTION_MAPPING)
check("without naming_rules: the schemas holding tables only",
      len(without_rules["schema_usage"]) == len(step1["tables"].drop_duplicates(["label", "schema_name"])))

finish()
