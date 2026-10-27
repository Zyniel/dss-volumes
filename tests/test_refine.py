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

results = refine.refine(step2, step1["tables"], step1["role_settings"], DATABASES, CONNECTION_MAPPING)
check("outputs", sorted(results) == ["dataset_locations", "dataset_roles", "dataset_storage", "dataset_tables",
                                    "project_consumption", "table_ownership"], sorted(results))
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
check("use case and layer from the schema name",
      owned.loc[("PG app", "sales_dsswork", "SALES_orders"), ["schema_use_case", "schema_layer"]].tolist()
      == ["SALES", "DSSWORK"] and pd.isna(owned.loc[("PG app", "appx", "mv_totals"), "schema_use_case"]))

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
check("database dataset owning its table: the table size, counted",
      oracle_orders.size_source == "TABLE" and oracle_orders.counted == "YES"
      and abs(oracle_orders.size_mb - owned.loc[("ORA", "SALES_DSSWORK", "ORDERS"), "total_mb"]) < 0.001)
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

total_owned = consumption.database_mb.sum()
not_owned = ownership[ownership.status != "OWNED"].total_mb.sum()
check("owned by projects + not owned = all the space of step 1 tables",
      abs(total_owned + not_owned - step1["tables"].total_mb.sum()) < 0.01)

finish()
