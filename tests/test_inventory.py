"""Step 2: dbmon.inventory with the stand-in DSS client (extraction only), using the role
settings collected for real from the PostgreSQL test container.

    python tests/test_inventory.py
"""
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import check, finish, install_fake_dataiku
from fake_dss import DATABASES, FakeClient

install_fake_dataiku()
from dbmon import collect, inventory  # noqa: E402

# --- role settings, for real -------------------------------------------------------------------
step1, step1_log = collect.collect_all(DATABASES[["connection", "engine", "label"]], parallel=2)
check("step 1 collected on both engines", (step1_log.status == "OK").all(), step1_log.to_string())
role_settings = step1["role_settings"]

# --- inventory -----------------------------------------------------------------------------
results = inventory.collect_inventory(FakeClient(), DATABASES, role_settings)
check("outputs", sorted(results) == ["dss_connections", "dss_dataset_locations", "dss_datasets",
                                    "dss_flow_links", "dss_projects", "inventory_log"], sorted(results))

log = results["inventory_log"].set_index("project_key")
check("log: SALES and FIN OK, BROKEN FAILED with its error",
      log.loc["SALES", "status"] == "OK" and log.loc["FIN", "status"] == "OK"
      and log.loc["BROKEN", "status"] == "FAILED" and "flow graph" in log.loc["BROKEN", "error"])

projects = results["dss_projects"].set_index("project_key")
check("dss_projects: use case from the CDP-UseCase tag, or none",
      projects.loc["SALES", ["use_case", "use_case_source"]].tolist() == ["SALES", "TAG"]
      and projects.loc["FIN", "use_case_source"] == "NONE" and pd.isna(projects.loc["FIN", "use_case"])
      and "BROKEN" not in projects.index)

links = results["dss_flow_links"]
check("links: 7 dataset links, the managed folder left out",
      len(links) == 7 and "folder1" not in set(links.dataset_name), links.to_string())
foreign = links[links.foreign == "YES"]
check("links: FIN reads SALES.orders_clean as a foreign dataset",
      foreign[["project_key", "dataset_project", "dataset_name", "direction"]].values.tolist()
      == [["FIN", "SALES", "orders_clean", "input"]])

datasets = results["dss_datasets"].set_index(["project_key", "dataset_name"])
check("dss_datasets: no flow or location column (computed in step 3)",
      "flow_role" not in datasets.columns and "location_status" not in datasets.columns)
check("dss_datasets: variables replaced, local project variable overriding standard",
      datasets.loc[("SALES", "raw_orders"), "resolved_table"] == "SALES_orders"
      and datasets.loc[("FIN", "fin_summary"), "resolved_schema"] == "fin_dssout"
      and datasets.loc[("SALES", "notes_file"), "resolved_path"] == "SALES/notes")
shared = datasets.loc[("SALES", "orders_clean")]
check("dss_datasets: orders_clean shared with FIN", shared.shared == "YES" and shared.shared_with == "FIN")
check("metrics: rows, YES", datasets.loc[("SALES", "raw_orders"), ["metric_rows", "is_populated"]].tolist() == [3000, "YES"])
check("metrics: 0 rows, NO", datasets.loc[("SALES", "orders_clean"), "is_populated"] == "NO")
check("metrics: partitions summed when there is no whole-dataset value",
      datasets.loc[("SALES", "orders_ora"), "metric_rows"] == 15)
check("metrics: size only, in MB, YES",
      datasets.loc[("FIN", "fin_summary"), ["metric_size", "is_populated"]].tolist() == [2.0, "YES"])
check("metrics: never computed, UNKNOWN", datasets.loc[("FIN", "missing"), "is_populated"] == "UNKNOWN")

# --- candidate locations ---------------------------------------------------------------------
locations = results["dss_dataset_locations"]


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
check("schema from the dataset settings", candidates("FIN", "fin_summary") == [("CANDIDATE", "DATASET", "fin_dssout", "ledger")])
check("per-user connection: one row, PER_USER", [c[0] for c in candidates("SALES", "per_user")] == ["PER_USER"])
check("SQL query dataset: NO_TABLE", [c[0] for c in candidates("SALES", "query_ds")] == ["NO_TABLE"])
check("file dataset: NOT_SQL", [c[0] for c in candidates("SALES", "notes_file")] == ["NOT_SQL"])
check("connection missing from databases: UNMAPPED with its candidate",
      candidates("SALES", "elsewhere") == [("UNMAPPED", "DATASET", "public", "t")])
labels = locations.drop_duplicates(["project_key", "dataset_name"]).set_index(["project_key", "dataset_name"])
check("database label from dss_connections, spaces ignored",
      labels.loc[("SALES", "orders_clean"), "database_label"] == "PG app"
      and labels.loc[("SALES", "orders_ora"), "database_label"] == "ORA")

connections = results["dss_connections"].set_index("connection")
check("connections: login user, credentials mode, no password copied",
      connections.loc["PG_SALES", "login_user"] == "dbmon"
      and connections.loc["PG_PERUSER", "credentials_mode"] == "PER_USER"
      and "hidden" not in connections.to_string() and "never-copied" not in connections.to_string()
      and connections.loc["PG_SALES", "jdbc_properties"] == '{"ssl": "true"}', connections.to_string())


# --- DSS's own location comes first, the settings follow -------------------------------------
def read_location(project_key, dataset_name):
    if dataset_name == "raw_orders":
        return {"info": {"schema": "sales_dssout", "table": "SALES_orders"}}
    raise Exception("not an input of this recipe")


with_dss = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, read_location)
locations = with_dss["dss_dataset_locations"]
check("DSS location as rank 1, the settings as fallback candidates",
      candidates("SALES", "raw_orders") == [("CANDIDATE", "DSS_LOCATION", "sales_dssout", "SALES_orders"),
                                            ("CANDIDATE", "SEARCH_PATH", "sales_dsswork", "SALES_orders"),
                                            ("CANDIDATE", "SEARCH_PATH", "public", "SALES_orders")],
      candidates("SALES", "raw_orders"))
check("DSS location not readable: noted on the candidates",
      "DSS location not readable" in locations[locations.dataset_name == "orders_clean"].note.iloc[0])


# --- projects read in parallel give the same outputs as one after the other ------------------
in_parallel = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, parallel=4)
one_by_one = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, parallel=1)
for name in in_parallel:
    parallel_rows = in_parallel[name]
    sequential_rows = one_by_one[name]
    if "seconds" in parallel_rows.columns:
        parallel_rows = parallel_rows.drop(columns="seconds")
        sequential_rows = sequential_rows.drop(columns="seconds")
    same = parallel_rows.fillna("-").astype(str).equals(sequential_rows.fillna("-").astype(str))
    check("parallel = 4 and parallel = 1 give the same %s, same order" % name, same)
check("inventory_log: seconds per project", in_parallel["inventory_log"]["seconds"].notna().all())

finish()
