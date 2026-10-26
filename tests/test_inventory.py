"""Step 2: dbmon.inventory with the stand-in DSS client, against tables and role settings
collected for real from the test containers.

    python tests/test_inventory.py
"""
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import check, finish, install_fake_dataiku
from fake_dss import DATABASES, FakeClient

install_fake_dataiku()
from dbmon import collect, inventory  # noqa: E402

# --- step 1, for real --------------------------------------------------------------------
step1, step1_log = collect.collect_all(DATABASES[["connection", "engine", "label"]], parallel=2)
check("step 1 collected on both engines", (step1_log.status == "OK").all(), step1_log.to_string())
role_settings = step1["role_settings"]
check("role_settings: the 2 PostgreSQL settings, kept once, none on Oracle",
      len(role_settings) == 2 and set(role_settings.engine) == {"postgresql"}
      and role_settings.database_name.isna().all(), role_settings.to_string())

# --- inventory -----------------------------------------------------------------------------
results = inventory.collect_inventory(FakeClient(), DATABASES, step1["tables"], step1["role_settings"])
datasets = results["dss_datasets"].set_index(["project_key", "dataset_name"])
pd.set_option("display.width", 250)
print(datasets[["dataset_type", "flow_role", "resolved_schema", "resolved_table", "schema_source",
                "location_status", "metric_rows", "metric_size", "is_populated"]].to_string())

log = results["inventory_log"].set_index("project_key")
check("log: SALES and FIN OK, BROKEN FAILED with its error",
      log.loc["SALES", "status"] == "OK" and log.loc["FIN", "status"] == "OK"
      and log.loc["BROKEN", "status"] == "FAILED" and "flow graph" in log.loc["BROKEN", "error"])

links = results["dss_flow_links"]
check("links: 5 dataset links, the managed folder left out",
      len(links) == 5 and "folder1" not in set(links.dataset_name), links.to_string())
foreign = links[links.foreign == "YES"]
check("links: FIN reads SALES.orders_clean as a foreign dataset",
      foreign[["project_key", "dataset_project", "dataset_name", "direction"]].values.tolist()
      == [["FIN", "SALES", "orders_clean", "input"]])

roles = datasets.flow_role.to_dict()
check("roles: raw_orders source, recipe outputs target, the others orphan",
      roles[("SALES", "raw_orders")] == "source" and roles[("SALES", "orders_clean")] == "target"
      and roles[("SALES", "orders_ora")] == "target" and roles[("FIN", "fin_summary")] == "target"
      and roles[("SALES", "notes_file")] == "orphan" and roles[("FIN", "missing")] == "orphan", roles)
shared = datasets.loc[("SALES", "orders_clean")]
check("sharing: orders_clean exposed to FIN and read by FIN, 1 producer, 1 consumer",
      shared.shared == "YES" and shared.shared_with == "FIN" and shared.used_by_projects == "FIN"
      and shared.n_producers == 1 and shared.n_consumers == 1)
check("use case from the CDP-UseCase tag",
      datasets.loc[("SALES", "raw_orders"), "use_case"] == "SALES"
      and pd.isna(datasets.loc[("FIN", "fin_summary"), "use_case"]))


def location(key):
    row = datasets.loc[key]
    return (row.schema_source, row.resolved_schema, row.resolved_table, row.location_status)


check("empty schema, PostgreSQL: search_path of dbmon in appdb, ${projectKey} replaced",
      location(("SALES", "raw_orders")) == ("SEARCH_PATH", "sales_dsswork", "SALES_orders", "FOUND"),
      location(("SALES", "raw_orders")))
check("empty schema: a post-connect SET search_path wins over the role setting",
      location(("SALES", "orders_clean")) == ("POST_CONNECT", "sales_dssout", "SALES_orders", "FOUND"),
      location(("SALES", "orders_clean")))
check("empty schema, Oracle: the login user, upper case",
      location(("SALES", "orders_ora")) == ("LOGIN_USER", "SALES_DSSWORK", "ORDERS", "FOUND"),
      location(("SALES", "orders_ora")))
check("schema from a project variable, local overriding standard",
      location(("FIN", "fin_summary")) == ("DATASET", "fin_dssout", "ledger", "FOUND"), location(("FIN", "fin_summary")))
check("different case found ignoring case, schema written as in the database",
      location(("SALES", "case_test")) == ("DATASET", "fin_dssout", "LEDGER", "FOUND"), location(("SALES", "case_test")))
check("table missing in the database: NOT_FOUND", datasets.loc[("FIN", "missing"), "location_status"] == "NOT_FOUND")
check("per-user connection: PER_USER, UNRESOLVED",
      datasets.loc[("SALES", "per_user"), ["schema_source", "location_status"]].tolist() == ["PER_USER", "UNRESOLVED"])
check("SQL query dataset: NO_TABLE", datasets.loc[("SALES", "query_ds"), "location_status"] == "NO_TABLE")
check("file dataset: NOT_SQL, with its resolved path",
      datasets.loc[("SALES", "notes_file"), "location_status"] == "NOT_SQL"
      and datasets.loc[("SALES", "notes_file"), "resolved_path"] == "SALES/notes")
check("connection missing from databases: UNMAPPED",
      datasets.loc[("SALES", "elsewhere"), "location_status"] == "UNMAPPED")
check("database label from dss_connections, spaces ignored",
      datasets.loc[("SALES", "orders_clean"), "database_label"] == "PG app"
      and datasets.loc[("SALES", "orders_ora"), "database_label"] == "ORA")

check("metrics: rows, YES", datasets.loc[("SALES", "raw_orders"), ["metric_rows", "is_populated"]].tolist() == [3000, "YES"])
check("metrics: 0 rows, NO", datasets.loc[("SALES", "orders_clean"), "is_populated"] == "NO")
check("metrics: partitions summed when there is no whole-dataset value",
      datasets.loc[("SALES", "orders_ora"), "metric_rows"] == 15
      and datasets.loc[("SALES", "orders_ora"), "partitioned"] == "YES")
check("metrics: size only, in MB, YES",
      datasets.loc[("FIN", "fin_summary"), ["metric_size", "is_populated"]].tolist() == [2.0, "YES"])
check("metrics: never computed, UNKNOWN", datasets.loc[("FIN", "missing"), "is_populated"] == "UNKNOWN")

connections = results["dss_connections"].set_index("connection")
check("connections: login user, credentials mode, no password copied",
      connections.loc["PG_SALES", "login_user"] == "dbmon"
      and connections.loc["PG_PERUSER", "credentials_mode"] == "PER_USER"
      and "hidden" not in connections.to_string() and "never-copied" not in connections.to_string()
      and connections.loc["PG_SALES", "jdbc_properties"] == '{"ssl": "true"}', connections.to_string())


# --- DSS's own location wins when it can be read ----------------------------------------------
def read_location(project_key, dataset_name):
    if dataset_name == "raw_orders":
        return {"info": {"schema": "sales_dssout", "table": "SALES_orders"}}
    raise Exception("not an input of this recipe")


with_dss = inventory.collect_inventory(FakeClient(), DATABASES, step1["tables"], step1["role_settings"], read_location)
located = with_dss["dss_datasets"].set_index(["project_key", "dataset_name"])
check("DSS location used when given, the settings otherwise",
      located.loc[("SALES", "raw_orders"), ["schema_source", "resolved_schema", "location_status"]].tolist()
      == ["DSS_LOCATION", "sales_dssout", "FOUND"]
      and located.loc[("SALES", "orders_clean"), "schema_source"] == "POST_CONNECT")

finish()
