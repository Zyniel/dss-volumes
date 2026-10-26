"""Step 2: dbmon.inventory with the stand-in DSS client (extraction only), using the role
settings collected for real from the PostgreSQL test container.

    python tests/test_inventory.py
"""
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import check, finish, install_fake_dataiku
from fake_dss import CONNECTION_MAPPING, DATABASES, FakeClient

install_fake_dataiku()
from dbmon import collect, inventory  # noqa: E402

# --- role settings, for real -------------------------------------------------------------------
step1, step1_log = collect.collect_all(DATABASES[["connection", "engine", "label"]], parallel=2)
check("step 1 collected on both engines", (step1_log.status == "OK").all(), step1_log.to_string())
role_settings = step1["role_settings"]

# --- inventory -----------------------------------------------------------------------------
results = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, connection_mapping=CONNECTION_MAPPING)
check("outputs", sorted(results) == ["dss_connections", "dss_dataset_locations", "dss_dataset_shares", "dss_datasets",
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
shares = results["dss_dataset_shares"]
check("sharing: orders_clean shared with 1 project, one row per target in dss_dataset_shares",
      shared.shared == "YES" and shared.n_shared_with == 1 and "shared_with" not in datasets.columns
      and shares.values.tolist() == [["SALES", "orders_clean", "FIN"]], shares.to_string())
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
check("database label through audit_connection_mapping, spaces ignored",
      labels.loc[("SALES", "orders_clean"), "database_label"] == "PG app"
      and labels.loc[("SALES", "orders_ora"), "database_label"] == "ORA")

connections = results["dss_connections"].set_index("connection")
check("connections: login user, credentials mode, no password copied",
      connections.loc["PG_SALES", "effective_user"] == "dbmon"
      and connections.loc["PG_PERUSER", "credentials_mode"] == "PER_USER"
      and "hidden" not in connections.to_string() and "never-copied" not in connections.to_string()
      and connections.loc["PG_SALES", "driver_properties"] == '{"ssl": "true"}', connections.to_string())


# --- DSS's own location comes first, the settings follow -------------------------------------
def read_location(project_key, dataset_name):
    if dataset_name == "raw_orders":
        return {"info": {"schema": "sales_dssout", "table": "SALES_orders"}}
    raise Exception("not an input of this recipe")


with_dss = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, read_location,
                                       connection_mapping=CONNECTION_MAPPING)
locations = with_dss["dss_dataset_locations"]
check("DSS location as rank 1, the settings as fallback candidates",
      candidates("SALES", "raw_orders") == [("CANDIDATE", "DSS_LOCATION", "sales_dssout", "SALES_orders"),
                                            ("CANDIDATE", "SEARCH_PATH", "sales_dsswork", "SALES_orders"),
                                            ("CANDIDATE", "SEARCH_PATH", "public", "SALES_orders")],
      candidates("SALES", "raw_orders"))
check("DSS location not readable: noted on the candidates",
      "DSS location not readable" in locations[locations.dataset_name == "orders_clean"].note.iloc[0])


# --- projects read in parallel give the same outputs as one after the other ------------------
in_parallel = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, parallel=4,
                                          connection_mapping=CONNECTION_MAPPING)
one_by_one = inventory.collect_inventory(FakeClient(), DATABASES, role_settings, parallel=1,
                                         connection_mapping=CONNECTION_MAPPING)
for name in in_parallel:
    parallel_rows = in_parallel[name]
    sequential_rows = one_by_one[name]
    if "seconds" in parallel_rows.columns:
        parallel_rows = parallel_rows.drop(columns="seconds")
        sequential_rows = sequential_rows.drop(columns="seconds")
    same = parallel_rows.fillna("-").astype(str).equals(sequential_rows.fillna("-").astype(str))
    check("parallel = 4 and parallel = 1 give the same %s, same order" % name, same)
check("inventory_log: seconds per project", in_parallel["inventory_log"]["seconds"].notna().all())


# --- long values: a dataset shared with 1000 projects, a 15000-character note --------------------
from dbmon import schemas  # noqa: E402
many = pd.DataFrame({"project_key": ["P"] * 1000, "dataset_name": ["d"] * 1000,
                     "target_project": ["PROJECT_%04d" % i for i in range(1000)]})
check("1000 sharing projects: 1000 rows, each value short",
      len(schemas.conform(many, "dss_dataset_shares")) == 1000)
long_note = schemas.conform(pd.DataFrame({"note": ["x" * 15000]}), "dss_dataset_locations")["note"].iloc[0]
check("free text over the limit is cut to the declared length, ending with ...",
      len(long_note) == schemas.LONG_TEXT_LENGTH and long_note.endswith("..."))
declared = schemas.dss_schema("dss_datasets")
lengths_ok = True
for column in declared:
    if column["type"] == "string" and "maxLength" not in column:
        lengths_ok = False
check("every string column has a declared maxLength", lengths_ok)


# --- recipe types and JDBC URLs ----------------------------------------------------------------
recipe_types = {}
for link in links.itertuples(index=False):
    recipe_types[link.recipe_name] = link.recipe_type
check("recipe type read from the recipe settings", recipe_types == {"compute_clean": "python", "join": "join", "copy": "sync"},
      recipe_types)
def connection_values(name, columns):
    values = []
    for column in columns:
        value = connections.loc[name, column]
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            values.append(None)
        else:
            values.append(str(value))
    return values


check("fields only: mode FIELDS, effective = fields, no JDBC values",
      connection_values("PG_SALES", ["connection_mode", "effective_host", "effective_port",
                                     "effective_database_or_service", "jdbc_url", "stale_fields"])
      == ["FIELDS", "pg", "5432", "appdb", None, "NO"])
check("useURL ticked over old fields: both kept, effective = the URL values, stale_fields YES",
      connection_values("ORA_URL", ["connection_mode", "field_host", "field_port", "field_database_or_service",
                                    "jdbc_vendor", "jdbc_host", "jdbc_port", "jdbc_database_or_service", "jdbc_user",
                                    "effective_host", "effective_port", "effective_database_or_service",
                                    "effective_user", "stale_fields"])
      == ["JDBC", "old-host", "1600", "OLDSVC", "oracle", "ora", "1521", "FREEPDB1", "sales_dssout",
          "ora", "1521", "FREEPDB1", "sales_dssout", "YES"],
      connections.loc["ORA_URL"].to_string())
check("the raw JDBC URL is kept unchanged",
      connections.loc["ORA_URL", "jdbc_url"] == "jdbc:oracle:thin:sales_dssout/url-secret@//ora:1521/FREEPDB1")
check("JDBC URL typed into the host field: parsed and used, stale_fields YES, URL parameters without the password",
      connection_values("PG_HOST_URL", ["connection_mode", "effective_host", "effective_port",
                                        "effective_database_or_service", "jdbc_properties", "stale_fields"])
      == ["FIELDS", "pg", "5432", "appdb", '{"ssl": "true"}', "YES"],
      connections.loc["PG_HOST_URL"].to_string())
check("dataset on that connection: the URL user as Oracle login user",
      candidates("SALES", "via_url") == [("CANDIDATE", "LOGIN_USER", "SALES_DSSOUT", "LEDGER")], candidates("SALES", "via_url"))


# --- audit_connection_mapping ----------------------------------------------------------------
from dbmon.inventory import audit_connections  # noqa: E402
audits = audit_connections(DATABASES, CONNECTION_MAPPING)
check("admin connections are their own audit connection",
      audits["ADM_PG_APP"] == [("ADM_PG_APP", "PG app")] and audits["ADM_ORA"] == [("ADM_ORA", "ORA")])
check("a mapped connection leads to its admin connection and label",
      audits["PG_SALES"] == [("ADM_PG_APP", "PG app")] and audits["ORA_URL"] == [("ADM_ORA", "ORA")])
check("mapped to an admin connection databases does not list: nothing", "ORA_LOST" not in audits)
two = audit_connections(DATABASES, pd.DataFrame([("SHARED", "ADM_PG_APP"), ("SHARED", "ADM_ORA")],
                                                columns=["dss_connection", "audit_connection"]))
check("a connection mapped to two admin connections leads to both",
      two["SHARED"] == [("ADM_PG_APP", "PG app"), ("ADM_ORA", "ORA")])
names = results["dss_dataset_locations"].drop_duplicates(["project_key", "dataset_name"]).set_index(
    ["project_key", "dataset_name"])
check("connection_name = the dataset's own connection, audit_connection_name = the mapped admin connection",
      names.loc[("SALES", "raw_orders"), ["connection_name", "audit_connection_name"]].tolist()
      == ["PG_SALES", "ADM_PG_APP"]
      and names.loc[("SALES", "orders_ora"), ["connection_name", "audit_connection_name"]].tolist()
      == ["ORA_SALES", "ADM_ORA"])
check("unmapped connection: audit_connection_name empty",
      names.loc[("SALES", "elsewhere"), "connection_name"] == "PG_UNMAPPED"
      and pd.isna(names.loc[("SALES", "elsewhere"), "audit_connection_name"]))
without_mapping = inventory.collect_inventory(FakeClient(), DATABASES, role_settings)
unmapped = without_mapping["dss_dataset_locations"]
check("without the mapping: datasets of non-admin connections are UNMAPPED",
      set(unmapped[unmapped.dataset_name == "raw_orders"].candidate_status) == {"UNMAPPED"})

finish()
