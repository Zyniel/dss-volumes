"""Step 2: dbmon.dss_extract with the stand-in DSS client. No database is needed.

    python tests/test_extract.py
"""
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import check, finish, install_fake_dataiku
import fake_dss
from fake_dss import AUTOMATION_PROJECTS, FakeClient, UnreachableClient

install_fake_dataiku()
from dbmon import dss_extract, schemas  # noqa: E402

OUTPUTS = ["dss_connection_permissions", "dss_connections", "dss_dataset_shares", "dss_datasets", "dss_flow_links",
           "dss_group_members", "dss_groups", "dss_managed_folders", "dss_project_tags", "dss_projects", "dss_users",
           "inventory_log"]

fake_dss.CALLS.clear()
results = dss_extract.extract_dss(FakeClient(), parallel=4)
calls = dict(fake_dss.CALLS)
check("outputs", sorted(results) == OUTPUTS, sorted(results))

all_declared = True
for name in results:
    try:
        schemas.conform(results[name], name)
    except ValueError as error:
        print(error)
        all_declared = False
check("every column of every output is declared in schemas.py", all_declared)

# --- API calls ---------------------------------------------------------------------------------
check("one call per run: projects, instance variables, connections, users, groups",
      calls.get("list_projects") == 1 and calls.get("get_global_variables") == 1 and calls.get("list_connections") == 1
      and calls.get("list_users") == 1 and calls.get("list_groups") == 1, calls)
check("one call per project: datasets, recipes, folders, flow (settings only when the flow is read)",
      calls.get("list_datasets") == 3 and calls.get("list_recipes") == 3 and calls.get("list_managed_folders") == 3
      and calls.get("get_flow") == 3 and calls.get("get_settings") == 2, calls)
check("recipe types from the recipe list: no recipe read", calls.get("get_recipe", 0) == 0, calls)
check("project variables read only where a setting uses one (FIN)", calls.get("get_variables") == 1, calls)
check("metrics: one call per dataset and folder (12 in SALES, 6 in FIN)", calls.get("get_last_metric_values") == 18,
      calls)
check("connection details only for a variable or a missing user (PG_SALES, PG_PERUSER, ORA_URL)",
      calls.get("get_connection_info") == 3, calls)

fake_dss.CALLS.clear()
no_metrics = dss_extract.extract_dss(FakeClient(), parallel=1, metrics=dss_extract.METRICS_NONE)
check("metrics NONE: no metrics call, is_populated UNKNOWN",
      fake_dss.CALLS.get("get_last_metric_values", 0) == 0
      and set(no_metrics["dss_datasets"].is_populated) == {"UNKNOWN"})

# --- projects and tags ---------------------------------------------------------------------------
log = results["inventory_log"].set_index("project_key")
check("log: SALES and FIN OK, BROKEN FAILED with its error",
      log.loc["SALES", "status"] == "OK" and log.loc["FIN", "status"] == "OK"
      and log.loc["BROKEN", "status"] == "FAILED" and "flow graph" in log.loc["BROKEN", "error"])
check("log: datasets, folders and links counted", log.loc["SALES", ["datasets", "managed_folders", "links"]].tolist()
      == [11, 1, 5])

projects = results["dss_projects"].set_index("project_key")
check("dss_projects: name and owner from the project list, failed project left out",
      projects.loc["SALES", ["project_name", "owner_login"]].tolist() == ["Sales project", "alice"]
      and "BROKEN" not in projects.index)
tags = results["dss_project_tags"]
check("dss_project_tags: one row per tag, Category:Value split",
      tags.fillna("-").values.tolist() == [["SALES", "CDP-UseCase:SALES", "CDP-UseCase", "SALES"],
                               ["FIN", "finance", "-", "-"],
                               ["FIN", "CDP-Domain:Finance", "CDP-Domain", "Finance"]], tags.values.tolist())

# --- flow links ----------------------------------------------------------------------------------
links = results["dss_flow_links"]
check("links: 10, datasets and the managed folder", len(links) == 10
      and links[links.object_type == "FOLDER"].object_name.tolist() == ["folder1"], links.to_string())
foreign = links[links.foreign == "YES"]
check("links: FIN reads SALES.orders_clean as a foreign dataset",
      foreign[["project_key", "object_project", "object_name", "direction"]].values.tolist()
      == [["FIN", "SALES", "orders_clean", "input"]])
recipe_types = {}
for link in links.itertuples(index=False):
    recipe_types[link.recipe_name] = link.recipe_type
check("recipe type from the recipe list", recipe_types == {"compute_clean": "python", "join": "join", "copy": "sync"},
      recipe_types)

# --- datasets and folders ------------------------------------------------------------------------
datasets = results["dss_datasets"].set_index(["project_key", "dataset_name"])
check("dss_datasets: raw settings only, nothing computed from other datasets",
      "flow_role" not in datasets.columns and "dataset_nature" not in datasets.columns)
check("variables replaced, local project variable overriding standard",
      datasets.loc[("SALES", "raw_orders"), "resolved_table"] == "SALES_orders"
      and datasets.loc[("FIN", "fin_summary"), "resolved_schema"] == "fin_dssout"
      and datasets.loc[("SALES", "notes_file"), "resolved_path"] == "SALES/notes")
shared = datasets.loc[("SALES", "orders_clean")]
shares = results["dss_dataset_shares"]
check("sharing: orders_clean shared with 1 project, one row per target",
      shared.shared == "YES" and shared.n_shared_with == 1
      and shares.values.tolist() == [["SALES", "orders_clean", "FIN"]], shares.to_string())
check("metrics: rows, YES", datasets.loc[("SALES", "raw_orders"), ["metric_rows", "is_populated"]].tolist() == [3000, "YES"])
check("metrics: 0 rows, NO", datasets.loc[("SALES", "orders_clean"), "is_populated"] == "NO")
check("metrics: partitions summed when there is no whole-dataset value",
      datasets.loc[("SALES", "orders_ora"), "metric_rows"] == 15)
check("metrics: size only, in MB, YES",
      datasets.loc[("FIN", "fin_summary"), ["metric_size", "is_populated"]].tolist() == [2.0, "YES"])
check("metrics: never computed, UNKNOWN", datasets.loc[("FIN", "missing"), "is_populated"] == "UNKNOWN")
never_built = datasets.loc[("FIN", "never_built")]
check("metrics not readable (dataset never built): project still OK, metrics empty, error kept and counted",
      log.loc["FIN", "status"] == "OK" and never_built.is_populated == "UNKNOWN"
      and "Root path" in never_built.metrics_error and log.loc["FIN", "metric_errors"] == 1
      and log.loc["SALES", "metric_errors"] == 0)

folders = results["dss_managed_folders"].set_index("folder_id")
check("dss_managed_folders: settings, resolved path, size in MB and files",
      folders.loc["folder1", ["project_key", "folder_name", "folder_type", "connection", "resolved_path",
                              "metric_size", "metric_files", "is_populated"]].tolist()
      == ["SALES", "exports", "Filesystem", "filesystem_managed", "SALES/exports", 5.0, 3, "YES"]
      and "metric_rows" not in folders.columns, folders.to_string())

# --- connections and permissions -----------------------------------------------------------------
connections = results["dss_connections"].set_index("connection")
check("connections: login user, credentials mode, no password copied",
      connections.loc["PG_SALES", "effective_user"] == "dbmon"
      and connections.loc["PG_PERUSER", "credentials_mode"] == "PER_USER"
      and "hidden" not in connections.to_string() and "never-copied" not in connections.to_string()
      and connections.loc["PG_SALES", "driver_properties"] == '{"ssl": "true"}', connections.to_string())
check("connection with a variable: resolved by its details", connections.loc["PG_SALES", "effective_host"] == "pg")
check("connection used as listed: type and credentials mode kept",
      connections.loc["ORA_SALES", ["connection_type", "credentials_mode", "effective_user"]].tolist()
      == ["Oracle", "GLOBAL", "sales_dsswork"] and connections.loc["s3_lake", "connection_type"] == "EC2")


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

permissions = results["dss_connection_permissions"]
pg_sales = permissions[permissions.connection == "PG_SALES"]
check("permissions: one row per allowed group, freely usable by and details readable by",
      pg_sales[["permission", "scope", "group_name"]].values.tolist()
      == [["FREELY_USABLE_BY", "ALLOWED", "data_team"], ["FREELY_USABLE_BY", "ALLOWED", "sales"],
          ["DETAILS_READABLE_BY", "ALLOWED", "data_team"]], pg_sales.to_string())
post_connect = permissions[permissions.connection == "PG_POSTCONNECT"]
check("permissions: ALL and NONE scopes on one row each, no group",
      post_connect[["permission", "scope"]].values.tolist()
      == [["FREELY_USABLE_BY", "ALL"], ["DETAILS_READABLE_BY", "NONE"]]
      and post_connect.group_name.isna().all())

# --- security ------------------------------------------------------------------------------------
users = results["dss_users"]
check("dss_users: one row per user, no email",
      users.login.tolist() == ["alice", "bob"] and "email" not in users.columns
      and "example.com" not in users.to_string() and users.enabled.tolist() == ["YES", "NO"])
check("dss_groups: one row per group", results["dss_groups"].group_name.tolist() == ["data_team", "sales", "administrators"]
      and results["dss_groups"].set_index("group_name").loc["administrators", "admin"] == "YES")
check("dss_group_members: one row per group and user",
      results["dss_group_members"].values.tolist() == [["data_team", "alice"], ["sales", "alice"], ["sales", "bob"]])

# --- projects read in parallel give the same outputs as one after the other ------------------
in_parallel = dss_extract.extract_dss(FakeClient(), parallel=4)
one_by_one = dss_extract.extract_dss(FakeClient(), parallel=1)
for name in in_parallel:
    parallel_rows = in_parallel[name]
    sequential_rows = one_by_one[name]
    if "seconds" in parallel_rows.columns:
        parallel_rows = parallel_rows.drop(columns="seconds")
        sequential_rows = sequential_rows.drop(columns="seconds")
    same = parallel_rows.fillna("-").astype(str).equals(sequential_rows.fillna("-").astype(str))
    check("parallel = 4 and parallel = 1 give the same %s, same order" % name, same)
check("inventory_log: seconds per project", in_parallel["inventory_log"]["seconds"].notna().all())

# --- several instances ---------------------------------------------------------------------------
asked = []


def get_admin_client(instance_name):
    """Stand-in for com.arval.cdplib.client.get_remote_admin_client."""
    asked.append(instance_name)
    if instance_name == "DESIGN":
        return FakeClient()
    if instance_name == "AUTOMATION":
        return FakeClient(AUTOMATION_PROJECTS)
    if instance_name == "DOWN":
        return UnreachableClient()
    raise Exception("unknown instance %s" % instance_name)


local = FakeClient()
instance_rows = [
    {"instance_name": "DESIGN"},
    {"instance_name": " AUTOMATION "},
    {"instance_name": float("nan")},
    {"instance_name": "NO_SUCH"},
    {"instance_name": "DOWN"},
]
instances = dss_extract.connect_instances(instance_rows, get_admin_client, local)
by_name = {}
for instance in instances:
    by_name[instance["name"]] = instance
check("instances: one client per instance_name through the admin client library, empty names skipped",
      asked == ["DESIGN", "AUTOMATION", "NO_SUCH", "DOWN"] and "client" in by_name["AUTOMATION"]
      and by_name["DESIGN"]["client"] is not local, asked)
check("instances: a client that cannot be made is an error of that instance only",
      "unknown instance" in by_name["NO_SUCH"]["error"] and "client" in by_name["DOWN"])
check("instances: no row = the local instance, named local",
      dss_extract.connect_instances([], get_admin_client, local) == [{"name": "local", "client": local}])

several = dss_extract.extract_instances(instances, parallel=2)
check("several instances: every output starts with dss_instance",
      sorted(several) == OUTPUTS and all(several[name].columns[0] == "dss_instance" for name in several))
instance_log = several["inventory_log"]
failed_instances = instance_log[instance_log.project_key.isna()]
check("several instances: an instance that cannot be read is one FAILED row, the others are read",
      sorted(failed_instances.dss_instance) == ["DOWN", "NO_SUCH"] and set(failed_instances.status) == {"FAILED"}
      and "connection refused" in failed_instances.set_index("dss_instance").loc["DOWN", "error"])
sales = several["dss_projects"][several["dss_projects"].project_key == "SALES"]
check("several instances: the same project key on two instances is two projects",
      sorted(sales.dss_instance) == ["AUTOMATION", "DESIGN"]
      and several["dss_datasets"][several["dss_datasets"].dss_instance == "AUTOMATION"].dataset_name.tolist()
      == ["orders_iot"])
declared_ok = True
for name in several:
    try:
        schemas.conform(several[name], name)
    except ValueError as error:
        print(error)
        declared_ok = False
check("several instances: every column declared", declared_ok)

# --- long values ---------------------------------------------------------------------------------
many = pd.DataFrame({"project_key": ["P"] * 1000, "dataset_name": ["d"] * 1000,
                     "target_project": ["PROJECT_%04d" % i for i in range(1000)]})
check("1000 sharing projects: 1000 rows, each value short",
      len(schemas.conform(many, "dss_dataset_shares")) == 1000)
long_error = schemas.conform(pd.DataFrame({"error": ["x" * 15000]}), "inventory_log")["error"].iloc[0]
check("free text over the limit is cut to the declared length, ending with ...",
      len(long_error) == schemas.LONG_TEXT_LENGTH and long_error.endswith("..."))
lengths_ok = True
for name in schemas.SCHEMAS:
    for column in schemas.dss_schema(name):
        if column["type"] == "string" and "maxLength" not in column:
            lengths_ok = False
check("every string column has a declared maxLength", lengths_ok)

finish()
