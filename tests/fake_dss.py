"""A stand-in for the DSS public API client (dataikuapi), returning the structures of DSS 13.4.4,
with three projects:

SALES   use case SALES. raw_orders (source) -> compute_clean -> orders_clean, orders_ora,
        local_out and the folder "exports" (all produced); orders_clean is shared with FIN.
        Unmanaged orphans covering every location case; old_copy points to a table no other
        dataset uses (UNUSED).
FIN     reads SALES.orders_clean and lake_input (S3) -> join -> fin_summary -> copy ->
        dup_writer, which writes the same table as SALES.orders_clean (two owners). Schema
        from a project variable. A managed folder written by no recipe (idle). never_built:
        partitioned, never built, DSS fails to give its metrics.
BROKEN  its flow graph cannot be read: the project fails.

AUTOMATION_PROJECTS: an Automation node. Its own SALES project (same key as on the Design
node) writes the Oracle table SALES_DSSOUT.IOT_T, which no dataset of the Design node uses.

Every API call is counted in CALLS, so that the tests can check how many are made.
"""
import pandas as pd

# The databases dataset of the tests: the admin connections
DATABASES = pd.DataFrame([
    ("ADM_PG_APP", "postgresql", "PG app"),
    ("ADM_ORA", "oracle", "ORA"),
], columns=["connection", "engine", "label"])

# The audit_connection_mapping dataset: connections used by datasets -> admin connection.
# PG_UNMAPPED is missing on purpose; ORA_LOST is mapped to an admin connection databases
# does not list.
CONNECTION_MAPPING = pd.DataFrame([
    ("PG_SALES", "ADM_PG_APP"),
    ("PG_PERUSER", "ADM_PG_APP"),
    (" PG_POSTCONNECT ", "ADM_PG_APP"),
    ("ORA_SALES", "ADM_ORA"),
    ("ORA_URL", "ADM_ORA"),
    ("ORA_LOST", "ADM_NOT_LISTED"),
], columns=["dss_connection", "audit_connection"])

CALLS = {}


def count(call_name):
    CALLS[call_name] = CALLS.get(call_name, 0) + 1


def dataset(name, dataset_type, connection=None, schema="", table="", mode="table", managed=False,
            path=None, dimensions=()):
    params = {"connection": connection, "schema": schema, "table": table, "mode": mode}
    if path is not None:
        params = {"connection": connection, "path": path}
    partition_dimensions = []
    for dimension in dimensions:
        partition_dimensions.append({"name": dimension})
    return {"name": name, "type": dataset_type, "managed": managed, "params": params,
            "partitioning": {"dimensions": partition_dimensions}}


def folder(folder_id, name, folder_type, connection, path):
    return {"id": folder_id, "name": name, "type": folder_type, "params": {"connection": connection, "path": path}}


def metric(metric_id, values_by_partition):
    last_values = []
    for partition in values_by_partition:
        last_values.append({"partition": partition, "value": str(values_by_partition[partition])})
    return {"metric": {"id": metric_id}, "lastValues": last_values}


def node(node_type, ref, predecessors=(), successors=(), project_key=None):
    # as on DSS: the graph does not say the recipe type, the recipe list does
    result = {"type": node_type, "ref": ref, "predecessors": list(predecessors), "successors": list(successors)}
    if project_key is not None:
        result["projectKey"] = project_key
    return result


PROJECTS = {
    "SALES": {
        "tags": ["CDP-UseCase:SALES"],
        "variables": {"standard": {}, "local": {}},
        "exposed": {"objects": [{"type": "DATASET", "localName": "orders_clean",
                                 "rules": [{"targetProject": "FIN"}]}]},
        "datasets": [
            dataset("raw_orders", "PostgreSQL", "PG_SALES", "", "${projectKey}_orders"),
            dataset("orders_clean", "PostgreSQL", "PG_POSTCONNECT", "", "SALES_orders", managed=True),
            dataset("orders_ora", "Oracle", "ORA_SALES", "", "ORDERS", managed=True, dimensions=["day"]),
            dataset("case_test", "PostgreSQL", "PG_SALES", "Fin_Dssout", "LEDGER"),
            dataset("notes_file", "Filesystem", "filesystem_managed", path="${projectKey}/notes"),
            dataset("per_user", "PostgreSQL", "PG_PERUSER", "", "x"),
            dataset("query_ds", "PostgreSQL", "PG_SALES", mode="query"),
            dataset("elsewhere", "PostgreSQL", "PG_UNMAPPED", "public", "t"),
            dataset("old_copy", "PostgreSQL", "PG_SALES", "appx", "mv_totals"),
            dataset("via_url", "Oracle", "ORA_URL", "", "LEDGER"),
            dataset("local_out", "Filesystem", "filesystem_managed", path="${projectKey}/local_out", managed=True),
        ],
        "folders": [folder("folder1", "exports", "Filesystem", "filesystem_managed", "${projectKey}/exports")],
        "recipes": {"compute_clean": "python"},
        "metrics": {
            "raw_orders": [metric("records:COUNT_RECORDS", {"NP": 3000})],
            "orders_clean": [metric("records:COUNT_RECORDS", {"NP": 0})],
            "orders_ora": [metric("records:COUNT_RECORDS", {"2026-10-01": 10, "2026-10-02": 5})],
            "notes_file": [metric("basic:SIZE", {"NP": 1048576})],
            "local_out": [metric("basic:SIZE", {"NP": 4194304})],
            "folder1": [metric("basic:SIZE", {"NP": 5242880}), metric("basic:COUNT_FILES", {"NP": 3})],
        },
        "graph": {
            "raw_orders": node("COMPUTABLE_DATASET", "raw_orders", successors=["compute_clean"]),
            "compute_clean": node("RUNNABLE_RECIPE", "compute_clean", ["raw_orders"],
                                  ["orders_clean", "orders_ora", "local_out", "folder1"]),
            "orders_clean": node("COMPUTABLE_DATASET", "orders_clean", ["compute_clean"]),
            "orders_ora": node("COMPUTABLE_DATASET", "orders_ora", ["compute_clean"]),
            "local_out": node("COMPUTABLE_DATASET", "local_out", ["compute_clean"]),
            "folder1": node("COMPUTABLE_FOLDER", "folder1", ["compute_clean"]),
        },
    },
    "FIN": {
        "tags": ["finance", "CDP-Domain:Finance"],
        "variables": {"standard": {"finSchema": "wrong"}, "local": {"finSchema": "fin_dssout"}},
        "exposed": {"objects": []},
        "datasets": [
            dataset("fin_summary", "PostgreSQL", "PG_SALES", "${finSchema}", "ledger", managed=True),
            dataset("missing", "PostgreSQL", "PG_SALES", "appx", "nope"),
            dataset("dup_writer", "PostgreSQL", "PG_SALES", "sales_dssout", "SALES_orders", managed=True),
            dataset("lake_input", "S3", "s3_lake", path="/lake/fin"),
            # partitioned, never built: DSS cannot give its metrics
            dataset("never_built", "Filesystem", "filesystem_managed", path="${projectKey}/never_built", managed=True,
                    dimensions=["snapshot_day"]),
        ],
        "metrics_failing": ["never_built"],
        "folders": [folder("idle1", "old drops", "S3", "s3_lake", "/drops")],
        "recipes": {"join": "join", "copy": "sync"},
        "metrics": {
            "fin_summary": [metric("basic:SIZE", {"NP": 2097152})],
            "lake_input": [metric("basic:SIZE", {"NP": 10485760})],
            "idle1": [metric("basic:SIZE", {"NP": 1048576})],
        },
        "graph": {
            "SALES.orders_clean": node("COMPUTABLE_DATASET", "SALES.orders_clean", successors=["join"],
                                       project_key="SALES"),
            "lake_input": node("COMPUTABLE_DATASET", "lake_input", successors=["join"]),
            "join": node("RUNNABLE_RECIPE", "join", ["SALES.orders_clean", "lake_input"], ["fin_summary"]),
            "fin_summary": node("COMPUTABLE_DATASET", "fin_summary", ["join"], ["copy"]),
            "copy": node("RUNNABLE_RECIPE", "copy", ["fin_summary"], ["dup_writer"]),
            "dup_writer": node("COMPUTABLE_DATASET", "dup_writer", ["copy"]),
        },
    },
    "BROKEN": {"tags": [], "variables": {"standard": {}, "local": {}}, "exposed": {"objects": []},
               "datasets": [], "folders": [], "recipes": {}, "metrics": {}, "graph": None},
}

AUTOMATION_PROJECTS = {
    "SALES": {
        "tags": ["CDP-UseCase:SALES"],
        "variables": {"standard": {}, "local": {}},
        "exposed": {"objects": []},
        "datasets": [dataset("orders_iot", "Oracle", "ORA_URL", "", "IOT_T", managed=True)],
        "folders": [],
        "recipes": {"build_iot": "python"},
        "metrics": {},
        "graph": {
            "build_iot": node("RUNNABLE_RECIPE", "build_iot", [], ["orders_iot"]),
            "orders_iot": node("COMPUTABLE_DATASET", "orders_iot", ["build_iot"]),
        },
    },
}

# Connection details, as get_connection(name).get_info() returns them (resolved)
CONNECTIONS = {
    "PG_SALES": {"type": "PostgreSQL", "credentialsMode": "GLOBAL",
                 "resolvedParams": {"host": "pg", "port": 5432, "db": "appdb", "user": "dbmon",
                                    "properties": [{"name": "password", "value": "hidden"},
                                                   {"name": "ssl", "value": "true"}]},
                 "resolvedBasicCredential": {"user": "dbmon", "password": "never-copied"}},
    "PG_POSTCONNECT": {"type": "PostgreSQL", "credentialsMode": "GLOBAL",
                       "resolvedParams": {"host": "pg", "db": "appdb", "user": "dbmon",
                                          "postConnectStatementsExpandedAndSplit":
                                              ["SET search_path TO sales_dssout, public"]}},
    "PG_PERUSER": {"type": "PostgreSQL", "credentialsMode": "PER_USER", "resolvedParams": {"host": "pg", "db": "appdb"}},
    "PG_UNMAPPED": {"type": "PostgreSQL", "credentialsMode": "GLOBAL",
                    "resolvedParams": {"host": "x", "db": "x", "user": "u"}},
    "ORA_SALES": {"type": "Oracle", "credentialsMode": "GLOBAL",
                  "resolvedParams": {"host": "ora", "port": 1521, "db": "FREEPDB1", "user": "sales_dsswork"}},
    # "useURL" ticked after the fields were filled: DSS uses the URL, the fields are left over
    "ORA_URL": {"type": "Oracle", "credentialsMode": "GLOBAL",
                "resolvedParams": {"useURL": True, "host": "old-host", "port": 1600, "db": "OLDSVC",
                                   "url": "jdbc:oracle:thin:sales_dssout/url-secret@//ora:1521/FREEPDB1"}},
    # a JDBC URL typed into the host field, "useURL" not ticked
    "PG_HOST_URL": {"type": "PostgreSQL", "credentialsMode": "GLOBAL",
                    "resolvedParams": {"host": "jdbc:postgresql://pg:5432/appdb?ssl=true&password=p1", "user": "dbmon"}},
    "filesystem_managed": {"type": "Filesystem", "resolvedParams": {"root": "/data/managed"}},
    "s3_lake": {"type": "EC2", "resolvedParams": {"bucket": "lake"}},
}

# Permissions of the connections, as list_connections() returns them. The others: usable by
# the groups listed (none), details readable by nobody.
PERMISSIONS = {
    "PG_SALES": {"usableBy": "ALLOWED", "allowedGroups": ["data_team", "sales"],
                 "detailsReadability": {"readableBy": "ALLOWED", "allowedGroups": ["data_team"]}},
    "PG_POSTCONNECT": {"usableBy": "ALL", "allowedGroups": [],
                       "detailsReadability": {"readableBy": "NONE", "allowedGroups": []}},
}


def listed_connection(name):
    """A connection as list_connections() returns it: the parameters before resolution.
    PG_SALES has a variable in its host and its user in a credential: its details are read.
    PG_PERUSER has no user: its details are read. The others are used as listed."""
    details = CONNECTIONS[name]
    params = dict(details["resolvedParams"])
    if name == "PG_SALES":
        params["host"] = "${pgHost}"
        del params["user"]
    definition = {"name": name, "type": details["type"], "params": params,
                  "usableBy": "ALLOWED", "allowedGroups": [], "detailsReadability": {"readableBy": "NONE"}}
    if "credentialsMode" in details:
        definition["credentialsMode"] = details["credentialsMode"]
    definition.update(PERMISSIONS.get(name, {}))
    return definition


USERS = [
    {"login": "alice", "displayName": "Alice", "email": "alice@example.com", "sourceType": "LOCAL", "enabled": True,
     "userProfile": "DESIGNER", "groups": ["data_team", "sales"]},
    {"login": "bob", "displayName": "Bob", "email": "bob@example.com", "sourceType": "LDAP", "enabled": False,
     "userProfile": "READER", "groups": ["sales"]},
]
GROUPS = [
    {"name": "data_team", "description": "Data team", "sourceType": "LOCAL", "admin": False},
    {"name": "sales", "description": "Sales", "sourceType": "LDAP", "admin": False},
    {"name": "administrators", "description": "DSS admins", "sourceType": "LOCAL", "admin": True},
]


class Handle(object):
    """An object whose attributes are given at creation."""

    def __init__(self, **attributes):
        self.__dict__.update(attributes)


class FakeProject(object):
    def __init__(self, project_key, definition):
        self.project_key = project_key
        self.definition = definition

    def list_datasets(self):
        count("list_datasets")
        items = []
        for definition in self.definition["datasets"]:
            raw = dict(definition)
            raw["projectKey"] = self.project_key
            items.append(Handle(_data=raw))
        return items

    def list_recipes(self):
        count("list_recipes")
        items = []
        for recipe_name in self.definition["recipes"]:
            raw = {"name": recipe_name, "type": self.definition["recipes"][recipe_name], "projectKey": self.project_key}
            items.append(Handle(_data=raw))
        return items

    def list_managed_folders(self):
        count("list_managed_folders")
        items = []
        for definition in self.definition["folders"]:
            items.append(dict(definition))
        return items

    def get_variables(self):
        count("get_variables")
        return self.definition["variables"]

    def get_settings(self):
        count("get_settings")
        raw = {"exposedObjects": self.definition["exposed"]}
        return Handle(get_raw=lambda: raw)

    def get_flow(self):
        count("get_flow")
        if self.definition["graph"] is None:
            raise Exception("flow graph not readable")
        graph = Handle(data={"nodes": self.definition["graph"]})
        return Handle(get_graph=lambda: graph)

    def get_recipe(self, recipe_name):
        count("get_recipe")
        raise Exception("the recipe types come from list_recipes")

    def metrics_of(self, object_name):
        count("get_last_metric_values")
        if object_name in self.definition.get("metrics_failing", []):
            raise Exception("Root path of the dataset %s does not exist" % object_name)
        raw = {"metrics": self.definition["metrics"].get(object_name, [])}
        return Handle(get_raw=lambda: raw)

    def get_dataset(self, dataset_name):
        return Handle(get_last_metric_values=lambda partition="": self.metrics_of(dataset_name))

    def get_managed_folder(self, folder_id):
        return Handle(get_last_metric_values=lambda: self.metrics_of(folder_id))


class FakeClient(object):
    """One DSS instance. projects: its projects (default: the Design node, PROJECTS)."""

    def __init__(self, projects=None):
        self.projects = projects
        if self.projects is None:
            self.projects = PROJECTS

    def list_projects(self):
        count("list_projects")
        projects = []
        for project_key in self.projects:
            projects.append({"projectKey": project_key, "name": project_key.title() + " project",
                             "ownerLogin": "alice", "tags": self.projects[project_key]["tags"]})
        return projects

    def get_project(self, project_key):
        return FakeProject(project_key, self.projects[project_key])


    def get_global_variables(self):
        count("get_global_variables")
        return {"instanceVar": "x"}

    def list_connections(self, as_type="dictitems"):
        count("list_connections")
        listed = {}
        for name in CONNECTIONS:
            listed[name] = listed_connection(name)
        return listed

    def get_connection(self, name):
        info = CONNECTIONS[name]

        def get_info(contextual_project_key=None):
            count("get_connection_info")
            return info

        return Handle(get_info=get_info)

    def list_users(self):
        count("list_users")
        return USERS

    def list_groups(self):
        count("list_groups")
        return GROUPS


class UnreachableClient(object):
    """An instance that cannot be reached."""

    def get_global_variables(self):
        raise Exception("connection refused")

    def list_projects(self):
        raise Exception("connection refused")
