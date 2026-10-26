"""A stand-in for the DSS public API client (dataikuapi), returning the structures of DSS 13.4.4,
with three projects:

SALES   use case SALES. raw_orders (source) -> compute_clean -> orders_clean, orders_ora
        (targets); orders_clean is shared with FIN. Orphans covering every location case;
        old_copy points to a table no other dataset uses (NO_FLOW).
FIN     reads SALES.orders_clean -> join -> fin_summary -> copy -> dup_writer, which writes
        the same table as SALES.orders_clean (two owners). Schema from a project variable.
BROKEN  its flow graph cannot be read: the project fails.
"""
import pandas as pd

# The databases dataset of the tests: admin connections and the DSS connections they hold
DATABASES = pd.DataFrame([
    ("ADM_PG_APP", "postgresql", "PG app", "PG_SALES,PG_PERUSER, PG_POSTCONNECT"),
    ("ADM_ORA", "oracle", "ORA", "ORA_SALES"),
], columns=["connection", "engine", "label", "dss_connections"])


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


def metric(metric_id, values_by_partition):
    last_values = []
    for partition in values_by_partition:
        last_values.append({"partition": partition, "value": str(values_by_partition[partition])})
    return {"metric": {"id": metric_id}, "lastValues": last_values}


def node(node_type, ref, predecessors=(), successors=(), sub_type=None, project_key=None):
    result = {"type": node_type, "ref": ref, "predecessors": list(predecessors), "successors": list(successors)}
    if sub_type is not None:
        result["subType"] = sub_type
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
        ],
        "metrics": {
            "raw_orders": [metric("records:COUNT_RECORDS", {"NP": 3000})],
            "orders_clean": [metric("records:COUNT_RECORDS", {"NP": 0})],
            "orders_ora": [metric("records:COUNT_RECORDS", {"2026-10-01": 10, "2026-10-02": 5})],
        },
        "graph": {
            "raw_orders": node("COMPUTABLE_DATASET", "raw_orders", successors=["compute_clean"]),
            "compute_clean": node("RUNNABLE_RECIPE", "compute_clean", ["raw_orders"],
                                  ["orders_clean", "orders_ora", "folder1"], sub_type="python"),
            "orders_clean": node("COMPUTABLE_DATASET", "orders_clean", ["compute_clean"]),
            "orders_ora": node("COMPUTABLE_DATASET", "orders_ora", ["compute_clean"]),
            "folder1": node("COMPUTABLE_FOLDER", "folder1", ["compute_clean"]),
        },
    },
    "FIN": {
        "tags": ["finance"],
        "variables": {"standard": {"finSchema": "wrong"}, "local": {"finSchema": "fin_dssout"}},
        "exposed": {"objects": []},
        "datasets": [
            dataset("fin_summary", "PostgreSQL", "PG_SALES", "${finSchema}", "ledger", managed=True),
            dataset("missing", "PostgreSQL", "PG_SALES", "appx", "nope"),
            dataset("dup_writer", "PostgreSQL", "PG_SALES", "sales_dssout", "SALES_orders", managed=True),
        ],
        "metrics": {"fin_summary": [metric("basic:SIZE", {"NP": 2097152})]},
        "graph": {
            "SALES.orders_clean": node("COMPUTABLE_DATASET", "SALES.orders_clean", successors=["join"],
                                       project_key="SALES"),
            "join": node("RUNNABLE_RECIPE", "join", ["SALES.orders_clean"], ["fin_summary"], sub_type="join"),
            "fin_summary": node("COMPUTABLE_DATASET", "fin_summary", ["join"], ["copy"]),
            "copy": node("RUNNABLE_RECIPE", "copy", ["fin_summary"], ["dup_writer"], sub_type="sync"),
            "dup_writer": node("COMPUTABLE_DATASET", "dup_writer", ["copy"]),
        },
    },
    "BROKEN": {"tags": [], "variables": {"standard": {}, "local": {}}, "exposed": {"objects": []},
               "datasets": [], "metrics": {}, "graph": None},
}

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
    "filesystem_managed": {"type": "Filesystem", "resolvedParams": {"root": "/data/managed"}},
}


class Handle(object):
    """An object whose attributes are given at creation."""

    def __init__(self, **attributes):
        self.__dict__.update(attributes)


class FakeProject(object):
    def __init__(self, project_key):
        self.project_key = project_key
        self.definition = PROJECTS[project_key]

    def list_datasets(self):
        items = []
        for definition in self.definition["datasets"]:
            raw = dict(definition)
            raw["projectKey"] = self.project_key
            items.append(Handle(_data=raw))
        return items

    def get_variables(self):
        return self.definition["variables"]

    def get_settings(self):
        raw = {"exposedObjects": self.definition["exposed"]}
        return Handle(get_raw=lambda: raw)

    def get_flow(self):
        if self.definition["graph"] is None:
            raise Exception("flow graph not readable")
        graph = Handle(data={"nodes": self.definition["graph"]})
        return Handle(get_graph=lambda: graph)

    def get_dataset(self, dataset_name):
        raw = {"metrics": self.definition["metrics"].get(dataset_name, [])}
        computed = Handle(get_raw=lambda: raw)
        return Handle(get_last_metric_values=lambda partition="": computed)


class FakeClient(object):
    def list_projects(self):
        projects = []
        for project_key in PROJECTS:
            projects.append({"projectKey": project_key, "tags": PROJECTS[project_key]["tags"]})
        return projects

    def get_project(self, project_key):
        return FakeProject(project_key)

    def get_global_variables(self):
        return {"instanceVar": "x"}

    def list_connections(self, as_type="dictitems"):
        names = {}
        for name in CONNECTIONS:
            names[name] = None
        return names

    def get_connection(self, name):
        info = CONNECTIONS[name]
        return Handle(get_info=lambda contextual_project_key=None: info)
