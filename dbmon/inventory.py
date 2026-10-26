"""Inventory of DSS: projects, datasets, flow links, connections, and where each SQL dataset
may be stored. Read through the DSS public API, with an admin API key. Notebooks are not read.

Results (step 2, extraction only: nothing here reads the step 1 tables):
- dss_projects           one row per project: tags, use case and where it comes from
- dss_datasets           one row per dataset: settings, storage as configured, sharing, metrics
- dss_dataset_shares     one row per shared dataset and project it is exposed to
- dss_flow_links         one row per recipe input or output (the flow graph of every project)
- dss_connections        one row per connection: where it points (no secret)
- dss_dataset_locations  one row per dataset and candidate schema, best candidate first
- inventory_log          one row per project: OK or FAILED

The step 3 SQL matches dss_dataset_locations against the step 1 tables, computes the flow
roles from dss_flow_links, and the ownership of the tables.

Candidate schemas of a SQL dataset, in the order the database would use them
(schema_source):
1. DSS_LOCATION        the location DSS resolves itself, when the recipe can read it
2. DATASET             the dataset's own schema setting
3. CONNECTION_DEFAULT  the default schema of the connection
4. JDBC_PROPERTY       the JDBC property currentSchema (PostgreSQL)
5. POST_CONNECT        ALTER SESSION SET CURRENT_SCHEMA / SET search_path in the
                       connection's post-connect statements
6. LOGIN_USER          Oracle: the schema of the login user
   SEARCH_PATH         PostgreSQL: the search_path of the login user (role_settings), by
                       default "$user", public: one candidate per schema of the path
DSS_LOCATION comes first, the settings follow as fallback candidates.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from dbmon import use_case
from dbmon.utils.jdbc_parser import parse_jdbc

# DSS dataset types read as SQL tables, and their engine
SQL_DATASET_TYPES = {
    "Oracle": "oracle",
    "PostgreSQL": "postgresql",
}

METRIC_ROWS = "records:COUNT_RECORDS"
METRIC_SIZE = "basic:SIZE"

# Parameters never copied out of a connection
SECRET_WORDS = ("password", "secret", "token", "credential", "key")

ORACLE_CURRENT_SCHEMA = re.compile(r"ALTER\s+SESSION\s+SET\s+CURRENT_SCHEMA\s*=\s*\"?([^\"\s;]+)\"?", re.IGNORECASE)
POSTGRES_SEARCH_PATH = re.compile(r"SET\s+(?:SESSION\s+)?search_path\s*(?:=|TO)\s*(.+?);?\s*$", re.IGNORECASE)
VARIABLE = re.compile(r"\$\{([^}]+)\}")

LINK_COLUMNS = ["project_key", "recipe_name", "recipe_type", "direction",
                "dataset_project", "dataset_name", "foreign"]
SHARE_COLUMNS = ["project_key", "dataset_name", "target_project"]
LOCATION_COLUMNS = ["project_key", "dataset_name", "candidate_rank", "candidate_status",
                    "connection_name", "audit_connection_name", "database_label",
                    "schema_name", "table_name", "schema_source", "note"]


# ---------------------------------------------------------------------------------------
# All projects
# ---------------------------------------------------------------------------------------

def collect_inventory(client, databases, role_settings, read_location=None, parallel=4, connection_mapping=None):
    """Read every project of the DSS instance.

    client         dataikuapi client with an admin API key (dataiku.api_client() in DSS)
    databases      the databases dataset: the admin connections and their labels
    role_settings  the role settings collected in step 1 (one snapshot)
    read_location  optional function (project_key, dataset_name) -> location info dict,
                   DSS's own resolved location (dataiku.Dataset.get_location_info). None:
                   candidates from the settings only
    parallel       projects read at the same time. 1: one after the other
    connection_mapping  the audit_connection_mapping dataset: columns dss_connection (a
                   connection used by datasets) and audit_connection (the admin connection of
                   databases that exports its tables). None: admin connections only

    Returns {result name: DataFrame}. The rows keep the order of the project list.
    """
    instance_variables = read_instance_variables(client)
    project_list = client.list_projects()

    workers = int(parallel)
    if workers < 1:
        workers = 1

    def read_one(project):
        return read_one_project(client, project, instance_variables, read_location)

    # Read the projects, up to `workers` at the same time. map() keeps their order.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(read_one, project_list))

    projects = []
    links = []
    datasets = []
    shares = []
    dss_locations = {}
    log_entries = []
    for outcome in outcomes:
        log_entries.append(outcome["log_entry"])
        if outcome["project_row"] is None:
            continue  # this project failed
        projects.append(outcome["project_row"])
        links.extend(outcome["links"])
        datasets.extend(outcome["datasets"])
        shares.extend(outcome["shares"])
        dss_locations.update(outcome["dss_locations"])

    connections = read_connections(client)
    locations = dataset_locations(datasets, connections, databases, connection_mapping, role_settings, dss_locations)

    return {
        "dss_projects": pd.DataFrame(projects),
        "dss_datasets": pd.DataFrame(datasets),
        "dss_dataset_shares": pd.DataFrame(shares, columns=SHARE_COLUMNS),
        "dss_flow_links": pd.DataFrame(links, columns=LINK_COLUMNS),
        "dss_connections": connections,
        "dss_dataset_locations": pd.DataFrame(locations, columns=LOCATION_COLUMNS),
        "inventory_log": pd.DataFrame(log_entries),
    }


def read_one_project(client, project, instance_variables, read_location):
    """Everything read from one project, as a dict: log_entry, project_row (None when the
    project failed), links, datasets, shares, dss_locations."""
    project_key = project["projectKey"]
    started = time.time()
    outcome = {
        "log_entry": {"project_key": project_key, "status": "OK", "error": None},
        "project_row": None,
        "links": [],
        "datasets": [],
        "shares": [],
        "dss_locations": {},
    }

    try:
        project_row, project_links, project_datasets, project_shares = read_project(client, project, instance_variables)
    except Exception as error:
        outcome["log_entry"]["status"] = "FAILED"
        outcome["log_entry"]["error"] = str(error)[:2000]  # first 2000 characters
        outcome["log_entry"]["seconds"] = round(time.time() - started, 1)
        print("FAILED %s: %s" % (project_key, error))
        return outcome

    if read_location is not None:
        for dataset in project_datasets:
            key = (project_key, dataset["dataset_name"])
            outcome["dss_locations"][key] = read_dss_location(read_location, project_key, dataset["dataset_name"])

    outcome["project_row"] = project_row
    outcome["links"] = project_links
    outcome["datasets"] = project_datasets
    outcome["shares"] = project_shares
    outcome["log_entry"]["datasets"] = len(project_datasets)
    outcome["log_entry"]["links"] = len(project_links)
    outcome["log_entry"]["seconds"] = round(time.time() - started, 1)
    return outcome


# ---------------------------------------------------------------------------------------
# One project
# ---------------------------------------------------------------------------------------

def read_project(client, project, instance_variables):
    """(project row, flow links, datasets, shares) of one project."""
    project_key = project["projectKey"]
    project_handle = client.get_project(project_key)

    tags = project.get("tags", [])
    project_use_case, use_case_source = use_case.project_use_case_and_source(project_key, tags)
    project_row = {
        "project_key": project_key,
        "project_name": project.get("name"),
        "project_tags": join_values(tags, keep_order=True),
        "use_case": project_use_case,
        "use_case_source": use_case_source,
    }

    variables = read_project_variables(project_handle, project_key, instance_variables)
    shared_with = read_exposed_datasets(project_handle)
    links = read_flow_links(project_handle, project_key)

    datasets = []
    shares = []
    for item in project_handle.list_datasets():
        raw = item._data
        dataset_name = raw["name"]
        params = raw.get("params", {})
        partitioning = raw.get("partitioning", {})
        dimensions = []
        for dimension in partitioning.get("dimensions", []):
            dimensions.append(dimension.get("name"))

        dataset = {
            "project_key": project_key,
            "dataset_name": dataset_name,
            "dataset_type": raw.get("type"),
            "managed": yes_no(raw.get("managed", False)),
            "partitioned": yes_no(len(dimensions) > 0),
            "partition_dimensions": join_values(dimensions, keep_order=True),
            "connection": first_value(params, ["connection"]),
            "mode": first_value(params, ["mode"]),
            "schema": first_value(params, ["schema"]),
            "table": first_value(params, ["table"]),
            "catalog": first_value(params, ["catalog"]),
            "path": first_value(params, ["path"]),
        }

        # Variables of the settings replaced (${projectKey}, project and instance variables)
        dataset["resolved_schema"] = substitute_variables(dataset["schema"], variables)
        dataset["resolved_table"] = substitute_variables(dataset["table"], variables)
        dataset["resolved_path"] = substitute_variables(dataset["path"], variables)

        # Sharing: one row per target project in dss_dataset_shares, the count here
        targets = shared_with.get(dataset_name, [])
        dataset["shared"] = yes_no(len(targets) > 0)
        dataset["n_shared_with"] = len(targets)
        for target in targets:
            shares.append({"project_key": project_key, "dataset_name": dataset_name, "target_project": target})

        # Metrics last computed by DSS
        partition = ""
        if dimensions:
            partition = "ALL"
        metrics = project_handle.get_dataset(dataset_name).get_last_metric_values(partition)
        raw_metrics = metrics.get_raw()
        dataset["metric_rows"] = metric_value(raw_metrics, METRIC_ROWS)
        size_bytes = metric_value(raw_metrics, METRIC_SIZE)
        dataset["metric_size"] = None
        if size_bytes is not None:
            dataset["metric_size"] = round(size_bytes / 1048576.0, 3)
        dataset["is_populated"] = is_populated(dataset["metric_rows"], dataset["metric_size"])

        datasets.append(dataset)

    return project_row, links, datasets, shares


def read_flow_links(project_handle, project_key):
    """One dict per recipe input or output, from the flow graph (which has no notebooks)."""
    nodes = project_handle.get_flow().get_graph().data["nodes"]

    links = []
    for node_id, node in nodes.items():
        if node["type"] != "RUNNABLE_RECIPE":
            continue

        recipe_name = node.get("ref", node_id)
        recipe_type = read_recipe_type(project_handle, recipe_name)

        for direction, neighbour_ids in (("input", node["predecessors"]), ("output", node["successors"])):
            for neighbour_id in neighbour_ids:
                neighbour = nodes.get(neighbour_id, {})
                if neighbour.get("type") != "COMPUTABLE_DATASET":
                    continue  # managed folder, saved model...
                dataset_project, dataset_name = dataset_reference(neighbour, neighbour_id, project_key)
                links.append({
                    "project_key": project_key,
                    "recipe_name": recipe_name,
                    "recipe_type": recipe_type,
                    "direction": direction,
                    "dataset_project": dataset_project,
                    "dataset_name": dataset_name,
                    "foreign": yes_no(dataset_project != project_key),
                })
    return links


def read_recipe_type(project_handle, recipe_name):
    """Type of a recipe (sync, python, join...), from its settings: the "type" field of the
    recipe's JSON definition. None when the settings cannot be read."""
    try:
        settings = project_handle.get_recipe(recipe_name).get_settings()
    except Exception as error:
        print("recipe %s: settings not readable: %s" % (recipe_name, error))
        return None
    return settings.type


def dataset_reference(node, node_id, project_key):
    """(project, dataset name) of a dataset node. A dataset of another project is written
    PROJECT.name, or carries its projectKey."""
    reference = node.get("ref", node_id)
    owner = node.get("projectKey")
    if owner and owner != project_key:
        if reference.startswith(owner + "."):
            reference = reference[len(owner) + 1:]
        return owner, reference
    if "." in reference:
        owner, name = reference.split(".", 1)
        return owner, name
    return project_key, reference


def read_exposed_datasets(project_handle):
    """{dataset name: [projects it is exposed to]}, from the project settings."""
    settings = project_handle.get_settings().get_raw()
    exposed = {}
    for exposed_object in settings.get("exposedObjects", {}).get("objects", []):
        if exposed_object.get("type") != "DATASET":
            continue
        targets = []
        for rule in exposed_object.get("rules", []):
            targets.append(rule.get("targetProject"))
        exposed[exposed_object.get("localName")] = targets
    return exposed


def read_project_variables(project_handle, project_key, instance_variables):
    """Variables usable in the dataset settings: instance, then project standard, then
    project local (each overrides the previous one), and projectKey."""
    variables = {}
    variables.update(instance_variables)
    project_variables = project_handle.get_variables()
    variables.update(project_variables.get("standard", {}))
    variables.update(project_variables.get("local", {}))
    variables["projectKey"] = project_key
    return variables


def read_instance_variables(client):
    """Instance variables (admin), as a plain dict."""
    instance_variables = client.get_global_variables()
    result = {}
    for name in instance_variables:
        result[name] = instance_variables[name]
    return result


def read_dss_location(read_location, project_key, dataset_name):
    """DSS's own location of a dataset: {"schema", "table"} or {"error"}."""
    try:
        location = read_location(project_key, dataset_name)
    except Exception as error:
        return {"error": str(error)[:200]}
    info = location.get("info", {})
    return {"schema": info.get("schema"), "table": info.get("table")}


# ---------------------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------------------

def read_connections(client):
    """One row per connection of the instance: where it points (no secret)."""
    rows = []
    for name in client.list_connections(as_type="dictitems"):
        try:
            info = client.get_connection(name).get_info()
        except Exception as error:
            rows.append({"connection": name, "error": str(error)[:500]})
            continue
        rows.append(connection_row(name, info))
    return pd.DataFrame(rows)


def connection_row(name, info):
    """Row of dss_connections from a connection info (DSSConnectionInfo or dict)."""
    # Resolved parameters (variables replaced) when DSS gives them, else the raw ones
    params = info.get("resolvedParams")
    if not params:
        params = info.get("params")
    if not params:
        params = {}
    credential = info.get("resolvedBasicCredential")
    if not credential:
        credential = {}

    driver_properties = {}
    for prop in params.get("properties", []):
        prop_name = prop.get("name", "")
        if is_secret(prop_name) or prop.get("secret"):
            continue
        driver_properties[prop_name] = prop.get("value")

    post_connect = params.get("postConnectStatementsExpandedAndSplit")
    if not post_connect and params.get("postConnectStatements"):
        post_connect = [params["postConnectStatements"]]

    row = {
        "connection": name,
        "connection_type": info.get("type"),
        "credentials_mode": info.get("credentialsMode"),
    }
    row.update(connection_target(params, credential))
    row["default_schema"] = first_value(params, ["defaultSchema", "schema"])
    row["driver_properties"] = None
    row["post_connect_statements"] = None
    row["naming_rule"] = None
    if driver_properties:
        row["driver_properties"] = json.dumps(driver_properties, sort_keys=True)
    if post_connect:
        row["post_connect_statements"] = json.dumps(post_connect)
    if params.get("namingRule"):
        row["naming_rule"] = json.dumps(params["namingRule"], sort_keys=True)
    return row


def connection_target(params, credential):
    """Where a connection points, from its separate fields and from its JDBC URL, and what
    DSS really uses.

    connection_mode   JDBC when "useURL" is ticked (DSS then uses the URL), else FIELDS
    field_*           host, port, database or service, user as typed in the fields
    jdbc_url          the JDBC URL as configured, unchanged. It can hold a password: the
                      dataset holding it must be restricted accordingly
    jdbc_*            the same parsed from the JDBC URL (dbmon.utils.jdbc_parser), its
                      vendor, and its parameters (jdbc_properties, secrets left out)
    effective_*       what DSS really uses: the JDBC values in JDBC mode, else the fields.
                      A JDBC URL typed into the host field is parsed and used too
    stale_fields      YES when the fields hold values that differ from what DSS uses
                      (fields left over before "useURL" was ticked, or a URL in the host)
    """
    target = {
        "connection_mode": "FIELDS",
        "field_host": first_value(params, ["host"]),
        "field_port": text_or_none(first_value(params, ["port"])),
        "field_database_or_service": first_value(params, ["db", "serviceName", "sid"]),
        "field_user": first_value(credential, ["user"]),
        "jdbc_url": None,
        "jdbc_vendor": None,
        "jdbc_host": None,
        "jdbc_port": None,
        "jdbc_database_or_service": None,
        "jdbc_user": None,
        "jdbc_properties": None,
        "effective_host": None,
        "effective_port": None,
        "effective_database_or_service": None,
        "effective_user": None,
        "stale_fields": "NO",
    }
    if target["field_user"] is None:
        target["field_user"] = first_value(params, ["user"])
    use_url = params.get("useURL")
    if use_url is True or str(use_url).lower() == "true":
        target["connection_mode"] = "JDBC"

    # The JDBC URL: in its own parameter, or typed into the host field by mistake
    url = first_value(params, ["url", "jdbcUrl"])
    host_is_url = False
    if target["field_host"] is not None and str(target["field_host"]).lower().startswith("jdbc:"):
        host_is_url = True
    if host_is_url:
        if url is None or target["connection_mode"] == "FIELDS":
            url = target["field_host"]

    if url is not None and str(url).lower().startswith("jdbc:"):
        jdbc = parse_jdbc(str(url))
        target["jdbc_url"] = jdbc.raw
        target["jdbc_vendor"] = jdbc.vendor
        target["jdbc_host"] = jdbc.host
        target["jdbc_port"] = text_or_none(jdbc.port)
        target["jdbc_database_or_service"] = jdbc.database
        target["jdbc_user"] = jdbc.user
        url_parameters = jdbc_url_parameters(jdbc.raw)
        if url_parameters:
            target["jdbc_properties"] = json.dumps(url_parameters, sort_keys=True)

    # What DSS really uses: the JDBC values in JDBC mode (or a URL in the host), else the fields
    use_jdbc = False
    if target["jdbc_host"] is not None:
        if target["connection_mode"] == "JDBC" or host_is_url:
            use_jdbc = True
    if use_jdbc:
        target["effective_host"] = target["jdbc_host"]
        target["effective_port"] = target["jdbc_port"]
        target["effective_database_or_service"] = target["jdbc_database_or_service"]
    else:
        target["effective_host"] = target["field_host"]
        target["effective_port"] = target["field_port"]
        target["effective_database_or_service"] = target["field_database_or_service"]

    # In DSS the user and password fields stay separate from the URL: the field user first
    target["effective_user"] = target["field_user"]
    if target["effective_user"] is None:
        target["effective_user"] = target["jdbc_user"]

    # Fields that hold something else than what DSS uses
    if host_is_url:
        target["stale_fields"] = "YES"
    elif use_jdbc:
        pairs = [("field_host", "jdbc_host"), ("field_port", "jdbc_port"),
                 ("field_database_or_service", "jdbc_database_or_service")]
        for field_column, jdbc_column in pairs:
            field_value = text_or_none(target[field_column])
            jdbc_value = text_or_none(target[jdbc_column])
            if field_value is not None and field_value != jdbc_value:
                target["stale_fields"] = "YES"

    return target


def jdbc_url_parameters(url):
    """Parameters written in a JDBC URL (?a=1&b=2 or ;a=1;b=2), secrets left out."""
    parameters = {}
    if url is None:
        return parameters
    parts = re.split(r"[?;&]", str(url))
    for part in parts[1:]:
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        if name == "" or is_secret(name):
            continue
        parameters[name] = value.strip()
    return parameters


def text_or_none(value):
    """Value as text, None when empty."""
    if value is None or value == "":
        return None
    return str(value)


def is_secret(name):
    lowered = name.lower()
    for word in SECRET_WORDS:
        if word in lowered:
            return True
    return False


# ---------------------------------------------------------------------------------------
# Candidate locations of the datasets
# ---------------------------------------------------------------------------------------

def dataset_locations(datasets, connections, databases, connection_mapping, role_settings, dss_locations):
    """Rows of dss_dataset_locations for every dataset (list of dicts).

    connection_name        the dataset's own connection (the real one)
    audit_connection_name  the admin connection whose step 1 export holds its tables, from
                           audit_connection_mapping. An admin connection of databases used by
                           a dataset is its own audit connection. Empty when unmapped
    database_label         the label of that admin connection in databases
    A connection mapped to several admin connections gives its candidates once per admin
    connection.

    candidate_status
      CANDIDATE   a candidate schema and table, to look up in the step 1 tables
      UNMAPPED    candidates exist, but the connection leads to no database
      PER_USER    the connection logs in as each DSS user: no candidate
      UNRESOLVED  a variable could not be replaced, or no login user: no candidate
      NO_TABLE    SQL query dataset: no table
      NOT_SQL     not stored in Oracle or PostgreSQL
    """
    connection_by_name = {}
    if not connections.empty:
        for row in connections.to_dict("records"):
            connection_by_name[row["connection"]] = without_missing(row)
    audits_by_connection = audit_connections(databases, connection_mapping)

    rows = []
    for dataset in datasets:
        key = (dataset["project_key"], dataset["dataset_name"])
        connection = connection_by_name.get(dataset.get("connection"), {})
        audits = audits_by_connection.get(dataset.get("connection"), [])
        dss_location = dss_locations.get(key)
        if not audits:
            rows.extend(locations_of(dataset, connection, None, None, role_settings, dss_location))
            continue
        for audit_connection, label in audits:
            rows.extend(locations_of(dataset, connection, audit_connection, label, role_settings, dss_location))
    return rows


def locations_of(dataset, connection, audit_connection, label, role_settings, dss_location):
    """Candidate rows of one dataset, for one of its audit connections."""
    base = {
        "project_key": dataset["project_key"],
        "dataset_name": dataset["dataset_name"],
        "candidate_rank": None,
        "candidate_status": None,
        "connection_name": dataset.get("connection"),
        "audit_connection_name": audit_connection,
        "database_label": label,
        "schema_name": None,
        "table_name": dataset.get("resolved_table"),
        "schema_source": None,
        "note": None,
    }

    engine = SQL_DATASET_TYPES.get(dataset.get("dataset_type"))
    if engine is None:
        return [without_candidate(base, "NOT_SQL", "not stored in Oracle or PostgreSQL")]
    if dataset.get("mode") == "query" or not dataset.get("resolved_table"):
        return [without_candidate(base, "NO_TABLE", "SQL query dataset, no table")]

    note = None
    if dss_location is not None and dss_location.get("error"):
        note = "DSS location not readable: %s" % dss_location["error"]

    # (schema_source, schema, table), best first
    candidates = []
    if dss_location is not None and dss_location.get("schema") and dss_location.get("table"):
        candidates.append(("DSS_LOCATION", dss_location["schema"], dss_location["table"]))

    if has_variable_left(dataset.get("resolved_table")) or has_variable_left(dataset.get("resolved_schema")):
        if not candidates:
            return [without_candidate(base, "UNRESOLVED", "variable that could not be replaced")]
    else:
        source, schemas, settings_note = schema_candidates(dataset, connection, engine, role_settings)
        if settings_note is not None and note is None:
            note = settings_note
        for schema in schemas:
            candidates.append((source, schema, dataset["resolved_table"]))
        if not candidates:
            return [without_candidate(base, source, note)]

    status = "CANDIDATE"
    if label is None:
        status = "UNMAPPED"
        note = "connection not in audit_connection_mapping, nor an admin connection of databases"

    rows = []
    rank = 0
    for source, schema, table in candidates:
        rank = rank + 1
        row = dict(base)
        row["candidate_rank"] = rank
        row["candidate_status"] = status
        row["schema_name"] = schema
        row["table_name"] = table
        row["schema_source"] = source
        row["note"] = note
        rows.append(row)
    return rows


def without_candidate(base, status, note):
    """The single row of a dataset without candidate schema."""
    row = dict(base)
    row["candidate_status"] = status
    if status in ("PER_USER", "UNRESOLVED"):
        row["schema_source"] = status
    row["note"] = note
    return row


def schema_candidates(dataset, connection, engine, role_settings):
    """(schema_source, candidate schemas, note) from the settings of a SQL dataset.
    No candidate: schema_source is PER_USER or UNRESOLVED."""
    if dataset.get("resolved_schema"):
        return "DATASET", [dataset["resolved_schema"]], None

    if connection.get("default_schema"):
        return "CONNECTION_DEFAULT", [connection["default_schema"]], None

    properties = {}
    if connection.get("driver_properties"):
        properties = json.loads(connection["driver_properties"])
    for prop_name in properties:
        if prop_name.lower() == "currentschema" and properties[prop_name]:
            return "JDBC_PROPERTY", split_schemas(properties[prop_name], None), None

    statements = []
    if connection.get("post_connect_statements"):
        statements = json.loads(connection["post_connect_statements"])
    for statement in statements:
        oracle_match = ORACLE_CURRENT_SCHEMA.search(statement)
        if oracle_match:
            return "POST_CONNECT", [oracle_match.group(1)], None
        postgres_match = POSTGRES_SEARCH_PATH.search(statement)
        if postgres_match:
            return "POST_CONNECT", split_schemas(postgres_match.group(1), connection.get("effective_user")), None
    note = None
    if statements:
        note = "post-connect statements without a recognised schema setting"

    if connection.get("credentials_mode") == "PER_USER":
        return "PER_USER", [], "connection logs in as each DSS user"

    login_user = connection.get("effective_user")
    if not login_user:
        return "UNRESOLVED", [], "no login user on the connection"

    if engine == "oracle":
        # An unquoted Oracle user name is stored in upper case
        return "LOGIN_USER", [login_user.upper()], note

    search_path = user_search_path(login_user, connection.get("effective_database_or_service"), role_settings)
    return "SEARCH_PATH", split_schemas(search_path, login_user), note


def user_search_path(login_user, database, role_settings):
    """search_path of a PostgreSQL role in a database, from the most to the least precise
    setting, as PostgreSQL applies them. Default: "$user", public."""
    if role_settings is None or role_settings.empty:
        return '"$user", public'

    precise_first = [(login_user, database), (login_user, None), (None, database)]
    for role, setting_database in precise_first:
        for row in role_settings.to_dict("records"):
            row = without_missing(row)
            if row.get("role_name") == role and row.get("setting_database") == setting_database:
                return row["search_path"]
    return '"$user", public'


def split_schemas(search_path, login_user):
    """Schemas of a search_path, in order. "$user" becomes the login user."""
    schemas = []
    for part in search_path.split(","):
        schema = part.strip().strip('"').strip("'")
        if schema == "":
            continue
        if schema == "$user":
            if login_user is None:
                continue
            schema = login_user
        schemas.append(schema)
    return schemas


def audit_connections(databases, connection_mapping):
    """{DSS connection: [(audit connection, database label)]}.

    An admin connection of databases is its own audit connection, with the label of its row
    (the connection name when the label is empty). A connection of audit_connection_mapping
    leads to the admin connections it is mapped to.
    """
    label_of_admin = {}
    for row in databases.to_dict("records"):
        row = without_missing(row)
        label = row.get("label")
        if label is None or str(label).strip() == "":
            label = row["connection"]
        label_of_admin[row["connection"]] = label

    audits = {}
    for admin_connection in label_of_admin:
        audits[admin_connection] = [(admin_connection, label_of_admin[admin_connection])]

    if connection_mapping is None or connection_mapping.empty:
        return audits
    for row in connection_mapping.to_dict("records"):
        row = without_missing(row)
        connection = row.get("dss_connection")
        admin_connection = row.get("audit_connection")
        if connection is None or admin_connection is None:
            continue
        admin_connection = str(admin_connection).strip()
        label = label_of_admin.get(admin_connection)
        if label is None:
            continue  # mapped to a connection that databases does not list
        connection = str(connection).strip()
        if connection not in audits:
            audits[connection] = []
        if (admin_connection, label) not in audits[connection]:
            audits[connection].append((admin_connection, label))
    return audits


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

def metric_value(raw_metrics, metric_id):
    """Last value of a metric: the whole-dataset value (NP or ALL), else the sum of the
    partition values. None when the metric was never computed."""
    for metric in raw_metrics.get("metrics", []):
        if metric.get("metric", {}).get("id") != metric_id:
            continue
        values = metric.get("lastValues", [])
        for value in values:
            if value.get("partition") in ("NP", "ALL"):
                return to_number(value.get("value"))
        total = None
        for value in values:
            number = to_number(value.get("value"))
            if number is None:
                continue
            if total is None:
                total = 0
            total = total + number
        return total
    return None


def is_populated(metric_rows, metric_size):
    """YES, NO, or UNKNOWN when DSS never computed the metrics."""
    if metric_rows is not None:
        if metric_rows > 0:
            return "YES"
        return "NO"
    if metric_size is not None:
        if metric_size > 0:
            return "YES"
        return "NO"
    return "UNKNOWN"


def substitute_variables(text, variables):
    """Replace ${name} by its value. Unknown variables are left as they are."""
    if text is None:
        return None

    def replace(match):
        name = match.group(1)
        if name in variables:
            return str(variables[name])
        return match.group(0)

    return VARIABLE.sub(replace, text)


def has_variable_left(text):
    return text is not None and "${" in text


def join_values(values, keep_order=False):
    """Values comma separated (sorted unless keep_order). None when there is none."""
    texts = []
    for value in values:
        if value is not None and value != "":
            texts.append(str(value))
    if not texts:
        return None
    if not keep_order:
        texts = sorted(set(texts))
    return ",".join(texts)


def first_value(params, names):
    """First non-empty value among params[name] for the names, in order. None if none."""
    for name in names:
        value = params.get(name)
        if value is not None and value != "":
            return value
    return None


def without_missing(record):
    """Copy of a DataFrame record with the missing values (NaN) as None."""
    cleaned = {}
    for name, value in record.items():
        if not isinstance(value, (list, dict)) and pd.isna(value):
            cleaned[name] = None
        else:
            cleaned[name] = value
    return cleaned


def to_number(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def yes_no(flag):
    if flag:
        return "YES"
    return "NO"
