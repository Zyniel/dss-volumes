"""Inventory of the DSS datasets involved in recipes, and of where they are stored.

Read through the DSS public API, with an admin API key. Notebooks are not read.

Results:
- dss_flow_links   one row per recipe input or output (the flow graph of every project)
- dss_datasets     one row per dataset: settings, storage, flow role, sharing, metrics
- dss_connections  one row per connection: where it points
- inventory_log    one row per project: OK or FAILED

Flow role of a dataset, over the recipes of every project:
- target   output of at least one recipe
- source   input of at least one recipe, output of none
- orphan   neither input nor output of any recipe

Where a SQL dataset points to: the schema is resolved in this order, the first one that
gives a value wins (schema_source):
1. DATASET             the dataset's own schema setting
2. CONNECTION_DEFAULT  the default schema of the connection
3. JDBC_PROPERTY       the JDBC property currentSchema (PostgreSQL)
4. POST_CONNECT        ALTER SESSION SET CURRENT_SCHEMA / SET search_path in the
                       connection's post-connect statements
5. PER_USER            the connection logs in as each DSS user: cannot be told
6. LOGIN_USER          Oracle: the schema of the login user
   SEARCH_PATH         PostgreSQL: the search_path of the login user (role_settings),
                       by default "$user", public
The candidate schemas are then checked against the tables collected in step 1.
"""
import json
import re

import pandas as pd

from dbmon import use_case

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


# ---------------------------------------------------------------------------------------
# All projects
# ---------------------------------------------------------------------------------------

def collect_inventory(client, databases, tables, role_settings, read_location=None):
    """Read every project of the DSS instance.

    client         dataikuapi client with an admin API key (dataiku.api_client() in DSS)
    databases      the databases dataset: maps DSS connections to the monitored databases
    tables         the tables collected in step 1 (one snapshot)
    role_settings  the role settings collected in step 1 (one snapshot)
    read_location  optional function (project_key, dataset_name) -> location info dict,
                   DSS's own resolved location (dataiku.Dataset.get_location_info). None:
                   the location is resolved from the settings only

    Returns {result name: DataFrame}.
    """
    instance_variables = read_instance_variables(client)

    links = []
    datasets = []
    log_entries = []
    for project in client.list_projects():
        project_key = project["projectKey"]
        log_entry = {"project_key": project_key, "status": "OK", "error": None}
        try:
            project_links, project_datasets = read_project(client, project, instance_variables, read_location)
        except Exception as error:
            log_entry["status"] = "FAILED"
            log_entry["error"] = str(error)[:2000]  # first 2000 characters
            print("FAILED %s: %s" % (project_key, error))
        else:
            links.extend(project_links)
            datasets.extend(project_datasets)
            log_entry["datasets"] = len(project_datasets)
            log_entry["links"] = len(project_links)
        log_entries.append(log_entry)

    links = pd.DataFrame(links, columns=LINK_COLUMNS)
    datasets = pd.DataFrame(datasets)
    connections = read_connections(client)

    if not datasets.empty:
        add_flow_roles(datasets, links)
        add_locations(datasets, connections, databases, tables, role_settings)

    return {
        "dss_flow_links": links,
        "dss_datasets": datasets,
        "dss_connections": connections,
        "inventory_log": pd.DataFrame(log_entries),
    }


# ---------------------------------------------------------------------------------------
# One project
# ---------------------------------------------------------------------------------------

LINK_COLUMNS = ["project_key", "recipe_name", "recipe_type", "direction",
                "dataset_project", "dataset_name", "foreign"]


def read_project(client, project, instance_variables, read_location):
    """Flow links and datasets of one project. Returns (links, datasets), lists of dicts."""
    project_key = project["projectKey"]
    project_handle = client.get_project(project_key)

    tags = project.get("tags", [])
    project_use_case = use_case.project_use_case(project_key, tags)
    variables = read_project_variables(project_handle, project_key, instance_variables)
    shared_with = read_exposed_datasets(project_handle)
    links = read_flow_links(project_handle, project_key)

    datasets = []
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
            "project_tags": ",".join(tags),
            "use_case": project_use_case,
            "dataset_name": dataset_name,
            "dataset_type": raw.get("type"),
            "managed": yes_no(raw.get("managed", False)),
            "partitioned": yes_no(len(dimensions) > 0),
            "partition_dimensions": join_values(dimensions),
            "connection": params.get("connection"),
            "mode": params.get("mode"),
            "schema": first_value(params, ["schema"]),
            "table": first_value(params, ["table"]),
            "catalog": first_value(params, ["catalog"]),
            "path": first_value(params, ["path"]),
        }

        # Variables of the settings replaced (${projectKey}, project and instance variables)
        dataset["resolved_schema"] = substitute_variables(dataset["schema"], variables)
        dataset["resolved_table"] = substitute_variables(dataset["table"], variables)
        dataset["resolved_path"] = substitute_variables(dataset["path"], variables)

        # DSS's own resolved location, when it can be read
        if read_location is not None:
            add_dss_location(dataset, read_location)

        # Sharing
        targets = shared_with.get(dataset_name, [])
        dataset["shared"] = yes_no(len(targets) > 0)
        dataset["shared_with"] = join_values(targets)

        # Metrics last computed by DSS
        metrics = project_handle.get_dataset(dataset_name).get_last_metric_values("ALL" if dimensions else "")
        dataset["metric_rows"] = metric_value(metrics.get_raw(), METRIC_ROWS)
        size_bytes = metric_value(metrics.get_raw(), METRIC_SIZE)
        dataset["metric_size"] = None
        if size_bytes is not None:
            dataset["metric_size"] = round(size_bytes / 1048576.0, 3)
        dataset["is_populated"] = is_populated(dataset["metric_rows"], dataset["metric_size"])

        datasets.append(dataset)

    return links, datasets


def read_flow_links(project_handle, project_key):
    """One dict per recipe input or output, from the flow graph (which has no notebooks)."""
    nodes = project_handle.get_flow().get_graph().data["nodes"]

    links = []
    for node_id, node in nodes.items():
        if node["type"] != "RUNNABLE_RECIPE":
            continue

        recipe_name = node.get("ref", node_id)
        recipe_type = node.get("subType")

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


def dataset_reference(node, node_id, project_key):
    """(project, dataset name) of a dataset node. A dataset of another project is written
    PROJECT.name, or carries its projectKey."""
    reference = node.get("ref", node_id)
    if node.get("projectKey") and node["projectKey"] != project_key:
        if reference.startswith(node["projectKey"] + "."):
            reference = reference[len(node["projectKey"]) + 1:]
        return node["projectKey"], reference
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


def add_dss_location(dataset, read_location):
    """Overwrite the resolved schema and table with DSS's own location, when DSS gives one."""
    try:
        location = read_location(dataset["project_key"], dataset["dataset_name"])
    except Exception as error:
        dataset["location_note"] = "DSS location not readable: %s" % str(error)[:200]
        return

    info = location.get("info", {})
    if info.get("schema"):
        dataset["resolved_schema"] = info["schema"]
        dataset["schema_source"] = "DSS_LOCATION"
    if info.get("table"):
        dataset["resolved_table"] = info["table"]
    if info.get("path"):
        dataset["resolved_path"] = info["path"]


# ---------------------------------------------------------------------------------------
# Flow roles
# ---------------------------------------------------------------------------------------

def add_flow_roles(datasets, links):
    """Add flow_role, n_producers, n_consumers and used_by_projects to datasets (in place)."""
    producers = {}
    consumers = {}
    consuming_projects = {}
    for link in links.itertuples(index=False):
        key = (link.dataset_project, link.dataset_name)
        if link.direction == "output":
            if key not in producers:
                producers[key] = 0
            producers[key] = producers[key] + 1
        else:
            if key not in consumers:
                consumers[key] = 0
            consumers[key] = consumers[key] + 1
            if link.project_key != link.dataset_project:
                if key not in consuming_projects:
                    consuming_projects[key] = set()
                consuming_projects[key].add(link.project_key)

    roles = []
    n_producers = []
    n_consumers = []
    used_by = []
    for dataset in datasets.itertuples(index=False):
        key = (dataset.project_key, dataset.dataset_name)
        produced = producers.get(key, 0)
        consumed = consumers.get(key, 0)
        if produced > 0:
            roles.append("target")
        elif consumed > 0:
            roles.append("source")
        else:
            roles.append("orphan")
        n_producers.append(produced)
        n_consumers.append(consumed)
        used_by.append(join_values(consuming_projects.get(key, set())))

    datasets["flow_role"] = roles
    datasets["n_producers"] = n_producers
    datasets["n_consumers"] = n_consumers
    datasets["used_by_projects"] = used_by


# ---------------------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------------------

def read_connections(client):
    """One row per connection of the instance: where it points (no secret)."""
    rows = []
    for name in client.list_connections(as_type="dictitems"):
        row = {"connection": name}
        try:
            info = client.get_connection(name).get_info()
        except Exception as error:
            row["error"] = str(error)[:500]
            rows.append(row)
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

    login_user = first_value(credential, ["user"])
    if login_user is None:
        login_user = first_value(params, ["user"])
    properties = {}
    for prop in params.get("properties", []):
        prop_name = prop.get("name", "")
        if is_secret(prop_name) or prop.get("secret"):
            continue
        properties[prop_name] = prop.get("value")

    post_connect = params.get("postConnectStatementsExpandedAndSplit")
    if not post_connect and params.get("postConnectStatements"):
        post_connect = [params["postConnectStatements"]]

    return {
        "connection": name,
        "connection_type": info.get("type"),
        "credentials_mode": info.get("credentialsMode"),
        "host": params.get("host"),
        "port": params.get("port"),
        "database_or_service": first_value(params, ["db", "serviceName", "sid"]),
        "login_user": login_user,
        "default_schema": first_value(params, ["defaultSchema", "schema"]),
        "jdbc_properties": json.dumps(properties, sort_keys=True) if properties else None,
        "post_connect_statements": json.dumps(post_connect) if post_connect else None,
        "naming_rule": json.dumps(params.get("namingRule"), sort_keys=True) if params.get("namingRule") else None,
    }


def is_secret(name):
    lowered = name.lower()
    for word in SECRET_WORDS:
        if word in lowered:
            return True
    return False


# ---------------------------------------------------------------------------------------
# Where a dataset points to
# ---------------------------------------------------------------------------------------

def add_locations(datasets, connections, databases, tables, role_settings):
    """Add database_label, schema_source, schema_candidates, location_status and
    location_note to datasets (in place)."""
    connection_by_name = {}
    for row in connections.to_dict("records"):
        connection_by_name[row["connection"]] = without_missing(row)

    label_by_connection = database_labels(databases)
    table_index = index_tables(tables)

    new_columns = {"database_label": [], "schema_source": [], "schema_candidates": [],
                   "location_status": [], "location_note": [], "resolved_schema": []}
    for dataset in datasets.to_dict("records"):
        dataset = without_missing(dataset)
        result = locate(dataset, connection_by_name, label_by_connection, table_index, role_settings)
        for column in new_columns:
            new_columns[column].append(result[column])

    for column in new_columns:
        datasets[column] = new_columns[column]


def locate(dataset, connection_by_name, label_by_connection, table_index, role_settings):
    """Where one dataset points to. Returns the values of the location columns."""
    result = {
        "database_label": None,
        "schema_source": dataset.get("schema_source"),
        "schema_candidates": None,
        "location_status": None,
        "location_note": dataset.get("location_note"),
        "resolved_schema": dataset.get("resolved_schema"),
    }

    engine = SQL_DATASET_TYPES.get(dataset.get("dataset_type"))
    if engine is None:
        result["location_status"] = "NOT_SQL"
        return result
    if dataset.get("mode") == "query" or not dataset.get("resolved_table"):
        result["location_status"] = "NO_TABLE"
        result["location_note"] = "SQL query dataset, no table"
        return result
    has_variable_left = False
    for value in (dataset.get("resolved_table"), dataset.get("resolved_schema")):
        if value is not None and "${" in value:
            has_variable_left = True
    if has_variable_left:
        result["location_status"] = "UNRESOLVED"
        result["location_note"] = "variable that could not be replaced"
        return result

    connection = connection_by_name.get(dataset.get("connection"), {})
    result["database_label"] = label_by_connection.get(dataset.get("connection"))

    # The candidate schemas, in the order the database would try them
    if result["schema_source"] == "DSS_LOCATION":
        candidates = [result["resolved_schema"]]
    else:
        source, candidates, note = schema_candidates(dataset, connection, engine, role_settings)
        result["schema_source"] = source
        if note:
            result["location_note"] = note
    result["schema_candidates"] = join_values(candidates, keep_order=True)

    if not candidates:
        result["location_status"] = "UNRESOLVED"
        return result
    if result["database_label"] is None:
        result["location_status"] = "UNMAPPED"
        result["location_note"] = "connection not listed in the databases dataset"
        result["resolved_schema"] = candidates[0]
        return result

    # Check the candidates against the tables of the database
    status, schema = find_table(result["database_label"], candidates, dataset["resolved_table"], table_index)
    result["location_status"] = status
    if schema is not None:
        result["resolved_schema"] = schema
    else:
        result["resolved_schema"] = candidates[0]
    return result


def schema_candidates(dataset, connection, engine, role_settings):
    """(schema_source, candidate schemas, note) for a SQL dataset."""
    if dataset.get("resolved_schema"):
        return "DATASET", [dataset["resolved_schema"]], None

    if connection.get("default_schema"):
        return "CONNECTION_DEFAULT", [connection["default_schema"]], None

    properties = {}
    if connection.get("jdbc_properties"):
        properties = json.loads(connection["jdbc_properties"])
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
            return "POST_CONNECT", split_schemas(postgres_match.group(1), connection.get("login_user")), None
    note = None
    if statements:
        note = "post-connect statements without a recognised schema setting"

    if connection.get("credentials_mode") == "PER_USER":
        return "PER_USER", [], "connection logs in as each DSS user"

    login_user = connection.get("login_user")
    if not login_user:
        return "UNRESOLVED", [], "no login user on the connection"

    if engine == "oracle":
        # An unquoted Oracle user name is stored in upper case
        return "LOGIN_USER", [login_user.upper()], note

    search_path = user_search_path(login_user, connection.get("database_or_service"), role_settings)
    return "SEARCH_PATH", split_schemas(search_path, login_user), note


def user_search_path(login_user, database, role_settings):
    """search_path of a PostgreSQL role in a database, from the most to the least precise
    setting, as PostgreSQL applies them. Default: "$user", public."""
    if role_settings is None or role_settings.empty:
        return '"$user", public'

    precise_first = [(login_user, database), (login_user, None), (None, database)]
    for role, setting_database in precise_first:
        for row in role_settings.to_dict("records"):
            if same_or_both_empty(row.get("role_name"), role) and same_or_both_empty(row.get("setting_database"), setting_database):
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


def find_table(database_label, candidates, table_name, table_index):
    """(status, schema) of the first candidate schema holding the table: exact names first,
    then ignoring case. AMBIGUOUS when ignoring case matches several tables."""
    for schema in candidates:
        if (database_label, schema, table_name) in table_index["exact"]:
            return "FOUND", schema

    for schema in candidates:
        key = (database_label, schema.lower(), table_name.lower())
        matches = table_index["lower"].get(key, [])
        if len(matches) == 1:
            return "FOUND", matches[0]
        if len(matches) > 1:
            return "AMBIGUOUS", None
    return "NOT_FOUND", None


def index_tables(tables):
    """Lookups of the step 1 tables, by (label, schema, table): exact, and lower case."""
    exact = set()
    lower = {}
    if tables is None or tables.empty:
        return {"exact": exact, "lower": lower}
    for row in tables[["label", "schema_name", "table_name"]].itertuples(index=False):
        exact.add((row.label, row.schema_name, row.table_name))
        key = (row.label, str(row.schema_name).lower(), str(row.table_name).lower())
        if key not in lower:
            lower[key] = []
        lower[key].append(row.schema_name)
    return {"exact": exact, "lower": lower}


def database_labels(databases):
    """{DSS connection: database label}. Each row of databases maps its admin connection,
    and the connections listed in its optional column dss_connections (comma separated)."""
    labels = {}
    for row in databases.to_dict("records"):
        label = row.get("label")
        if label is None or pd.isna(label) or str(label).strip() == "":
            label = row["connection"]
        labels[row["connection"]] = label
        listed = row.get("dss_connections")
        if listed is None or pd.isna(listed):
            continue
        for name in str(listed).split(","):
            if name.strip():
                labels[name.strip()] = label
    return labels


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


def same_or_both_empty(value, expected):
    if expected is None:
        return value is None or pd.isna(value)
    return value == expected


def yes_no(flag):
    if flag:
        return "YES"
    return "NO"
