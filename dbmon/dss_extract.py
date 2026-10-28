"""DSS raw extraction: what DSS knows, read through the public API with an admin API key.
Needs nothing else: neither the database extraction nor any mapping. Notebooks are not read.

Results, one dataset each, close to what the API returns:
- dss_projects                one row per project
- dss_project_tags            one row per project and tag (Category:Value tags split)
- dss_datasets                one row per dataset: settings, storage as configured, metrics
- dss_managed_folders         one row per managed folder: settings, metrics
- dss_dataset_shares          one row per shared dataset and project it is exposed to
- dss_flow_links              one row per recipe input or output, dataset or managed folder
- dss_connections             one row per connection: where it points
- dss_connection_permissions  one row per connection, permission and allowed group
- dss_groups                  one row per group
- dss_group_members           one row per group and user
- dss_users                   one row per user (no email)
- inventory_log               one row per project: OK or FAILED

Several instances: extract_instances reads each DSS instance of the instances dataset
(Design, Automation nodes...) and puts its name in dss_instance, in front of every row. An instance that cannot
be read gives one FAILED row in inventory_log (project_key empty); the others are still read.

Calls are kept low: lists first (projects, connections, users, groups: one call per run;
datasets, recipes, managed folders, flow graph, settings: one call per project). Per object:
the metrics (one call per dataset or folder, see METRICS), connection details only for the
connections whose parameters hold variables or no user, project variables only for the
projects that need them.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from dbmon import progress
from dbmon.utils.jdbc_parser import parse_jdbc

METRIC_ROWS = "records:COUNT_RECORDS"
METRIC_SIZE = "basic:SIZE"
METRIC_FILES = "basic:COUNT_FILES"

# Which objects get their last computed metrics read (one call each)
METRICS_ALL = "ALL"      # every dataset and managed folder
METRICS_NONE = "NONE"    # none

# Parameters never copied out of a connection (the raw JDBC URL is kept on purpose)
SECRET_WORDS = ("password", "secret", "token", "credential", "key")

VARIABLE = re.compile(r"\$\{([^}]+)\}")

LINK_COLUMNS = ["project_key", "recipe_name", "recipe_type", "direction",
                "object_type", "object_project", "object_name", "foreign"]
SHARE_COLUMNS = ["project_key", "dataset_name", "target_project"]
TAG_COLUMNS = ["project_key", "tag", "tag_category", "tag_value"]
PERMISSION_COLUMNS = ["connection", "permission", "scope", "group_name"]
MEMBER_COLUMNS = ["group_name", "login"]


# ---------------------------------------------------------------------------------------
# Several instances
# ---------------------------------------------------------------------------------------

RESULT_NAMES = ["dss_projects", "dss_project_tags", "dss_datasets", "dss_managed_folders", "dss_dataset_shares",
                "dss_flow_links", "dss_connections", "dss_connection_permissions", "dss_groups",
                "dss_group_members", "dss_users", "inventory_log"]


def connect_instances(instance_rows, get_admin_client, local_client):
    """[{"name", "client"} or {"name", "error"}], one per row of the instances dataset.

    instance_rows     rows of instances (dicts), with at least instance_name. Rows without a
                      name are skipped
    get_admin_client  function instance_name -> admin client of that instance (in DSS:
                      com.arval.cdplib.client.get_remote_admin_client)
    local_client      client of the instance the recipe runs on (dataiku.api_client()), used
                      only when instances has no row: that instance is then named "local"
    An instance whose client cannot be made gets its error; the others are still read.
    """
    instances = []
    for row in instance_rows:
        name = cell_text(row.get("instance_name"))
        if name is None:
            continue
        try:
            client = get_admin_client(name)
        except Exception as error:
            instances.append({"name": name, "error": str(error)[:500]})
            continue
        instances.append({"name": name, "client": client})

    if not instances:
        return [{"name": "local", "client": local_client}]
    return instances


def extract_instances(instances, parallel=4, metrics=None):
    """Read every instance of connect_instances. Returns {result name: DataFrame}, every row
    starting with dss_instance. An instance that fails gives one FAILED row in inventory_log."""
    if metrics is None:
        metrics = METRICS_ALL
    frames = {}
    for name in RESULT_NAMES:
        frames[name] = []

    for instance in instances:
        name = instance["name"]
        progress.log("instance %s: started" % name)
        if "error" in instance:
            results = {"inventory_log": instance_failure(instance["error"])}
        else:
            try:
                results = extract_dss(instance["client"], parallel=parallel, metrics=metrics)
            except Exception as error:
                results = {"inventory_log": instance_failure(str(error)[:2000])}
        if "dss_projects" not in results:
            progress.log("instance %s: FAILED: %s" % (name, results["inventory_log"]["error"].iloc[0]))
        for result_name in results:
            rows = results[result_name]
            rows.insert(0, "dss_instance", name)
            frames[result_name].append(rows)

    combined = {}
    for name in RESULT_NAMES:
        if frames[name]:
            combined[name] = pd.concat(frames[name], ignore_index=True)
        else:
            combined[name] = pd.DataFrame()
    return combined


def instance_failure(error):
    """inventory_log of an instance that could not be read: one FAILED row."""
    return pd.DataFrame([{"project_key": None, "status": "FAILED", "error": error}])


# ---------------------------------------------------------------------------------------
# Whole instance
# ---------------------------------------------------------------------------------------

def extract_dss(client, parallel=4, metrics=METRICS_ALL):
    """Read the whole DSS instance. Returns {result name: DataFrame}.

    client    dataikuapi client with an admin API key (dataiku.api_client() in DSS)
    parallel  projects read at the same time. 1: one after the other
    metrics   ALL: read the last computed metrics of every dataset and folder. NONE: none
    """
    started = time.time()
    instance_variables = read_instance_variables(client)
    project_list = client.list_projects()

    workers = int(parallel)
    if workers < 1:
        workers = 1
    progress.log("DSS extraction: %d projects, %d at the same time, metrics %s" % (len(project_list), workers, metrics))
    counter = progress.Counter(len(project_list))

    def read_one(project):
        return read_one_project(client, project, instance_variables, metrics, counter)

    # Read the projects, up to `workers` at the same time. map() keeps their order.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(read_one, project_list))

    results = {
        "dss_projects": [], "dss_project_tags": [], "dss_datasets": [], "dss_managed_folders": [],
        "dss_dataset_shares": [], "dss_flow_links": [],
    }
    log_entries = []
    for outcome in outcomes:
        log_entries.append(outcome["log_entry"])
        for name in results:
            results[name].extend(outcome[name])

    n_failed = 0
    for entry in log_entries:
        if entry["status"] == "FAILED":
            n_failed = n_failed + 1
    progress.log("projects done: %d read, %d failed, %d datasets, %d folders, %d links in %s s"
                 % (len(log_entries) - n_failed, n_failed, len(results["dss_datasets"]),
                    len(results["dss_managed_folders"]), len(results["dss_flow_links"]), seconds_since(started)))

    connections, permissions = read_connections(client)
    groups = read_groups(client)
    users, members = read_users(client)
    progress.log("security: %d groups, %d users, %d group memberships" % (len(groups), len(users), len(members)))
    progress.log("DSS extraction done in %s s" % seconds_since(started))

    return {
        "dss_projects": pd.DataFrame(results["dss_projects"]),
        "dss_project_tags": pd.DataFrame(results["dss_project_tags"], columns=TAG_COLUMNS),
        "dss_datasets": pd.DataFrame(results["dss_datasets"]),
        "dss_managed_folders": pd.DataFrame(results["dss_managed_folders"]),
        "dss_dataset_shares": pd.DataFrame(results["dss_dataset_shares"], columns=SHARE_COLUMNS),
        "dss_flow_links": pd.DataFrame(results["dss_flow_links"], columns=LINK_COLUMNS),
        "dss_connections": connections,
        "dss_connection_permissions": permissions,
        "dss_groups": groups,
        "dss_group_members": members,
        "dss_users": users,
        "inventory_log": pd.DataFrame(log_entries),
    }


# ---------------------------------------------------------------------------------------
# One project
# ---------------------------------------------------------------------------------------

def read_one_project(client, project, instance_variables, metrics, counter=None):
    """Everything read from one project, as a dict of row lists, plus its log entry.
    counter: progress.Counter of the projects done, for the progress traces."""
    project_key = project["projectKey"]
    started = time.time()
    outcome = {
        "log_entry": {"project_key": project_key, "status": "OK", "error": None},
        "dss_projects": [], "dss_project_tags": [], "dss_datasets": [], "dss_managed_folders": [],
        "dss_dataset_shares": [], "dss_flow_links": [],
    }
    try:
        read_project(client, project, instance_variables, metrics, outcome)
    except Exception as error:
        for name in outcome:
            if name != "log_entry":
                outcome[name] = []
        outcome["log_entry"]["status"] = "FAILED"
        outcome["log_entry"]["error"] = str(error)[:2000]  # first 2000 characters
    else:
        outcome["log_entry"]["datasets"] = len(outcome["dss_datasets"])
        outcome["log_entry"]["managed_folders"] = len(outcome["dss_managed_folders"])
        outcome["log_entry"]["links"] = len(outcome["dss_flow_links"])
        metric_errors = 0
        for row in outcome["dss_datasets"] + outcome["dss_managed_folders"]:
            if row["metrics_error"] is not None:
                metric_errors = metric_errors + 1
        outcome["log_entry"]["metric_errors"] = metric_errors
    outcome["log_entry"]["seconds"] = seconds_since(started)

    done = ""
    if counter is not None:
        done = " " + counter.next()
    entry = outcome["log_entry"]
    if entry["status"] == "FAILED":
        progress.log("project%s %s: FAILED in %s s: %s" % (done, project_key, entry["seconds"], entry["error"]))
    else:
        message = "project%s %s: OK, %d datasets, %d folders, %d links in %s s" % (
            done, project_key, entry["datasets"], entry["managed_folders"], entry["links"], entry["seconds"])
        if entry["metric_errors"] > 0:
            message = message + ", metrics not readable for %d of them" % entry["metric_errors"]
        progress.log(message)
    return outcome


def read_project(client, project, instance_variables, metrics, outcome):
    """Fill outcome with the rows of one project."""
    project_key = project["projectKey"]
    project_handle = client.get_project(project_key)

    # Project and tags: from the project list, no extra call
    tags = project.get("tags", [])
    outcome["dss_projects"].append({
        "project_key": project_key,
        "project_name": project.get("name"),
        "owner_login": project.get("ownerLogin"),
    })
    for tag in tags:
        outcome["dss_project_tags"].append(tag_row(project_key, tag))

    # Lists: one call each
    dataset_items = project_handle.list_datasets()
    recipe_items = project_handle.list_recipes()
    folder_items = project_handle.list_managed_folders()
    nodes = project_handle.get_flow().get_graph().data["nodes"]
    shared_with = read_exposed_datasets(project_handle)
    progress.log("project %s: %d datasets, %d recipes, %d folders listed, reading their details"
                 % (project_key, len(dataset_items), len(recipe_items), len(folder_items)))

    # Variables: only when a dataset or folder uses one other than projectKey
    raw_datasets = []
    for item in dataset_items:
        raw_datasets.append(raw_of(item))
    raw_folders = []
    for item in folder_items:
        raw_folders.append(raw_of(item))
    variables = {"projectKey": project_key}
    if needs_project_variables(raw_datasets + raw_folders):
        variables = read_project_variables(project_handle, project_key, instance_variables)

    # Recipe types: from the recipe list (the "type" field of each recipe definition)
    recipe_types = {}
    for item in recipe_items:
        raw = raw_of(item)
        recipe_types[raw.get("name")] = raw.get("type")

    outcome["dss_flow_links"].extend(flow_links(nodes, project_key, recipe_types))

    for raw in raw_datasets:
        dataset = dataset_row(raw, project_key, variables)
        targets = shared_with.get(dataset["dataset_name"], [])
        dataset["shared"] = yes_no(len(targets) > 0)
        dataset["n_shared_with"] = len(targets)
        for target in targets:
            outcome["dss_dataset_shares"].append(
                {"project_key": project_key, "dataset_name": dataset["dataset_name"], "target_project": target})
        raw_metrics = {}
        dataset["metrics_error"] = None
        if metrics == METRICS_ALL:
            raw_metrics, dataset["metrics_error"] = read_dataset_metrics(project_handle, dataset)
        add_metrics(dataset, raw_metrics)
        outcome["dss_datasets"].append(dataset)

    for raw in raw_folders:
        folder = folder_row(raw, project_key, variables)
        raw_metrics = {}
        folder["metrics_error"] = None
        if metrics == METRICS_ALL:
            raw_metrics, folder["metrics_error"] = read_folder_metrics(project_handle, folder)
        add_metrics(folder, raw_metrics)
        del folder["metric_rows"]  # a folder holds files, not records
        outcome["dss_managed_folders"].append(folder)


def read_dataset_metrics(project_handle, dataset):
    """(last computed metrics, error) of a dataset: all partitions together when it is
    partitioned. A dataset never built (no data folder yet) makes DSS fail: its metrics stay
    empty and the error is kept, the project is still read."""
    partition = ""
    if dataset["partitioned"] == "YES":
        partition = "ALL"
    try:
        raw_metrics = project_handle.get_dataset(dataset["dataset_name"]).get_last_metric_values(partition).get_raw()
    except Exception as error:
        return {}, str(error)[:500]
    return raw_metrics, None


def read_folder_metrics(project_handle, folder):
    """(last computed metrics, error) of a managed folder, like read_dataset_metrics."""
    try:
        raw_metrics = project_handle.get_managed_folder(folder["folder_id"]).get_last_metric_values().get_raw()
    except Exception as error:
        return {}, str(error)[:500]
    return raw_metrics, None


def tag_row(project_key, tag):
    """Row of dss_project_tags. A tag of a global category is written Category:Value."""
    row = {"project_key": project_key, "tag": tag, "tag_category": None, "tag_value": None}
    if ":" in tag:
        category, value = tag.split(":", 1)
        row["tag_category"] = category.strip()
        row["tag_value"] = value.strip()
    return row


def dataset_row(raw, project_key, variables):
    """Row of dss_datasets from a dataset list item."""
    params = raw.get("params", {})
    dimensions = []
    for dimension in raw.get("partitioning", {}).get("dimensions", []):
        dimensions.append(dimension.get("name"))

    dataset = {
        "project_key": project_key,
        "dataset_name": raw["name"],
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
    dataset["resolved_schema"] = substitute_variables(dataset["schema"], variables)
    dataset["resolved_table"] = substitute_variables(dataset["table"], variables)
    dataset["resolved_path"] = substitute_variables(dataset["path"], variables)
    return dataset


def folder_row(raw, project_key, variables):
    """Row of dss_managed_folders from a managed folder list item."""
    params = raw.get("params", {})
    folder = {
        "project_key": project_key,
        "folder_id": raw.get("id"),
        "folder_name": raw.get("name"),
        "folder_type": raw.get("type"),
        "connection": first_value(params, ["connection"]),
        "path": first_value(params, ["path"]),
    }
    folder["resolved_path"] = substitute_variables(folder["path"], variables)
    return folder


def add_metrics(row, raw_metrics):
    """metric_rows, metric_size (MB), metric_files and is_populated, from the last computed
    metrics. Empty when DSS never computed them."""
    row["metric_rows"] = metric_value(raw_metrics, METRIC_ROWS)
    size_bytes = metric_value(raw_metrics, METRIC_SIZE)
    row["metric_size"] = None
    if size_bytes is not None:
        row["metric_size"] = round(size_bytes / 1048576.0, 3)
    row["metric_files"] = metric_value(raw_metrics, METRIC_FILES)
    row["is_populated"] = is_populated(row["metric_rows"], row["metric_size"], row["metric_files"])


def flow_links(nodes, project_key, recipe_types):
    """Rows of dss_flow_links: one per recipe input or output, dataset or managed folder."""
    links = []
    for node_id, node in nodes.items():
        if node["type"] != "RUNNABLE_RECIPE":
            continue
        recipe_name = node.get("ref", node_id)

        for direction, neighbour_ids in (("input", node["predecessors"]), ("output", node["successors"])):
            for neighbour_id in neighbour_ids:
                neighbour = nodes.get(neighbour_id, {})
                if neighbour.get("type") == "COMPUTABLE_DATASET":
                    object_type = "DATASET"
                elif neighbour.get("type") == "COMPUTABLE_FOLDER":
                    object_type = "FOLDER"
                else:
                    continue  # saved model...
                object_project, object_name = object_reference(neighbour, neighbour_id, project_key)
                links.append({
                    "project_key": project_key,
                    "recipe_name": recipe_name,
                    "recipe_type": recipe_types.get(recipe_name),
                    "direction": direction,
                    "object_type": object_type,
                    "object_project": object_project,
                    "object_name": object_name,
                    "foreign": yes_no(object_project != project_key),
                })
    return links


def object_reference(node, node_id, project_key):
    """(project, name) of a flow object. An object of another project is written
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


def needs_project_variables(raw_objects):
    """True when a setting of these datasets or folders uses a variable other than projectKey."""
    for raw in raw_objects:
        params = raw.get("params", {})
        for name in ("schema", "table", "path"):
            value = params.get(name)
            if value is None:
                continue
            for variable in VARIABLE.findall(str(value)):
                if variable != "projectKey":
                    return True
    return False


def read_project_variables(project_handle, project_key, instance_variables):
    """Variables usable in the settings: instance, then project standard, then project
    local (each overrides the previous one), and projectKey."""
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


# ---------------------------------------------------------------------------------------
# Connections and their permissions
# ---------------------------------------------------------------------------------------

def read_connections(client):
    """(dss_connections, dss_connection_permissions), from the connection list (admin): one
    call. Connection details (get_info) only for the connections whose parameters hold
    variables or no user."""
    started = time.time()
    rows = []
    permissions = []
    n_details = 0
    listed = client.list_connections(as_type="dictitems")
    progress.log("connections: %d listed, reading their details where needed" % len(listed))
    for name in listed:
        definition = raw_of(listed[name])
        params = definition.get("params") or {}
        info = definition
        if needs_connection_details(params):
            n_details = n_details + 1
            try:
                info = client.get_connection(name).get_info()
            except Exception as error:
                rows.append({"connection": name, "error": str(error)[:500]})
                continue
        rows.append(connection_row(name, info, definition))
        permissions.extend(permission_rows(name, definition))
    progress.log("connections done: %d, details read for %d, %d permission rows in %s s"
                 % (len(rows), n_details, len(permissions), seconds_since(started)))
    return pd.DataFrame(rows), pd.DataFrame(permissions, columns=PERMISSION_COLUMNS)


def needs_connection_details(params):
    """True when the listed parameters are not enough: a variable to resolve, or a database
    connection without user (its user may come from a credential)."""
    for value in params.values():
        if isinstance(value, str) and "${" in value:
            return True
    points_to_a_server = first_value(params, ["host", "url", "jdbcUrl"]) is not None
    if points_to_a_server and not first_value(params, ["user"]):
        return True
    return False


def permission_rows(name, definition):
    """Rows of dss_connection_permissions: "Freely usable by" and "Details readable by".
    scope: ALL (everyone), ALLOWED (the listed groups, one row each) or NONE."""
    rows = []
    usable_scope = definition.get("usableBy")
    usable_groups = definition.get("allowedGroups") or []
    rows.extend(permission_scope_rows(name, "FREELY_USABLE_BY", usable_scope, usable_groups))

    readability = definition.get("detailsReadability") or {}
    readable_scope = readability.get("readableBy")
    readable_groups = readability.get("allowedGroups") or []
    rows.extend(permission_scope_rows(name, "DETAILS_READABLE_BY", readable_scope, readable_groups))
    return rows


def permission_scope_rows(name, permission, scope, groups):
    if scope is None:
        return []
    scope = str(scope).upper()
    if scope != "ALLOWED" or not groups:
        return [{"connection": name, "permission": permission, "scope": scope, "group_name": None}]
    rows = []
    for group in groups:
        rows.append({"connection": name, "permission": permission, "scope": scope, "group_name": group})
    return rows


def connection_row(name, info, definition):
    """Row of dss_connections. info: the connection details (get_info) or the listed
    definition itself."""
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

    credentials_mode = info.get("credentialsMode")
    if credentials_mode is None:
        credentials_mode = definition.get("credentialsMode")

    row = {
        "connection": name,
        "connection_type": info.get("type") or definition.get("type"),
        "credentials_mode": credentials_mode,
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


# ---------------------------------------------------------------------------------------
# Security: groups and users
# ---------------------------------------------------------------------------------------

def read_groups(client):
    """dss_groups, from the group list: one call."""
    rows = []
    for item in client.list_groups():
        raw = raw_of(item)
        rows.append({
            "group_name": raw.get("name"),
            "description": raw.get("description"),
            "source_type": raw.get("sourceType"),
            "admin": yes_no(raw.get("admin", False)),
        })
    return pd.DataFrame(rows)


def read_users(client):
    """(dss_users, dss_group_members), from the user list: one call. No email."""
    users = []
    members = []
    for item in client.list_users():
        raw = raw_of(item)
        login = raw.get("login")
        users.append({
            "login": login,
            "display_name": raw.get("displayName"),
            "source_type": raw.get("sourceType"),
            "enabled": yes_no(raw.get("enabled", True)),
            "user_profile": raw.get("userProfile"),
        })
        for group in raw.get("groups", []):
            members.append({"group_name": group, "login": login})
    return pd.DataFrame(users), pd.DataFrame(members, columns=MEMBER_COLUMNS)


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

def raw_of(item):
    """The dictionary behind a list item of the API (a dict, or an object holding _data)."""
    if isinstance(item, dict):
        return item
    data = getattr(item, "_data", None)
    if isinstance(data, dict):
        return data
    raise TypeError("unexpected list item: %r" % (item,))


def metric_value(raw_metrics, metric_id):
    """Last value of a metric: the whole-object value (NP or ALL), else the sum of the
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


def is_populated(metric_rows, metric_size, metric_files=None):
    """YES, NO, or UNKNOWN when DSS never computed the metrics."""
    if metric_rows is None:
        metric_rows = metric_files
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


def is_secret(name):
    lowered = name.lower()
    for word in SECRET_WORDS:
        if word in lowered:
            return True
    return False


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


def cell_text(value):
    """A cell of an editable dataset as stripped text. None when empty (None, NaN, blank)."""
    if value is None:
        return None
    if not isinstance(value, str) and pd.isna(value):
        return None
    text = str(value).strip()
    if text == "":
        return None
    return text


def text_or_none(value):
    """Value as text, None when empty."""
    if value is None or value == "":
        return None
    return str(value)


def seconds_since(started):
    """Seconds elapsed since started (a time.time() value), one decimal."""
    return round(time.time() - started, 1)


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
