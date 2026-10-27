"""Refinement: ties the DSS extraction to the database extraction, and tells what each project
is responsible for. Plain Python on the extracted datasets: no API call, no SQL.

Inputs
- the DSS extraction (dbmon.dss_extract): dss_projects, dss_project_tags, dss_datasets,
  dss_managed_folders, dss_flow_links, dss_connections
- the database extraction (dbmon.collect): tables, role_settings
- databases (the admin connections) and audit_connection_mapping (connection used by
  datasets -> admin connection whose export holds its tables)

Results
- dataset_roles        one row per dataset or managed folder: who writes and reads it, its nature
- dataset_locations    one row per dataset and candidate schema, best candidate first
- dataset_tables       one row per dataset: the step 1 table it is stored in, when found
- table_ownership      one row per step 1 table: its space and the dataset that owns it
- dataset_storage      one row per dataset or managed folder: where its data is, its size,
                       and whether that size goes to its project
- project_consumption  one row per project: the space it is responsible for

dataset_nature: whether the data belongs to the project
  PRODUCED          written by a recipe of the flow                      counts
  MANAGED_IDLE      managed by DSS (or a managed folder), written by none counts
  EXTERNAL_SOURCE   not managed, read by a recipe (data of someone else)  does not count
  EXTERNAL_UNUSED   not managed, neither read nor written                 does not count

storage_kind: where the data is
  DATABASE          an Oracle or PostgreSQL table: size from the step 1 tables
  LOCAL_STORAGE     files on a Filesystem connection, uploaded files: size from the metrics
  EXTERNAL_STORAGE  anything else (S3, HDFS, other SQL databases...): size from the metrics
  NO_STORAGE        a SQL query dataset: nothing stored

Candidate schemas of a SQL dataset, in the order the database would use them
(schema_source):
1. DATASET             the dataset's own schema setting
2. CONNECTION_DEFAULT  the default schema of the connection
3. JDBC_PROPERTY       the JDBC property currentSchema (PostgreSQL)
4. POST_CONNECT        ALTER SESSION SET CURRENT_SCHEMA / SET search_path in the
                       connection's post-connect statements
5. LOGIN_USER          Oracle: the schema of the login user
   SEARCH_PATH         PostgreSQL: the search_path of the login user (role_settings), by
                       default "$user", public: one candidate per schema of the path
"""
import json
import re

import pandas as pd

from dbmon import use_case

# DSS dataset types read as tables of the monitored databases, and their engine
SQL_DATASET_TYPES = {
    "Oracle": "oracle",
    "PostgreSQL": "postgresql",
}
# Connection and object types whose files are stored on the DSS server
LOCAL_TYPES = ("Filesystem", "UploadedFiles")

# Natures whose space goes to the project
COUNTED_NATURES = ("PRODUCED", "MANAGED_IDLE")

ORACLE_CURRENT_SCHEMA = re.compile(r"ALTER\s+SESSION\s+SET\s+CURRENT_SCHEMA\s*=\s*\"?([^\"\s;]+)\"?", re.IGNORECASE)
POSTGRES_SEARCH_PATH = re.compile(r"SET\s+(?:SESSION\s+)?search_path\s*(?:=|TO)\s*(.+?);?\s*$", re.IGNORECASE)
VARIABLE = re.compile(r"\$\{([^}]+)\}")
SCHEMA_CONVENTION = re.compile(r"^(.+)_(DSSWORK|DSSOUT)$")

ROLE_COLUMNS = ["project_key", "object_type", "object_name", "managed", "n_producers", "n_consumers",
                "n_consuming_projects", "dataset_nature"]
LOCATION_COLUMNS = ["project_key", "dataset_name", "candidate_rank", "candidate_status",
                    "connection_name", "audit_connection_name", "database_label",
                    "schema_name", "table_name", "schema_source", "note"]
MATCH_COLUMNS = ["project_key", "dataset_name", "match_status", "connection_name", "audit_connection_name",
                 "database_label", "schema_name", "table_name", "candidate_rank", "schema_source", "note"]
OWNERSHIP_COLUMNS = ["label", "engine", "server_name", "database_name", "schema_name", "table_name", "standalone",
                     "total_mb", "table_mb", "index_mb", "lob_mb", "other_mb", "status", "owner_project",
                     "owner_dataset", "owner_nature", "multiple_owners", "n_owning_datasets", "n_source_datasets",
                     "n_unused_datasets", "n_referencing_projects", "schema_use_case", "schema_layer"]
STORAGE_COLUMNS = ["project_key", "object_type", "object_name", "display_name", "storage_type", "connection",
                   "dataset_nature", "storage_kind", "size_mb", "size_source", "counted", "note"]


def refine(dss, tables, role_settings, databases, connection_mapping):
    """Every result of the refinement. Returns {result name: DataFrame}.

    dss                 {name: DataFrame} of the DSS extraction (dss_projects, dss_datasets...)
    tables              the step 1 tables of the same day
    role_settings       the step 1 role_settings of the same day (can be empty)
    databases           the admin connections and their labels
    connection_mapping  audit_connection_mapping: dss_connection -> audit_connection. None:
                        admin connections only
    """
    datasets = records(dss["dss_datasets"])
    folders = records(dss["dss_managed_folders"])
    links = records(dss["dss_flow_links"])
    projects = records(dss["dss_projects"])
    tags = records(dss["dss_project_tags"])
    table_rows = records(tables)

    connection_by_name = {}
    for connection in records(dss["dss_connections"]):
        connection_by_name[connection["connection"]] = connection

    roles = dataset_roles(datasets, folders, links)
    locations = dataset_locations(datasets, connection_by_name, databases, connection_mapping, role_settings)
    matches = dataset_tables(locations, table_rows)
    ownership = table_ownership(table_rows, matches, roles)
    storage = dataset_storage(datasets, folders, roles, matches, ownership, connection_by_name)
    consumption = project_consumption(projects, tags, roles, matches, ownership, storage)

    return {
        "dataset_roles": pd.DataFrame(roles, columns=ROLE_COLUMNS),
        "dataset_locations": pd.DataFrame(locations, columns=LOCATION_COLUMNS),
        "dataset_tables": pd.DataFrame(matches, columns=MATCH_COLUMNS),
        "table_ownership": pd.DataFrame(ownership, columns=OWNERSHIP_COLUMNS),
        "dataset_storage": pd.DataFrame(storage, columns=STORAGE_COLUMNS),
        "project_consumption": pd.DataFrame(consumption),
    }


# ---------------------------------------------------------------------------------------
# dataset_roles: who writes and reads each dataset and folder, and its nature
# ---------------------------------------------------------------------------------------

def dataset_roles(datasets, folders, links):
    """Rows of dataset_roles, datasets first, then managed folders."""
    producers = {}
    consumers = {}
    consuming_projects = {}
    for link in links:
        key = (link["object_type"], link["object_project"], link["object_name"])
        if link["direction"] == "output":
            producers[key] = producers.get(key, 0) + 1
            continue
        consumers[key] = consumers.get(key, 0) + 1
        if link.get("foreign") == "YES":
            if key not in consuming_projects:
                consuming_projects[key] = set()
            consuming_projects[key].add(link["project_key"])

    objects = []
    for dataset in datasets:
        objects.append(("DATASET", dataset["project_key"], dataset["dataset_name"], dataset.get("managed") == "YES"))
    for folder in folders:
        # a managed folder is always managed by DSS
        objects.append(("FOLDER", folder["project_key"], folder["folder_id"], True))

    rows = []
    for object_type, project_key, object_name, managed in objects:
        key = (object_type, project_key, object_name)
        n_producers = producers.get(key, 0)
        n_consumers = consumers.get(key, 0)
        rows.append({
            "project_key": project_key,
            "object_type": object_type,
            "object_name": object_name,
            "managed": yes_no(managed),
            "n_producers": n_producers,
            "n_consumers": n_consumers,
            "n_consuming_projects": len(consuming_projects.get(key, set())),
            "dataset_nature": nature_of(n_producers, n_consumers, managed),
        })
    return rows


def nature_of(n_producers, n_consumers, managed):
    if n_producers > 0:
        return "PRODUCED"
    if managed:
        return "MANAGED_IDLE"
    if n_consumers > 0:
        return "EXTERNAL_SOURCE"
    return "EXTERNAL_UNUSED"


# ---------------------------------------------------------------------------------------
# dataset_locations: candidate schemas and tables of each dataset
# ---------------------------------------------------------------------------------------

def dataset_locations(datasets, connection_by_name, databases, connection_mapping, role_settings):
    """Rows of dataset_locations for every dataset.

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
    audits_by_connection = audit_connections(databases, connection_mapping)

    rows = []
    for dataset in datasets:
        connection = connection_by_name.get(dataset.get("connection"), {})
        audits = audits_by_connection.get(dataset.get("connection"), [])
        if not audits:
            rows.extend(locations_of(dataset, connection, None, None, role_settings))
            continue
        for audit_connection, label in audits:
            rows.extend(locations_of(dataset, connection, audit_connection, label, role_settings))
    return rows


def locations_of(dataset, connection, audit_connection, label, role_settings):
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
    if has_variable_left(dataset.get("resolved_table")) or has_variable_left(dataset.get("resolved_schema")):
        return [without_candidate(base, "UNRESOLVED", "variable that could not be replaced")]

    source, schemas, note = schema_candidates(dataset, connection, engine, role_settings)
    if not schemas:
        return [without_candidate(base, source, note)]

    status = "CANDIDATE"
    if label is None:
        status = "UNMAPPED"
        note = "connection not in audit_connection_mapping, nor an admin connection of databases"

    rows = []
    rank = 0
    for schema in schemas:
        rank = rank + 1
        row = dict(base)
        row["candidate_rank"] = rank
        row["candidate_status"] = status
        row["schema_name"] = schema
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

    settings = records(role_settings)
    precise_first = [(login_user, database), (login_user, None), (None, database)]
    for role, setting_database in precise_first:
        for row in settings:
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
    for row in records(databases):
        label = row.get("label")
        if label is None or str(label).strip() == "":
            label = row["connection"]
        label_of_admin[row["connection"]] = label

    audits = {}
    for admin_connection in label_of_admin:
        audits[admin_connection] = [(admin_connection, label_of_admin[admin_connection])]

    if connection_mapping is None or connection_mapping.empty:
        return audits
    for row in records(connection_mapping):
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
# dataset_tables: the step 1 table of each dataset
# ---------------------------------------------------------------------------------------

def dataset_tables(locations, table_rows):
    """Rows of dataset_tables: one per dataset.

    Each candidate is looked up in the step 1 tables: exact names first, then ignoring case.
    The best candidate wins: an exact match, then a match ignoring case, then an ambiguous
    one, then none; among equals, the lowest candidate_rank.

    match_status   FOUND       the table exists in step 1
                   AMBIGUOUS   several tables match once case is ignored
                   NOT_FOUND   no candidate schema holds the table
                   UNMAPPED, PER_USER, UNRESOLVED, NO_TABLE, NOT_SQL: no candidate to look
                   up (see dataset_locations.candidate_status)
    schema_name, table_name   FOUND: as named in the database. Otherwise: the best candidate
    """
    exact = set()
    ignoring_case = {}
    for table in table_rows:
        exact.add((table["label"], table["schema_name"], table["table_name"]))
        loose_key = (table["label"], upper(table["schema_name"]), upper(table["table_name"]))
        if loose_key not in ignoring_case:
            ignoring_case[loose_key] = []
        ignoring_case[loose_key].append((table["schema_name"], table["table_name"]))

    # candidates of each dataset, in the order of dataset_locations
    candidates_by_dataset = {}
    dataset_order = []
    for location in locations:
        key = (location["project_key"], location["dataset_name"])
        if key not in candidates_by_dataset:
            candidates_by_dataset[key] = []
            dataset_order.append(key)
        candidates_by_dataset[key].append(location)

    rows = []
    for key in dataset_order:
        best = None
        best_score = None
        for candidate in candidates_by_dataset[key]:
            match_kind, found_schema, found_table = match_of(candidate, exact, ignoring_case)
            rank = candidate.get("candidate_rank")
            if rank is None:
                rank = 999999
            score = (match_kind, rank)
            if best_score is None or score < best_score:
                best_score = score
                best = (candidate, match_kind, found_schema, found_table)

        candidate, match_kind, found_schema, found_table = best
        if match_kind in (1, 2):
            status = "FOUND"
        elif match_kind == 3:
            status = "AMBIGUOUS"
        elif candidate["candidate_status"] == "CANDIDATE":
            status = "NOT_FOUND"
        else:
            status = candidate["candidate_status"]

        row = {
            "project_key": candidate["project_key"],
            "dataset_name": candidate["dataset_name"],
            "match_status": status,
            "connection_name": candidate.get("connection_name"),
            "audit_connection_name": candidate.get("audit_connection_name"),
            "database_label": candidate.get("database_label"),
            "schema_name": candidate.get("schema_name"),
            "table_name": candidate.get("table_name"),
            "candidate_rank": candidate.get("candidate_rank"),
            "schema_source": candidate.get("schema_source"),
            "note": candidate.get("note"),
        }
        if status == "FOUND":
            row["schema_name"] = found_schema
            row["table_name"] = found_table
        rows.append(row)
    return rows


def match_of(candidate, exact, ignoring_case):
    """(match kind, schema, table) of one candidate. Kind: 1 exact, 2 ignoring case,
    3 ambiguous (several tables ignoring case), 4 none."""
    if candidate["candidate_status"] != "CANDIDATE":
        return 4, None, None
    label = candidate["database_label"]
    schema = candidate["schema_name"]
    table = candidate["table_name"]
    if (label, schema, table) in exact:
        return 1, schema, table
    matching = ignoring_case.get((label, upper(schema), upper(table)), [])
    if len(matching) == 1:
        return 2, matching[0][0], matching[0][1]
    if len(matching) > 1:
        return 3, None, None
    return 4, None, None


# ---------------------------------------------------------------------------------------
# table_ownership: who owns the space of each step 1 table
# ---------------------------------------------------------------------------------------

def table_ownership(table_rows, matches, roles):
    """Rows of table_ownership: one per step 1 table, so that its space is counted once.

    status         OWNED         a PRODUCED or MANAGED_IDLE dataset points to it: its space
                                 goes to the owner
                   READ_ONLY     only EXTERNAL_SOURCE datasets point to it (read, not written)
                   UNUSED        only EXTERNAL_UNUSED datasets point to it
                   UNREFERENCED  no dataset points to it
    owner_project, owner_dataset, owner_nature
                   the owning dataset: PRODUCED before MANAGED_IDLE, then the smallest
                   project key, then dataset name. multiple_owners = YES when several
    n_owning_datasets, n_source_datasets, n_unused_datasets   datasets pointing to it, by nature
    n_referencing_projects   projects of all those datasets
    schema_use_case, schema_layer   from the schema name <UC>_DSSWORK / <UC>_DSSOUT (upper
                   case). Empty outside the convention
    """
    nature_by_dataset = {}
    for role in roles:
        if role["object_type"] == "DATASET":
            nature_by_dataset[(role["project_key"], role["object_name"])] = role["dataset_nature"]

    pointing_by_table = {}
    for match in matches:
        if match["match_status"] != "FOUND":
            continue
        table_key = (match["database_label"], match["schema_name"], match["table_name"])
        if table_key not in pointing_by_table:
            pointing_by_table[table_key] = []
        nature = nature_by_dataset.get((match["project_key"], match["dataset_name"]))
        pointing_by_table[table_key].append((match["project_key"], match["dataset_name"], nature))

    rows = []
    for table in table_rows:
        table_key = (table["label"], table["schema_name"], table["table_name"])
        pointing = pointing_by_table.get(table_key, [])

        owners = []
        n_source = 0
        n_unused = 0
        projects = set()
        for project_key, dataset_name, nature in pointing:
            projects.add(project_key)
            if nature in COUNTED_NATURES:
                owners.append(owner_sort_key(project_key, dataset_name, nature))
            elif nature == "EXTERNAL_SOURCE":
                n_source = n_source + 1
            else:
                n_unused = n_unused + 1
        owners.sort()

        row = {}
        for column in ("label", "engine", "server_name", "database_name", "schema_name", "table_name",
                       "standalone", "total_mb", "table_mb", "index_mb", "lob_mb", "other_mb"):
            row[column] = table.get(column)

        if owners:
            row["status"] = "OWNED"
        elif n_source > 0:
            row["status"] = "READ_ONLY"
        elif n_unused > 0:
            row["status"] = "UNUSED"
        else:
            row["status"] = "UNREFERENCED"

        row["owner_project"] = None
        row["owner_dataset"] = None
        row["owner_nature"] = None
        if owners:
            first_owner = owners[0]
            row["owner_project"] = first_owner[1]
            row["owner_dataset"] = first_owner[2]
            row["owner_nature"] = first_owner[3]
        row["multiple_owners"] = yes_no(len(owners) > 1)
        row["n_owning_datasets"] = len(owners)
        row["n_source_datasets"] = n_source
        row["n_unused_datasets"] = n_unused
        row["n_referencing_projects"] = len(projects)

        row["schema_use_case"] = None
        row["schema_layer"] = None
        convention = SCHEMA_CONVENTION.match(upper(table["schema_name"]) or "")
        if convention:
            row["schema_use_case"] = convention.group(1)
            row["schema_layer"] = convention.group(2)
        rows.append(row)
    return rows


def owner_sort_key(project_key, dataset_name, nature):
    """Sort key of a possible owner: PRODUCED first, then project key, then dataset name."""
    priority = 1
    if nature == "PRODUCED":
        priority = 0
    return (priority, project_key, dataset_name, nature)


# ---------------------------------------------------------------------------------------
# dataset_storage: where the data of each dataset and folder is, its size, who pays for it
# ---------------------------------------------------------------------------------------

def dataset_storage(datasets, folders, roles, matches, ownership, connection_by_name):
    """Rows of dataset_storage, datasets first, then managed folders.

    size_mb       DATABASE: the total of its step 1 table. Otherwise: the size metric
    size_source   TABLE, METRIC, or NONE when unknown
    counted       YES when the size goes to the project: a LOCAL or EXTERNAL object of a
                  counted nature, or the owner of a DATABASE table (table_ownership)
    """
    nature_by_object = {}
    for role in roles:
        nature_by_object[(role["object_type"], role["project_key"], role["object_name"])] = role["dataset_nature"]

    match_by_dataset = {}
    for match in matches:
        match_by_dataset[(match["project_key"], match["dataset_name"])] = match

    table_by_key = {}
    for table in ownership:
        table_by_key[(table["label"], table["schema_name"], table["table_name"])] = table

    rows = []
    for dataset in datasets:
        project_key = dataset["project_key"]
        name = dataset["dataset_name"]
        connection_type = connection_by_name.get(dataset.get("connection"), {}).get("connection_type")
        row = {
            "project_key": project_key,
            "object_type": "DATASET",
            "object_name": name,
            "display_name": name,
            "storage_type": dataset.get("dataset_type"),
            "connection": dataset.get("connection"),
            "dataset_nature": nature_by_object.get(("DATASET", project_key, name)),
            "storage_kind": dataset_storage_kind(dataset, connection_type),
            "size_mb": None,
            "size_source": "NONE",
            "counted": "NO",
            "note": None,
        }
        if row["storage_kind"] == "DATABASE":
            database_size(row, match_by_dataset.get((project_key, name)), table_by_key)
        elif row["storage_kind"] != "NO_STORAGE":
            metric_size(row, dataset.get("metric_size"))
        rows.append(row)

    for folder in folders:
        project_key = folder["project_key"]
        connection_type = connection_by_name.get(folder.get("connection"), {}).get("connection_type")
        row = {
            "project_key": project_key,
            "object_type": "FOLDER",
            "object_name": folder["folder_id"],
            "display_name": folder.get("folder_name"),
            "storage_type": folder.get("folder_type"),
            "connection": folder.get("connection"),
            "dataset_nature": nature_by_object.get(("FOLDER", project_key, folder["folder_id"])),
            "storage_kind": files_storage_kind(folder.get("folder_type"), connection_type),
            "size_mb": None,
            "size_source": "NONE",
            "counted": "NO",
            "note": None,
        }
        metric_size(row, folder.get("metric_size"))
        rows.append(row)
    return rows


def dataset_storage_kind(dataset, connection_type):
    if dataset.get("mode") == "query":
        return "NO_STORAGE"
    if dataset.get("dataset_type") in SQL_DATASET_TYPES:
        return "DATABASE"
    return files_storage_kind(dataset.get("dataset_type"), connection_type)


def files_storage_kind(object_type, connection_type):
    if connection_type in LOCAL_TYPES or object_type in LOCAL_TYPES:
        return "LOCAL_STORAGE"
    return "EXTERNAL_STORAGE"


def database_size(row, match, table_by_key):
    """Size of a DATABASE dataset: its step 1 table, counted when the dataset owns it."""
    if match is None or match["match_status"] != "FOUND":
        status = "no table"
        if match is not None:
            status = match["match_status"]
        row["note"] = "table not found in step 1: %s" % status
        return
    table = table_by_key.get((match["database_label"], match["schema_name"], match["table_name"]))
    if table is None:
        return
    row["size_mb"] = table.get("total_mb")
    row["size_source"] = "TABLE"
    owner = (table.get("owner_project"), table.get("owner_dataset"))
    if owner == (row["project_key"], row["object_name"]):
        row["counted"] = "YES"
    elif table.get("owner_project") is not None:
        row["note"] = "space counted on %s.%s" % owner


def metric_size(row, size):
    """Size of a LOCAL or EXTERNAL object: its size metric, counted by its nature."""
    if size is not None:
        row["size_mb"] = size
        row["size_source"] = "METRIC"
    else:
        row["note"] = "size metric never computed"
    if row["dataset_nature"] in COUNTED_NATURES:
        row["counted"] = "YES"


# ---------------------------------------------------------------------------------------
# project_consumption: the space each project is responsible for
# ---------------------------------------------------------------------------------------

def project_consumption(projects, tags, roles, matches, ownership, storage):
    """Rows of project_consumption: one per project read.

    use_case, use_case_source   from the project tags (TAG, RULE or NONE)
    n_*                         its datasets and folders, by nature; datasets found or not
    database_mb (and table, index, lob)  its owned tables (table_ownership)
    local_storage_mb, external_storage_mb   its counted files (dataset_storage)
    total_mb                    database_mb + local_storage_mb + external_storage_mb
    idle_mb                     part of total_mb held by MANAGED_IDLE objects (written by no
                                recipe: candidates for clean-up)
    database_read_mb            tables its datasets point to without owning them (not UNUSED)
    database_unused_mb          UNUSED tables its datasets point to
    external_read_mb            files of its EXTERNAL_SOURCE datasets (data of someone else)
    n_size_unknown              objects of a counted nature whose size is unknown
    Nothing of the *_read_mb and *_unused_mb is part of total_mb.
    """
    tags_by_project = {}
    for tag in tags:
        if tag["project_key"] not in tags_by_project:
            tags_by_project[tag["project_key"]] = []
        tags_by_project[tag["project_key"]].append(tag["tag"])

    rows = []
    for project in projects:
        project_key = project["project_key"]
        project_use_case, source = use_case.project_use_case_and_source(project_key, tags_by_project.get(project_key, []))
        row = {
            "project_key": project_key,
            "project_name": project.get("project_name"),
            "use_case": project_use_case,
            "use_case_source": source,
            "n_datasets": 0,
            "n_managed_folders": 0,
            "n_produced": 0,
            "n_managed_idle": 0,
            "n_external_source": 0,
            "n_external_unused": 0,
            "n_datasets_found": 0,
            "n_datasets_not_found": 0,
            "n_owned_tables": 0,
            "database_mb": 0.0,
            "database_table_mb": 0.0,
            "database_index_mb": 0.0,
            "database_lob_mb": 0.0,
            "local_storage_mb": 0.0,
            "external_storage_mb": 0.0,
            "total_mb": 0.0,
            "idle_mb": 0.0,
            "database_read_mb": 0.0,
            "database_unused_mb": 0.0,
            "external_read_mb": 0.0,
            "n_size_unknown": 0,
        }
        rows.append(row)
    row_by_project = {}
    for row in rows:
        row_by_project[row["project_key"]] = row

    for role in roles:
        row = row_by_project.get(role["project_key"])
        if row is None:
            continue
        if role["object_type"] == "DATASET":
            row["n_datasets"] = row["n_datasets"] + 1
        else:
            row["n_managed_folders"] = row["n_managed_folders"] + 1
        counter = "n_" + role["dataset_nature"].lower()
        row[counter] = row[counter] + 1

    for match in matches:
        row = row_by_project.get(match["project_key"])
        if row is None:
            continue
        if match["match_status"] == "FOUND":
            row["n_datasets_found"] = row["n_datasets_found"] + 1
        if match["match_status"] == "NOT_FOUND":
            row["n_datasets_not_found"] = row["n_datasets_not_found"] + 1

    table_by_key = {}
    for table in ownership:
        table_by_key[(table["label"], table["schema_name"], table["table_name"])] = table
        row = row_by_project.get(table["owner_project"])
        if row is None:
            continue
        row["n_owned_tables"] = row["n_owned_tables"] + 1
        row["database_mb"] = row["database_mb"] + number(table.get("total_mb"))
        row["database_table_mb"] = row["database_table_mb"] + number(table.get("table_mb"))
        row["database_index_mb"] = row["database_index_mb"] + number(table.get("index_mb"))
        row["database_lob_mb"] = row["database_lob_mb"] + number(table.get("lob_mb"))

    for item in storage:
        row = row_by_project.get(item["project_key"])
        if row is None:
            continue
        size = number(item.get("size_mb"))
        kind = item["storage_kind"]
        if item["dataset_nature"] in COUNTED_NATURES and kind != "NO_STORAGE" and item.get("size_mb") is None:
            row["n_size_unknown"] = row["n_size_unknown"] + 1
        if item["counted"] == "YES":
            if kind == "LOCAL_STORAGE":
                row["local_storage_mb"] = row["local_storage_mb"] + size
            if kind == "EXTERNAL_STORAGE":
                row["external_storage_mb"] = row["external_storage_mb"] + size
            if item["dataset_nature"] == "MANAGED_IDLE":
                row["idle_mb"] = row["idle_mb"] + size
        if item["dataset_nature"] == "EXTERNAL_SOURCE" and kind in ("LOCAL_STORAGE", "EXTERNAL_STORAGE"):
            row["external_read_mb"] = row["external_read_mb"] + size

    # tables the datasets of a project point to without owning them, once per project
    seen = set()
    for match in matches:
        if match["match_status"] != "FOUND":
            continue
        project_key = match["project_key"]
        table_key = (match["database_label"], match["schema_name"], match["table_name"])
        if (project_key, table_key) in seen:
            continue
        seen.add((project_key, table_key))
        row = row_by_project.get(project_key)
        table = table_by_key.get(table_key)
        if row is None or table is None:
            continue
        size = number(table.get("total_mb"))
        if table["status"] == "UNUSED":
            row["database_unused_mb"] = row["database_unused_mb"] + size
        elif table.get("owner_project") != project_key:
            row["database_read_mb"] = row["database_read_mb"] + size

    # Sums of decimal numbers carry tiny binary errors (1.1880000000000002): keep 3 decimals,
    # like the sizes they add up
    for row in rows:
        row["total_mb"] = row["database_mb"] + row["local_storage_mb"] + row["external_storage_mb"]
        for column in row:
            if column.endswith("_mb"):
                row[column] = round(row[column], 3)
    return rows


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

def records(rows):
    """Rows of a DataFrame as a list of dicts, the missing values (NaN) as None."""
    if rows is None or rows.empty:
        return []
    result = []
    for record in rows.to_dict("records"):
        result.append(without_missing(record))
    return result


def without_missing(record):
    """Copy of a DataFrame record with the missing values (NaN) as None."""
    cleaned = {}
    for name, value in record.items():
        if not isinstance(value, (list, dict)) and pd.isna(value):
            cleaned[name] = None
        else:
            cleaned[name] = value
    return cleaned


def has_variable_left(text):
    return text is not None and VARIABLE.search(str(text)) is not None


def upper(text):
    if text is None:
        return None
    return str(text).upper()


def number(value):
    """value as a float, 0 when missing."""
    if value is None:
        return 0.0
    return float(value)


def yes_no(flag):
    if flag:
        return "YES"
    return "NO"
