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
- database_usage       one row per database, schema and DSS relation: piloted space and
                       to review, for the administrators
- schema_usage         one row per schema of every database, even empty: its space and how
                       it splits by DSS relation, in MB and in % of the schema

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

dss_relation: what DSS has to do with a step 1 table (table_ownership, database_usage)
  PILOTED               owned by a PRODUCED dataset
  PILOTED_IDLE          owned by MANAGED_IDLE datasets only                  to review
  READ_BY_DSS           only EXTERNAL_SOURCE datasets point to it
  DECLARED_UNUSED       only EXTERNAL_UNUSED datasets point to it            to review
  UNRELATED_DSS_SCHEMA  no dataset points to it, in a DSS schema             to review
  UNRELATED             no dataset points to it, in another schema
  STANDALONE            an object of no table (sequence, temporary segment)
A DSS schema: one whose name follows a naming rule (detected_use_case), or holding a table
a dataset points to.

Space always comes with every metric of the step 1 tables (TABLE_METRICS).

Several DSS instances (Design and Automation nodes...) can write to the same databases. The
DSS extraction then carries dss_instance on every row, and every dataset, project and
connection is told apart by it: the same project key on two instances is two projects.
Without dss_instance (a single instance), it is empty everywhere.

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
import time

import pandas as pd

from dbmon import progress, use_case

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

# Metric columns of the step 1 tables: every space figure carries all of them
TABLE_METRICS = ["total_mb", "table_mb", "index_mb", "lob_mb", "other_mb", "n_indexes", "n_segments"]
TABLE_IDENTITY = ["label", "engine", "sql_variant", "server_name", "database_name", "schema_name", "table_name",
                  "standalone"]
REVIEW_RELATIONS = ("PILOTED_IDLE", "DECLARED_UNUSED", "UNRELATED_DSS_SCHEMA")
# Datasets of a database whose table could not be found: an unrelated table may be theirs
UNLOCATED_STATUSES = ("NOT_FOUND", "AMBIGUOUS", "PER_USER", "UNRESOLVED")

ROLE_COLUMNS = ["dss_instance", "project_key", "object_type", "object_name", "managed", "n_producers", "n_consumers",
                "n_consuming_projects", "dataset_nature"]
LOCATION_COLUMNS = ["dss_instance", "project_key", "dataset_name", "candidate_rank", "candidate_status",
                    "connection_name", "audit_connection_name", "database_label",
                    "schema_name", "table_name", "schema_source", "note"]
MATCH_COLUMNS = ["dss_instance", "project_key", "dataset_name", "match_status", "connection_name", "audit_connection_name",
                 "database_label", "schema_name", "table_name", "candidate_rank", "schema_source", "note"]
OWNERSHIP_COLUMNS = (TABLE_IDENTITY + TABLE_METRICS
                     + ["detected_use_case", "detected_layer", "status", "owner_instance", "owner_project",
                        "owner_dataset",
                        "owner_nature", "multiple_owners", "n_owning_datasets", "n_source_datasets",
                        "n_unused_datasets", "n_referencing_projects", "dss_schema", "dss_relation",
                        "to_review"])
STORAGE_COLUMNS = ["dss_instance", "project_key", "object_type", "object_name", "display_name", "storage_type", "connection",
                   "dataset_nature", "storage_kind", "size_mb", "table_mb", "index_mb", "lob_mb", "other_mb",
                   "n_indexes", "n_segments", "size_source", "counted", "note"]
USAGE_KEY = ["label", "engine", "sql_variant", "server_name", "database_name", "schema_name", "dss_schema",
             "detected_use_case", "detected_layer", "dss_relation", "to_review"]
USAGE_COLUMNS = USAGE_KEY + ["n_tables"] + TABLE_METRICS + ["n_datasets_unlocated", "n_projects_failed"]

# Every dss_relation, in the order of the schema_usage columns
DSS_RELATIONS = ["PILOTED", "PILOTED_IDLE", "READ_BY_DSS", "DECLARED_UNUSED", "UNRELATED_DSS_SCHEMA", "UNRELATED",
                 "STANDALONE"]
# Relations where at least one DSS dataset points to the table
RELATED_RELATIONS = ("PILOTED", "PILOTED_IDLE", "READ_BY_DSS", "DECLARED_UNUSED")
SCHEMA_IDENTITY = ["label", "engine", "sql_variant", "server_name", "database_name", "schema_name"]

# Groups of tables in schema_usage, each one a prefix of its columns: one per dss_relation,
# then two cumulated ones
SCHEMA_GROUPS = ["piloted", "piloted_idle", "read_by_dss", "declared_unused", "unrelated_dss_schema", "unrelated",
                 "standalone", "dss_related", "to_review"]


def schema_groups_of(relation):
    """Prefixes of the schema_usage groups a table of this dss_relation counts in: its
    relation, dss_related when a dataset points to it, to_review when it is a table to review."""
    groups = [relation.lower()]
    if relation in RELATED_RELATIONS:
        groups.append("dss_related")
    if relation in REVIEW_RELATIONS:
        groups.append("to_review")
    return groups


def schema_usage_columns():
    """Columns of schema_usage: identity, schema totals, then for each group its tables,
    every metric of the step 1 tables, and its share of the schema's space."""
    columns = SCHEMA_IDENTITY + ["dss_schema", "detected_use_case", "detected_layer", "n_tables"] + TABLE_METRICS
    for group in SCHEMA_GROUPS:
        columns.append(group + "_n_tables")
        for metric in TABLE_METRICS:
            columns.append(group + "_" + metric)
        columns.append(group + "_pct")
    columns = columns + ["n_datasets_unlocated", "n_projects_failed"]
    return columns


SCHEMA_USAGE_COLUMNS = schema_usage_columns()


def refine(dss, tables, role_settings, databases, connection_mapping, naming_rules=None):
    """Every result of the refinement. Returns {result name: DataFrame}.

    dss                 {name: DataFrame} of the DSS extraction (dss_projects, dss_datasets...,
                        and inventory_log: the projects that failed, optional)
    tables              the step 1 tables of the same day
    role_settings       the step 1 role_settings of the same day (can be empty)
    databases           the admin connections and their labels
    connection_mapping  audit_connection_mapping: dss_connection -> audit_connection. None:
                        admin connections only
    naming_rules        the step 1 naming_rules of the same day: every schema of every
                        database, so that schema_usage lists the empty ones too. None: the
                        schemas holding tables only
    """
    started = time.time()
    datasets = records(dss["dss_datasets"])
    folders = records(dss["dss_managed_folders"])
    links = records(dss["dss_flow_links"])
    projects = records(dss["dss_projects"])
    tags = records(dss["dss_project_tags"])
    project_log = records(dss.get("inventory_log"))
    table_rows = records(tables)
    progress.log("Refinement: %d projects, %d datasets, %d folders, %d flow links, %d step 1 tables"
                 % (len(projects), len(datasets), len(folders), len(links), len(table_rows)))

    # connections by instance and name: two instances can have connections of the same name
    connection_by_name = {}
    for connection in records(dss["dss_connections"]):
        connection_by_name[(instance_of(connection), connection["connection"])] = connection

    block_started = time.time()
    roles = dataset_roles(datasets, folders, links)
    log_block("dataset_roles", roles, block_started)

    block_started = time.time()
    locations = dataset_locations(datasets, connection_by_name, databases, connection_mapping, role_settings)
    log_block("dataset_locations", locations, block_started)

    block_started = time.time()
    matches = dataset_tables(locations, table_rows)
    n_found = 0
    for match in matches:
        if match["match_status"] == "FOUND":
            n_found = n_found + 1
    log_block("dataset_tables", matches, block_started, "%d datasets found in step 1" % n_found)

    block_started = time.time()
    ownership = table_ownership(table_rows, matches, roles)
    n_owned = 0
    for table in ownership:
        if table["status"] == "OWNED":
            n_owned = n_owned + 1
    log_block("table_ownership", ownership, block_started, "%d tables owned by a project" % n_owned)

    block_started = time.time()
    storage = dataset_storage(datasets, folders, roles, matches, ownership, connection_by_name)
    log_block("dataset_storage", storage, block_started)

    block_started = time.time()
    consumption = project_consumption(projects, tags, roles, matches, ownership, storage)
    log_block("project_consumption", consumption, block_started)

    block_started = time.time()
    usage = database_usage(ownership, matches, project_log)
    n_to_review = 0
    for group in usage:
        if group["to_review"] == "YES":
            n_to_review = n_to_review + group["n_tables"]
    log_block("database_usage", usage, block_started, "%d tables to review" % n_to_review)

    block_started = time.time()
    schemas = schema_usage(ownership, records(naming_rules), matches, project_log)
    n_empty = 0
    for schema in schemas:
        if schema["n_tables"] == 0:
            n_empty = n_empty + 1
    log_block("schema_usage", schemas, block_started, "%d schemas without table" % n_empty)
    progress.log("Refinement done in %s s" % round(time.time() - started, 1))

    return {
        "dataset_roles": pd.DataFrame(roles, columns=ROLE_COLUMNS),
        "dataset_locations": pd.DataFrame(locations, columns=LOCATION_COLUMNS),
        "dataset_tables": pd.DataFrame(matches, columns=MATCH_COLUMNS),
        "table_ownership": pd.DataFrame(ownership, columns=OWNERSHIP_COLUMNS),
        "dataset_storage": pd.DataFrame(storage, columns=STORAGE_COLUMNS),
        "project_consumption": pd.DataFrame(consumption),
        "database_usage": pd.DataFrame(usage, columns=USAGE_COLUMNS),
        "schema_usage": pd.DataFrame(schemas, columns=SCHEMA_USAGE_COLUMNS),
    }


def log_block(name, rows, block_started, detail=None):
    """Progress trace of one result: its rows and seconds, and a detail when given."""
    message = "%s: %d rows in %s s" % (name, len(rows), round(time.time() - block_started, 1))
    if detail is not None:
        message = message + ", " + detail
    progress.log(message)


# ---------------------------------------------------------------------------------------
# dataset_roles: who writes and reads each dataset and folder, and its nature
# ---------------------------------------------------------------------------------------

def dataset_roles(datasets, folders, links):
    """Rows of dataset_roles, datasets first, then managed folders."""
    producers = {}
    consumers = {}
    consuming_projects = {}
    for link in links:
        key = (instance_of(link), link["object_type"], link["object_project"], link["object_name"])
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
        objects.append((instance_of(dataset), "DATASET", dataset["project_key"], dataset["dataset_name"],
                        dataset.get("managed") == "YES"))
    for folder in folders:
        # a managed folder is always managed by DSS
        objects.append((instance_of(folder), "FOLDER", folder["project_key"], folder["folder_id"], True))

    rows = []
    for instance, object_type, project_key, object_name, managed in objects:
        key = (instance, object_type, project_key, object_name)
        n_producers = producers.get(key, 0)
        n_consumers = consumers.get(key, 0)
        rows.append({
            "dss_instance": instance,
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
    connection. A row of audit_connection_mapping with a dss_instance applies to that
    instance only, and wins over a row without one.

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
        instance = instance_of(dataset)
        connection = connection_by_name.get((instance, dataset.get("connection")), {})
        audits = audits_by_connection.get((instance, dataset.get("connection")))
        if audits is None:
            audits = audits_by_connection.get((None, dataset.get("connection")), [])
        if not audits:
            rows.extend(locations_of(dataset, connection, None, None, role_settings))
            continue
        for audit_connection, label in audits:
            rows.extend(locations_of(dataset, connection, audit_connection, label, role_settings))
    return rows


def locations_of(dataset, connection, audit_connection, label, role_settings):
    """Candidate rows of one dataset, for one of its audit connections."""
    base = {
        "dss_instance": instance_of(dataset),
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
    """{(DSS instance, DSS connection): [(audit connection, database label)]}.

    An admin connection of databases is its own audit connection, with the label of its row
    (the connection name when the label is empty), on every instance (DSS instance None). A
    connection of audit_connection_mapping leads to the admin connections it is mapped to: on
    the instance of its optional dss_instance column, or on every instance when it is empty.
    """
    label_of_admin = {}
    for row in records(databases):
        label = row.get("label")
        if label is None or str(label).strip() == "":
            label = row["connection"]
        label_of_admin[row["connection"]] = label

    audits = {}
    for admin_connection in label_of_admin:
        audits[(None, admin_connection)] = [(admin_connection, label_of_admin[admin_connection])]

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
        instance = row.get("dss_instance")
        if instance is not None and str(instance).strip() == "":
            instance = None
        if instance is not None:
            instance = str(instance).strip()
        key = (instance, connection)
        if key not in audits:
            audits[key] = []
        if (admin_connection, label) not in audits[key]:
            audits[key].append((admin_connection, label))
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
        key = (location.get("dss_instance"), location["project_key"], location["dataset_name"])
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
            "dss_instance": candidate.get("dss_instance"),
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
    owner_instance, owner_project, owner_dataset, owner_nature
                   the owning dataset: PRODUCED before MANAGED_IDLE, then the smallest
                   instance, project key, dataset name. multiple_owners = YES when several
    n_owning_datasets, n_source_datasets, n_unused_datasets   datasets pointing to it, by nature
    n_referencing_projects   projects of all those datasets (one per instance and project key)
    detected_use_case, detected_layer   as the naming rules of the database detect them
                   (step 1, naming_rules.sql)
    dss_schema, dss_relation, to_review   see the module documentation
    The identity and every metric of the step 1 table are copied.
    """
    nature_by_dataset = {}
    for role in roles:
        if role["object_type"] == "DATASET":
            nature_by_dataset[(role.get("dss_instance"), role["project_key"], role["object_name"])] = \
                role["dataset_nature"]

    pointing_by_table = {}
    for match in matches:
        if match["match_status"] != "FOUND":
            continue
        table_key = (match["database_label"], match["schema_name"], match["table_name"])
        if table_key not in pointing_by_table:
            pointing_by_table[table_key] = []
        dataset_key = (match.get("dss_instance"), match["project_key"], match["dataset_name"])
        nature = nature_by_dataset.get(dataset_key)
        pointing_by_table[table_key].append((match.get("dss_instance"), match["project_key"], match["dataset_name"],
                                             nature))

    rows = []
    for table in table_rows:
        table_key = (table["label"], table["schema_name"], table["table_name"])
        pointing = pointing_by_table.get(table_key, [])

        owners = []
        n_source = 0
        n_unused = 0
        projects = set()
        for instance, project_key, dataset_name, nature in pointing:
            projects.add((instance, project_key))
            if nature in COUNTED_NATURES:
                owners.append(owner_sort_key(instance, project_key, dataset_name, nature))
            elif nature == "EXTERNAL_SOURCE":
                n_source = n_source + 1
            else:
                n_unused = n_unused + 1
        owners.sort()

        row = {}
        for column in TABLE_IDENTITY + TABLE_METRICS + ["detected_use_case", "detected_layer"]:
            row[column] = table.get(column)

        if owners:
            row["status"] = "OWNED"
        elif n_source > 0:
            row["status"] = "READ_ONLY"
        elif n_unused > 0:
            row["status"] = "UNUSED"
        else:
            row["status"] = "UNREFERENCED"

        row["owner_instance"] = None
        row["owner_project"] = None
        row["owner_dataset"] = None
        row["owner_nature"] = None
        if owners:
            first_owner = owners[0]
            row["owner_instance"] = first_owner[5]
            row["owner_project"] = first_owner[2]
            row["owner_dataset"] = first_owner[3]
            row["owner_nature"] = first_owner[4]
        row["multiple_owners"] = yes_no(len(owners) > 1)
        row["n_owning_datasets"] = len(owners)
        row["n_source_datasets"] = n_source
        row["n_unused_datasets"] = n_unused
        row["n_referencing_projects"] = len(projects)
        rows.append(row)

    # DSS schemas: named by a naming rule, or holding a table a dataset points to
    dss_schemas = set()
    for row in rows:
        if row["detected_use_case"] is not None or row["status"] != "UNREFERENCED":
            dss_schemas.add((row["label"], row["schema_name"]))

    for row in rows:
        row["dss_schema"] = yes_no((row["label"], row["schema_name"]) in dss_schemas)
        row["dss_relation"] = dss_relation(row)
        row["to_review"] = yes_no(row["dss_relation"] in REVIEW_RELATIONS)
    return rows


def dss_relation(row):
    """What DSS has to do with a table of table_ownership (see the module documentation)."""
    if row.get("standalone") == "YES":
        return "STANDALONE"
    if row["status"] == "OWNED":
        if row["owner_nature"] == "PRODUCED":
            return "PILOTED"
        return "PILOTED_IDLE"
    if row["status"] == "READ_ONLY":
        return "READ_BY_DSS"
    if row["status"] == "UNUSED":
        return "DECLARED_UNUSED"
    if row["dss_schema"] == "YES":
        return "UNRELATED_DSS_SCHEMA"
    return "UNRELATED"


def owner_sort_key(instance, project_key, dataset_name, nature):
    """Sort key of a possible owner: PRODUCED first, then instance, project key, dataset
    name. The instance itself comes last (it can be None, which does not sort)."""
    priority = 1
    if nature == "PRODUCED":
        priority = 0
    instance_text = ""
    if instance is not None:
        instance_text = instance
    return (priority, instance_text, project_key, dataset_name, nature, instance)


# ---------------------------------------------------------------------------------------
# dataset_storage: where the data of each dataset and folder is, its size, who pays for it
# ---------------------------------------------------------------------------------------

def dataset_storage(datasets, folders, roles, matches, ownership, connection_by_name):
    """Rows of dataset_storage, datasets first, then managed folders.

    size_mb       DATABASE: the total of its step 1 table. Otherwise: the size metric
    table_mb, index_mb, lob_mb, other_mb, n_indexes, n_segments
                  DATABASE: the other metrics of its step 1 table. Otherwise empty
    size_source   TABLE, METRIC, or NONE when unknown
    counted       YES when the size goes to the project: a LOCAL or EXTERNAL object of a
                  counted nature, or the owner of a DATABASE table (table_ownership)
    """
    nature_by_object = {}
    for role in roles:
        key = (role.get("dss_instance"), role["object_type"], role["project_key"], role["object_name"])
        nature_by_object[key] = role["dataset_nature"]

    match_by_dataset = {}
    for match in matches:
        match_by_dataset[(match.get("dss_instance"), match["project_key"], match["dataset_name"])] = match

    table_by_key = {}
    for table in ownership:
        table_by_key[(table["label"], table["schema_name"], table["table_name"])] = table

    rows = []
    for dataset in datasets:
        instance = instance_of(dataset)
        project_key = dataset["project_key"]
        name = dataset["dataset_name"]
        connection_type = connection_by_name.get((instance, dataset.get("connection")), {}).get("connection_type")
        row = {
            "dss_instance": instance,
            "project_key": project_key,
            "object_type": "DATASET",
            "object_name": name,
            "display_name": name,
            "storage_type": dataset.get("dataset_type"),
            "connection": dataset.get("connection"),
            "dataset_nature": nature_by_object.get((instance, "DATASET", project_key, name)),
            "storage_kind": dataset_storage_kind(dataset, connection_type),
            "size_mb": None,
            "table_mb": None, "index_mb": None, "lob_mb": None, "other_mb": None,
            "n_indexes": None, "n_segments": None,
            "size_source": "NONE",
            "counted": "NO",
            "note": None,
        }
        if row["storage_kind"] == "DATABASE":
            database_size(row, match_by_dataset.get((instance, project_key, name)), table_by_key)
        elif row["storage_kind"] != "NO_STORAGE":
            metric_size(row, dataset.get("metric_size"))
        rows.append(row)

    for folder in folders:
        instance = instance_of(folder)
        project_key = folder["project_key"]
        connection_type = connection_by_name.get((instance, folder.get("connection")), {}).get("connection_type")
        row = {
            "dss_instance": instance,
            "project_key": project_key,
            "object_type": "FOLDER",
            "object_name": folder["folder_id"],
            "display_name": folder.get("folder_name"),
            "storage_type": folder.get("folder_type"),
            "connection": folder.get("connection"),
            "dataset_nature": nature_by_object.get((instance, "FOLDER", project_key, folder["folder_id"])),
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
    for metric in TABLE_METRICS:
        if metric != "total_mb":
            row[metric] = table.get(metric)
    row["size_source"] = "TABLE"
    owner = (table.get("owner_instance"), table.get("owner_project"), table.get("owner_dataset"))
    if owner == (row.get("dss_instance"), row["project_key"], row["object_name"]):
        row["counted"] = "YES"
    elif table.get("owner_project") is not None:
        owner_name = "%s.%s" % (table.get("owner_project"), table.get("owner_dataset"))
        if table.get("owner_instance") is not None:
            owner_name = table.get("owner_instance") + ":" + owner_name
        row["note"] = "space counted on %s" % owner_name


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
    """Rows of project_consumption: one per project read (per instance and project key).

    use_case, use_case_source   from the project tags (TAG, RULE or NONE)
    n_*                         its datasets and folders, by nature; datasets found or not
    database_mb, database_table_mb, database_index_mb, database_lob_mb, database_other_mb,
    database_n_indexes, database_n_segments   every metric of its owned tables (table_ownership)
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
        key = (instance_of(tag), tag["project_key"])
        if key not in tags_by_project:
            tags_by_project[key] = []
        tags_by_project[key].append(tag["tag"])

    rows = []
    for project in projects:
        instance = instance_of(project)
        project_key = project["project_key"]
        project_tags = tags_by_project.get((instance, project_key), [])
        project_use_case, source = use_case.project_use_case_and_source(project_key, project_tags)
        row = {
            "dss_instance": instance,
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
            "database_other_mb": 0.0,
            "database_n_indexes": 0,
            "database_n_segments": 0,
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
        row_by_project[(row.get("dss_instance"), row["project_key"])] = row

    for role in roles:
        row = row_by_project.get((role.get("dss_instance"), role["project_key"]))
        if row is None:
            continue
        if role["object_type"] == "DATASET":
            row["n_datasets"] = row["n_datasets"] + 1
        else:
            row["n_managed_folders"] = row["n_managed_folders"] + 1
        counter = "n_" + role["dataset_nature"].lower()
        row[counter] = row[counter] + 1

    for match in matches:
        row = row_by_project.get((match.get("dss_instance"), match["project_key"]))
        if row is None:
            continue
        if match["match_status"] == "FOUND":
            row["n_datasets_found"] = row["n_datasets_found"] + 1
        if match["match_status"] == "NOT_FOUND":
            row["n_datasets_not_found"] = row["n_datasets_not_found"] + 1

    table_by_key = {}
    for table in ownership:
        table_by_key[(table["label"], table["schema_name"], table["table_name"])] = table
        row = row_by_project.get((table["owner_instance"], table["owner_project"]))
        if row is None:
            continue
        row["n_owned_tables"] = row["n_owned_tables"] + 1
        row["database_mb"] = row["database_mb"] + number(table.get("total_mb"))
        row["database_table_mb"] = row["database_table_mb"] + number(table.get("table_mb"))
        row["database_index_mb"] = row["database_index_mb"] + number(table.get("index_mb"))
        row["database_lob_mb"] = row["database_lob_mb"] + number(table.get("lob_mb"))
        row["database_other_mb"] = row["database_other_mb"] + number(table.get("other_mb"))
        row["database_n_indexes"] = row["database_n_indexes"] + int(number(table.get("n_indexes")))
        row["database_n_segments"] = row["database_n_segments"] + int(number(table.get("n_segments")))

    for item in storage:
        row = row_by_project.get((item.get("dss_instance"), item["project_key"]))
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
        project = (match.get("dss_instance"), match["project_key"])
        table_key = (match["database_label"], match["schema_name"], match["table_name"])
        if (project, table_key) in seen:
            continue
        seen.add((project, table_key))
        row = row_by_project.get(project)
        table = table_by_key.get(table_key)
        if row is None or table is None:
            continue
        size = number(table.get("total_mb"))
        if table["status"] == "UNUSED":
            row["database_unused_mb"] = row["database_unused_mb"] + size
        elif (table.get("owner_instance"), table.get("owner_project")) != project:
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
# database_usage: piloted space and tables to review, per database and schema
# ---------------------------------------------------------------------------------------

def database_usage(ownership, matches, project_log):
    """Rows of database_usage: the tables of table_ownership grouped by database, schema and
    dss_relation, with their count and every metric of the step 1 tables summed.

    n_datasets_unlocated  datasets on that database whose table could not be found
                          (NOT_FOUND, AMBIGUOUS, PER_USER, UNRESOLVED)
    n_projects_failed     projects the DSS extraction could not read
    When either is above 0, a table counted UNRELATED may belong to one of them: the
    unrelated figures are then an upper bound.
    """
    unlocated_by_label = unlocated_datasets(matches)
    n_projects_failed = failed_projects(project_log)

    groups = {}
    group_order = []
    for table in ownership:
        key = []
        for column in USAGE_KEY:
            key.append(table.get(column))
        key = tuple(key)
        if key not in groups:
            group = {}
            for column in USAGE_KEY:
                group[column] = table.get(column)
            group["n_tables"] = 0
            for metric in TABLE_METRICS:
                group[metric] = 0.0
            groups[key] = group
            group_order.append(key)
        group = groups[key]
        group["n_tables"] = group["n_tables"] + 1
        for metric in TABLE_METRICS:
            group[metric] = group[metric] + number(table.get(metric))

    rows = []
    for key in group_order:
        group = groups[key]
        for metric in TABLE_METRICS:
            if metric.endswith("_mb"):
                group[metric] = round(group[metric], 3)  # sums of decimals: keep 3 decimals
            else:
                group[metric] = int(group[metric])
        group["n_datasets_unlocated"] = unlocated_by_label.get(group["label"], 0)
        group["n_projects_failed"] = n_projects_failed
        rows.append(group)
    return rows


def unlocated_datasets(matches):
    """{database label: datasets whose table could not be found on it}."""
    unlocated_by_label = {}
    for match in matches:
        if match["match_status"] not in UNLOCATED_STATUSES:
            continue
        label = match.get("database_label")
        unlocated_by_label[label] = unlocated_by_label.get(label, 0) + 1
    return unlocated_by_label


def failed_projects(project_log):
    """Projects the DSS extraction could not read."""
    n_projects_failed = 0
    for entry in project_log:
        if entry.get("status") == "FAILED":
            n_projects_failed = n_projects_failed + 1
    return n_projects_failed


# ---------------------------------------------------------------------------------------
# schema_usage: one line per schema, its space split by dss_relation
# ---------------------------------------------------------------------------------------

def schema_usage(ownership, naming_rule_rows, matches, project_log):
    """Rows of schema_usage: one per schema of every database, the schemas without table
    included (from naming_rules), to filter afterwards.

    n_tables, total_mb ... n_segments   the schema: every metric of its tables summed
    For each group (SCHEMA_GROUPS), its columns prefixed by its name:
      <group>_n_tables                    its tables
      <group>_total_mb ... <group>_n_segments   every metric of those tables summed
      <group>_pct                         <group>_total_mb in % of the schema's total_mb (0 to
                                          100, 2 decimals). Empty when the schema holds 0 MB
    Groups: one per dss_relation (piloted ... standalone), whose % add up to 100, then
      dss_related   the tables a DSS dataset points to, whatever its role: PILOTED,
                    PILOTED_IDLE, READ_BY_DSS, DECLARED_UNUSED
      to_review     the tables to review: PILOTED_IDLE, DECLARED_UNUSED, UNRELATED_DSS_SCHEMA
    n_datasets_unlocated, n_projects_failed   reliability, as in database_usage
    The space of a schema is the space of its tables: an index owned by another schema
    counts with its table, as in the step 1 tables.
    """
    unlocated_by_label = unlocated_datasets(matches)
    n_projects_failed = failed_projects(project_log)

    schemas = {}
    schema_order = []

    # Every schema the naming rules list, with its detected use case and layer
    for rule in naming_rule_rows:
        if rule.get("name_kind") != "SCHEMA":
            continue
        key = (rule.get("label"), rule.get("name"))
        if key in schemas:
            continue
        schema = new_schema_row(rule, rule.get("name"))
        schemas[key] = schema
        schema_order.append(key)

    # The tables, added to their schema (a schema missing from the naming rules is added)
    for table in ownership:
        key = (table.get("label"), table.get("schema_name"))
        if key not in schemas:
            schema = new_schema_row(table, table.get("schema_name"))
            schemas[key] = schema
            schema_order.append(key)
        schema = schemas[key]
        schema["n_tables"] = schema["n_tables"] + 1
        if table.get("dss_schema") == "YES":
            schema["dss_schema"] = "YES"
        for metric in TABLE_METRICS:
            schema[metric] = schema[metric] + number(table.get(metric))
        for group in schema_groups_of(table["dss_relation"]):
            schema[group + "_n_tables"] = schema[group + "_n_tables"] + 1
            for metric in TABLE_METRICS:
                column = group + "_" + metric
                schema[column] = schema[column] + number(table.get(metric))

    rows = []
    for key in schema_order:
        schema = schemas[key]
        # sums of decimals: keep 3 decimals
        round_metrics(schema, "")
        for group in SCHEMA_GROUPS:
            round_metrics(schema, group + "_")
            schema[group + "_pct"] = percent_of(schema[group + "_total_mb"], schema["total_mb"])
        schema["n_datasets_unlocated"] = unlocated_by_label.get(schema["label"], 0)
        schema["n_projects_failed"] = n_projects_failed
        rows.append(schema)
    return rows


def new_schema_row(source, schema_name):
    """An empty row of schema_usage, its identity copied from a naming rule or a table."""
    schema = {}
    for column in SCHEMA_IDENTITY:
        schema[column] = source.get(column)
    schema["schema_name"] = schema_name
    schema["detected_use_case"] = source.get("detected_use_case")
    schema["detected_layer"] = source.get("detected_layer")
    # a schema named by a rule is a DSS schema; one holding a table a dataset points to too
    schema["dss_schema"] = yes_no(schema["detected_use_case"] is not None)
    schema["n_tables"] = 0
    for metric in TABLE_METRICS:
        schema[metric] = 0.0
    for group in SCHEMA_GROUPS:
        schema[group + "_n_tables"] = 0
        for metric in TABLE_METRICS:
            schema[group + "_" + metric] = 0.0
        schema[group + "_pct"] = None
    return schema


def round_metrics(row, prefix):
    """The metrics prefix + TABLE_METRICS of row: MB kept with 3 decimals, counts whole."""
    for metric in TABLE_METRICS:
        column = prefix + metric
        if metric.endswith("_mb"):
            row[column] = round(row[column], 3)
        else:
            row[column] = int(row[column])


def percent_of(part, whole):
    """part in % of whole, 2 decimals. None when whole is 0."""
    if whole == 0:
        return None
    return round(part * 100.0 / whole, 2)


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

def instance_of(row):
    """DSS instance of a row of the DSS extraction. None when the extraction has none."""
    return row.get("dss_instance")


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
