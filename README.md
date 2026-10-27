# dbmon: storage of the DSS databases

Shows how much the Oracle and PostgreSQL databases used by DSS store, and who stores it: per database, storage unit (tablespace), use case, project, table and object. Each database is read through a dedicated DSS connection with an admin account.

Earlier work is kept as reference only in [archive/](archive/README.md).

## Flow

```
databases (editable) ─► 1 Database extraction (Python runs SQL) ─► storage_units, role_settings, objects, tables, collect_log
DSS API              ─► 2 DSS extraction      (Python, DSS API) ─► dss_projects, dss_project_tags, dss_datasets,
                                                                    dss_managed_folders, dss_dataset_shares, dss_flow_links,
                                                                    dss_connections, dss_connection_permissions,
                                                                    dss_groups, dss_group_members, dss_users, inventory_log
1 + 2 + databases
+ audit_connection_mapping ─► 3 Refinement     (Python, no API)  ─► dataset_roles, dataset_locations, dataset_tables,
                                                                    table_ownership, dataset_storage, project_consumption
```

Steps 1 and 2 do not depend on each other: they can run first, in any order. Step 3 only reads datasets.

| Step | State |
|---|---|
| 1 Database extraction | done, tested on PostgreSQL 16 and Oracle 26ai Free |
| 2 DSS extraction | written, tested with a stand-in DSS client: to check on a real DSS 13.2 / 15.2 |
| 3 Refinement | written, tested on the step 1 data of the test containers |

## Layout

```
dbmon/                    Python package, imported in the DSS project library as python/dbmon
├── collect.py            runs the SQL files through each admin connection, sums objects per table (step 1)
├── dss_extract.py        reads DSS through its API: raw datasets (step 2)
├── refine.py             ties datasets to tables, natures, ownership, project consumption (step 3)
├── schemas.py            schema of every output dataset
├── use_case.py           use case of a DSS project
└── sql/
    ├── oracle/           identity, storage_units, role_settings, objects
    └── postgresql/       the same four files
tests/                    tests on local Oracle and PostgreSQL containers, see tests/README.md
recipes/
├── collect_storage.py    code of the step 1 recipe
├── extract_dss.py        code of the step 2 recipe
└── refine_inventory.py   code of the step 3 recipe
```

## Step 1: collect

### Set up

1. Create one DSS connection per database, with an admin account:
   - Oracle: one per PDB (or non-CDB), with `SELECT_CATALOG_ROLE` or `SELECT ANY DICTIONARY`. `V$ASM_DISKGROUP_STAT` must be readable.
   - PostgreSQL: one per database, with `pg_monitor`.
2. Import `dbmon/` into the project library as `python/dbmon` (Libraries, Import from Git).
3. Create an editable dataset `databases`, one row per database:

   | Column | |
   |---|---|
   | `connection` | the admin connection |
   | `engine` | `oracle` or `postgresql` |
   | `label` | optional. Name shown in the reports, copied on every row. Empty: the connection name. Give one, it stays the same when a server moves |
   | `environment` | optional. `PROD`, `DEV`..., copied on every row |
   | `objects` | optional. `N`: only the tablespaces of that database, no objects nor tables. Empty: `Y` |

4. Create a Python recipe with `databases` as input and `storage_units`, `role_settings`, `objects`, `tables` and `collect_log` as outputs. Paste [recipes/collect_storage.py](recipes/collect_storage.py).
5. Partition the five outputs by day: time dimension `snapshot_day`, period Day. On a SQL connection, partition on the column `snapshot_day`. Running the recipe again the same day replaces that day, and old days can be dropped by partition. The recipe refuses to build any partition but today's: the databases can only be read as they are now.
6. Create the scenario:
   - Step "Build": the five outputs, partition `CURRENT_DAY`.
   - Step "Verify rules or run checks": `collect_log`, partition `CURRENT_DAY`. In its Data Quality tab, add the rule "Column values in set" on `status` with the single value `OK`: a database that failed makes this step fail.
   - Trigger: daily, out of business hours. Reporter: a mail when the scenario does not end in success.

### First run

Before the daily runs, time the collection on your largest database: leave only that row in `databases` and build today's partition. `collect_log` gives the seconds of each query (`storage_units_seconds`, `objects_seconds`). The same files run in SQL*Plus with `SET TIMING ON`. DSS sets no time limit on these queries, unless the connection defines one.

On the databases, the sessions show as module `DBMON` with the query as action (Oracle, `V$SESSION`), or as application `dbmon <query>` (PostgreSQL, `pg_stat_activity`).

`PARALLEL` at the top of the recipe sets how many databases are read at the same time (4). Set it to 1 to read them one after the other.

### Datasets

| Dataset | One row per | Columns |
|---|---|---|
| `storage_units` | tablespace | [oracle](dbmon/sql/oracle/storage_units.sql), [postgresql](dbmon/sql/postgresql/storage_units.sql) |
| `objects` | object and tablespace, partitions summed | [oracle](dbmon/sql/oracle/objects.sql), [postgresql](dbmon/sql/postgresql/objects.sql) |
| `tables` | table: all its objects summed | [below](#tables) |
| `collect_log` | database and run | `status` OK or FAILED, `error`, `objects` Y or N, rows and seconds per query, `seconds` in total |

The application data of a database is the sum of its objects. Its allocated space is the sum of its tablespaces on Oracle; PostgreSQL tablespaces are shared by the whole cluster, so their space is known per server.

Every row of `storage_units`, `objects` and `tables` starts with `snapshot_day`, `collected_utc`, `label`, `environment`, `dss_connection` and the identity of the database ([identity.sql](dbmon/sql/oracle/identity.sql)): `engine`, `server_name`, `database_name`, `unique_name`, `role`, `version`. PostgreSQL tablespaces belong to the whole cluster: they are kept once per server, with an empty `database_name`.

### tables

`collect.tables` sums the objects of each table, found through `parent_schema` and `parent_table`:

- Oracle: data with every partition and subpartition, IOT top index and overflow, nested-table storage, indexes (also those owned by another schema), LOB segments and their LOB indexes. A materialized view: its container table and its indexes.
- PostgreSQL: the data of every partition under the root table, indexes, TOAST tables and their indexes.

An object that belongs to no table (sequence, cluster, temporary segment of a statement in progress) keeps a row of its own, with `standalone = YES`. The total of `total_mb` is always the total of `objects.size_mb`.

| Column | Meaning |
|---|---|
| `schema_name`, `table_name` | the table (schema of the table, even for an index owned by another schema), or the object itself when `standalone = YES` |
| `standalone` | YES for an object that belongs to no table |
| `total_mb` | all its objects |
| `table_mb`, `index_mb`, `lob_mb`, `other_mb` | the same, split by family: data, indexes, LOB or TOAST storage, other (standalone objects only) |
| `n_indexes` | indexes |
| `partitioned` | YES when its data or indexes are partitioned |
| `n_segments` | storage units summed: segments on Oracle, relations on PostgreSQL |
| `tablespaces` | tablespaces holding its objects, comma separated |
| `compression` | compression of its data (Oracle), comma separated when partitions differ |

### Changing a query

- Each SQL file runs as-is in SQL*Plus, SQLcl or psql: try a change there first.
- Both engines return the same columns in the same order, because their rows go to the same dataset. A column that does not exist on one engine is `NULL` there.
- Declare a new column in [dbmon/schemas.py](dbmon/schemas.py), at the same place. The recipes write that schema on every run, even when a result has no row (Oracle `role_settings` is always empty), so the datasets always have a schema and it never changes with the data. A column missing from `schemas.py` stops the recipe with its name. Every string column has a maximum length there (`STRING_LENGTH`, `LONG_TEXT_LENGTH` for free text), written to DSS as `maxLength`; a longer value is cut, ending with `...`. A list of values never goes in one string: it becomes a dataset of its own, with one row per value, and a count (`n_*`) where it came from.
- Oracle files keep SQL*Plus rules: no blank line inside the statement, no `&`.
- Run [tests/test_collect.py](tests/README.md) afterwards.

## Step 2: DSS extraction

[dbmon/dss_extract.py](dbmon/dss_extract.py) reads DSS through its public API: a first layer of raw datasets, close to what the API returns. It needs no input. Notebooks are not read: a dataset only used by a notebook is in no pipeline.

### Set up

1. Create a Python recipe without input, with the twelve outputs below. Paste [recipes/extract_dss.py](recipes/extract_dss.py). Partition the outputs by day like step 1.
2. Run it in the same scenario as step 1, as a user with an admin API key ("Run as" in the scenario settings): connections, users, groups and instance variables need admin rights.
3. Restrict the access to `dss_connections`: `jdbc_url` is the URL as configured and can hold a password.

`PARALLEL` sets how many projects are read at the same time (4). `METRICS = "NONE"` skips the metrics, one call per dataset and folder, if runs get too slow.

API calls: one per run for the projects, connections, users, groups and instance variables; per project one for each list (datasets, recipes, managed folders), the flow graph and the settings (sharing); project variables only when a setting uses one; connection details only for connections whose listed parameters hold a variable or no user; the metrics, one per dataset and folder.

### Datasets

| Dataset | One row per | Content |
|---|---|---|
| `dss_projects` | project | `project_name`, `owner_login` |
| `dss_project_tags` | project and tag | `tag`, and for a `Category:Value` tag `tag_category`, `tag_value` |
| `dss_datasets` | dataset | type, `managed`, partitioning, `connection`, `mode`, `schema`, `table`, `catalog`, `path`; the same with `${...}` replaced (`resolved_*`: instance, then project standard, then project local variables, and `projectKey`); `shared`, `n_shared_with` (the projects themselves: `dss_dataset_shares`); `metric_rows`, `metric_size` (MB), `metric_files` and `is_populated` (`YES`, `NO`, `UNKNOWN`) from the metrics DSS last computed (never computed by dbmon) |
| `dss_managed_folders` | managed folder | `folder_id`, `folder_name`, `folder_type`, `connection`, `path`, `resolved_path`, `metric_size` (MB), `metric_files`, `is_populated` |
| `dss_dataset_shares` | shared dataset and project it is exposed to | `project_key`, `dataset_name`, `target_project` |
| `dss_flow_links` | recipe input or output | `project_key`, `recipe_name`, `recipe_type` (from the recipe list), `direction`, `object_type` (`DATASET` or `FOLDER`), `object_project`, `object_name` (a folder: its id), `foreign` (object of another project). From each project's flow graph; models are left out |
| `dss_connections` | connection | `connection_type`, `credentials_mode`, `connection_mode` (`JDBC` when "useURL" is ticked, else `FIELDS`); `field_host`, `field_port`, `field_database_or_service`, `field_user` as typed in the fields; `jdbc_url` as configured, and `jdbc_vendor`, `jdbc_host`, `jdbc_port`, `jdbc_database_or_service`, `jdbc_user`, `jdbc_properties` (URL parameters, secrets left out) parsed by `dbmon.utils.jdbc_parser.parse_jdbc` (a library of the environment, next to `dbmon` in the project library); `effective_host`, `effective_port`, `effective_database_or_service`, `effective_user`: what DSS really uses (the URL in JDBC mode, or a URL typed into the host field, else the fields); `stale_fields` YES when the fields differ from what DSS uses; `default_schema`, `driver_properties`, `post_connect_statements`, `naming_rule`. `jdbc_url` can hold a password: restrict this dataset accordingly. No other secret is copied |
| `dss_connection_permissions` | connection, permission and allowed group | `permission`: `FREELY_USABLE_BY` or `DETAILS_READABLE_BY`; `scope`: `ALL`, `ALLOWED` (one row per group in `group_name`) or `NONE` |
| `dss_groups` | group | `description`, `source_type`, `admin` |
| `dss_group_members` | group and user | `group_name`, `login` |
| `dss_users` | user | `display_name`, `source_type`, `enabled`, `user_profile`. No email |
| `inventory_log` | project | `status` OK or FAILED, `error`, datasets, folders and links read, `seconds` |

## Step 3: refinement

[dbmon/refine.py](dbmon/refine.py) ties the datasets of step 2 to the tables of step 1 and tells what each project is responsible for. Plain Python on datasets: no API call, no SQL.

### Set up

1. Create the dataset `audit_connection_mapping`, one row per DSS connection used by datasets: `dss_connection` (that connection) and `audit_connection` (the admin connection of `databases` whose step 1 export holds its tables). That is how a dataset is tied to a monitored database: its connection leads to the admin connection, and that one to its `label` in `databases`. Admin connections used directly by datasets need no row. A connection mapped to several admin connections gets its candidates once per database.
2. Create a Python recipe with `databases`, `audit_connection_mapping`, `tables`, `role_settings`, `dss_projects`, `dss_project_tags`, `dss_datasets`, `dss_managed_folders`, `dss_flow_links` and `dss_connections` as inputs, and the six datasets below as outputs. Paste [recipes/refine_inventory.py](recipes/refine_inventory.py). Partition the outputs by day; partition dependency "Equals" on the partitioned inputs (the recipe keeps their latest day if it reads more).
3. Build it in the scenario after steps 1 and 2.

### Nature and storage of a dataset

Whether a dataset's space belongs to its project depends on its nature:

| `dataset_nature` | | Counts for the project |
|---|---|---|
| `PRODUCED` | written by a recipe of the flow | yes |
| `MANAGED_IDLE` | managed by DSS (managed folders always are), written by no recipe | yes |
| `EXTERNAL_SOURCE` | not managed, read by a recipe: data of someone else | no |
| `EXTERNAL_UNUSED` | not managed, neither read nor written | no |

And where its data is:

| `storage_kind` | | Size from |
|---|---|---|
| `DATABASE` | an Oracle or PostgreSQL table | its step 1 table |
| `LOCAL_STORAGE` | files on a Filesystem connection, uploaded files, Filesystem managed folders | the size metric |
| `EXTERNAL_STORAGE` | anything else: S3, HDFS, other SQL databases... | the size metric |
| `NO_STORAGE` | a SQL query dataset | nothing stored |

### Datasets

| Dataset | One row per | Content |
|---|---|---|
| `dataset_roles` | dataset or managed folder | `object_type`, `managed`, `n_producers`, `n_consumers`, `n_consuming_projects`, `dataset_nature` |
| `dataset_locations` | dataset, audit connection and candidate schema | `candidate_rank` (best first), `candidate_status`, `connection_name` (the dataset's real connection), `audit_connection_name` (the admin connection from `audit_connection_mapping`, empty when unmapped), `database_label`, `schema_name`, `table_name`, `schema_source`, `note` |
| `dataset_tables` | dataset | `match_status` (`FOUND`, `AMBIGUOUS`, `NOT_FOUND`, or the reason there is no candidate), `database_label`, `schema_name`, `table_name` (as in the database when found), the candidate kept. Exact names first, then ignoring case; then the best rank |
| `table_ownership` | step 1 table | its sizes, `status`, `owner_project`, `owner_dataset`, `owner_nature`, `multiple_owners`, datasets pointing to it by nature, `n_referencing_projects`, `schema_use_case`, `schema_layer` |
| `dataset_storage` | dataset or managed folder | `dataset_nature`, `storage_kind`, `size_mb`, `size_source` (`TABLE`, `METRIC`, `NONE`), `counted` (YES when the size goes to the project), `note` |
| `project_consumption` | project | `use_case`, `use_case_source`; datasets and folders by nature, found or not; `database_mb` (and by family), `local_storage_mb`, `external_storage_mb`, `total_mb` (the three summed), `idle_mb` (part held by `MANAGED_IDLE` objects); `database_read_mb`, `database_unused_mb`, `external_read_mb`; `n_size_unknown` |

### Candidate locations

When a SQL dataset has no schema, the database decides. The candidates, best first (`schema_source`):

| `schema_source` | |
|---|---|
| `DATASET` | the dataset's schema setting |
| `CONNECTION_DEFAULT` | the connection's default schema |
| `JDBC_PROPERTY` | the JDBC property `currentSchema` (PostgreSQL) |
| `POST_CONNECT` | `ALTER SESSION SET CURRENT_SCHEMA` or `SET search_path` in the connection's post-connect statements |
| `LOGIN_USER` | Oracle: the schema of the login user |
| `SEARCH_PATH` | PostgreSQL: the login user's `search_path` from `role_settings` (role in this database, then the role, then the database), by default `"$user", public`: one candidate per schema |

`candidate_status`: `CANDIDATE` (to look up in step 1), `UNMAPPED` (the connection leads to no database: not in `audit_connection_mapping`, nor an admin connection of `databases`), and, with a single row and no candidate, `PER_USER` (the connection logs in as each DSS user), `UNRESOLVED`, `NO_TABLE` (SQL query dataset), `NOT_SQL`.

### Ownership of a table

| `table_ownership.status` | |
|---|---|
| `OWNED` | a `PRODUCED` or `MANAGED_IDLE` dataset points to it: its space goes to that dataset's project. Several: `PRODUCED` first, then the smallest project key, then dataset name, and `multiple_owners = YES` |
| `READ_ONLY` | only `EXTERNAL_SOURCE` datasets point to it: data read, written by no recipe |
| `UNUSED` | only `EXTERNAL_UNUSED` datasets point to it |
| `UNREFERENCED` | no dataset points to it |

`UNUSED` and `UNREFERENCED` tables are the possible orphans; `schema_use_case` (from `<UC>_DSSWORK` / `<UC>_DSSOUT`) gives their use case even without a project.

The space of a table is counted once. For one day and one database, the total of `table_ownership` equals the total of `tables`, and the database space of all projects plus the space of the tables no project owns equals it too. `database_read_mb`, `database_unused_mb` and `external_read_mb` are never part of `total_mb`. Per use case, group `project_consumption` by `use_case`, and `table_ownership` by `schema_use_case` for the space no project owns.

## Use case of a project

`dbmon.use_case.project_use_case(project_key, tags)` is the only place that decides the use case of a DSS project. It reads the `CDP-UseCase` global tag category (`CDP-UseCase:<use case>` in the project's tags), and falls back on the current criteria until every project is tagged.

## Compatibility

Python 3.9 and later, DSS 13.2 and 15.2. Step 1 only uses `SQLExecutor2.query_to_df` and dataset reads and writes. Step 2 uses the public API (`dataikuapi`), checked against the 13.4.4 sources. Step 3 only reads and writes datasets.
