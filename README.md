# dbmon: storage of the DSS databases

Shows how much the Oracle and PostgreSQL databases used by DSS store, and who stores it: per database, storage unit (tablespace), use case, project, table and object. Each database is read through a dedicated DSS connection with an admin account.

Earlier work is kept as reference only in [archive/](archive/README.md).

## Flow

```
databases (editable) ─► 1 Database inventory  (Python runs SQL)  ─► storage_units, role_settings, objects, tables, collect_log
DSS API              ─► 2 Dataiku inventory   (Python, DSS API)  ─► dss_projects, dss_datasets, dss_dataset_shares, dss_flow_links,
                                                                     dss_connections, dss_dataset_locations, inventory_log
                        3 Analysis            (Oracle SQL)       ─► dataset_roles, dataset_tables, table_ownership,
                                                                     project_consumption
```

| Step | State |
|---|---|
| 1 Database inventory | done, tested on PostgreSQL 16 and Oracle 26ai Free |
| 2 Dataiku inventory | written, tested with a stand-in DSS client: to check on a real DSS 13.2 / 15.2 |
| 3 Analysis | written, tested in Oracle |

## Layout

```
dbmon/                    Python package, imported in the DSS project library as python/dbmon
├── collect.py            runs the SQL files through each admin connection, sums objects per table
├── inventory.py          DSS datasets, flow roles, storage locations (step 2)
├── schemas.py            schema of every output dataset
├── use_case.py           use case of a DSS project
└── sql/
    ├── oracle/           identity, storage_units, role_settings, objects
    └── postgresql/       the same four files
tests/                    tests on local Oracle and PostgreSQL containers, see tests/README.md
recipes/
├── collect_storage.py    code of the step 1 recipe
├── inventory_dss.py      code of the step 2 recipe
└── sql/                  the four step 3 SQL recipes (Oracle)
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

## Step 2: Dataiku inventory

[dbmon/inventory.py](dbmon/inventory.py) reads every project through the DSS public API. Extraction only: it does not read the step 1 tables, the links are made in step 3. Notebooks are not read: a dataset only used by a notebook is in no pipeline.

### Set up

1. In `databases`, add the optional column `dss_connections`: the DSS connections that point to that database, comma separated. That is how a dataset is tied to a monitored database.
2. Create a Python recipe with `databases` and `role_settings` as inputs, and `dss_projects`, `dss_datasets`, `dss_dataset_shares`, `dss_flow_links`, `dss_connections`, `dss_dataset_locations` and `inventory_log` as outputs. Paste [recipes/inventory_dss.py](recipes/inventory_dss.py). Partition the outputs by day like step 1; it uses the latest day of `role_settings`.
3. Run it in the same scenario, after step 1, as a user with an admin API key ("Run as" in the scenario settings): connections and instance variables need admin rights.

### Datasets

| Dataset | One row per | Content |
|---|---|---|
| `dss_projects` | project | `project_name`, `project_tags`, `use_case`, `use_case_source` (`TAG`, `RULE` or `NONE`, see [use case](#use-case-of-a-project)) |
| `dss_datasets` | dataset | type, `managed`, partitioning, `connection`, `mode`, `schema`, `table`, `catalog`, `path`; the same with `${...}` replaced (`resolved_*`: instance, then project standard, then project local variables, and `projectKey`); `shared`, `n_shared_with` (the projects themselves: `dss_dataset_shares`); `metric_rows`, `metric_size` (MB) and `is_populated` (`YES`, `NO`, `UNKNOWN`) from the metrics DSS last computed (never computed by dbmon) |
| `dss_dataset_shares` | shared dataset and project it is exposed to | `project_key`, `dataset_name`, `target_project` |
| `dss_flow_links` | recipe input or output | `project_key`, `recipe_name`, `recipe_type` (from the recipe settings: `get_recipe(name).get_settings().type`), `direction`, `dataset_project`, `dataset_name`, `foreign` (dataset of another project). From each project's flow graph; managed folders and models are left out |
| `dss_connections` | connection | `connection_type`, `credentials_mode`, `jdbc_vendor`, `host`, `port`, `database_or_service`, `login_user`, `default_schema`, `jdbc_properties`, `post_connect_statements`, `naming_rule`. A connection defined by a JDBC URL gets them from the URL, through `dbmon.utils.jdbc_parser.parse_jdbc`: a library of the environment, not in this repository, expected next to `dbmon` in the project library. The URL itself is never kept. No password or other secret |
| `dss_dataset_locations` | dataset and candidate schema | `candidate_rank` (best first), `candidate_status`, `database_label`, `schema_name`, `table_name`, `schema_source`, `note` |

### Candidate locations

When a SQL dataset has no schema, the database decides. The candidates, best first (`schema_source`):

| `schema_source` | |
|---|---|
| `DSS_LOCATION` | the location DSS resolves itself (`get_location_info`), when the recipe can read it. The settings follow as fallback candidates |
| `DATASET` | the dataset's schema setting |
| `CONNECTION_DEFAULT` | the connection's default schema |
| `JDBC_PROPERTY` | the JDBC property `currentSchema` (PostgreSQL) |
| `POST_CONNECT` | `ALTER SESSION SET CURRENT_SCHEMA` or `SET search_path` in the connection's post-connect statements |
| `LOGIN_USER` | Oracle: the schema of the login user |
| `SEARCH_PATH` | PostgreSQL: the login user's `search_path` from `role_settings` (role in this database, then the role, then the database), by default `"$user", public`: one candidate per schema |

`candidate_status`: `CANDIDATE` (to look up in step 1), `UNMAPPED` (connection not in `databases`), and, with a single row and no candidate, `PER_USER` (the connection logs in as each DSS user), `UNRESOLVED`, `NO_TABLE` (SQL query dataset), `NOT_SQL`.

## Step 3: project and dataset analysis (Oracle SQL)

Four DSS SQL query recipes on the Oracle connection holding the monitoring datasets, in [recipes/sql](recipes/sql). No Python: every result is a dataset to query or join.

### Set up

- Store all the monitoring datasets on that Oracle connection, partitioned by day on `snapshot_day`.
- The SQL names the tables and columns as DSS creates them on Oracle: quoted, lower case, and the table named like the dataset (`"tables"`, `"dss_flow_links"`...). If the connection's naming rule adds a prefix, set the table name of each dataset to its name, or edit the `FROM` lines.
- Each recipe reads the partition being built (`'$DKU_DST_snapshot_day'`): partition dependency "Equals" on every input, output partition `CURRENT_DAY`.

| Recipe | Inputs | Output, one row per | Content |
|---|---|---|---|
| [dataset_roles.sql](recipes/sql/dataset_roles.sql) | `dss_datasets`, `dss_flow_links` | `dataset_roles`: dataset | `n_producers`, `n_consumers`, `n_consuming_projects`, `flow_role`: `target` (output of a recipe), `source` (input, output of none), `orphan` (neither) |
| [dataset_tables.sql](recipes/sql/dataset_tables.sql) | `dss_dataset_locations`, `tables` | `dataset_tables`: dataset | `match_status` (`FOUND`, `AMBIGUOUS`, `NOT_FOUND`, or the reason there is no candidate), `database_label`, `schema_name`, `table_name` (as in the database when found), the candidate kept |
| [table_ownership.sql](recipes/sql/table_ownership.sql) | `tables`, `dataset_tables`, `dataset_roles` | `table_ownership`: step 1 table | its sizes, `status`, `owner_project`, `owner_dataset`, `multiple_owners`, datasets pointing to it by role, `n_referencing_projects`, `schema_use_case`, `schema_layer` |
| [project_consumption.sql](recipes/sql/project_consumption.sql) | `dss_projects`, `dataset_roles`, `dataset_tables`, `table_ownership` | `project_consumption`: project | `use_case`, datasets by role and match, `n_owned_tables`, `owned_mb` (and by family), `read_mb`, `no_flow_mb` |

`table_ownership.status`:

| Status | |
|---|---|
| `OWNED` | a target dataset writes it: its space goes to that dataset's project. Several targets: the smallest project key, then dataset name, and `multiple_owners = YES` |
| `READ_ONLY` | only source datasets point to it: data read, written by no recipe |
| `NO_FLOW` | only datasets that are in no pipeline point to it |
| `UNREFERENCED` | no dataset points to it |

`NO_FLOW` and `UNREFERENCED` tables are the possible orphans; `schema_use_case` (from `<UC>_DSSWORK` / `<UC>_DSSOUT`) gives their use case even without a project.

The space of a table is counted once. For one day and one database, the total of `table_ownership` equals the total of `tables`, and the owned space of all projects plus the space of the tables no project owns equals it too. `read_mb` and `no_flow_mb` are never added to `owned_mb`. Per use case, group `project_consumption` by `use_case`, and `table_ownership` by `schema_use_case` for the space no project owns.

## Use case of a project

`dbmon.use_case.project_use_case(project_key, tags)` is the only place that decides the use case of a DSS project. It reads the `CDP-UseCase` global tag category (`CDP-UseCase:<use case>` in the project's tags), and falls back on the current criteria until every project is tagged.

## Compatibility

Python 3.9 and later, DSS 13.2 and 15.2. Step 1 only uses `SQLExecutor2.query_to_df` and dataset reads and writes. Step 2 uses the public API (`dataikuapi`), checked against the 13.4.4 sources.
