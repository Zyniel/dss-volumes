# dbmon: storage of the DSS databases

Shows how much the Oracle and PostgreSQL databases used by DSS store, and who stores it: per database, storage unit (tablespace), use case, project, table and object. Each database is read through a dedicated DSS connection with an admin account.

Earlier work is kept as reference only in [archive/](archive/README.md).

## Flow

```
databases (editable) ─► 1 Collect     (SQL per engine)  ─► storage_units, objects, tables, collect_log
DSS API              ─► 2 Inventory   (DSS API)         ─► dss_projects, dss_datasets, dss_connections
                        3 Reconcile   (visual recipes)  ─► tables and objects with use case, project, status
                        4 Aggregate   (Group recipes)   ─► by database, storage unit, use case, project
```

| Step | State |
|---|---|
| 1 Collect | done |
| 2 Inventory | to do |
| 3 Reconcile | to do |
| 4 Aggregate | to do |

## Layout

```
dbmon/                    Python package, imported in the DSS project library as python/dbmon
├── collect.py            runs the SQL files through each admin connection, sums objects per table
├── use_case.py           use case of a DSS project
└── sql/
    ├── oracle/           identity, storage_units, objects
    └── postgresql/       the same three files
recipes/
└── collect_storage.py    code of the step 1 recipe
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

4. Create a Python recipe with `databases` as input and `storage_units`, `objects`, `tables` and `collect_log` as outputs. Paste [recipes/collect_storage.py](recipes/collect_storage.py).
5. Partition the four outputs by day: time dimension `snapshot_day`, period Day. On a SQL connection, partition on the column `snapshot_day`. Running the recipe again the same day replaces that day, and old days can be dropped by partition. The recipe refuses to build any partition but today's: the databases can only be read as they are now.
6. Create the scenario:
   - Step "Build": the four outputs, partition `CURRENT_DAY`.
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
- Oracle files keep SQL*Plus rules: no blank line inside the statement, no `&`.

## Use case of a project

`dbmon.use_case.project_use_case(project_key, tags)` is the only place that decides the use case of a DSS project. It reads the `CDP-UseCase` global tag category (`CDP-UseCase:<use case>` in the project's tags), and falls back on the current criteria until every project is tagged.

## Compatibility

Python 3.9 and later, DSS 13.2 and 15.2. Step 1 only uses `SQLExecutor2.query_to_df` and dataset reads and writes.
