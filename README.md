# dbmon: storage of the DSS databases

Shows how much the Oracle and PostgreSQL databases used by DSS store, and who stores it: per database, storage unit (tablespace), use case, project, table and object. Each database is read through a dedicated DSS connection with an admin account.

Earlier work is kept as reference only in [archive/](archive/README.md).

## Flow

```
databases (editable) ─► 1 Collect     (SQL per engine)  ─► storage_units, objects, collect_log
DSS API              ─► 2 Inventory   (DSS API)         ─► dss_projects, dss_datasets, dss_connections
                        3 Reconcile   (visual recipes)  ─► tables, objects with use case, project, status
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
├── collect.py            runs the SQL files through each admin connection
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
3. Create an editable dataset `databases` with two columns: `connection` (the admin connection) and `engine` (`oracle` or `postgresql`).
4. Create a Python recipe with `databases` as input and `storage_units`, `objects` and `collect_log` as outputs. Paste [recipes/collect_storage.py](recipes/collect_storage.py).
5. Run it from a daily scenario. To keep the history, tick "Append instead of overwrite" on the outputs.

### Datasets

| Dataset | One row per | Columns |
|---|---|---|
| `storage_units` | tablespace | [oracle](dbmon/sql/oracle/storage_units.sql), [postgresql](dbmon/sql/postgresql/storage_units.sql) |
| `objects` | object and tablespace, partitions summed | [oracle](dbmon/sql/oracle/objects.sql), [postgresql](dbmon/sql/postgresql/objects.sql) |
| `collect_log` | database and run | `status` OK or FAILED, `error`, `seconds`, rows per result |

The size of a table is the sum of its objects (`parent_schema`, `parent_table`): data, indexes, LOB segments, all partitions. The application data of a database is the sum of its objects. Its allocated space is the sum of its tablespaces on Oracle; PostgreSQL tablespaces are shared by the whole cluster, so their space is known per server.

Every row of `storage_units` and `objects` starts with `collected_utc`, `dss_connection` and the identity of the database ([identity.sql](dbmon/sql/oracle/identity.sql)): `engine`, `server_name`, `database_name`, `unique_name`, `role`, `version`. PostgreSQL tablespaces belong to the whole cluster: they are kept once per server, with an empty `database_name`.

### Changing a query

- Each SQL file runs as-is in SQL*Plus, SQLcl or psql: try a change there first.
- Both engines return the same columns in the same order, because their rows go to the same dataset. A column that does not exist on one engine is `NULL` there.
- Oracle files keep SQL*Plus rules: no blank line inside the statement, no `&`.

## Use case of a project

`dbmon.use_case.project_use_case(project_key, tags)` is the only place that decides the use case of a DSS project. It reads the `CDP-UseCase` global tag category (`CDP-UseCase:<use case>` in the project's tags), and falls back on the current criteria until every project is tagged.

## Compatibility

Python 3.9 and later, DSS 13.2 and 15.2. Step 1 only uses `SQLExecutor2.query_to_df` and dataset reads and writes.
