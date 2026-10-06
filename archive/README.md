# Archive

Earlier developments for the database storage monitoring requirement, kept as inspiration for the new EPIC. Nothing here is maintained or used by current code. The files were moved here unchanged: `git log --follow <file>` shows their history.

| Folder | Content | State |
|---|---|---|
| `oracle-queries/` | Six standalone Oracle 19c queries, run once per database or PDB: tablespaces (01), UC objects (02), UC tables (03), table lifecycle (03b), AWR daily tablespace history (04), AWR object activity (05). Its README explains how to run and stack them. | Complete. 04 and 05 need the Diagnostics Pack. |
| `dss/` | `dbmon`, a Dataiku project library built from those queries: a column contract (`model.py`, `COLUMNS.md`), one adapter per engine (Oracle, PostgreSQL), collection across connections, an optional use-case layer (`uc.py`). | Prototype, see below. |

## State of `dss/`

- The code and `COLUMNS.md` are the last version (commit `66962e7`). `README.md` and `examples/` still describe the previous Oracle-only version (`owners=`, `engine.py`), which is commit `010388e`.
- The PostgreSQL adapter was tested against PostgreSQL 16 with a `pg_monitor`-only account. The Oracle adapter was only tested with canned results.
- `tests/test_dbmon.py` needs a PostgreSQL 16 container named `dbmon-pg`, loaded with `tests/pg_fixture.sql`. Run it as `python archive/dss/tests/test_dbmon.py archive`.
- Left out of the library: the AWR queries (04, 05), and the use-case configuration checks of 01 (`config_issues`, and live space split by expected owner, other UC and non-UC schemas).

## Worth reusing

**Oracle**
- Compute free space as datafile size minus segments: `DBA_FREE_SPACE` becomes very slow when the recycle bin is large.
- Recognise recycle bin segments by joining `DBA_RECYCLEBIN` on owner and name, not by the `BIN$` prefix: indexes and LOB segments restored by `FLASHBACK TABLE … TO BEFORE DROP` keep their `BIN$` names.
- Avoid `DBA_TAB_STATISTICS`: `STALE_STATS` is computed row by row, and Exadata real-time statistics add a second row per table.
- Read each dictionary view once, in a `/*+ MATERIALIZE */` CTE, so that the join predicate is not pushed into `DBA_OBJECTS` or `DBA_TAB_MODIFICATIONS`. This fixed the lifecycle part of 03 on production (see 03b).
- `CON_DBID` can be 0: use `NVL(NULLIF(con_dbid, 0), dbid)`.
- On RAC the `DBA_` views cover the whole database: query once, with `V$` views. `GV$` returns one row per instance.
- `V$ASM_DISKGROUP_STAT` gives ASM capacity without disk discovery.

**PostgreSQL**
- Tablespaces belong to the cluster, not to a database: collect them once per server.
- TOAST tables play the role of LOB segments. Partitions roll up to `pg_partition_root`.
- `reltuples` is -1 before the first `ANALYZE` (PostgreSQL 14+).
- `pg_monitor` is enough: it includes `pg_read_all_stats`, which `pg_tablespace_size` needs.

**Other engines** (desk review, no code written)
- "Database" is the level above schemas in Oracle (PDB), PostgreSQL and Databricks (catalog), but the schema level itself in MySQL, Teradata and MongoDB.
- Every engine reports table size and a row count. Tablespace placement, quotas, LOB size and changes since the last statistics exist only on some.
- MySQL 8 caches `information_schema.TABLES` statistics for 24 hours: run `SET SESSION information_schema_stats_expiry = 0` first.
- Teradata space views return one row per AMP: sum them, and compare the fullest AMP with the average to measure skew. Sizes include the fallback copy.
- Databricks sizes need one `DESCRIBE DETAIL` per table and cover the current version only. MongoDB is not SQL (`$collStats`, `dbStats`).
