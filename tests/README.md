# Tests

Run against two local test databases in Docker. No DSS is needed: `harness.py` stands in for the `dataiku` module and runs the SQL inside the containers, and `fake_dss.py` stands in for the DSS API client.

```
python tests/setup_containers.py     # start the containers, load the test data (safe to run again)
python tests/test_collect.py         # step 1: SQL files and dbmon.collect, both engines
python tests/test_inventory.py       # step 2: dbmon.inventory
python tests/test_analysis.py        # step 3: the Oracle SQL recipes, in a scratch schema DBMON_STORE
python tests/test_recipes.py         # the two recipe files, run as DSS would
```

Each test prints one line per check (`ok` or `FAIL`) and exits with 1 when a check fails.

| Container | Image | Test data |
|---|---|---|
| `dbmon-pg` | `postgres:16-alpine` | [fixtures/postgresql.sql](fixtures/postgresql.sql): use-case schemas, TOAST, partitions, a materialized view, a sequence, a tablespace, search_path settings |
| `dbmon-ora` | `gvenzl/oracle-free:latest` | [fixtures/oracle.sql](fixtures/oracle.sql): use-case tablespaces and quotas, LOBs, partitions, an IOT with overflow, a materialized view, a temporary and an external table, the recycle bin, a table restored from it |

The admin accounts have the same rights as in production: `pg_monitor` on PostgreSQL, `CREATE SESSION` and `SELECT_CATALOG_ROLE` on Oracle. Their passwords are test values of these local containers only.

Change a SQL file: run `test_collect.py`. It checks that the file still runs as-is, that both engines still return the same columns, and the sizes on the test data.
