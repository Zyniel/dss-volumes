# dbmon columns

Generated from `dbmon/model.py`. Every row starts with the identity columns, then the keys, then the columns of its result, then `details`.

## Identity (every row)

| Column | Meaning |
|---|---|
| `collected_utc` | start of the call that collected the row (UTC) |
| `dss_connection` | Dataiku connection the row was read through |
| `engine` | oracle or postgresql |
| `server_uid` | key of the server: Oracle CDB (or non-CDB), PostgreSQL cluster |
| `db_uid` | key of the database: Oracle PDB (or non-CDB), PostgreSQL database. NULL on rows that belong to the whole server |
| `server_name` | Oracle DB_NAME, PostgreSQL cluster_name or host:port |
| `database_name` | Oracle container name, PostgreSQL database. NULL on server rows |
| `unique_name` | physical copy that answered: Oracle DB_UNIQUE_NAME, PostgreSQL host:port |
| `role` | PRIMARY or STANDBY |
| `version` | server version |
| label columns | extra keys given with the connections, after `collected_utc` |

## tablespaces

One row per tablespace. Fact returned by the adapters. Grain: tablespace_name. Filters: tablespaces.

| Column | Kind | Meaning |
|---|---|---|
| `tablespace_uid` | key | tablespace: tablespace_name, within its storage |
| `tablespace_name` | text | tablespace. |
| `contents` | text | PERMANENT, TEMPORARY or UNDO. |
| `allocated_mb` | number (MB) | space the tablespace takes on storage. |
| `used_mb` | number (MB) | space used by live objects. |
| `reclaimable_mb` | number (MB) | space held by dropped objects the engine reuses before growing (Oracle recycle bin). NULL where the concept does not exist. |
| `free_mb` | number (MB) | free space inside the allocated files. NULL when unknown. |
| `max_mb` | number (MB) | declared ceiling. NULL when unbounded or unknown. |
| `details` | text | engine-only columns, as a JSON object |

## schemas

One row per schema and tablespace it is tied to: default tablespace, quotas, tablespaces holding its objects, recycle bin. Fact returned by the adapters. Grain: schema_name, tablespace_name. Filters: schemas.

| Column | Kind | Meaning |
|---|---|---|
| `schema_uid` | key | schema: schema_name, within its database |
| `tablespace_uid` | key | tablespace: tablespace_name, within its storage |
| `schema_name` | text | schema. |
| `owner` | text | role owning the schema (Oracle: the schema itself). |
| `tablespace_name` | text | tablespace. |
| `is_default_ts` | text | Y for the schema's default tablespace. |
| `quota_status` | text | UNLIMITED, LIMITED or NONE. NULL where quotas do not exist. |
| `quota_max_mb` | number (MB) | quota ceiling. |
| `quota_used_mb` | number (MB) | space charged to the quota. |
| `reclaimable_mb` | number (MB) | dropped objects still held (Oracle recycle bin). |
| `details` | text | engine-only columns, as a JSON object |

## objects

One row per object and tablespace, partitions summed. Fact returned by the adapters. Grain: schema_name, object_name, object_type, tablespace_name. Filters: schemas, table.

| Column | Kind | Meaning |
|---|---|---|
| `object_uid` | key | object: schema_name + object_name + object_type + tablespace_name, within its database |
| `table_uid` | key | table: parent_schema + parent_table, within its database |
| `schema_uid` | key | schema: schema_name, within its database |
| `tablespace_uid` | key | tablespace: tablespace_name, within its storage |
| `schema_name` | text | schema of the object. LOB / TOAST storage: the schema of its table. |
| `object_name` | text | exact name. |
| `object_type` | text | the engine's own type: INDEX, LOBSEGMENT, TOAST TABLE, SEQUENCE.... |
| `family` | text | TABLE, INDEX, LOB, LOBINDEX or OTHER. |
| `parent_schema` | text | schema of the table the object belongs to. |
| `parent_table` | text | table the object belongs to. NULL when there is none. |
| `tablespace_name` | text | tablespace. |
| `ts_kind` | text | DEFAULT (default tablespace), QUOTA (another tablespace where the schema holds a quota) or OTHER. |
| `partitioned` | text | YES when the row sums several partitions. |
| `n_segments` | integer | storage units summed into the row (partitions). |
| `size_mb` | number (MB) | allocated storage. |
| `compression` | text | the engine's compression setting. NULL when not applicable. |
| `details` | text | engine-only columns, as a JSON object |

## table_attributes

One row per table: kind and optimizer statistics. Fact returned by the adapters. Grain: schema_name, table_name. Filters: schemas, table.

| Column | Kind | Meaning |
|---|---|---|
| `table_uid` | key | table: schema_name + table_name, within its database |
| `schema_uid` | key | schema: schema_name, within its database |
| `schema_name` | text | schema. |
| `table_name` | text | table. |
| `table_kind` | text | HEAP, PARTITIONED, IOT, CLUSTERED, TEMPORARY, UNLOGGED, MATERIALIZED VIEW or FOREIGN. |
| `storage_created` | text | YES when the table has storage of its own (NO, or N/A for a partitioned table). |
| `row_estimate` | number | rows according to the optimizer statistics. |
| `avg_row_len` | number (bytes) | average row length from the statistics. NULL when unknown. |
| `last_analyzed` | date | last statistics gathering. |
| `details` | text | engine-only columns, as a JSON object |

## lifecycle

One row per table: creation, last DDL and changes since the last statistics. Fact returned by the adapters. Grain: schema_name, table_name. Filters: schemas, table.

| Column | Kind | Meaning |
|---|---|---|
| `table_uid` | key | table: schema_name + table_name, within its database |
| `schema_uid` | key | schema: schema_name, within its database |
| `schema_name` | text | schema. |
| `table_name` | text | table. |
| `created` | date | creation time. NULL where the engine does not record it. |
| `last_ddl_time` | date | last DDL. NULL where the engine does not record it. |
| `row_estimate` | number | rows according to the optimizer statistics. |
| `last_analyzed` | date | last statistics gathering. |
| `dml_since_stats` | number | rows inserted, updated or deleted since the last statistics. |
| `truncated_since_stats` | text | YES when truncated since the last statistics. NULL when unknown. |
| `dml_pct_since_stats` | number | 100 * dml_since_stats / row_estimate. Computed by dbmon. |
| `stale_est` | text | YES when truncated or when the DML exceeds 10 % of the rows (Oracle's default STALE_PERCENT), NULL when never analysed. Computed by dbmon. |
| `details` | text | engine-only columns, as a JSON object |

## tables

One row per table: its objects rolled up (indexes, LOB / TOAST, partitions), plus its attributes. Objects that belong to no table get a row of their own. Derived from objects, table_attributes. Grain: schema_name, table_name. Filters: schemas, table.

| Column | Kind | Meaning |
|---|---|---|
| `table_uid` | key | table: schema_name + table_name, within its database |
| `schema_uid` | key | schema: schema_name, within its database |
| `schema_name` | text | schema. |
| `table_name` | text | table, or the object's own name when it belongs to no table. |
| `table_kind` | text | from table_attributes, or the object type when it belongs to no table. |
| `storage_created` | text | from table_attributes. |
| `n_segments` | integer | storage units rolled up. |
| `total_mb` | number (MB) | all its objects. |
| `table_mb` | number (MB) | family TABLE. |
| `index_mb` | number (MB) | family INDEX. |
| `lob_mb` | number (MB) | family LOB. |
| `lobindex_mb` | number (MB) | family LOBINDEX. |
| `other_mb` | number (MB) | family OTHER. |
| `in_default_ts_mb` | number (MB) | in the schema's default tablespace. |
| `in_quota_ts_mb` | number (MB) | in another tablespace where the schema holds a quota. |
| `in_other_ts_mb` | number (MB) | anywhere else. |
| `n_tablespaces` | integer | tablespaces holding its objects. |
| `tablespaces` | text | those tablespaces, comma separated. |
| `n_indexes` | integer | indexes. |
| `n_lob_segments` | integer | LOB / TOAST objects. |
| `n_table_partitions` | integer | table partitions. |
| `table_compression` | text | compression settings of its table data, comma separated. |
| `row_estimate` | number | from table_attributes. |
| `avg_row_len` | number (bytes) | from table_attributes. |
| `est_row_data_mb` | number (MB) | row_estimate * avg_row_len: above table_mb means compression gain, far below means empty space. |
| `last_analyzed` | date | from table_attributes. |
| `details` | text | engine-only columns, as a JSON object |

