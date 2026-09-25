-- dbmon Oracle adapter, fact "lifecycle": creation, last DDL and changes since the last
-- statistics, per table. Derived from 03b_table_lifecycle.sql (repository root): each
-- dictionary view is read once and materialised before the hash joins, and
-- DBA_TAB_STATISTICS is not used (row-by-row STALE_STATS, and a second row per table
-- with Exadata real-time statistics). dml_pct_since_stats and stale_est are computed by
-- the core, the same way for every engine.
-- Rendered by dbmon/adapters/oracle, not runnable as-is. Placeholders:
--   scope_filter     schemas in scope (default: non Oracle-maintained)
--   table_filter     keeps one table in DBA_TABLES
--   object_filter    same, in DBA_OBJECTS
--   mods_filter      same, in DBA_TAB_MODIFICATIONS
WITH
scope AS (
  SELECT /*+ MATERIALIZE */
         u.username
  FROM   dba_users u
  WHERE  {{scope_filter}}
),
tabs AS (
  SELECT /*+ MATERIALIZE */
         t.owner, t.table_name, t.num_rows, t.last_analyzed
  FROM   dba_tables t
  WHERE  t.owner IN (SELECT s.username FROM scope s)
  AND    t.dropped = 'NO'
  AND    NVL(t.iot_type, 'IOT') = 'IOT'
  AND    t.nested = 'NO'
  {{table_filter}}
),
objs AS (
  SELECT /*+ MATERIALIZE */
         o.owner, o.object_name, o.created, o.last_ddl_time
  FROM   dba_objects o
  WHERE  o.owner IN (SELECT s.username FROM scope s)
  AND    o.object_type = 'TABLE'
  AND    o.subobject_name IS NULL
  {{object_filter}}
),
-- DML since the last statistics gathering, flushed from memory periodically
mods AS (
  SELECT /*+ MATERIALIZE */
         m.table_owner, m.table_name, m.inserts, m.updates, m.deletes, m.truncated,
         m.timestamp                                               AS last_dml_ts
  FROM   dba_tab_modifications m
  WHERE  m.table_owner IN (SELECT s.username FROM scope s)
  AND    m.partition_name    IS NULL
  AND    m.subpartition_name IS NULL
  {{mods_filter}}
)
SELECT /*+ USE_HASH(o m) */
       t.owner                                                     AS schema_name,
       t.table_name,
       o.created,
       o.last_ddl_time,
       t.num_rows                                                  AS row_estimate,
       t.last_analyzed,
       NVL(m.inserts, 0) + NVL(m.updates, 0) + NVL(m.deletes, 0)   AS dml_since_stats,
       NVL(m.truncated, 'NO')                                      AS truncated_since_stats,
       -- engine details
       m.inserts                                                   AS dml_inserts,
       m.updates                                                   AS dml_updates,
       m.deletes                                                   AS dml_deletes,
       m.last_dml_ts
FROM   tabs t
       LEFT JOIN objs o ON  o.owner       = t.owner
                        AND o.object_name = t.table_name
       LEFT JOIN mods m ON  m.table_owner = t.owner
                        AND m.table_name  = t.table_name
ORDER  BY t.owner, t.table_name
