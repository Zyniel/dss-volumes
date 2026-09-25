-- dbmon Oracle adapter, fact "table_attributes": one row per table, kind and statistics.
-- Same table list as 03_table_usage.sql (repository root): IOT overflow / mapping
-- tables and nested table storage are left out, their segments belong to the parent.
-- Rendered by dbmon/adapters/oracle, not runnable as-is. Placeholders:
--   scope_filter     schemas in scope (default: non Oracle-maintained)
--   table_filter     keeps one table
WITH
scope AS (
  SELECT /*+ MATERIALIZE */
         u.username
  FROM   dba_users u
  WHERE  {{scope_filter}}
)
SELECT t.owner                                                    AS schema_name,
       t.table_name,
       CASE
         WHEN t.temporary = 'Y'          THEN 'TEMPORARY'
         WHEN t.iot_type = 'IOT'         THEN 'IOT'
         WHEN t.cluster_name IS NOT NULL THEN 'CLUSTERED'
         WHEN t.partitioned = 'YES'      THEN 'PARTITIONED'
         ELSE 'HEAP'
       END                                                        AS table_kind,
       t.segment_created                                          AS storage_created,
       t.num_rows                                                 AS row_estimate,
       t.avg_row_len,
       t.last_analyzed,
       -- engine details
       t.logging,
       t.cluster_name,
       t.degree
FROM   dba_tables t
WHERE  t.owner IN (SELECT s.username FROM scope s)
AND    t.dropped = 'NO'
AND    NVL(t.iot_type, 'IOT') = 'IOT'
AND    t.nested = 'NO'
{{table_filter}}
ORDER  BY t.owner, t.table_name
