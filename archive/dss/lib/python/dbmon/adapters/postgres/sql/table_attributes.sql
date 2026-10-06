-- dbmon PostgreSQL adapter, fact "table_attributes": one row per table, kind and statistics.
-- Tables are pg_class relations of kind table, partitioned table, materialized view or
-- foreign table; partitions are left out (rolled up into the root). A partitioned table
-- takes the sums and latest analysis of its leaf partitions.
-- reltuples is -1 (PostgreSQL 14+) or 0 (older) before the first ANALYZE.
-- Rendered by dbmon/adapters/postgres, not runnable as-is. Placeholders:
--   scope_filter     schemas in scope (default: every non-system schema)
--   table_filter     keeps one table
WITH
scope AS (
  SELECT n.oid, n.nspname
  FROM   pg_namespace n
  WHERE  {{scope_filter}}
)
SELECT s.nspname                                                AS schema_name,
       c.relname                                                AS table_name,
       CASE
         WHEN c.relkind = 'p'        THEN 'PARTITIONED'
         WHEN c.relkind = 'm'        THEN 'MATERIALIZED VIEW'
         WHEN c.relkind = 'f'        THEN 'FOREIGN'
         WHEN c.relpersistence = 't' THEN 'TEMPORARY'
         WHEN c.relpersistence = 'u' THEN 'UNLOGGED'
         ELSE 'HEAP'
       END                                                      AS table_kind,
       CASE WHEN c.relkind IN ('r', 'm') THEN 'YES' ELSE 'NO' END AS storage_created,
       CASE WHEN c.relkind = 'p' THEN leaves.row_estimate
            ELSE NULLIF(c.reltuples, -1) END                    AS row_estimate,
       CAST(NULL AS numeric)                                    AS avg_row_len,
       CASE WHEN c.relkind = 'p' THEN leaves.last_analyzed
            ELSE GREATEST(st.last_analyze, st.last_autoanalyze) END AS last_analyzed,
       -- engine details
       c.relpersistence                                         AS persistence,
       leaves.n_partitions
FROM   pg_class c
       JOIN scope s ON s.oid = c.relnamespace
       LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
       LEFT JOIN LATERAL (
         SELECT SUM(GREATEST(l.reltuples, 0))                          AS row_estimate,
                MAX(GREATEST(ls.last_analyze, ls.last_autoanalyze))    AS last_analyzed,
                COUNT(*)                                               AS n_partitions
         FROM   pg_partition_tree(c.oid::regclass) p
                JOIN pg_class l ON l.oid = p.relid
                LEFT JOIN pg_stat_all_tables ls ON ls.relid = l.oid
         WHERE  p.isleaf
         AND    c.relkind = 'p'
       ) leaves ON c.relkind = 'p'
WHERE  c.relkind IN ('r', 'p', 'm', 'f')
AND    NOT c.relispartition
{{table_filter}}
ORDER  BY s.nspname, c.relname
