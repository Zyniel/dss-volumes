-- dbmon PostgreSQL adapter, fact "lifecycle": changes since the last statistics, per table.
-- PostgreSQL does not record creation or DDL times, nor truncations since ANALYZE:
-- those columns are NULL. n_mod_since_analyze counts the rows modified since the last
-- analysis; a partitioned table sums its leaf partitions. Statistics counters restart
-- after a crash or pg_stat_reset().
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
       CAST(NULL AS timestamp)                                  AS created,
       CAST(NULL AS timestamp)                                  AS last_ddl_time,
       CASE WHEN c.relkind = 'p' THEN leaves.row_estimate
            ELSE NULLIF(c.reltuples, -1) END                    AS row_estimate,
       CASE WHEN c.relkind = 'p' THEN leaves.last_analyzed
            ELSE GREATEST(st.last_analyze, st.last_autoanalyze) END AS last_analyzed,
       CASE WHEN c.relkind = 'p' THEN leaves.n_mod_since_analyze
            ELSE st.n_mod_since_analyze END                     AS dml_since_stats,
       CAST(NULL AS text)                                       AS truncated_since_stats,
       -- engine details
       st.n_live_tup,
       st.n_dead_tup,
       st.last_vacuum,
       st.last_autovacuum
FROM   pg_class c
       JOIN scope s ON s.oid = c.relnamespace
       LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
       LEFT JOIN LATERAL (
         SELECT SUM(GREATEST(l.reltuples, 0))                          AS row_estimate,
                MAX(GREATEST(ls.last_analyze, ls.last_autoanalyze))    AS last_analyzed,
                SUM(ls.n_mod_since_analyze)                            AS n_mod_since_analyze
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
