-- dbmon PostgreSQL adapter, fact "schemas": one row per schema and tablespace it is tied
-- to: the database default tablespace, and the tablespaces holding its relations.
-- PostgreSQL has no quotas and no recycle bin: those columns are NULL.
-- Rendered by dbmon/adapters/postgres, not runnable as-is. Placeholders:
--   scope_filter     schemas in scope (default: every non-system schema)
WITH
scope AS (
  SELECT n.oid, n.nspname, pg_get_userbyid(n.nspowner) AS owner
  FROM   pg_namespace n
  WHERE  {{scope_filter}}
),
db AS (
  SELECT d.dattablespace AS default_ts
  FROM   pg_database d
  WHERE  d.datname = current_database()
),
-- tablespaces holding relations of each schema (reltablespace 0 = the database default)
used AS (
  SELECT DISTINCT c.relnamespace AS nsp,
         CASE WHEN c.reltablespace = 0 THEN db.default_ts ELSE c.reltablespace END AS ts
  FROM   pg_class c
         CROSS JOIN db
  WHERE  c.relnamespace IN (SELECT s.oid FROM scope s)
  AND    c.relkind IN ('r', 'i', 'm', 'S')
),
pairs AS (
  SELECT s.oid AS nsp, db.default_ts AS ts FROM scope s CROSS JOIN db
  UNION
  SELECT u.nsp, u.ts FROM used u
)
SELECT s.nspname                                                AS schema_name,
       s.owner,
       t.spcname                                                AS tablespace_name,
       CASE WHEN p.ts = db.default_ts THEN 'Y' ELSE 'N' END     AS is_default_ts,
       CAST(NULL AS text)                                       AS quota_status,
       CAST(NULL AS numeric)                                    AS quota_max_mb,
       CAST(NULL AS numeric)                                    AS quota_used_mb,
       CAST(NULL AS numeric)                                    AS reclaimable_mb
FROM   pairs p
       JOIN scope s         ON s.oid = p.nsp
       JOIN pg_tablespace t ON t.oid = p.ts
       CROSS JOIN db
ORDER  BY s.nspname, t.spcname
