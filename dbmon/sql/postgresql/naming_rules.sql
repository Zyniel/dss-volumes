-- =============================================================================
-- naming_rules.sql (PostgreSQL)
-- One row per schema of the database (system schemas left out, as in objects.sql) and
-- per tablespace of the cluster, with the use case and layer its name tells. The
-- collector copies them onto objects and tables (by schema) and onto storage_units (by
-- tablespace). Tablespaces are shared by the whole cluster: kept once per server.
-- -----------------------------------------------------------------------------
-- name_kind          SCHEMA or TABLESPACE
-- name               the schema or tablespace
-- detected_use_case  default rule: <UC> in <UC>_DSSWORK / <UC>_DSSOUT for a schema,
--                    <UC>_DSSWORK_<anything> / <UC>_DSSOUT_<anything> for a tablespace
--                    (upper case). NULL outside the convention
-- detected_layer     DSSWORK or DSSOUT. NULL outside the convention
-- Databases with other rules: copy this file into sql/postgresql-<variant>/ and change
-- the patterns or the expressions; set engine = postgresql-<variant> in databases.
-- =============================================================================
WITH
names AS (
  SELECT 'SCHEMA'                                                   AS name_kind,
         n.nspname                                                  AS name,
         '^(.+)_(DSSWORK|DSSOUT)$'                                  AS pattern
  FROM   pg_namespace n
  WHERE  n.nspname NOT LIKE 'pg\_%'
  AND    n.nspname <> 'information_schema'
  UNION ALL
  SELECT 'TABLESPACE',
         t.spcname,
         '^(.+)_(DSSWORK|DSSOUT)(_.*)?$'
  FROM   pg_tablespace t
)
SELECT n.name_kind,
       CAST(n.name AS text)                                         AS name,
       (regexp_match(upper(n.name), n.pattern))[1]                  AS detected_use_case,
       (regexp_match(upper(n.name), n.pattern))[2]                  AS detected_layer
FROM   names n
ORDER  BY n.name_kind, n.name;
