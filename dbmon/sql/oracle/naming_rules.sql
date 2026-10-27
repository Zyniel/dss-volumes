-- =============================================================================
-- naming_rules.sql (Oracle)
-- One row per schema (not Oracle-maintained) and per tablespace, with the use case and
-- layer its name tells. The collector copies them onto objects and tables (by schema)
-- and onto storage_units (by tablespace).
-- -----------------------------------------------------------------------------
-- name_kind          SCHEMA or TABLESPACE
-- name               the schema or tablespace
-- detected_use_case  default rule: <UC> in <UC>_DSSWORK / <UC>_DSSOUT for a schema,
--                    <UC>_DSSWORK_<anything> / <UC>_DSSOUT_<anything> for a tablespace
--                    (upper case). NULL outside the convention
-- detected_layer     DSSWORK or DSSOUT. NULL outside the convention
-- Databases with other rules: copy this file into sql/oracle-<variant>/ and change
-- the patterns or the expressions; set engine = oracle-<variant> in databases.
-- =============================================================================
WITH
names AS (
  SELECT 'SCHEMA'                                                   AS name_kind,
         u.username                                                 AS name,
         '^(.+)_(DSSWORK|DSSOUT)$'                                  AS pattern
  FROM   dba_users u
  WHERE  u.oracle_maintained = 'N'
  UNION ALL
  SELECT 'TABLESPACE',
         t.tablespace_name,
         '^(.+)_(DSSWORK|DSSOUT)(_.*)?$'
  FROM   dba_tablespaces t
)
SELECT n.name_kind,
       n.name,
       REGEXP_SUBSTR(UPPER(n.name), n.pattern, 1, 1, NULL, 1)       AS detected_use_case,
       REGEXP_SUBSTR(UPPER(n.name), n.pattern, 1, 1, NULL, 2)       AS detected_layer
FROM   names n
ORDER  BY n.name_kind, n.name;
