-- =============================================================================
-- role_settings.sql (PostgreSQL)
-- One row per search_path set with ALTER ROLE ... SET search_path or
-- ALTER DATABASE ... SET search_path. Settings belong to the whole cluster: the
-- collector keeps them once per server, with database_name empty.
-- Used by the DSS inventory to tell which schema a dataset without schema points to.
-- -----------------------------------------------------------------------------
-- role_name         role the setting applies to. NULL: every role
-- setting_database  database the setting applies to. NULL: every database
-- search_path       the value, e.g. "$user", public
-- Without any row, PostgreSQL uses its default search_path: "$user", public
-- (unless postgresql.conf changes it).
-- =============================================================================
SELECT r.rolname                                                    AS role_name,
       d.datname                                                    AS setting_database,
       substring(c.setting FROM '^search_path=(.*)$')               AS search_path
FROM   pg_db_role_setting s
       LEFT JOIN pg_roles r    ON r.oid = s.setrole
       LEFT JOIN pg_database d ON d.oid = s.setdatabase
       CROSS JOIN LATERAL unnest(s.setconfig) AS c(setting)
WHERE  c.setting LIKE 'search\_path=%'
ORDER  BY r.rolname, d.datname;
