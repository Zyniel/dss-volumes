-- =============================================================================
-- storage_units.sql (PostgreSQL)
-- One row per tablespace of the cluster. Tablespaces are shared by all the databases
-- of a cluster: the collector keeps them once per server, with database_name empty.
-- -----------------------------------------------------------------------------
-- contents          always PERMANENT
-- location          directory of the tablespace (pg_default: base, pg_global: global,
--                   both in the data directory)
-- allocated_mb      size of the directory. A tablespace preallocates nothing, so
--                   used_mb is the same, and free_mb / max_mb are NULL: the free space
--                   of the file system is not visible from SQL
-- n_files, recyclebin_mb, raw_allocated_mb: Oracle only, NULL
--
-- Needs pg_monitor (or a superuser): pg_tablespace_size needs pg_read_all_stats, and
-- data_directory needs pg_read_all_settings.
-- =============================================================================
SELECT t.spcname                                                    AS tablespace_name,
       'PERMANENT'                                                  AS contents,
       CASE t.spcname
         WHEN 'pg_default' THEN current_setting('data_directory') || '/base'
         WHEN 'pg_global'  THEN current_setting('data_directory') || '/global'
         ELSE pg_tablespace_location(t.oid)
       END                                                          AS location,
       CAST(NULL AS integer)                                        AS n_files,
       ROUND(pg_tablespace_size(t.oid) / 1048576.0, 3)              AS allocated_mb,
       ROUND(pg_tablespace_size(t.oid) / 1048576.0, 3)              AS used_mb,
       CAST(NULL AS numeric)                                        AS recyclebin_mb,
       CAST(NULL AS numeric)                                        AS free_mb,
       CAST(NULL AS numeric)                                        AS max_mb,
       CAST(NULL AS numeric)                                        AS raw_allocated_mb
FROM   pg_tablespace t
ORDER  BY t.spcname;
