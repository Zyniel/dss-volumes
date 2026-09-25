-- dbmon PostgreSQL adapter, fact "tablespaces": one row per tablespace of the cluster.
-- Tablespaces are shared by every database of the cluster: collected once per server.
-- A PostgreSQL tablespace is a directory: no preallocation, no ceiling, no free space
-- visible from SQL, so allocated = used and free / max are NULL.
-- pg_tablespace_size needs pg_read_all_stats (part of pg_monitor) or CREATE on it.
-- Rendered by dbmon/adapters/postgres, not runnable as-is. Placeholders:
--   ts_filter        tablespaces returned (default: all)
SELECT t.spcname                                                AS tablespace_name,
       'PERMANENT'                                              AS contents,
       ROUND(pg_tablespace_size(t.oid) / 1048576.0, 3)          AS allocated_mb,
       ROUND(pg_tablespace_size(t.oid) / 1048576.0, 3)          AS used_mb,
       CAST(NULL AS numeric)                                    AS reclaimable_mb,
       CAST(NULL AS numeric)                                    AS free_mb,
       CAST(NULL AS numeric)                                    AS max_mb,
       -- engine details
       pg_get_userbyid(t.spcowner)                              AS spc_owner,
       NULLIF(pg_tablespace_location(t.oid), '')                AS location
FROM   pg_tablespace t
{{ts_filter}}
ORDER  BY t.spcname
