-- =============================================================================
-- storage_units.sql (Oracle)
-- One row per tablespace of the database (PDB or non-CDB), all schemas included.
-- -----------------------------------------------------------------------------
-- contents          PERMANENT, TEMPORARY or UNDO
-- location          ASM disk groups (+DATAC1...) or directories holding the files
-- n_files           datafiles or tempfiles
-- allocated_mb      size of the files
-- used_mb           live segments. TEMPORARY tablespaces: space in use right now
-- recyclebin_mb     segments of dropped objects still in the recycle bin. Oracle reuses
--                   this space before extending a file, but it is still held
-- free_mb           allocated - used - recyclebin (includes file headers: a few MB)
-- max_mb            size the files can autoextend to (allocated without autoextend)
-- raw_allocated_mb  allocated x ASM redundancy (2 for NORMAL, 3 for HIGH): disk space
--                   actually taken. NULL outside ASM or for other redundancy types
--
-- DBA_FREE_SPACE is not used on purpose: it becomes very slow with a large recycle bin.
-- Recycle bin segments are recognised by name in DBA_RECYCLEBIN, not by the BIN$ prefix:
-- indexes and LOB segments restored by FLASHBACK TABLE ... TO BEFORE DROP keep BIN$ names.
-- Needs SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY), including V$ASM_DISKGROUP_STAT.
-- =============================================================================
WITH
files AS (
  SELECT f.tablespace_name,
         f.bytes,
         CASE WHEN f.autoextensible = 'YES' THEN GREATEST(f.maxbytes, f.bytes)
              ELSE f.bytes
         END                                                         AS max_bytes,
         CASE WHEN f.file_name LIKE '+%' THEN REGEXP_SUBSTR(f.file_name, '^\+[^/]+')
              ELSE SUBSTR(f.file_name, 1, INSTR(f.file_name, '/', -1) - 1)
         END                                                         AS location
  FROM  (SELECT tablespace_name, file_name, bytes, maxbytes, autoextensible FROM dba_data_files
         UNION ALL
         SELECT tablespace_name, file_name, bytes, maxbytes, autoextensible FROM dba_temp_files) f
),
-- _STAT variant: no disk discovery, safe to query from a database instance
asm AS (
  SELECT '+' || g.name                                               AS location,
         MAX(CASE g.type WHEN 'EXTERN' THEN 1
                         WHEN 'NORMAL' THEN 2
                         WHEN 'HIGH'   THEN 3
             END)                                                    AS raw_factor
  FROM   v$asm_diskgroup_stat g
  GROUP  BY g.name
),
file_agg AS (
  SELECT f.tablespace_name,
         COUNT(*)                                                    AS n_files,
         SUM(f.bytes)                                                AS alloc_bytes,
         SUM(f.max_bytes)                                            AS max_bytes,
         CASE WHEN COUNT(a.raw_factor) = COUNT(*)
              THEN SUM(f.bytes * a.raw_factor)
         END                                                         AS raw_bytes,
         LISTAGG(DISTINCT f.location, ',') WITHIN GROUP (ORDER BY f.location) AS location
  FROM   files f
         LEFT JOIN asm a ON a.location = f.location
  GROUP  BY f.tablespace_name
),
rb AS (
  SELECT DISTINCT r.owner, r.object_name
  FROM   dba_recyclebin r
),
seg_agg AS (
  SELECT s.tablespace_name,
         SUM(CASE WHEN rb.object_name IS NULL THEN s.bytes ELSE 0 END) AS live_bytes,
         SUM(CASE WHEN rb.object_name IS NULL THEN 0 ELSE s.bytes END) AS rb_bytes
  FROM   dba_segments s
         LEFT JOIN rb ON rb.owner = s.owner AND rb.object_name = s.segment_name
  GROUP  BY s.tablespace_name
),
-- current use of TEMPORARY tablespaces
tum AS (
  SELECT m.tablespace_name, m.used_space
  FROM   dba_tablespace_usage_metrics m
)
SELECT t.tablespace_name,
       t.contents,
       f.location,
       f.n_files,
       ROUND(f.alloc_bytes / 1048576, 3)                                 AS allocated_mb,
       ROUND(CASE WHEN t.contents = 'TEMPORARY' THEN m.used_space * t.block_size
                  ELSE NVL(s.live_bytes, 0)
             END / 1048576, 3)                                           AS used_mb,
       CASE WHEN t.contents <> 'TEMPORARY'
            THEN ROUND(NVL(s.rb_bytes, 0) / 1048576, 3)
       END                                                               AS recyclebin_mb,
       ROUND((f.alloc_bytes
              - CASE WHEN t.contents = 'TEMPORARY' THEN m.used_space * t.block_size
                     ELSE NVL(s.live_bytes, 0) + NVL(s.rb_bytes, 0)
                END) / 1048576, 3)                                       AS free_mb,
       ROUND(f.max_bytes / 1048576, 3)                                   AS max_mb,
       ROUND(f.raw_bytes / 1048576, 3)                                   AS raw_allocated_mb
FROM   dba_tablespaces t
       LEFT JOIN file_agg f ON f.tablespace_name = t.tablespace_name
       LEFT JOIN seg_agg  s ON s.tablespace_name = t.tablespace_name
       LEFT JOIN tum      m ON m.tablespace_name = t.tablespace_name
ORDER  BY t.tablespace_name;
