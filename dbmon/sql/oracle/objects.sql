-- =============================================================================
-- objects.sql (Oracle)
-- One row per storage object and tablespace: a table, index or LOB segment with all
-- its partitions and subpartitions summed. An object whose partitions sit in several
-- tablespaces gives one row per tablespace.
-- Scope: schemas that are not Oracle-maintained. Live segments only: the recycle bin is
-- left out (recognised by name in DBA_RECYCLEBIN, not by the BIN$ prefix).
-- Tables without any segment are listed too, at 0 MB with n_segments = 0: tables never
-- written to (deferred segment creation), global temporary and external tables. So the
-- datasets pointing to them are found. Index-organised and clustered tables store their
-- data in an index or a cluster: they never get such a row.
-- -----------------------------------------------------------------------------
-- object_type    TABLE, INDEX, LOBSEGMENT, LOBINDEX, NESTED TABLE, CLUSTER, TEMPORARY...
-- family         TABLE   table data: heap, partitions, IOT top index and overflow,
--                        nested table storage, cluster
--                INDEX   indexes
--                LOB     LOB segments and their LOB indexes
--                OTHER   anything else, e.g. the TEMPORARY segment of a CREATE TABLE
--                        AS SELECT or an index build in progress
-- parent_schema, parent_table
--                table the object belongs to. NULL when there is none (cluster,
--                temporary segment)
-- partitioned    YES when the row sums partitions or subpartitions
-- n_segments     segments summed in the row
-- size_mb        allocated space
-- compression    table: NONE, BASIC, ADVANCED, QUERY LOW|HIGH, ARCHIVE LOW|HIGH (HCC)
--                index: NONE, ENABLED (prefix), ADVANCED LOW|HIGH
--                LOB: SECUREFILE <level> or BASICFILE
--                Several values when the partitions differ
-- Needs SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY).
-- =============================================================================
WITH
scope AS (
  SELECT /*+ MATERIALIZE */
         u.username
  FROM   dba_users u
  WHERE  u.oracle_maintained = 'N'
),
rb AS (
  SELECT DISTINCT r.owner, r.object_name
  FROM   dba_recyclebin r
),
-- live segments, one row per (sub)partition
seg AS (
  SELECT s.owner, s.segment_name, s.partition_name, s.segment_type, s.tablespace_name, s.bytes
  FROM   dba_segments s
         JOIN scope u ON u.username = s.owner
  WHERE  NOT EXISTS (SELECT 1
                     FROM   rb
                     WHERE  rb.owner       = s.owner
                     AND    rb.object_name = s.segment_name)
),
ix AS (
  SELECT i.owner, i.index_name, i.index_type, i.table_owner, i.table_name, i.compression
  FROM   dba_indexes i
         JOIN scope u ON u.username = i.owner
),
lb AS (
  SELECT l.owner, l.segment_name, l.table_name, l.securefile, l.compression
  FROM   dba_lobs l
         JOIN scope u ON u.username = l.owner
),
tb AS (
  SELECT t.owner, t.table_name, t.iot_name, t.compression, t.compress_for
  FROM   dba_tables t
         JOIN scope u ON u.username = t.owner
),
nt AS (
  SELECT n.owner, n.table_name, n.parent_table_name
  FROM   dba_nested_tables n
         JOIN scope u ON u.username = n.owner
),
tp AS (
  SELECT p.table_owner, p.table_name, p.partition_name, p.compression, p.compress_for
  FROM   dba_tab_partitions p
         JOIN scope u ON u.username = p.table_owner
),
tsp AS (
  SELECT sp.table_owner, sp.table_name, sp.subpartition_name, sp.compression, sp.compress_for
  FROM   dba_tab_subpartitions sp
         JOIN scope u ON u.username = sp.table_owner
),
-- tables without any segment (not dropped: dropped tables stay in DBA_TABLES)
no_seg AS (
  SELECT t.owner, t.table_name, t.tablespace_name, t.partitioned
  FROM   dba_tables t
         JOIN scope u ON u.username = t.owner
  WHERE  t.dropped = 'NO'
  AND    t.iot_type IS NULL
  AND    t.cluster_name IS NULL
  AND    t.nested = 'NO'
  AND    NOT EXISTS (SELECT 1
                     FROM   seg g
                     WHERE  g.owner        = t.owner
                     AND    g.segment_name = t.table_name)
),
-- type, family, parent table and compression of every segment
res AS (
  SELECT g.owner, g.segment_name, g.partition_name, g.tablespace_name, g.bytes,
         CASE
           WHEN g.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')  THEN 'TABLE'
           WHEN g.segment_type = 'LOBINDEX' OR i.index_type = 'LOB'                   THEN 'LOBINDEX'
           WHEN g.segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')  THEN 'INDEX'
           WHEN g.segment_type IN ('LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION')  THEN 'LOBSEGMENT'
           ELSE g.segment_type
         END                                                                          AS object_type,
         CASE
           WHEN g.segment_type IN ('LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION',
                                   'LOBINDEX') OR i.index_type = 'LOB'                THEN 'LOB'
           WHEN i.index_type = 'IOT - TOP'                                            THEN 'TABLE'
           WHEN g.segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')  THEN 'INDEX'
           WHEN g.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION',
                                   'NESTED TABLE', 'CLUSTER')                         THEN 'TABLE'
           ELSE 'OTHER'
         END                                                                          AS family,
         COALESCE(i.table_owner, g.owner)                                             AS parent_schema,
         COALESCE(l.table_name,
                  i.table_name,
                  t.iot_name,
                  n.parent_table_name,
                  CASE WHEN g.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION',
                                               'NESTED TABLE')
                       THEN g.segment_name
                  END)                                                                AS parent_table,
         CASE
           WHEN g.segment_type = 'TABLE SUBPARTITION'
             THEN DECODE(sp.compression, 'ENABLED', sp.compress_for, 'NONE')
           WHEN g.segment_type = 'TABLE PARTITION'
             THEN DECODE(p.compression, 'ENABLED', p.compress_for, 'NONE')
           WHEN g.segment_type IN ('TABLE', 'NESTED TABLE')
             THEN DECODE(t.compression, 'ENABLED', t.compress_for, 'NONE')
           WHEN i.index_name IS NOT NULL
             THEN DECODE(i.compression, 'DISABLED', 'NONE', i.compression)
           WHEN l.segment_name IS NOT NULL
             THEN DECODE(l.securefile, 'YES', 'SECUREFILE ' || l.compression, 'BASICFILE')
         END                                                                          AS compression
  FROM   seg g
         LEFT JOIN ix  i  ON  g.segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION', 'LOBINDEX')
                          AND i.owner      = g.owner
                          AND i.index_name = g.segment_name
         LEFT JOIN lb  l  ON  g.segment_type IN ('LOBSEGMENT', 'LOB PARTITION', 'LOB SUBPARTITION')
                          AND l.owner        = g.owner
                          AND l.segment_name = g.segment_name
         LEFT JOIN tb  t  ON  g.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION', 'NESTED TABLE')
                          AND t.owner      = g.owner
                          AND t.table_name = g.segment_name
         LEFT JOIN nt  n  ON  g.segment_type = 'NESTED TABLE'
                          AND n.owner      = g.owner
                          AND n.table_name = g.segment_name
         LEFT JOIN tp  p  ON  g.segment_type = 'TABLE PARTITION'
                          AND p.table_owner    = g.owner
                          AND p.table_name     = g.segment_name
                          AND p.partition_name = g.partition_name
         LEFT JOIN tsp sp ON  g.segment_type = 'TABLE SUBPARTITION'
                          AND sp.table_owner       = g.owner
                          AND sp.table_name        = g.segment_name
                          AND sp.subpartition_name = g.partition_name
),
-- objects with segments: their (sub)partitions summed
with_seg AS (
  SELECT r.owner                                                                      AS schema_name,
         r.segment_name                                                               AS object_name,
         r.object_type,
         r.family,
         r.parent_schema,
         r.parent_table,
         r.tablespace_name,
         MAX(CASE WHEN r.partition_name IS NOT NULL THEN 'YES' ELSE 'NO' END)         AS partitioned,
         COUNT(*)                                                                     AS n_segments,
         ROUND(SUM(r.bytes) / 1048576, 3)                                             AS size_mb,
         LISTAGG(DISTINCT r.compression, ',') WITHIN GROUP (ORDER BY r.compression)   AS compression
  FROM   res r
  GROUP  BY r.owner, r.segment_name, r.object_type, r.family,
            r.parent_schema, r.parent_table, r.tablespace_name
)
SELECT w.schema_name, w.object_name, w.object_type, w.family, w.parent_schema, w.parent_table,
       w.tablespace_name, w.partitioned, w.n_segments, w.size_mb, w.compression
FROM   with_seg w
UNION ALL
SELECT n.owner, n.table_name, 'TABLE', 'TABLE', n.owner, n.table_name,
       n.tablespace_name, DECODE(n.partitioned, 'YES', 'YES', 'NO'), 0, 0, CAST(NULL AS VARCHAR2(4000))
FROM   no_seg n
ORDER  BY schema_name, size_mb DESC;
