-- =============================================================================
-- objects.sql (PostgreSQL)
-- One row per storage object and tablespace, in the database the connection points
-- to: tables, materialized views, indexes, TOAST tables and their indexes, sequences.
-- The partitions of a table (or of an index) are summed under the root of its
-- partition tree, as Oracle sums partitions under their table.
-- Scope: every schema except pg_* and information_schema.
-- -----------------------------------------------------------------------------
-- object_type    TABLE, MATERIALIZED VIEW, INDEX, TOAST TABLE, TOAST INDEX, SEQUENCE
-- family         TABLE   table and materialized view data
--                INDEX   indexes
--                LOB     TOAST tables and their indexes: values stored out of line,
--                        the PostgreSQL counterpart of Oracle LOB segments
--                OTHER   sequences
-- parent_schema, parent_table
--                table the object belongs to (root of the partition tree). NULL for
--                sequences
-- tablespace_name  pg_default (or the database default) when the object has none
-- partitioned    YES when the row sums partitions
-- n_segments     relations summed in the row
-- size_mb        all forks: main, free space map, visibility map, init
-- compression    Oracle only, NULL
-- =============================================================================
WITH
scope AS (
  SELECT n.oid, n.nspname
  FROM   pg_namespace n
  WHERE  n.nspname NOT LIKE 'pg\_%'
  AND    n.nspname <> 'information_schema'
),
db AS (
  SELECT d.dattablespace AS default_ts
  FROM   pg_database d
  WHERE  d.datname = current_database()
),
-- tables, partitions and materialized views, with the root of each partition tree
tbl AS (
  SELECT c.oid, c.relname, c.relkind, c.relispartition, c.reltablespace, c.reltoastrelid,
         s.nspname,
         CASE WHEN c.relispartition THEN pg_partition_root(c.oid::regclass)::oid
              ELSE c.oid
         END                                                        AS root_oid
  FROM   pg_class c
         JOIN scope s ON s.oid = c.relnamespace
  WHERE  c.relkind IN ('r', 'p', 'm')
),
-- every relation with storage, attributed to its table
rel AS (
  -- table data
  SELECT t.oid, t.nspname AS schema_name, r.relname AS object_name,
         CASE WHEN t.relkind = 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END AS object_type,
         'TABLE' AS family, t.relispartition AS is_partition, t.reltablespace,
         r.nspname AS parent_schema, r.relname AS parent_table
  FROM   tbl t
         JOIN tbl r ON r.oid = t.root_oid
  WHERE  t.relkind IN ('r', 'm')
  UNION ALL
  -- indexes: partition indexes under the root of their index tree
  SELECT i.oid, t.nspname, ri.relname, 'INDEX', 'INDEX', i.relispartition, i.reltablespace,
         r.nspname, r.relname
  FROM   pg_index x
         JOIN pg_class i  ON i.oid = x.indexrelid
         JOIN tbl t       ON t.oid = x.indrelid
         JOIN tbl r       ON r.oid = t.root_oid
         JOIN pg_class ri ON ri.oid = CASE WHEN i.relispartition
                                           THEN pg_partition_root(i.oid::regclass)::oid
                                           ELSE i.oid
                                      END
  WHERE  i.relkind = 'i'
  UNION ALL
  -- TOAST tables
  SELECT tt.oid, t.nspname, tt.relname, 'TOAST TABLE', 'LOB', false, tt.reltablespace,
         r.nspname, r.relname
  FROM   tbl t
         JOIN pg_class tt ON tt.oid = t.reltoastrelid
         JOIN tbl r       ON r.oid = t.root_oid
  UNION ALL
  -- TOAST indexes
  SELECT ti.oid, t.nspname, ti.relname, 'TOAST INDEX', 'LOB', false, ti.reltablespace,
         r.nspname, r.relname
  FROM   tbl t
         JOIN pg_index x  ON x.indrelid = t.reltoastrelid
         JOIN pg_class ti ON ti.oid = x.indexrelid
         JOIN tbl r       ON r.oid = t.root_oid
  UNION ALL
  -- sequences: storage of their own, no table
  SELECT c.oid, s.nspname, c.relname, 'SEQUENCE', 'OTHER', false, c.reltablespace,
         CAST(NULL AS name), CAST(NULL AS name)
  FROM   pg_class c
         JOIN scope s ON s.oid = c.relnamespace
  WHERE  c.relkind = 'S'
)
SELECT o.schema_name,
       o.object_name,
       o.object_type,
       o.family,
       o.parent_schema,
       o.parent_table,
       ts.spcname                                                   AS tablespace_name,
       CASE WHEN bool_or(o.is_partition) THEN 'YES' ELSE 'NO' END   AS partitioned,
       COUNT(*)                                                     AS n_segments,
       ROUND(SUM(pg_relation_size(o.oid, 'main') + pg_relation_size(o.oid, 'fsm')
                 + pg_relation_size(o.oid, 'vm') + pg_relation_size(o.oid, 'init'))
             / 1048576.0, 3)                                        AS size_mb,
       CAST(NULL AS text)                                           AS compression
FROM   rel o
       CROSS JOIN db
       JOIN pg_tablespace ts ON ts.oid = CASE WHEN o.reltablespace = 0 THEN db.default_ts
                                             ELSE o.reltablespace
                                        END
GROUP  BY o.schema_name, o.object_name, o.object_type, o.family,
          o.parent_schema, o.parent_table, ts.spcname
ORDER  BY o.schema_name, size_mb DESC;
