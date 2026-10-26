-- =============================================================================
-- project_consumption.sql: DSS SQL query recipe (Oracle), step 3
-- One row per project: its datasets, the space it owns, and the space it only reads.
-- -----------------------------------------------------------------------------
-- Inputs   dss_projects, dataset_roles, dataset_tables, table_ownership (same snapshot_day)
-- Output   project_consumption                                          (partitioned)
--
-- use_case, use_case_source    from dss_projects (TAG, RULE or NONE)
-- n_datasets ...                datasets of the project by flow role, and by match
-- owned_mb, owned_*_mb          space of the tables the project owns (table_ownership)
-- read_mb                       space of the tables its datasets point to without the
--                               project owning them: never added to owned_mb
-- no_flow_mb                    space of the tables only its out-of-pipeline datasets
--                               point to (status NO_FLOW)
-- Per use case: group these rows by use_case. Space no project owns (READ_ONLY,
-- NO_FLOW, UNREFERENCED tables) is in table_ownership, by schema_use_case.
-- =============================================================================
WITH
projects AS (
  SELECT p."project_key", p."project_name", p."use_case", p."use_case_source"
  FROM   "dss_projects" p
  WHERE  p."snapshot_day" = '$DKU_DST_snapshot_day'
),
dataset_counts AS (
  SELECT r."project_key",
         COUNT(*)                                                            AS "n_datasets",
         SUM(CASE WHEN r."flow_role" = 'target' THEN 1 ELSE 0 END)           AS "n_target_datasets",
         SUM(CASE WHEN r."flow_role" = 'source' THEN 1 ELSE 0 END)           AS "n_source_datasets",
         SUM(CASE WHEN r."flow_role" = 'orphan' THEN 1 ELSE 0 END)           AS "n_orphan_datasets",
         SUM(CASE WHEN dt."match_status" = 'FOUND' THEN 1 ELSE 0 END)        AS "n_datasets_found",
         SUM(CASE WHEN dt."match_status" = 'NOT_FOUND' THEN 1 ELSE 0 END)    AS "n_datasets_not_found"
  FROM   "dataset_roles" r
         LEFT JOIN "dataset_tables" dt ON  dt."project_key"  = r."project_key"
                                       AND dt."dataset_name" = r."dataset_name"
                                       AND dt."snapshot_day" = r."snapshot_day"
  WHERE  r."snapshot_day" = '$DKU_DST_snapshot_day'
  GROUP  BY r."project_key"
),
owned AS (
  SELECT o."owner_project"                                                   AS "project_key",
         COUNT(*)                                                            AS "n_owned_tables",
         SUM(o."total_mb")                                                   AS "owned_mb",
         SUM(o."table_mb")                                                   AS "owned_table_mb",
         SUM(o."index_mb")                                                   AS "owned_index_mb",
         SUM(o."lob_mb")                                                     AS "owned_lob_mb"
  FROM   "table_ownership" o
  WHERE  o."snapshot_day" = '$DKU_DST_snapshot_day'
  AND    o."status" = 'OWNED'
  GROUP  BY o."owner_project"
),
-- each table a project's datasets point to, once per project
project_tables AS (
  SELECT DISTINCT dt."project_key", o."label", o."schema_name", o."table_name",
         o."status", o."owner_project", o."total_mb"
  FROM   "dataset_tables" dt
         JOIN "table_ownership" o ON  o."label"        = dt."database_label"
                                  AND o."schema_name"  = dt."schema_name"
                                  AND o."table_name"   = dt."table_name"
                                  AND o."snapshot_day" = dt."snapshot_day"
  WHERE  dt."snapshot_day" = '$DKU_DST_snapshot_day'
  AND    dt."match_status" = 'FOUND'
),
referenced AS (
  SELECT pt."project_key",
         SUM(CASE WHEN pt."status" <> 'NO_FLOW'
                   AND NVL(pt."owner_project", '-') <> pt."project_key"
                  THEN pt."total_mb" ELSE 0 END)                             AS "read_mb",
         SUM(CASE WHEN pt."status" = 'NO_FLOW' THEN pt."total_mb" ELSE 0 END) AS "no_flow_mb"
  FROM   project_tables pt
  GROUP  BY pt."project_key"
)
SELECT '$DKU_DST_snapshot_day'                                               AS "snapshot_day",
       p."project_key",
       p."project_name",
       p."use_case",
       p."use_case_source",
       NVL(d."n_datasets", 0)                                                AS "n_datasets",
       NVL(d."n_target_datasets", 0)                                         AS "n_target_datasets",
       NVL(d."n_source_datasets", 0)                                         AS "n_source_datasets",
       NVL(d."n_orphan_datasets", 0)                                         AS "n_orphan_datasets",
       NVL(d."n_datasets_found", 0)                                          AS "n_datasets_found",
       NVL(d."n_datasets_not_found", 0)                                      AS "n_datasets_not_found",
       NVL(w."n_owned_tables", 0)                                            AS "n_owned_tables",
       NVL(w."owned_mb", 0)                                                  AS "owned_mb",
       NVL(w."owned_table_mb", 0)                                            AS "owned_table_mb",
       NVL(w."owned_index_mb", 0)                                            AS "owned_index_mb",
       NVL(w."owned_lob_mb", 0)                                              AS "owned_lob_mb",
       NVL(r."read_mb", 0)                                                   AS "read_mb",
       NVL(r."no_flow_mb", 0)                                                AS "no_flow_mb"
FROM   projects p
       LEFT JOIN dataset_counts d ON d."project_key" = p."project_key"
       LEFT JOIN owned w          ON w."project_key" = p."project_key"
       LEFT JOIN referenced r     ON r."project_key" = p."project_key"
