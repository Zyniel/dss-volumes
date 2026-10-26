-- =============================================================================
-- table_ownership.sql: DSS SQL query recipe (Oracle), step 3
-- One row per table of step 1: its space, which datasets point to it, and who owns it.
-- -----------------------------------------------------------------------------
-- Inputs   tables, dataset_tables, dataset_roles   (same snapshot_day)
-- Output   table_ownership                         (partitioned by snapshot_day)
--
-- status         OWNED         a target dataset writes it: its space goes to the owner
--                READ_ONLY     only source datasets point to it (data read, not written)
--                NO_FLOW       only datasets that are in no pipeline point to it
--                UNREFERENCED  no dataset points to it
-- owner_project, owner_dataset
--                the target dataset that owns the space. Several targets: the smallest
--                project key, then dataset name, and multiple_owners = YES
-- n_target_datasets, n_source_datasets, n_orphan_datasets
--                datasets pointing to the table, by flow role
-- n_referencing_projects  projects of all those datasets (the list: dataset_tables)
-- schema_use_case, schema_layer
--                from the schema name <UC>_DSSWORK / <UC>_DSSOUT (upper case). Empty
--                outside the convention
-- The space of a table is counted once: total_mb summed over all the rows of one day
-- equals the total of step 1 tables.
-- =============================================================================
WITH
step1_tables AS (
  SELECT t."label", t."engine", t."server_name", t."database_name", t."schema_name", t."table_name",
         t."standalone", t."total_mb", t."table_mb", t."index_mb", t."lob_mb", t."other_mb"
  FROM   "tables" t
  WHERE  t."snapshot_day" = '$DKU_DST_snapshot_day'
),
-- datasets found in a step 1 table, with their flow role
pointing AS (
  SELECT dt."database_label", dt."schema_name", dt."table_name",
         dt."project_key", dt."dataset_name", r."flow_role"
  FROM   "dataset_tables" dt
         JOIN "dataset_roles" r ON  r."project_key"  = dt."project_key"
                                AND r."dataset_name" = dt."dataset_name"
                                AND r."snapshot_day" = dt."snapshot_day"
  WHERE  dt."snapshot_day" = '$DKU_DST_snapshot_day'
  AND    dt."match_status" = 'FOUND'
),
counts AS (
  SELECT p."database_label", p."schema_name", p."table_name",
         SUM(CASE WHEN p."flow_role" = 'target' THEN 1 ELSE 0 END)           AS "n_target_datasets",
         SUM(CASE WHEN p."flow_role" = 'source' THEN 1 ELSE 0 END)           AS "n_source_datasets",
         SUM(CASE WHEN p."flow_role" = 'orphan' THEN 1 ELSE 0 END)           AS "n_orphan_datasets",
         COUNT(DISTINCT p."project_key")                                     AS "n_referencing_projects"
  FROM   pointing p
  GROUP  BY p."database_label", p."schema_name", p."table_name"
),
-- the owner: the first target dataset by project key, then dataset name
owners AS (
  SELECT p."database_label", p."schema_name", p."table_name", p."project_key", p."dataset_name",
         ROW_NUMBER() OVER (PARTITION BY p."database_label", p."schema_name", p."table_name"
                            ORDER BY p."project_key", p."dataset_name")      AS "choice"
  FROM   pointing p
  WHERE  p."flow_role" = 'target'
)
SELECT '$DKU_DST_snapshot_day'                                               AS "snapshot_day",
       t."label",
       t."engine",
       t."server_name",
       t."database_name",
       t."schema_name",
       t."table_name",
       t."standalone",
       t."total_mb",
       t."table_mb",
       t."index_mb",
       t."lob_mb",
       t."other_mb",
       CASE
         WHEN NVL(c."n_target_datasets", 0) > 0 THEN 'OWNED'
         WHEN NVL(c."n_source_datasets", 0) > 0 THEN 'READ_ONLY'
         WHEN NVL(c."n_orphan_datasets", 0) > 0 THEN 'NO_FLOW'
         ELSE 'UNREFERENCED'
       END                                                                   AS "status",
       o."project_key"                                                       AS "owner_project",
       o."dataset_name"                                                      AS "owner_dataset",
       CASE WHEN NVL(c."n_target_datasets", 0) > 1 THEN 'YES' ELSE 'NO' END  AS "multiple_owners",
       NVL(c."n_target_datasets", 0)                                         AS "n_target_datasets",
       NVL(c."n_source_datasets", 0)                                         AS "n_source_datasets",
       NVL(c."n_orphan_datasets", 0)                                         AS "n_orphan_datasets",
       NVL(c."n_referencing_projects", 0)                                    AS "n_referencing_projects",
       REGEXP_SUBSTR(UPPER(t."schema_name"), '^(.+)_(DSSWORK|DSSOUT)$', 1, 1, NULL, 1) AS "schema_use_case",
       REGEXP_SUBSTR(UPPER(t."schema_name"), '^(.+)_(DSSWORK|DSSOUT)$', 1, 1, NULL, 2) AS "schema_layer"
FROM   step1_tables t
       LEFT JOIN counts c ON  c."database_label" = t."label"
                          AND c."schema_name"    = t."schema_name"
                          AND c."table_name"     = t."table_name"
       LEFT JOIN owners o ON  o."database_label" = t."label"
                          AND o."schema_name"    = t."schema_name"
                          AND o."table_name"     = t."table_name"
                          AND o."choice"         = 1
