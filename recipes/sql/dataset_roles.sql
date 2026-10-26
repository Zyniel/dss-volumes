-- =============================================================================
-- dataset_roles.sql: DSS SQL query recipe (Oracle), step 3
-- One row per dataset: how many recipes write and read it, and its flow role.
-- -----------------------------------------------------------------------------
-- Inputs   dss_datasets, dss_flow_links   (same snapshot_day)
-- Output   dataset_roles                  (partitioned by snapshot_day)
--
-- flow_role           target: output of at least one recipe
--                     source: input of at least one recipe, output of none
--                     orphan: neither input nor output of any recipe
-- n_producers         recipes writing the dataset
-- n_consumers         recipes reading it, in any project
-- consuming_projects  other projects whose recipes read it, comma separated
-- =============================================================================
WITH
datasets AS (
  SELECT d."project_key", d."dataset_name"
  FROM   "dss_datasets" d
  WHERE  d."snapshot_day" = '$DKU_DST_snapshot_day'
),
link_counts AS (
  SELECT l."dataset_project",
         l."dataset_name",
         SUM(CASE WHEN l."direction" = 'output' THEN 1 ELSE 0 END)           AS "n_producers",
         SUM(CASE WHEN l."direction" = 'input'  THEN 1 ELSE 0 END)           AS "n_consumers",
         LISTAGG(DISTINCT CASE WHEN l."direction" = 'input' AND l."foreign" = 'YES'
                               THEN l."project_key" END, ',')
           WITHIN GROUP (ORDER BY CASE WHEN l."direction" = 'input' AND l."foreign" = 'YES'
                                       THEN l."project_key" END)             AS "consuming_projects"
  FROM   "dss_flow_links" l
  WHERE  l."snapshot_day" = '$DKU_DST_snapshot_day'
  GROUP  BY l."dataset_project", l."dataset_name"
)
SELECT '$DKU_DST_snapshot_day'                                               AS "snapshot_day",
       d."project_key",
       d."dataset_name",
       NVL(c."n_producers", 0)                                               AS "n_producers",
       NVL(c."n_consumers", 0)                                               AS "n_consumers",
       c."consuming_projects",
       CASE
         WHEN NVL(c."n_producers", 0) > 0 THEN 'target'
         WHEN NVL(c."n_consumers", 0) > 0 THEN 'source'
         ELSE 'orphan'
       END                                                                   AS "flow_role"
FROM   datasets d
       LEFT JOIN link_counts c ON  c."dataset_project" = d."project_key"
                               AND c."dataset_name"    = d."dataset_name"
