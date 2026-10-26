-- =============================================================================
-- dataset_tables.sql: DSS SQL query recipe (Oracle), step 3
-- One row per dataset: the step 1 table it is stored in, when it can be found.
-- -----------------------------------------------------------------------------
-- Inputs   dss_dataset_locations, tables   (same snapshot_day)
-- Output   dataset_tables                  (partitioned by snapshot_day)
--
-- Each candidate schema of dss_dataset_locations is looked up in the step 1 tables:
-- exact names first (any candidate), then ignoring case. The best candidate wins:
-- an exact match before a match ignoring case, then the lowest candidate_rank.
--
-- match_status   FOUND       the table exists in step 1
--                AMBIGUOUS   several tables match once case is ignored
--                NOT_FOUND   no candidate schema holds the table
--                UNMAPPED, PER_USER, UNRESOLVED, NO_TABLE, NOT_SQL: no candidate to look
--                up (see dss_dataset_locations.candidate_status)
-- database_label, schema_name, table_name
--                FOUND: the table as named in the database. Otherwise: the best candidate
-- candidate_rank, schema_source, note: of the candidate kept
-- =============================================================================
WITH
candidates AS (
  SELECT c."project_key", c."dataset_name", c."candidate_rank", c."candidate_status",
         c."database_label", c."schema_name", c."table_name", c."schema_source", c."note"
  FROM   "dss_dataset_locations" c
  WHERE  c."snapshot_day" = '$DKU_DST_snapshot_day'
),
step1_tables AS (
  SELECT t."label", t."schema_name", t."table_name"
  FROM   "tables" t
  WHERE  t."snapshot_day" = '$DKU_DST_snapshot_day'
),
-- candidates whose table exists with exactly these names
exact_matches AS (
  SELECT c."project_key", c."dataset_name", c."candidate_rank",
         t."schema_name"                                                     AS "found_schema",
         t."table_name"                                                      AS "found_table"
  FROM   candidates c
         JOIN step1_tables t ON  t."label"       = c."database_label"
                             AND t."schema_name" = c."schema_name"
                             AND t."table_name"  = c."table_name"
  WHERE  c."candidate_status" = 'CANDIDATE'
),
-- candidates whose table exists once case is ignored, and how many tables match
loose_matches AS (
  SELECT c."project_key", c."dataset_name", c."candidate_rank",
         MIN(t."schema_name")                                                AS "found_schema",
         MIN(t."table_name")                                                 AS "found_table",
         COUNT(*)                                                            AS "n_matches"
  FROM   candidates c
         JOIN step1_tables t ON  t."label"              = c."database_label"
                             AND UPPER(t."schema_name") = UPPER(c."schema_name")
                             AND UPPER(t."table_name")  = UPPER(c."table_name")
  WHERE  c."candidate_status" = 'CANDIDATE'
  GROUP  BY c."project_key", c."dataset_name", c."candidate_rank"
),
-- every candidate with its kind of match: 1 exact, 2 ignoring case, 3 ambiguous, 4 none
scored AS (
  SELECT c."project_key", c."dataset_name", c."candidate_rank", c."candidate_status",
         c."database_label", c."schema_name", c."table_name", c."schema_source", c."note",
         CASE
           WHEN e."candidate_rank" IS NOT NULL THEN 1
           WHEN m."n_matches" = 1              THEN 2
           WHEN m."n_matches" > 1              THEN 3
           ELSE 4
         END                                                                 AS "match_kind",
         COALESCE(e."found_schema", CASE WHEN m."n_matches" = 1 THEN m."found_schema" END) AS "found_schema",
         COALESCE(e."found_table",  CASE WHEN m."n_matches" = 1 THEN m."found_table"  END) AS "found_table"
  FROM   candidates c
         LEFT JOIN exact_matches e ON  e."project_key"    = c."project_key"
                                   AND e."dataset_name"   = c."dataset_name"
                                   AND e."candidate_rank" = c."candidate_rank"
         LEFT JOIN loose_matches m ON  m."project_key"    = c."project_key"
                                   AND m."dataset_name"   = c."dataset_name"
                                   AND m."candidate_rank" = c."candidate_rank"
),
-- the best candidate of each dataset
best AS (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY s."project_key", s."dataset_name"
                            ORDER BY s."match_kind", s."candidate_rank" NULLS LAST) AS "choice"
  FROM   scored s
)
SELECT '$DKU_DST_snapshot_day'                                               AS "snapshot_day",
       b."project_key",
       b."dataset_name",
       CASE
         WHEN b."match_kind" IN (1, 2)            THEN 'FOUND'
         WHEN b."match_kind" = 3                  THEN 'AMBIGUOUS'
         WHEN b."candidate_status" = 'CANDIDATE'  THEN 'NOT_FOUND'
         ELSE b."candidate_status"
       END                                                                   AS "match_status",
       b."database_label",
       COALESCE(b."found_schema", b."schema_name")                           AS "schema_name",
       COALESCE(b."found_table", b."table_name")                             AS "table_name",
       b."candidate_rank",
       b."schema_source",
       b."note"
FROM   best b
WHERE  b."choice" = 1
