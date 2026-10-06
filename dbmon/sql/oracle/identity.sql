-- =============================================================================
-- identity.sql (Oracle)
-- One row: the database that answered. The collector puts these columns in front
-- of every row read through the same connection.
-- -----------------------------------------------------------------------------
-- server_name    DB_NAME of the CDB, or of the non-CDB
-- database_name  container (PDB) name, or the non-CDB name
-- unique_name    DB_UNIQUE_NAME: tells a primary from its standby databases
-- role           PRIMARY, PHYSICAL STANDBY...
-- version        full release, e.g. 19.21.0.0.0
-- Needs SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY).
-- =============================================================================
SELECT 'oracle'                              AS engine,
       d.name                                AS server_name,
       SYS_CONTEXT('USERENV', 'CON_NAME')    AS database_name,
       d.db_unique_name                      AS unique_name,
       d.database_role                       AS role,
       i.version_full                        AS version
FROM   v$database d
       CROSS JOIN v$instance i;
