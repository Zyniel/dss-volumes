-- =============================================================================
-- identity.sql (PostgreSQL)
-- One row: the database that answered. The collector puts these columns in front
-- of every row read through the same connection.
-- -----------------------------------------------------------------------------
-- server_name    cluster_name setting, or address:port when it is not set
-- database_name  the database the connection points to
-- unique_name    address:port of the server that answered
-- role           PRIMARY, or STANDBY when the server is in recovery
-- version        server version, e.g. 16.4
-- =============================================================================
SELECT 'postgresql'                                                         AS engine,
       COALESCE(NULLIF(current_setting('cluster_name'), ''),
                COALESCE(host(inet_server_addr()), 'localhost') || ':' || current_setting('port'))
                                                                            AS server_name,
       current_database()                                                   AS database_name,
       COALESCE(host(inet_server_addr()), 'localhost') || ':' || current_setting('port')
                                                                            AS unique_name,
       CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END      AS role,
       current_setting('server_version')                                    AS version;
