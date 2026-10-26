-- =============================================================================
-- role_settings.sql (Oracle)
-- Same columns as the PostgreSQL file, always empty: on Oracle a session without
-- schema works in the schema of its login user, there is no search_path.
-- (A post-connect ALTER SESSION SET CURRENT_SCHEMA is read from the DSS connection.)
-- =============================================================================
SELECT CAST(NULL AS VARCHAR2(128))                                  AS role_name,
       CAST(NULL AS VARCHAR2(128))                                  AS setting_database,
       CAST(NULL AS VARCHAR2(4000))                                 AS search_path
FROM   dual
WHERE  1 = 0;
