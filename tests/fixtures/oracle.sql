-- Test data of the Oracle container (Oracle Database Free, PDB FREEPDB1), run as SYSDBA.
-- The password of the admin account dbmon comes from the substitution variable dbmon_pw.
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
SET VERIFY OFF
ALTER SESSION SET CONTAINER = FREEPDB1;
COLUMN dir NEW_VALUE pdb_dir NOPRINT
SELECT SUBSTR(file_name, 1, INSTR(file_name, '/', -1) - 1) AS dir FROM dba_data_files WHERE ROWNUM = 1;
-- use-case tablespaces: DATA autoextends to a ceiling, the DSSOUT one cannot grow
CREATE TABLESPACE sales_dsswork_data  DATAFILE '&pdb_dir/sales_dsswork_data01.dbf'  SIZE 20M AUTOEXTEND ON NEXT 10M MAXSIZE 200M;
CREATE TABLESPACE sales_dsswork_index DATAFILE '&pdb_dir/sales_dsswork_index01.dbf' SIZE 10M AUTOEXTEND ON NEXT 10M MAXSIZE 100M;
CREATE TABLESPACE sales_dssout_data   DATAFILE '&pdb_dir/sales_dssout_data01.dbf'   SIZE 30M;
CREATE USER sales_dsswork NO AUTHENTICATION DEFAULT TABLESPACE sales_dsswork_data
  QUOTA UNLIMITED ON sales_dsswork_data QUOTA 50M ON sales_dsswork_index;
CREATE USER sales_dssout NO AUTHENTICATION DEFAULT TABLESPACE sales_dssout_data
  QUOTA UNLIMITED ON sales_dssout_data;
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW TO sales_dssout;
CREATE USER appx NO AUTHENTICATION DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
-- the admin account of the collector: dictionary access only
CREATE USER dbmon IDENTIFIED BY "&dbmon_pw";
GRANT CREATE SESSION, SELECT_CATALOG_ROLE TO dbmon;
-- heap table with out-of-line LOBs, an index in the INDEX tablespace, the PK index in DATA
CREATE TABLE sales_dsswork.orders (id NUMBER PRIMARY KEY, payload CLOB, amount NUMBER)
  LOB (payload) STORE AS SECUREFILE (TABLESPACE sales_dsswork_data);
INSERT INTO sales_dsswork.orders
SELECT LEVEL, TO_CLOB(RPAD('x', 4000, 'x')) || RPAD('y', 4000, 'y'), LEVEL FROM dual CONNECT BY LEVEL <= 1000;
COMMIT;
CREATE INDEX sales_dsswork.orders_amount_ix ON sales_dsswork.orders (amount) TABLESPACE sales_dsswork_index;
-- partitioned table, one partition compressed, local index
CREATE TABLE sales_dssout.ledger (id NUMBER, d DATE, amount NUMBER)
  PARTITION BY RANGE (d)
  (PARTITION p2025 VALUES LESS THAN (DATE '2026-01-01') COMPRESS BASIC,
   PARTITION p2026 VALUES LESS THAN (DATE '2027-01-01'));
INSERT INTO sales_dssout.ledger
SELECT LEVEL, DATE '2025-06-01' + MOD(LEVEL, 400), LEVEL FROM dual CONNECT BY LEVEL <= 20000;
COMMIT;
CREATE INDEX sales_dssout.ledger_d_ix ON sales_dssout.ledger (d) LOCAL;
-- index-organised table with an overflow segment
CREATE TABLE sales_dssout.iot_t (id NUMBER PRIMARY KEY, code VARCHAR2(10), notes VARCHAR2(2000))
  ORGANIZATION INDEX INCLUDING code OVERFLOW;
INSERT INTO sales_dssout.iot_t SELECT LEVEL, 'C' || LEVEL, RPAD('n', 500, 'n') FROM dual CONNECT BY LEVEL <= 500;
COMMIT;
CREATE MATERIALIZED VIEW sales_dssout.mv_totals AS
SELECT MOD(id, 10) AS k, COUNT(*) AS n FROM sales_dssout.ledger GROUP BY MOD(id, 10);
CREATE GLOBAL TEMPORARY TABLE sales_dssout.gtt (a NUMBER) ON COMMIT PRESERVE ROWS;
-- table never written to: no segment yet (deferred segment creation)
CREATE TABLE sales_dssout.empty_t (id NUMBER) SEGMENT CREATION DEFERRED;
CREATE TABLE sales_dssout.trunc_me AS SELECT LEVEL AS id FROM dual CONNECT BY LEVEL <= 5000;
-- dropped table: stays in the recycle bin
CREATE TABLE sales_dsswork.old_stuff AS SELECT LEVEL AS id, RPAD('o', 1000, 'o') AS pad FROM dual CONNECT BY LEVEL <= 3000;
CREATE INDEX sales_dsswork.old_stuff_ix ON sales_dsswork.old_stuff (id);
DROP TABLE sales_dsswork.old_stuff;
-- dropped then restored: its index keeps a BIN$ name
CREATE TABLE sales_dsswork.t_restore AS SELECT LEVEL AS id, 'v' AS v FROM dual CONNECT BY LEVEL <= 1000;
CREATE INDEX sales_dsswork.t_restore_ix ON sales_dsswork.t_restore (id);
DROP TABLE sales_dsswork.t_restore;
FLASHBACK TABLE sales_dsswork.t_restore TO BEFORE DROP;
-- schema outside the use cases, with an external table
CREATE TABLE appx.notes AS SELECT LEVEL AS id, 'note ' || LEVEL AS txt FROM dual CONNECT BY LEVEL <= 200;
CREATE DIRECTORY dbmon_ext AS '/tmp';
CREATE TABLE appx.ext_t (a VARCHAR2(10))
  ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY dbmon_ext LOCATION ('x.csv'));
EXEC DBMS_STATS.GATHER_SCHEMA_STATS('SALES_DSSWORK')
EXEC DBMS_STATS.GATHER_SCHEMA_STATS('SALES_DSSOUT')
EXEC DBMS_STATS.GATHER_SCHEMA_STATS('APPX')
EXIT
