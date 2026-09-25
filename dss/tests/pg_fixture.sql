CREATE ROLE dbmon LOGIN PASSWORD 'dbmon' IN ROLE pg_monitor;
CREATE TABLESPACE sales_dsswork_index LOCATION '/var/lib/postgresql/ts_index';
CREATE DATABASE appdb;
CREATE DATABASE otherdb;
\c appdb
CREATE SCHEMA sales_dsswork;
CREATE SCHEMA sales_dssout;
CREATE SCHEMA fin_dssout;
CREATE SCHEMA appx;
-- large text values go to TOAST
CREATE TABLE sales_dsswork."SALES_orders" (id int PRIMARY KEY, payload text, amount numeric);
INSERT INTO sales_dsswork."SALES_orders"
SELECT g, (SELECT string_agg(md5(g::text || i::text), '') FROM generate_series(1, 150) i), g
FROM   generate_series(1, 3000) g;
CREATE INDEX "SALES_orders_amount" ON sales_dsswork."SALES_orders"(amount) TABLESPACE sales_dsswork_index;
CREATE TABLE sales_dssout."SALES_orders" (id int, v text);
INSERT INTO sales_dssout."SALES_orders" SELECT g, 'x' FROM generate_series(1, 1000) g;
-- partitioned table with a partitioned index
CREATE TABLE fin_dssout.ledger (id int, d date, amount numeric) PARTITION BY RANGE (d);
CREATE TABLE fin_dssout.ledger_2025 PARTITION OF fin_dssout.ledger FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE fin_dssout.ledger_2026 PARTITION OF fin_dssout.ledger FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE INDEX ledger_d ON fin_dssout.ledger(d);
INSERT INTO fin_dssout.ledger SELECT g, DATE '2025-06-01' + (g % 400), g FROM generate_series(1, 20000) g;
CREATE MATERIALIZED VIEW appx.mv_totals AS SELECT id % 10 AS k, count(*) AS n FROM sales_dssout."SALES_orders" GROUP BY 1;
CREATE SEQUENCE appx.seq1;
CREATE UNLOGGED TABLE appx.scratch (a int);
ANALYZE;
-- changes after the statistics
UPDATE sales_dssout."SALES_orders" SET v = 'y' WHERE id <= 300;
DELETE FROM fin_dssout.ledger WHERE id <= 500;
SELECT pg_stat_force_next_flush();
\c otherdb
CREATE SCHEMA sales_dsswork;
CREATE TABLE sales_dsswork."SALES_orders" (id int);
INSERT INTO sales_dsswork."SALES_orders" SELECT generate_series(1, 100);
