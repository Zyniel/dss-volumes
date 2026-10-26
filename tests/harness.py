"""Shared by the tests: a stand-in for the dataiku module that runs the SQL inside the test
containers, and a check() that counts failures.

Connections named ADM_ORA... go to the Oracle container, the others to PostgreSQL:
ADM_PG_APP -> database appdb, ADM_PG_OTHER -> otherdb, ADM_PG_DOWN -> a database that
does not exist (to test failures).
"""
import io
import os
import subprocess
import sys
import types

sys.dont_write_bytecode = True
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

PG_CONTAINER = "dbmon-pg"
ORA_CONTAINER = "dbmon-ora"
# Test values of the local containers only
PG_TEST_PASSWORD = "dbmon"
ORACLE_TEST_PASSWORD = "Dbmon_Test_1"
PG_DATABASES = {"ADM_PG_APP": "appdb", "ADM_PG_OTHER": "otherdb", "ADM_PG_DOWN": "no_such_db"}

FAILURES = []


def check(label, condition, detail=""):
    """Print ok or FAIL for one check, with detail when it fails."""
    if condition:
        print("ok    %s" % label)
        return
    print("FAIL  %s  -> %s" % (label, detail))
    FAILURES.append(label)


def finish():
    """Print the number of failures and exit with 1 when there is any."""
    print("\n%d failure(s)" % len(FAILURES))
    if FAILURES:
        sys.exit(1)
    sys.exit(0)


def psql(database, script, user="dbmon"):
    """Run a psql script in the PostgreSQL container, CSV output."""
    command = ["docker", "exec", "-i", "-e", "PGPASSWORD=" + PG_TEST_PASSWORD, PG_CONTAINER, "psql", "-X", "-q",
               "-v", "ON_ERROR_STOP=1", "--csv", "-h", "127.0.0.1", "-U", user, "-d", database]
    return subprocess.run(command, input=script, capture_output=True, text=True, encoding="utf-8")


def sqlplus(script, logon=None):
    """Run a SQL*Plus script in the Oracle container, as dbmon on FREEPDB1 by default."""
    if logon is None:
        logon = "dbmon/%s@//localhost/FREEPDB1" % ORACLE_TEST_PASSWORD
    command = ["docker", "exec", "-i", ORA_CONTAINER, "sqlplus", "-s", "-L"] + logon.split(" ")
    return subprocess.run(command, input=script, capture_output=True, text=True, encoding="utf-8")


class PostgresExecutor(object):
    """Stand-in for SQLExecutor2 on a PostgreSQL connection."""

    def __init__(self, connection):
        self.database = PG_DATABASES.get(connection, "appdb")

    def query_to_df(self, sql, pre_queries=None, infer_from_schema=False):
        script = ""
        for pre_query in pre_queries or []:
            script = script + pre_query + ";\n"
        script = script + sql + ";\n"
        run = psql(self.database, script)
        if run.returncode != 0:
            raise Exception(run.stderr.strip())
        if run.stdout.strip() == "":
            return pd.DataFrame()
        return pd.read_csv(io.StringIO(run.stdout))


class OracleExecutor(object):
    """Stand-in for SQLExecutor2 on an Oracle connection. PL/SQL pre-queries run with a slash,
    as JDBC runs them as one statement."""

    def __init__(self, connection):
        self.connection = connection

    def query_to_df(self, sql, pre_queries=None, infer_from_schema=False):
        script = "WHENEVER SQLERROR EXIT FAILURE\nSET MARKUP CSV ON\nSET PAGESIZE 50000 FEEDBACK OFF\n"
        for pre_query in pre_queries or []:
            if pre_query.upper().startswith("BEGIN"):
                script = script + pre_query + "\n/\n"
            else:
                script = script + pre_query + ";\n"
        script = script + sql + ";\nEXIT\n"
        run = sqlplus(script)
        if run.returncode != 0:
            raise Exception(run.stdout[-1500:])
        if run.stdout.strip() == "":
            # SQL*Plus prints nothing, not even the header, when there is no row: DSS returns
            # the columns, so read them from the cursor description
            return pd.DataFrame(columns=self.column_names(sql))
        return pd.read_csv(io.StringIO(run.stdout))

    def column_names(self, sql):
        script = "\n".join([
            "SET SERVEROUTPUT ON FEEDBACK OFF",
            "DECLARE",
            "  cursor_id INTEGER := DBMS_SQL.OPEN_CURSOR;",
            "  n_columns INTEGER;",
            "  described DBMS_SQL.DESC_TAB;",
            "BEGIN",
            "  DBMS_SQL.PARSE(cursor_id, q'~" + sql + "~', DBMS_SQL.NATIVE);",
            "  DBMS_SQL.DESCRIBE_COLUMNS(cursor_id, n_columns, described);",
            "  FOR i IN 1 .. n_columns LOOP",
            "    DBMS_OUTPUT.PUT_LINE(described(i).col_name);",
            "  END LOOP;",
            "  DBMS_SQL.CLOSE_CURSOR(cursor_id);",
            "END;",
            "/",
            "EXIT",
            "",
        ])
        run = sqlplus(script)
        names = []
        for line in run.stdout.splitlines():
            if line.strip() != "":
                names.append(line.strip())
        return names


def executor_for(connection):
    if connection.startswith("ADM_ORA"):
        return OracleExecutor(connection)
    return PostgresExecutor(connection)


def install_fake_dataiku(extra=None):
    """Put a stand-in dataiku module in sys.modules. extra: other attributes to set on it."""
    module = types.ModuleType("dataiku")
    module.SQLExecutor2 = executor_for
    if extra:
        for name in extra:
            setattr(module, name, extra[name])
    sys.modules["dataiku"] = module
    # forget dbmon modules imported with a previous stand-in
    for name in list(sys.modules):
        if name == "dbmon" or name.startswith("dbmon."):
            del sys.modules[name]
    return module
