"""Start the two test databases and load their test data. Safe to run again.

    python tests/setup_containers.py

- dbmon-pg   PostgreSQL 16 (image postgres:16-alpine), test data tests/fixtures/postgresql.sql
- dbmon-ora  Oracle Database Free (image gvenzl/oracle-free), tests/fixtures/oracle.sql
Both are local test containers: the passwords below are test values only.
A missing image is downloaded by Docker (Oracle: about 1 to 4 GB).
"""
import os
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from harness import ORACLE_TEST_PASSWORD, PG_CONTAINER, ORA_CONTAINER  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def docker(arguments, input_text=None):
    """Run a docker command. Returns (exit code, output)."""
    run = subprocess.run(["docker"] + arguments, input=input_text, capture_output=True, text=True, encoding="utf-8")
    return run.returncode, run.stdout + run.stderr


def container_state(name):
    code, output = docker(["inspect", "--format", "{{.State.Running}}", name])
    if code != 0:
        return "missing"
    if output.strip() == "true":
        return "running"
    return "stopped"


def start(name, run_arguments):
    state = container_state(name)
    if state == "missing":
        print("creating %s" % name)
        code, output = docker(["run", "-d", "--name", name] + run_arguments)
        if code != 0:
            raise SystemExit(output)
    elif state == "stopped":
        print("starting %s" % name)
        docker(["start", name])
    else:
        print("%s already running" % name)


def wait_until(description, command):
    print("waiting for %s" % description)
    for attempt in range(180):
        code, output = docker(command)
        if code == 0:
            return
        time.sleep(5)
    raise SystemExit("%s not ready after 15 minutes" % description)


def setup_postgresql():
    start(PG_CONTAINER, ["-e", "POSTGRES_PASSWORD=postgres", "postgres:16-alpine"])
    wait_until("PostgreSQL", ["exec", PG_CONTAINER, "psql", "-U", "postgres", "-Atc", "SELECT 1"])

    code, output = docker(["exec", PG_CONTAINER, "psql", "-U", "postgres", "-Atc",
                           "SELECT count(*) FROM pg_database WHERE datname = 'appdb'"])
    if output.strip() == "1":
        print("PostgreSQL test data already loaded")
        return

    docker(["exec", "-u", "postgres", PG_CONTAINER, "mkdir", "-p", "/var/lib/postgresql/ts_index"])
    with open(os.path.join(FIXTURES, "postgresql.sql"), encoding="utf-8") as fixture:
        code, output = docker(["exec", "-i", PG_CONTAINER, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1"],
                              fixture.read())
    if code != 0:
        raise SystemExit(output)
    print("PostgreSQL test data loaded")


def setup_oracle():
    start(ORA_CONTAINER, ["-e", "ORACLE_RANDOM_PASSWORD=yes", "gvenzl/oracle-free:latest"])
    wait_until("Oracle", ["exec", "-i", ORA_CONTAINER, "bash", "-c",
                          "echo 'SELECT open_mode FROM v$pdbs;' | sqlplus -s / as sysdba | grep -q 'READ WRITE'"])

    code, output = docker(["exec", "-i", ORA_CONTAINER, "sqlplus", "-s", "/ as sysdba"],
                          "SET HEADING OFF FEEDBACK OFF\nALTER SESSION SET CONTAINER = FREEPDB1;\n"
                          "SELECT COUNT(*) FROM dba_users WHERE username = 'DBMON';\nEXIT\n")
    if output.split()[-1] == "1":
        # Already loaded: only make sure the test password is the one the tests use
        docker(["exec", "-i", ORA_CONTAINER, "sqlplus", "-s", "/ as sysdba"],
               "ALTER SESSION SET CONTAINER = FREEPDB1;\nALTER USER dbmon IDENTIFIED BY \"%s\";\nEXIT\n"
               % ORACLE_TEST_PASSWORD)
        print("Oracle test data already loaded")
        return

    with open(os.path.join(FIXTURES, "oracle.sql"), encoding="utf-8") as fixture:
        script = "DEFINE dbmon_pw = %s\n" % ORACLE_TEST_PASSWORD + fixture.read()
    code, output = docker(["exec", "-i", ORA_CONTAINER, "sqlplus", "-s", "/ as sysdba"], script)
    if code != 0:
        raise SystemExit(output)
    print("Oracle test data loaded")


setup_postgresql()
setup_oracle()
