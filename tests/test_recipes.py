"""The two recipe files, run as DSS would, with datasets kept in memory.

    python tests/test_recipes.py
"""
import contextlib
import datetime
import io
import os
import runpy
import sys

sys.dont_write_bytecode = True
import pandas as pd

from harness import REPO, check, finish, install_fake_dataiku
sys.path.insert(0, REPO)
from dbmon import schemas  # noqa: E402
from fake_dss import CONNECTION_MAPPING, DATABASES, FakeClient

WRITTEN = {}
SCHEMAS_WRITTEN = {}
INPUTS = {}


class FakeDataset(object):
    def __init__(self, name):
        self.name = name

    def get_dataframe(self):
        return INPUTS[self.name].copy()

    def write_schema(self, columns):
        SCHEMAS_WRITTEN[self.name] = columns

    def write_dataframe(self, rows):
        WRITTEN[self.name] = rows.copy()

    def get_location_info(self):
        raise Exception("not an input of this recipe")


def run_recipe(file_name, flow_variables):
    extra = {"Dataset": FakeDataset, "api_client": FakeClient}
    if flow_variables is not None:
        extra["dku_flow_variables"] = flow_variables
    install_fake_dataiku(extra)
    WRITTEN.clear()
    SCHEMAS_WRITTEN.clear()
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        runpy.run_path(os.path.join(REPO, "recipes", file_name), run_name="__main__")
    return printed.getvalue()


today = datetime.datetime.now(datetime.timezone.utc).astimezone().date().isoformat()

# --- collect_storage -------------------------------------------------------------------------
INPUTS["databases"] = pd.DataFrame([("ADM_PG_APP", "postgresql", "PG app"), ("ADM_PG_DOWN", "postgresql", None)],
                                   columns=["connection", "engine", "label"])
printed = run_recipe("collect_storage.py", {"DKU_DST_snapshot_day": today})
check("collect: the five outputs written",
      sorted(WRITTEN) == ["collect_log", "objects", "role_settings", "storage_units", "tables"], sorted(WRITTEN))
all_start_with_day = True
for name in WRITTEN:
    rows = WRITTEN[name]
    if rows.columns[0] != "snapshot_day" or not (rows.snapshot_day == today).all():
        all_start_with_day = False
check("collect: every output starts with snapshot_day = the partition", all_start_with_day)
check("collect: summary in the job log with the failure",
      "1 of 2 databases collected" in printed and "FAILED ADM_PG_DOWN" in printed, printed)

try:
    run_recipe("collect_storage.py", {"DKU_DST_snapshot_day": "2026-01-01"})
    check("collect: another day's partition refused", False)
except ValueError as error:
    check("collect: another day's partition refused, nothing written", "2026-01-01" in str(error) and not WRITTEN)

run_recipe("collect_storage.py", None)
check("collect: not partitioned, today in snapshot_day", (WRITTEN["objects"].snapshot_day == today).all())
pg_run_tables = WRITTEN["tables"]
pg_run_settings = WRITTEN["role_settings"]


def schema_columns(dataset_name):
    names = []
    for column in SCHEMAS_WRITTEN[dataset_name]:
        names.append(column["name"])
    return names


def declared_columns(dataset_name):
    names = []
    for column_name, column_type in schemas.SCHEMAS[dataset_name]:
        names.append(column_name)
    return names


all_declared = True
for name in WRITTEN:
    if schema_columns(name) != declared_columns(name) or list(WRITTEN[name].columns) != declared_columns(name):
        all_declared = False
check("collect: every output written with its declared schema, rows in the same columns", all_declared)

# Oracle only: role_settings has no row, but still gets its schema
INPUTS["databases"] = pd.DataFrame([("ADM_ORA", "oracle", "ORA")], columns=["connection", "engine", "label"])
run_recipe("collect_storage.py", {"DKU_DST_snapshot_day": today})
check("collect, Oracle only: role_settings written with 0 rows and its full schema",
      len(WRITTEN["role_settings"]) == 0 and schema_columns("role_settings") == declared_columns("role_settings")
      and list(WRITTEN["role_settings"].columns) == declared_columns("role_settings"))

# No database collects objects: objects and tables have no row, but still get their schema
INPUTS["databases"] = pd.DataFrame([("ADM_PG_APP", "postgresql", "N")], columns=["connection", "engine", "objects"])
run_recipe("collect_storage.py", {"DKU_DST_snapshot_day": today})
check("collect, objects = N everywhere: objects and tables written empty with their schema",
      len(WRITTEN["objects"]) == 0 and len(WRITTEN["tables"]) == 0
      and schema_columns("objects") == declared_columns("objects")
      and schema_columns("tables") == declared_columns("tables"))
check("collect: types as declared (bigint as whole numbers)",
      str(WRITTEN["collect_log"]["storage_units_rows"].dtype) == "Int64"
      and str(WRITTEN["storage_units"]["allocated_mb"].dtype) == "float64")

try:
    schemas.conform(pd.DataFrame({"tablespace_name": ["X"], "new_column": [1]}), "storage_units")
    check("an undeclared column stops the write with its name", False)
except ValueError as error:
    check("an undeclared column stops the write with its name", "new_column" in str(error))

# --- inventory_dss ---------------------------------------------------------------------------
INPUTS["databases"] = DATABASES
INPUTS["audit_connection_mapping"] = CONNECTION_MAPPING
INPUTS["role_settings"] = pg_run_settings
printed = run_recipe("inventory_dss.py", {"DKU_DST_snapshot_day": today})
INVENTORY_OUTPUTS = ["dss_connections", "dss_dataset_locations", "dss_dataset_shares", "dss_datasets", "dss_flow_links",
                     "dss_projects", "inventory_log"]
check("inventory: the seven outputs written", sorted(WRITTEN) == INVENTORY_OUTPUTS, sorted(WRITTEN))
check("inventory: summary with the failed project", "2 of 3 projects read" in printed and "FAILED BROKEN" in printed,
      printed)
inventory_declared = True
for name in WRITTEN:
    if schema_columns(name) != declared_columns(name):
        inventory_declared = False
check("inventory: every output written with its declared schema", inventory_declared)
locations = WRITTEN["dss_dataset_locations"]
raw_orders = locations[locations.dataset_name == "raw_orders"]
check("inventory: DSS location not readable, candidates from the settings, noted",
      raw_orders.schema_source.iloc[0] == "SEARCH_PATH" and "DSS location not readable" in raw_orders.note.iloc[0])

# Empty role_settings (first run, or Oracle only): the inventory still runs, default search_path
INPUTS["role_settings"] = schemas.conform(pd.DataFrame(), "role_settings")
run_recipe("inventory_dss.py", {"DKU_DST_snapshot_day": today})
locations = WRITTEN["dss_dataset_locations"]
check("inventory with empty role_settings: runs, PostgreSQL default search_path \"$user\", public",
      sorted(WRITTEN) == INVENTORY_OUTPUTS
      and locations[locations.dataset_name == "raw_orders"].schema_name.tolist() == ["dbmon", "public"])

finish()
