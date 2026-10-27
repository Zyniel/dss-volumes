"""The three recipe files, run as DSS would, with datasets kept in memory.

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

# --- extract_dss -----------------------------------------------------------------------------
INPUTS.clear()
printed = run_recipe("extract_dss.py", {"DKU_DST_snapshot_day": today})
EXTRACT_OUTPUTS = ["dss_connection_permissions", "dss_connections", "dss_dataset_shares", "dss_datasets",
                   "dss_flow_links", "dss_group_members", "dss_groups", "dss_managed_folders", "dss_project_tags",
                   "dss_projects", "dss_users", "inventory_log"]
check("extract: no input read, the twelve outputs written", sorted(WRITTEN) == EXTRACT_OUTPUTS, sorted(WRITTEN))
check("extract: summary with the failed project", "2 of 3 projects read" in printed and "FAILED BROKEN" in printed,
      printed)
extract_declared = True
for name in WRITTEN:
    if schema_columns(name) != declared_columns(name) or not (WRITTEN[name].snapshot_day == today).all():
        extract_declared = False
check("extract: every output written with its declared schema and the day", extract_declared)
extracted = dict(WRITTEN)

# --- refine_inventory ------------------------------------------------------------------------
REFINE_OUTPUTS = ["dataset_locations", "dataset_roles", "dataset_storage", "dataset_tables", "project_consumption",
                  "table_ownership"]
INPUTS["databases"] = DATABASES
INPUTS["audit_connection_mapping"] = CONNECTION_MAPPING
INPUTS["tables"] = pg_run_tables
INPUTS["role_settings"] = pg_run_settings
for name in extracted:
    INPUTS[name] = extracted[name]
run_recipe("refine_inventory.py", {"DKU_DST_snapshot_day": today})
check("refine: the six outputs written", sorted(WRITTEN) == REFINE_OUTPUTS, sorted(WRITTEN))
refine_declared = True
for name in WRITTEN:
    if schema_columns(name) != declared_columns(name):
        refine_declared = False
check("refine: every output written with its declared schema", refine_declared)
tables_found = WRITTEN["dataset_tables"].set_index(["project_key", "dataset_name"])
check("refine: datasets matched to the tables read back from the datasets",
      tables_found.loc[("SALES", "raw_orders"), "match_status"] == "FOUND")

# An older day in the inputs is ignored
older = extracted["dss_datasets"].copy()
older["snapshot_day"] = "2000-01-01"
older["dataset_name"] = older["dataset_name"] + "_old"
INPUTS["dss_datasets"] = pd.concat([older, extracted["dss_datasets"]], ignore_index=True)
run_recipe("refine_inventory.py", {"DKU_DST_snapshot_day": today})
check("refine: only the latest day of the inputs is used",
      not WRITTEN["dataset_storage"].object_name.str.endswith("_old").any())

# Empty role_settings (first run, or Oracle only): the refinement still runs, default search_path
INPUTS["dss_datasets"] = extracted["dss_datasets"]
INPUTS["role_settings"] = schemas.conform(pd.DataFrame(), "role_settings")
run_recipe("refine_inventory.py", {"DKU_DST_snapshot_day": today})
locations = WRITTEN["dataset_locations"]
check("refine with empty role_settings: runs, PostgreSQL default search_path \"$user\", public",
      sorted(WRITTEN) == REFINE_OUTPUTS
      and locations[locations.dataset_name == "raw_orders"].schema_name.tolist() == ["dbmon", "public"])

finish()
