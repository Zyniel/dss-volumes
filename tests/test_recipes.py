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
from fake_dss import DATABASES, FakeClient

WRITTEN = {}
INPUTS = {}


class FakeDataset(object):
    def __init__(self, name):
        self.name = name

    def get_dataframe(self):
        return INPUTS[self.name].copy()

    def write_with_schema(self, rows):
        WRITTEN[self.name] = rows.copy()

    def get_location_info(self):
        raise Exception("not an input of this recipe")


def run_recipe(file_name, flow_variables):
    extra = {"Dataset": FakeDataset, "api_client": FakeClient}
    if flow_variables is not None:
        extra["dku_flow_variables"] = flow_variables
    install_fake_dataiku(extra)
    WRITTEN.clear()
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

# --- inventory_dss ---------------------------------------------------------------------------
INPUTS["databases"] = DATABASES
INPUTS["tables"] = WRITTEN["tables"]
INPUTS["role_settings"] = WRITTEN["role_settings"]
printed = run_recipe("inventory_dss.py", {"DKU_DST_snapshot_day": today})
check("inventory: the four outputs written",
      sorted(WRITTEN) == ["dss_connections", "dss_datasets", "dss_flow_links", "inventory_log"], sorted(WRITTEN))
check("inventory: summary with the failed project", "2 of 3 projects read" in printed and "FAILED BROKEN" in printed,
      printed)
datasets = WRITTEN["dss_datasets"].set_index(["project_key", "dataset_name"])
check("inventory: DSS location not readable, settings used, table found",
      datasets.loc[("SALES", "raw_orders"), "location_status"] == "FOUND"
      and "DSS location not readable" in datasets.loc[("SALES", "raw_orders"), "location_note"])

finish()
