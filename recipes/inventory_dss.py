# DSS Python recipe: inventory of the datasets involved in recipes, and where they are stored.
#
# Run it as a user with an admin API key (scenario "Run as" an admin), after collect_storage.
#
# Inputs  databases              the same dataset as collect_storage. Optional column
#                                dss_connections: the DSS connections pointing to that
#                                database, comma separated
#         role_settings          from collect_storage: the latest day is used
# Outputs dss_projects           one row per project: tags, use case
#         dss_datasets           one row per dataset: settings, storage, sharing, metrics
#         dss_flow_links         one row per recipe input or output
#         dss_connections        one row per connection
#         dss_dataset_locations  one row per dataset and candidate schema, best first
#         inventory_log          one row per project: OK or FAILED
#
# Every row starts with snapshot_day, the day of the run. Partition the outputs by day on
# that column, like the outputs of collect_storage.
import datetime

import dataiku

from dbmon import inventory, schemas

# Projects read at the same time. 1: one after the other. Each project is a few admin API
# calls plus one per dataset (metrics): lower it if the DSS backend is busy during the run.
PARALLEL = 4

# Also ask DSS for the location it resolves itself (dataiku.Dataset.get_location_info).
# False: resolve from the dataset and connection settings only.
USE_DSS_LOCATION = True


def latest_day(rows):
    """Rows of the most recent snapshot_day only."""
    if rows.empty or "snapshot_day" not in rows.columns:
        return rows
    last_day = rows["snapshot_day"].max()
    return rows[rows["snapshot_day"] == last_day]


def read_location(project_key, dataset_name):
    dataset = dataiku.Dataset(project_key + "." + dataset_name)
    return dataset.get_location_info()


# ---------------------------------------------------------------------------------------
# Day of this run
# ---------------------------------------------------------------------------------------
now_utc = datetime.datetime.now(datetime.timezone.utc)
day = now_utc.astimezone().date().isoformat()
if hasattr(dataiku, "dku_flow_variables"):
    day = dataiku.dku_flow_variables.get("DKU_DST_snapshot_day", day)

# ---------------------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------------------
databases = dataiku.Dataset("databases").get_dataframe()
role_settings = latest_day(dataiku.Dataset("role_settings").get_dataframe())

location_reader = None
if USE_DSS_LOCATION:
    location_reader = read_location

client = dataiku.api_client()
results = inventory.collect_inventory(client, databases, role_settings, location_reader, parallel=PARALLEL)

# ---------------------------------------------------------------------------------------
# Write, every row starting with the day of the run. Every output gets its declared schema
# (dbmon/schemas.py), even when it has no row.
# ---------------------------------------------------------------------------------------
for name in results:
    rows = results[name]
    rows["snapshot_day"] = day
    written = schemas.write(dataiku.Dataset(name), rows, name)
    print("%s: %d rows" % (name, len(written)))

log = results["inventory_log"]
failed = log[log["status"] == "FAILED"]
print("%d of %d projects read" % (len(log) - len(failed), len(log)))
for index, failure in failed.iterrows():
    print("FAILED %s: %s" % (failure["project_key"], failure["error"]))
