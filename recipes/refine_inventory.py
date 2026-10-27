# DSS Python recipe: ties the DSS extraction to the database extraction, and tells what each
# project is responsible for. No API call, no SQL: plain Python on the datasets below.
#
# Run it after collect_storage and extract_dss, on the same day.
#
# Inputs  databases                 the same dataset as collect_storage: admin connections, labels
#         audit_connection_mapping  one row per DSS connection used by datasets: dss_connection,
#                                   and audit_connection, the admin connection of databases
#                                   that exports its tables
#         tables, role_settings     from collect_storage
#         dss_projects, dss_project_tags, dss_datasets, dss_managed_folders, dss_flow_links,
#         dss_connections           from extract_dss
# Outputs dataset_roles             one row per dataset or folder: writers, readers, nature
#         dataset_locations         one row per dataset and candidate schema, best first
#         dataset_tables            one row per dataset: the step 1 table it is stored in
#         table_ownership           one row per step 1 table: its space and its owner
#         dataset_storage           one row per dataset or folder: storage kind, size, counted
#         project_consumption       one row per project: the space it is responsible for
#
# The partitioned inputs are read for the day of the run (the most recent day they hold when
# the recipe reads more than one).
import datetime

import dataiku

from dbmon import refine, schemas

DSS_INPUTS = ["dss_projects", "dss_project_tags", "dss_datasets", "dss_managed_folders", "dss_flow_links",
              "dss_connections"]


def latest_day(rows):
    """Rows of the most recent snapshot_day only."""
    if rows.empty or "snapshot_day" not in rows.columns:
        return rows
    last_day = rows["snapshot_day"].max()
    return rows[rows["snapshot_day"] == last_day]


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
connection_mapping = dataiku.Dataset("audit_connection_mapping").get_dataframe()
tables = latest_day(dataiku.Dataset("tables").get_dataframe())
role_settings = latest_day(dataiku.Dataset("role_settings").get_dataframe())
dss = {}
for name in DSS_INPUTS:
    dss[name] = latest_day(dataiku.Dataset(name).get_dataframe())

results = refine.refine(dss, tables, role_settings, databases, connection_mapping)

# ---------------------------------------------------------------------------------------
# Write, every row starting with the day of the run. Every output gets its declared schema
# (dbmon/schemas.py), even when it has no row.
# ---------------------------------------------------------------------------------------
for name in results:
    rows = results[name]
    rows["snapshot_day"] = day
    written = schemas.write(dataiku.Dataset(name), rows, name)
    print("%s: %d rows" % (name, len(written)))
