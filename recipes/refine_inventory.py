# DSS Python recipe: ties the DSS extraction to the database extraction, and tells what each
# project is responsible for. No API call, no SQL: plain Python on the datasets below.
#
# Run it after collect_storage and extract_dss, on the same day.
#
# Inputs  databases                 the same dataset as collect_storage: admin connections, labels
#         audit_connection_mapping  one row per DSS connection used by datasets: dss_connection,
#                                   and audit_connection, the admin connection of databases
#                                   that exports its tables
#         tables, role_settings, naming_rules   from collect_storage
#         dss_projects, dss_project_tags, dss_datasets, dss_managed_folders, dss_flow_links,
#         dss_connections, inventory_log   from extract_dss
# Outputs dataset_roles             one row per dataset or folder: writers, readers, nature
#         dataset_locations         one row per dataset and candidate schema, best first
#         dataset_tables            one row per dataset: the step 1 table it is stored in
#         table_ownership           one row per step 1 table: its space and its owner
#         dataset_storage           one row per dataset or folder: storage kind, size, counted
#         project_consumption       one row per project: the space it is responsible for
#         database_usage            one row per database, schema and DSS relation: piloted
#                                   space and possible waste
#         schema_usage              one row per schema of every database, even empty: its
#                                   space split by DSS relation, in MB and %
#
# The partitioned inputs are read for the day of the run (the most recent day they hold when
# the recipe reads more than one).
import datetime

import dataiku

from dbmon import progress, refine, schemas

DSS_INPUTS = ["dss_projects", "dss_project_tags", "dss_datasets", "dss_managed_folders", "dss_flow_links",
              "dss_connections", "inventory_log"]


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
progress.log("refine_inventory: building the partition %s" % day)


def read_input(name, latest_only):
    """Rows of an input dataset (its latest day only when latest_only), with a progress trace."""
    rows = dataiku.Dataset(name).get_dataframe()
    if latest_only:
        rows = latest_day(rows)
    progress.log("%s: %d rows read" % (name, len(rows)))
    return rows


databases = read_input("databases", False)
connection_mapping = read_input("audit_connection_mapping", False)
tables = read_input("tables", True)
role_settings = read_input("role_settings", True)
naming_rules = read_input("naming_rules", True)
dss = {}
for name in DSS_INPUTS:
    dss[name] = read_input(name, True)

results = refine.refine(dss, tables, role_settings, databases, connection_mapping, naming_rules)

# ---------------------------------------------------------------------------------------
# Write, every row starting with the day of the run. Every output gets its declared schema
# (dbmon/schemas.py), even when it has no row.
# ---------------------------------------------------------------------------------------
for name in results:
    rows = results[name]
    rows["snapshot_day"] = day
    progress.log("%s: writing %d rows" % (name, len(rows)))
    written = schemas.write(dataiku.Dataset(name), rows, name)
    progress.log("%s: %d rows written" % (name, len(written)))
