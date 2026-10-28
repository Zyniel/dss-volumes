# DSS Python recipe: raw extraction of what DSS knows: projects, datasets, managed folders,
# flow links, connections and their permissions, groups and users.
#
# Run it as a user with an admin API key (scenario "Run as" an admin). It does not depend on
# the database extraction (collect_storage): both can run first.
#
# Input   instances                   one row per DSS instance to read (Design, Automation
#                                     nodes... writing to the monitored databases), with at
#                                     least instance_name: copied in dss_instance, and given to
#                                     com.arval.cdplib.client.get_remote_admin_client.
#                                     No row: this instance only, named "local".
# Outputs dss_projects                one row per project
#         dss_project_tags            one row per project and tag
#         dss_datasets                one row per dataset: settings, storage, sharing, metrics
#         dss_managed_folders         one row per managed folder: settings, metrics
#         dss_dataset_shares          one row per shared dataset and project it is exposed to
#         dss_flow_links              one row per recipe input or output (dataset or folder)
#         dss_connections             one row per connection: where it points
#         dss_connection_permissions  one row per connection, permission and allowed group
#         dss_groups                  one row per group
#         dss_group_members           one row per group and user
#         dss_users                   one row per user (no email)
#         inventory_log               one row per project: OK or FAILED
#
# dss_connections.jdbc_url is the JDBC URL as configured: it can hold a password. Restrict
# the access to that dataset accordingly.
#
# Every row starts with snapshot_day, the day of the run, and dss_instance. Partition the outputs by day on
# that column, like the outputs of collect_storage.
import datetime

import com.arval.cdplib.client as cdpclient
import dataiku

from dbmon import dss_extract, progress, schemas

# Projects read at the same time. 1: one after the other. Each project is a few admin API
# calls plus one per dataset and folder (metrics): lower it if the DSS backend is busy.
PARALLEL = 4

# Last computed metrics (rows, size, files): one API call per dataset and managed folder.
# "ALL": every dataset and folder. "NONE": none, when runs get too slow.
METRICS = "ALL"

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
progress.log("extract_dss: building the partition %s" % day)
instance_rows = dataiku.Dataset("instances").get_dataframe().to_dict("records")
instances = dss_extract.connect_instances(instance_rows, cdpclient.get_remote_admin_client, dataiku.api_client())
results = dss_extract.extract_instances(instances, parallel=PARALLEL, metrics=METRICS)

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

log = results["inventory_log"]
failed = log[log["status"] == "FAILED"]
progress.log("%d of %d projects read" % (len(log) - len(failed), len(log)))
for index, failure in failed.iterrows():
    progress.log("FAILED %s %s: %s" % (failure["dss_instance"], failure["project_key"], failure["error"]))
