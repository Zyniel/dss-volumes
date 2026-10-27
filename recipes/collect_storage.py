# DSS Python recipe: collect the storage of every database.
#
# Input   databases       one row per database to read, columns:
#                           connection   DSS connection with the admin account
#                           engine       oracle or postgresql
#                           objects      optional. N: tablespaces only, no objects nor tables
#                           label        optional. Name shown in the reports (default: the connection)
#                           environment  optional. PROD, DEV...
# Outputs storage_units   one row per tablespace
#         role_settings   PostgreSQL search_path settings of roles and databases
#         objects         one row per object and tablespace
#         tables          one row per table: its objects summed
#         collect_log     one row per database: OK or FAILED, error, seconds per query
#
# Every row starts with snapshot_day, the day of the run (YYYY-MM-DD). Partition the outputs
# by day on that column: running the recipe again the same day replaces that day.
import datetime

import dataiku

from dbmon import collect, progress, schemas

# Databases read at the same time. 1: one after the other.
PARALLEL = 4

# ---------------------------------------------------------------------------------------
# Day of this run
# ---------------------------------------------------------------------------------------
# The databases can only be read as they are now: the recipe refuses to fill another
# day's partition. Today is accepted in the server's time zone and in UTC.
now_utc = datetime.datetime.now(datetime.timezone.utc)
today_server = now_utc.astimezone().date().isoformat()
today_utc = now_utc.date().isoformat()

day = today_server
if hasattr(dataiku, "dku_flow_variables"):
    # Outputs partitioned by day: the partition being built
    day = dataiku.dku_flow_variables.get("DKU_DST_snapshot_day", today_server)

if day != today_server and day != today_utc:
    raise ValueError("only today's partition can be built (%s), not %s" % (today_server, day))

# ---------------------------------------------------------------------------------------
# Collect
# ---------------------------------------------------------------------------------------
progress.log("collect_storage: building the partition %s" % day)
databases = dataiku.Dataset("databases").get_dataframe()
progress.log("databases: %d rows read" % len(databases))
results, log = collect.collect_all(databases, parallel=PARALLEL)
results["collect_log"] = log

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

# ---------------------------------------------------------------------------------------
# Summary in the job log
# ---------------------------------------------------------------------------------------
failed = log[log["status"] == "FAILED"]
progress.log("%d of %d databases collected" % (len(log) - len(failed), len(log)))
for index, failure in failed.iterrows():
    progress.log("FAILED %s: %s" % (failure["dss_connection"], failure["error"]))
