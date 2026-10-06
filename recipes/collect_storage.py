# DSS Python recipe: collect the storage of every database.
#
# Input   databases       one row per database to read, columns:
#                           connection  DSS connection with the admin account
#                           engine      oracle or postgresql
# Outputs storage_units   one row per tablespace
#         objects         one row per object and tablespace
#         collect_log     one row per database: OK or FAILED, duration, error
#
# Run it from a daily scenario. To keep the history, tick "Append instead of overwrite"
# on the outputs (Inputs / Outputs tab of the recipe).
import dataiku

from dbmon import collect

databases = dataiku.Dataset("databases").get_dataframe()
results, log = collect.collect_all(databases)

for name, df in results.items():
    dataiku.Dataset(name).write_with_schema(df)
dataiku.Dataset("collect_log").write_with_schema(log)
