"""PostgreSQL adapter (12 or later): pg_class, pg_namespace, pg_index, pg_tablespace,
pg_stat_all_tables, pg_partition_root / pg_partition_tree.

The monitoring role needs pg_monitor (or at least pg_read_all_stats, for tablespace
sizes). pg_control_system() identifies the cluster; without access to it, replicas can
no longer be recognised and host:port identifies the server instead.
Tablespaces are shared by every database of a cluster: that fact is collected once
per server.
"""
import os

from ..base import Adapter, first_row, text, truthy


class PostgreSQL(Adapter):
    engine = "postgresql"
    dss_types = ("PostgreSQL",)
    storage_level = "server"
    server_facts = ("tablespaces",)
    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
    max_in_list = 10000

    IDENTITY_SQL = """
SELECT current_database()                                    AS database_name,
       pg_is_in_recovery()                                   AS in_recovery,
       current_setting('server_version')                     AS version,
       NULLIF(current_setting('cluster_name'), '')           AS cluster_name,
       COALESCE(host(inet_server_addr()), 'local') || ':' || current_setting('port') AS host_port
"""
    SYSTEM_SQL = "SELECT system_identifier::text AS system_identifier FROM pg_control_system()"

    # single-table filter of each template: placeholder -> predicate
    TABLE_FILTERS = {
        "objects": {"table_filter": "WHERE o.parent_table = %s"},
        "table_attributes": {"table_filter": "AND c.relname = %s"},
        "lifecycle": {"table_filter": "AND c.relname = %s"},
    }

    def identify(self, run):
        row = first_row(run(self.IDENTITY_SQL))
        try:
            system_id = text(first_row(run(self.SYSTEM_SQL))["system_identifier"])
        except Exception:
            system_id = ""
        host_port = text(row["host_port"])
        # the system identifier is shared by a primary and its physical replicas
        server = ("system", system_id) if system_id else ("host", host_port)
        return {
            "server_key": server,
            "db_key": server + (text(row["database_name"]),),
            "server_name": text(row["cluster_name"]) or host_port,
            "database_name": text(row["database_name"]),
            "unique_name": host_port,
            "role": "STANDBY" if truthy(row["in_recovery"]) else "PRIMARY",
            "version": text(row["version"]),
            "instance": host_port,
        }

    def session(self, fact):
        return ["SET application_name = %s" % self.literal(("dbmon:" + fact)[:63])]

    def filters(self, fact, schemas=None, table=None, tablespaces=None):
        if fact == "tablespaces":
            return {"ts_filter": self.where_in("t.spcname", tablespaces)}
        values = {"scope_filter": self.scope(schemas)}
        for placeholder, predicate in self.TABLE_FILTERS.get(fact, {}).items():
            values[placeholder] = predicate % self.literal(table) if table else ""
        return values

    def scope(self, schemas):
        """Condition on pg_namespace n: the schemas given, or every non-system schema
        (PostgreSQL reserves the pg_ prefix for its own schemas)."""
        if schemas is None:
            return "n.nspname NOT LIKE 'pg\\_%' AND n.nspname <> 'information_schema'"
        return self.in_list("n.nspname", schemas)

    def schemas_holding(self, table, schemas=None):
        return ("SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = %s AND c.relkind IN ('r', 'p', 'm', 'f') AND NOT c.relispartition "
                "AND %s" % (self.literal(table), self.scope(schemas)))
