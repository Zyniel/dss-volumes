"""Oracle adapter (19c): dictionary views DBA_*, V$DATABASE, V$INSTANCE, V$ASM_DISKGROUP_STAT.

The monitoring account needs SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY).
Tablespaces belong to a database (PDB or non-CDB), so every fact is per database.
"""
import os

from ..base import Adapter, first_row


class Oracle(Adapter):
    engine = "oracle"
    dss_types = ("Oracle",)
    storage_level = "database"
    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

    IDENTITY_SQL = """
SELECT d.name                                  AS db_name,
       d.dbid,
       d.db_unique_name,
       SYS_CONTEXT('USERENV', 'CON_NAME')      AS con_name,
       NVL(NULLIF(d.con_dbid, 0), d.dbid)      AS con_dbid,
       d.database_role,
       i.version,
       i.instance_name
FROM   v$database d
       CROSS JOIN v$instance i
"""

    # single-table filter of each template: placeholder -> predicate
    TABLE_FILTERS = {
        "objects": {"table_filter": "WHERE o.parent_table = %s"},
        "table_attributes": {"table_filter": "AND t.table_name = %s"},
        "lifecycle": {"table_filter": "AND t.table_name = %s",
                      "object_filter": "AND o.object_name = %s",
                      "mods_filter": "AND m.table_name = %s"},
    }

    def identify(self, run):
        row = first_row(run(self.IDENTITY_SQL))
        return {
            # logical identities: the same on every RAC node, service and Data Guard copy
            "server_key": (row["db_name"], int(row["dbid"])),
            "db_key": (row["con_name"], int(row["con_dbid"])),
            "server_name": row["db_name"],
            "database_name": row["con_name"],
            "unique_name": row["db_unique_name"],
            "role": "PRIMARY" if row["database_role"] == "PRIMARY" else "STANDBY",
            "version": row["version"],
            "instance": row["instance_name"],
        }

    def session(self, fact):
        return ["BEGIN DBMS_APPLICATION_INFO.SET_MODULE(module_name => 'DBMON', action_name => %s); END;"
                % self.literal(fact[:32])]

    def filters(self, fact, schemas=None, table=None, tablespaces=None):
        if fact == "tablespaces":
            return {"ts_filter": self.where_in("t.tablespace_name", tablespaces),
                    "seg_filter": self.where_in("s.tablespace_name", tablespaces)}
        values = {"scope_filter": self.scope(schemas)}
        for placeholder, predicate in self.TABLE_FILTERS.get(fact, {}).items():
            values[placeholder] = predicate % self.literal(table) if table else ""
        return values

    def scope(self, schemas):
        """Condition on DBA_USERS u: the schemas given, or every schema not maintained by Oracle."""
        return "u.oracle_maintained = 'N'" if schemas is None else self.in_list("u.username", schemas)

    def schemas_holding(self, table, schemas=None):
        return ("SELECT t.owner FROM dba_tables t WHERE t.table_name = %s AND t.owner IN "
                "(SELECT u.username FROM dba_users u WHERE %s)" % (self.literal(table), self.scope(schemas)))
