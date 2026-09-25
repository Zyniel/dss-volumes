"""What an adapter provides, and the helpers adapters share.

An adapter describes one database engine: how to identify the database behind a
connection, and how to read each fact of the contract (dbmon.model) from the
engine's own catalog. It returns raw facts only: everything that interprets them
(keys, rollups, placement, use cases) lives in the core, written once.
"""
import os
import re

_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


class Adapter(object):
    engine = None               # value of the "engine" identity column
    dss_types = ()              # Dataiku connection types handled by this adapter
    storage_level = "database"  # where tablespaces live: "database" or "server"
    server_facts = ()           # facts describing the whole server, collected once per server
    sql_dir = None              # folder with one template per fact: <fact>.sql
    max_in_list = 1000          # values per IN (...) list

    # --- to implement ----------------------------------------------------------------
    def identify(self, run):
        """Identity of the database behind a connection.

        run(sql) executes a query and returns a DataFrame. Returns a dict with:
          server_key, db_key  tuples identifying the server and the database logically:
                              the same on every node, service or replica
          server_name, database_name, unique_name, role (PRIMARY or STANDBY), version,
          instance (the node that answered, for the run log)
        """
        raise NotImplementedError

    def session(self, fact):
        """Statements run before a query, in the same session (e.g. naming the session)."""
        return []

    def filters(self, fact, schemas=None, table=None, tablespaces=None):
        """Placeholder -> SQL fragment for the filters of one fact."""
        raise NotImplementedError

    def schemas_holding(self, table, schemas=None):
        """SQL returning, in its first column, the schemas in scope holding that table."""
        raise NotImplementedError

    # --- shared ------------------------------------------------------------------------
    def level(self, fact):
        return "server" if fact in self.server_facts else "database"

    def render(self, fact, schemas=None, table=None, tablespaces=None):
        """SQL text of a fact with its filters applied."""
        values = self.filters(fact, schemas=schemas, table=table, tablespaces=tablespaces)
        sql = self.template(fact)
        found = set(_PLACEHOLDER.findall(sql))
        if found != set(values):
            raise RuntimeError("%s adapter: placeholders of %s.sql and filters differ: %s"
                               % (self.engine, fact, sorted(found ^ set(values))))
        for key, value in values.items():
            sql = sql.replace("{{%s}}" % key, value)
        return sql

    def template(self, fact):
        cache = self.__dict__.setdefault("_templates", {})
        if fact not in cache:
            with open(os.path.join(self.sql_dir, fact + ".sql"), encoding="utf-8") as f:
                cache[fact] = f.read()
        return cache[fact]

    @staticmethod
    def literal(value):
        """SQL string literal, quotes doubled (same syntax in Oracle and PostgreSQL)."""
        return "'" + str(value).replace("'", "''") + "'"

    def in_list(self, column, values):
        """column IN (...), split in chunks of max_in_list; an empty list selects nothing."""
        if not values:
            return "1 = 0"
        n = self.max_in_list
        parts = ["%s IN (%s)" % (column, ", ".join(self.literal(v) for v in values[i:i + n]))
                 for i in range(0, len(values), n)]
        return parts[0] if len(parts) == 1 else "(" + " OR ".join(parts) + ")"

    def where_in(self, column, values):
        return "" if values is None else "WHERE " + self.in_list(column, values)


def first_row(df):
    """First row of a result, column names lower-cased."""
    return {str(c).lower(): v for c, v in df.iloc[0].items()}


def text(value):
    """String value, '' for None / NaN."""
    if value is None or (isinstance(value, float) and value != value):
        return ""
    return str(value).strip()


def truthy(value):
    """Boolean from a driver value: True / 't' / 'true' / 1."""
    return text(value).lower() in ("t", "true", "1", "yes", "y")
