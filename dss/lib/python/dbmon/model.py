"""The dbmon contract: what every adapter returns and what the library produces.

Declarative only: no SQL, no Dataiku calls. Adapters (dbmon.adapters) return the
columns of each fact. The core checks every result against this contract, moves
the columns it does not know into the JSON column "details", adds the identity
columns and the keys, and derives the results that have sources.
"""

TEXT, NUMBER, INTEGER, DATE = "text", "number", "integer", "date"


class Column(object):
    """One column of the contract.

    kind      text, number, integer or date
    computed  filled by the core, not by the adapters
    """

    def __init__(self, name, kind, meaning, unit=None, computed=False):
        self.name, self.kind, self.meaning, self.unit, self.computed = name, kind, meaning, unit, computed


class Result(object):
    """One result: a fact returned by the adapters, or a result derived from facts.

    grain    columns identifying a row
    filters  filters it accepts: schemas, table, tablespaces
    keys     uid column -> (entity, columns, scope). The uid hashes the entity, the
             identity of its scope and the column values. scope: "database", "server",
             or "storage" (where the engine keeps tablespaces: an Oracle PDB, a whole
             PostgreSQL cluster)
    sources  facts a derived result is computed from (None for a fact)
    """

    def __init__(self, name, doc, grain, filters, columns, keys, sources=None):
        self.name, self.doc, self.grain, self.filters = name, doc, grain, filters
        self.columns, self.keys, self.sources = columns, keys, sources

    @property
    def names(self):
        return [c.name for c in self.columns]

    @property
    def required(self):
        """Columns an adapter must return."""
        return [c.name for c in self.columns if not c.computed]


# Identity columns, in front of every row
FRONT = [
    Column("collected_utc", DATE, "start of the call that collected the row (UTC)"),
    Column("dss_connection", TEXT, "Dataiku connection the row was read through"),
]
IDENTITY = [
    Column("engine", TEXT, "oracle or postgresql"),
    Column("server_uid", TEXT, "key of the server: Oracle CDB (or non-CDB), PostgreSQL cluster"),
    Column("db_uid", TEXT, "key of the database: Oracle PDB (or non-CDB), PostgreSQL database. "
                           "NULL on rows that belong to the whole server"),
    Column("server_name", TEXT, "Oracle DB_NAME, PostgreSQL cluster_name or host:port"),
    Column("database_name", TEXT, "Oracle container name, PostgreSQL database. NULL on server rows"),
    Column("unique_name", TEXT, "physical copy that answered: Oracle DB_UNIQUE_NAME, PostgreSQL host:port"),
    Column("role", TEXT, "PRIMARY or STANDBY"),
    Column("version", TEXT, "server version"),
]
DETAILS = Column("details", TEXT, "engine-only columns, as a JSON object")

TABLE_KEYS = {"table_uid": ("table", ["schema_name", "table_name"], "database"),
              "schema_uid": ("schema", ["schema_name"], "database")}

RESULTS = {}


def _add(result):
    RESULTS[result.name] = result


_add(Result(
    "tablespaces", "One row per tablespace.",
    grain=["tablespace_name"],
    filters=("tablespaces",),
    columns=[
        Column("tablespace_name", TEXT, "tablespace"),
        Column("contents", TEXT, "PERMANENT, TEMPORARY or UNDO"),
        Column("allocated_mb", NUMBER, "space the tablespace takes on storage", "MB"),
        Column("used_mb", NUMBER, "space used by live objects", "MB"),
        Column("reclaimable_mb", NUMBER, "space held by dropped objects the engine reuses before growing "
                                         "(Oracle recycle bin). NULL where the concept does not exist", "MB"),
        Column("free_mb", NUMBER, "free space inside the allocated files. NULL when unknown", "MB"),
        Column("max_mb", NUMBER, "declared ceiling. NULL when unbounded or unknown", "MB"),
    ],
    keys={"tablespace_uid": ("tablespace", ["tablespace_name"], "storage")},
))

_add(Result(
    "schemas", "One row per schema and tablespace it is tied to: default tablespace, quotas, "
               "tablespaces holding its objects, recycle bin.",
    grain=["schema_name", "tablespace_name"],
    filters=("schemas",),
    columns=[
        Column("schema_name", TEXT, "schema"),
        Column("owner", TEXT, "role owning the schema (Oracle: the schema itself)"),
        Column("tablespace_name", TEXT, "tablespace"),
        Column("is_default_ts", TEXT, "Y for the schema's default tablespace"),
        Column("quota_status", TEXT, "UNLIMITED, LIMITED or NONE. NULL where quotas do not exist"),
        Column("quota_max_mb", NUMBER, "quota ceiling", "MB"),
        Column("quota_used_mb", NUMBER, "space charged to the quota", "MB"),
        Column("reclaimable_mb", NUMBER, "dropped objects still held (Oracle recycle bin)", "MB"),
    ],
    keys={"schema_uid": ("schema", ["schema_name"], "database"),
          "tablespace_uid": ("tablespace", ["tablespace_name"], "storage")},
))

_add(Result(
    "objects", "One row per object and tablespace, partitions summed.",
    grain=["schema_name", "object_name", "object_type", "tablespace_name"],
    filters=("schemas", "table"),
    columns=[
        Column("schema_name", TEXT, "schema of the object. LOB / TOAST storage: the schema of its table"),
        Column("object_name", TEXT, "exact name"),
        Column("object_type", TEXT, "the engine's own type: INDEX, LOBSEGMENT, TOAST TABLE, SEQUENCE..."),
        Column("family", TEXT, "TABLE, INDEX, LOB, LOBINDEX or OTHER"),
        Column("parent_schema", TEXT, "schema of the table the object belongs to"),
        Column("parent_table", TEXT, "table the object belongs to. NULL when there is none"),
        Column("tablespace_name", TEXT, "tablespace"),
        Column("ts_kind", TEXT, "DEFAULT (default tablespace), QUOTA (another tablespace where the "
                                "schema holds a quota) or OTHER"),
        Column("partitioned", TEXT, "YES when the row sums several partitions"),
        Column("n_segments", INTEGER, "storage units summed into the row (partitions)"),
        Column("size_mb", NUMBER, "allocated storage", "MB"),
        Column("compression", TEXT, "the engine's compression setting. NULL when not applicable"),
    ],
    keys={"object_uid": ("object", ["schema_name", "object_name", "object_type", "tablespace_name"], "database"),
          "table_uid": ("table", ["parent_schema", "parent_table"], "database"),
          "schema_uid": ("schema", ["schema_name"], "database"),
          "tablespace_uid": ("tablespace", ["tablespace_name"], "storage")},
))

_add(Result(
    "table_attributes", "One row per table: kind and optimizer statistics.",
    grain=["schema_name", "table_name"],
    filters=("schemas", "table"),
    columns=[
        Column("schema_name", TEXT, "schema"),
        Column("table_name", TEXT, "table"),
        Column("table_kind", TEXT, "HEAP, PARTITIONED, IOT, CLUSTERED, TEMPORARY, UNLOGGED, "
                                   "MATERIALIZED VIEW or FOREIGN"),
        Column("storage_created", TEXT, "YES when the table has storage of its own (NO, or N/A for "
                                        "a partitioned table)"),
        Column("row_estimate", NUMBER, "rows according to the optimizer statistics"),
        Column("avg_row_len", NUMBER, "average row length from the statistics. NULL when unknown", "bytes"),
        Column("last_analyzed", DATE, "last statistics gathering"),
    ],
    keys=TABLE_KEYS,
))

_add(Result(
    "lifecycle", "One row per table: creation, last DDL and changes since the last statistics.",
    grain=["schema_name", "table_name"],
    filters=("schemas", "table"),
    columns=[
        Column("schema_name", TEXT, "schema"),
        Column("table_name", TEXT, "table"),
        Column("created", DATE, "creation time. NULL where the engine does not record it"),
        Column("last_ddl_time", DATE, "last DDL. NULL where the engine does not record it"),
        Column("row_estimate", NUMBER, "rows according to the optimizer statistics"),
        Column("last_analyzed", DATE, "last statistics gathering"),
        Column("dml_since_stats", NUMBER, "rows inserted, updated or deleted since the last statistics"),
        Column("truncated_since_stats", TEXT, "YES when truncated since the last statistics. NULL when unknown"),
        Column("dml_pct_since_stats", NUMBER, "100 * dml_since_stats / row_estimate", computed=True),
        Column("stale_est", TEXT, "YES when truncated or when the DML exceeds 10 % of the rows (Oracle's "
                                  "default STALE_PERCENT), NULL when never analysed", computed=True),
    ],
    keys=TABLE_KEYS,
))

_add(Result(
    "tables", "One row per table: its objects rolled up (indexes, LOB / TOAST, partitions), "
              "plus its attributes. Objects that belong to no table get a row of their own.",
    grain=["schema_name", "table_name"],
    filters=("schemas", "table"),
    sources=("objects", "table_attributes"),
    columns=[
        Column("schema_name", TEXT, "schema"),
        Column("table_name", TEXT, "table, or the object's own name when it belongs to no table"),
        Column("table_kind", TEXT, "from table_attributes, or the object type when it belongs to no table"),
        Column("storage_created", TEXT, "from table_attributes"),
        Column("n_segments", INTEGER, "storage units rolled up"),
        Column("total_mb", NUMBER, "all its objects", "MB"),
        Column("table_mb", NUMBER, "family TABLE", "MB"),
        Column("index_mb", NUMBER, "family INDEX", "MB"),
        Column("lob_mb", NUMBER, "family LOB", "MB"),
        Column("lobindex_mb", NUMBER, "family LOBINDEX", "MB"),
        Column("other_mb", NUMBER, "family OTHER", "MB"),
        Column("in_default_ts_mb", NUMBER, "in the schema's default tablespace", "MB"),
        Column("in_quota_ts_mb", NUMBER, "in another tablespace where the schema holds a quota", "MB"),
        Column("in_other_ts_mb", NUMBER, "anywhere else", "MB"),
        Column("n_tablespaces", INTEGER, "tablespaces holding its objects"),
        Column("tablespaces", TEXT, "those tablespaces, comma separated"),
        Column("n_indexes", INTEGER, "indexes"),
        Column("n_lob_segments", INTEGER, "LOB / TOAST objects"),
        Column("n_table_partitions", INTEGER, "table partitions"),
        Column("table_compression", TEXT, "compression settings of its table data, comma separated"),
        Column("row_estimate", NUMBER, "from table_attributes"),
        Column("avg_row_len", NUMBER, "from table_attributes", "bytes"),
        Column("est_row_data_mb", NUMBER, "row_estimate * avg_row_len: above table_mb means compression "
                                          "gain, far below means empty space", "MB"),
        Column("last_analyzed", DATE, "from table_attributes"),
    ],
    keys=TABLE_KEYS,
))

FACTS = tuple(name for name, r in RESULTS.items() if not r.sources)
DERIVED = tuple(name for name, r in RESULTS.items() if r.sources)
UID_COLUMNS = sorted({key for r in RESULTS.values() for key in r.keys})


def reserved():
    """Column names callers cannot use as labels."""
    return {c.name for c in FRONT + IDENTITY} | set(UID_COLUMNS) | {DETAILS.name}


def markdown():
    """The contract as Markdown tables, one per result."""
    lines = ["# dbmon columns", "",
             "Generated from `dbmon/model.py`. Every row starts with the identity columns, then the keys, "
             "then the columns of its result, then `details`.", "",
             "## Identity (every row)", "", "| Column | Meaning |", "|---|---|"]
    lines += ["| `%s` | %s |" % (c.name, c.meaning) for c in FRONT + IDENTITY]
    lines += ["| label columns | extra keys given with the connections, after `collected_utc` |", ""]
    for r in RESULTS.values():
        origin = "Derived from %s" % ", ".join(r.sources) if r.sources else "Fact returned by the adapters"
        lines += ["## %s" % r.name, "", "%s %s. Grain: %s. Filters: %s." % (
            r.doc, origin, ", ".join(r.grain), ", ".join(r.filters)), "",
            "| Column | Kind | Meaning |", "|---|---|---|"]
        for column, (entity, parts, scope) in r.keys.items():
            lines.append("| `%s` | key | %s: %s, within its %s |" % (column, entity, " + ".join(parts), scope))
        for c in r.columns:
            unit = " (%s)" % c.unit if c.unit else ""
            computed = " Computed by dbmon." if c.computed else ""
            lines.append("| `%s` | %s%s | %s.%s |" % (c.name, c.kind, unit, c.meaning, computed))
        lines += ["| `details` | text | %s |" % DETAILS.meaning, ""]
    return "\n".join(lines)
