"""Adapter registry: one adapter per database engine.

The adapter of a connection is, in order: the "engine" given with the connection,
the type of the Dataiku connection, or found by probing the database.
"""
import dataiku
from dataiku import SQLExecutor2

from .oracle import Oracle
from .postgres import PostgreSQL

ADAPTERS = {adapter.engine: adapter for adapter in (Oracle(), PostgreSQL())}
_ALIASES = {"postgres": "postgresql", "pg": "postgresql", "ora": "oracle"}
_BY_DSS_TYPE = {t: adapter for adapter in ADAPTERS.values() for t in adapter.dss_types}
# one query that only succeeds on each engine
_PROBES = [("postgresql", "SELECT current_setting('server_version_num') AS v"),
           ("oracle", "SELECT banner AS v FROM v$version WHERE ROWNUM = 1")]


def get(engine):
    """Adapter of an engine name (oracle, postgresql)."""
    name = _ALIASES.get(str(engine).strip().lower(), str(engine).strip().lower())
    if name not in ADAPTERS:
        raise ValueError("no adapter for engine %r, available: %s" % (engine, ", ".join(sorted(ADAPTERS))))
    return ADAPTERS[name]


def for_connection(connection, engine=None):
    """Adapter to use for a Dataiku connection."""
    if engine:
        return get(engine)
    dss_type = _dss_type(connection)
    if dss_type in _BY_DSS_TYPE:
        return _BY_DSS_TYPE[dss_type]
    errors = []
    for name, sql in _PROBES:
        try:
            SQLExecutor2(connection=connection).query_to_df(sql)
            return ADAPTERS[name]
        except Exception as exc:
            errors.append("%s probe: %s" % (name, " ".join(str(exc).split())[:300]))
    raise ValueError("cannot tell the engine of connection %s (Dataiku type %s), pass "
                     "{'connection': ..., 'engine': ...}. %s" % (connection, dss_type, " | ".join(errors)))


def _dss_type(connection):
    try:
        return dataiku.api_client().get_connection(connection).get_info().get_type()
    except Exception:
        return None
