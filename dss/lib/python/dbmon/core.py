"""dbmon core: runs the adapters on Dataiku connections and consolidates their facts.

Per call, every connection is identified through its adapter; connections reaching
the same database are merged, preferring the primary. Then, one database at a time
and one query at a time, the facts are fetched, checked against the contract
(dbmon.model), the derived results are computed, and every row gets the identity
columns and the keys. A fact fetched for one result is reused by the others.
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

import dataiku
from dataiku import SQLExecutor2

from . import adapters, model, rollups

logger = logging.getLogger("dbmon")
LOG_COLUMNS = ["dss_connection", "engine", "server_uid", "db_uid", "unique_name", "database_name",
               "instance", "role", "result", "filters", "status", "rows", "seconds", "message"]


def uid(*parts):
    """Deterministic key: 32 hexadecimal characters of SHA-256 over the exact values."""
    text = json.dumps([str(p) for p in parts], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def collect(name, connections, schemas=None, table=None, tablespaces=None, with_log=False):
    """One result on every connection, as one DataFrame.

    name         a fact or a derived result of the contract (dbmon.model.RESULTS)
    connections  a Dataiku connection name; a list of names or of dicts
                 {"connection": name, "engine": optional, other keys become label
                 columns}; or a DataFrame with a "connection" column
    schemas      exact schema names to read; default: every non-system schema
    table        one table, by exact name; tablespaces: tablespace names

    A database that fails is skipped with a warning; the call raises only when no
    database returned anything. with_log=True also returns the run log.
    """
    filters = _filters([name], schemas=schemas, table=table, tablespaces=tablespaces)
    run = _Run(connections)
    frames, errors = [], 0
    for db in run.databases:
        for _, frame in run.outputs(db, [name], filters):
            if frame is None:
                errors += 1
            else:
                frames.append(frame)
    if not frames and (errors or not run.databases):
        raise RuntimeError("%s returned nothing: %s" % (name, run.problems()))
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return (result, run.log_frame()) if with_log else result


def write(outputs, connections, schemas=None, table=None, tablespaces=None,
          runlog=None, snapshot_dimension=None):
    """Recipe helper: several results, each into its own dataset.

    outputs             {result name: dataset name}, all declared as recipe outputs
    filters             each result uses the ones it accepts; a filter no result
                        accepts is an error
    runlog              optional output dataset for the run log
    snapshot_dimension  time dimension of the outputs if they are partitioned by day:
                        any partition other than today's is refused, because the
                        results describe the databases as they are now

    Databases are processed one after the other; their rows are appended to the
    outputs as soon as they are computed. The call raises (after writing the run
    log) only if an output got nothing from any database. Returns the run log.
    """
    if not isinstance(outputs, dict) or not outputs:
        raise TypeError("outputs must map result names to dataset names")
    filters = _filters(list(outputs), schemas=schemas, table=table, tablespaces=tablespaces)
    if snapshot_dimension:
        _check_snapshot_partition(snapshot_dimension)
    run = _Run(connections)
    writers, done, errors = {}, dict.fromkeys(outputs, 0), dict.fromkeys(outputs, 0)
    try:
        for db in run.databases:
            for name, frame in run.outputs(db, list(outputs), filters):
                if frame is None:
                    errors[name] += 1
                    continue
                if name not in writers:
                    dataset = dataiku.Dataset(outputs[name])
                    dataset.write_schema_from_dataframe(frame)
                    writers[name] = (dataset.get_writer(), list(frame.columns))
                writer, columns = writers[name]
                writer.write_dataframe(frame[columns])
                done[name] += 1
    finally:
        for writer, _ in writers.values():
            writer.close()
    log_df = run.log_frame()
    if runlog:
        dataiku.Dataset(runlog).write_with_schema(log_df)
    failed = [outputs[n] for n in outputs if not done[n] and (errors[n] or not run.databases)]
    if failed:
        raise RuntimeError("No database returned data for %s: %s" % (", ".join(failed), run.problems()))
    return log_df


class _Fatal(ValueError):
    """A contract or configuration error: stops the call instead of skipping a database."""


class _Run(object):
    """One call: its timestamp, labels, the databases it reaches, and its log."""

    def __init__(self, connections):
        self.collected_utc = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        targets = _targets(connections)
        self.labels = list(dict.fromkeys(k for t in targets for k in t["labels"]))
        self.log = []
        self.servers_done = {}          # (server_uid, result) -> connection that produced it
        self.databases = self._identify(targets)

    # --- identity -----------------------------------------------------------------------
    def _identify(self, targets):
        found = []
        for target in targets:
            try:
                adapter = adapters.for_connection(target["connection"], target["engine"])
                identity = adapter.identify(lambda sql: _executor(target).query_to_df(sql))
            except Exception as exc:
                self.error(target, "identity", {}, exc)
                continue
            db = dict(target, adapter=adapter, **identity)
            db["engine"] = adapter.engine
            db["server_uid"] = uid("server", adapter.engine, *identity["server_key"])
            db["db_uid"] = uid("database", adapter.engine, *identity["db_key"])
            found.append(db)
        chosen = {}
        for db in found:
            kept = chosen.get(db["db_uid"])
            if kept is None or (db["role"] == "PRIMARY" and kept["role"] != "PRIMARY"):
                chosen[db["db_uid"]] = db
        databases = []
        for db in found:
            kept = chosen[db["db_uid"]]
            if kept is db:
                databases.append(db)
            else:
                self.add_log(db, "identity", {}, "DUPLICATE_DB", message="same database as " + kept["connection"])
        return databases

    # --- results of one database -------------------------------------------------------
    def outputs(self, db, names, filters):
        """Yield (name, frame) for each result on one database; frame is None on failure."""
        cache = {}
        for name in names:
            started = time.time()
            adapter, result = db["adapter"], model.RESULTS[name]
            server_level = not result.sources and adapter.level(name) == "server"
            if server_level and (db["server_uid"], name) in self.servers_done:
                self.add_log(db, name, filters[name], "SAME_SERVER",
                             message="collected through " + self.servers_done[(db["server_uid"], name)])
                continue
            try:
                if result.sources:
                    facts = {src: self._fact(db, src, filters[name], cache) for src in result.sources}
                    frame = rollups.derive(name, {src: f for src, (f, _) in facts.items()})
                    status = "NO_MATCH" if any(s == "NO_MATCH" for _, s in facts.values()) else "OK"
                else:
                    frame, status = self._fact(db, name, filters[name], cache)
                frame = self._decorate(name, rollups.post(name, frame), db, server_level)
            except _Fatal:
                raise
            except Exception as exc:
                self.error(db, name, filters[name], exc)
                yield name, None
                continue
            if server_level:
                self.servers_done[(db["server_uid"], name)] = db["connection"]
            self.add_log(db, name, filters[name], status, rows=len(frame),
                         seconds=round(time.time() - started, 1))
            yield name, frame

    def _fact(self, db, fact, filters, cache):
        """A fact checked against the contract, fetched once per database and filters."""
        key = (fact, tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in filters.items())))
        if key not in cache:
            try:
                cache[key] = self._fetch(db, fact, filters)
            except _Fatal:
                raise
            except Exception as exc:
                cache[key] = exc
        if isinstance(cache[key], Exception):
            raise cache[key]
        return cache[key]

    def _fetch(self, db, fact, filters):
        adapter = db["adapter"]
        schemas, table = filters.get("schemas"), filters.get("table")
        status = "OK"
        if table and not (schemas is not None and len(schemas) == 1):
            schemas = self._schemas_holding(db, table, schemas)
            status = "NO_MATCH" if not schemas else status
        sql = adapter.render(fact, schemas=schemas, table=table, tablespaces=filters.get("tablespaces"))
        df = _executor(db).query_to_df(sql, pre_queries=adapter.session(fact), infer_from_schema=True)
        return _conform(fact, df, adapter.engine), status

    def _schemas_holding(self, db, table, schemas):
        lookups = db.setdefault("lookups", {})
        key = (table, tuple(schemas) if schemas is not None else None)
        if key not in lookups:
            try:
                found = _executor(db).query_to_df(db["adapter"].schemas_holding(table, schemas))
                lookups[key] = sorted(str(v) for v in found.iloc[:, 0])
            except Exception as exc:
                lookups[key] = exc
        if isinstance(lookups[key], Exception):
            raise lookups[key]
        return lookups[key]

    # --- identity columns, keys, details ---------------------------------------------------
    def _decorate(self, name, frame, db, server_level):
        result = model.RESULTS[name]
        extras = [c for c in frame.columns if c not in result.names]
        out = pd.DataFrame(index=frame.index)
        out["collected_utc"] = self.collected_utc
        for label in self.labels:
            out[label] = db["labels"].get(label)
        out["dss_connection"] = db["connection"]
        for column in model.IDENTITY:
            value = db.get(column.name)
            if server_level and column.name in ("db_uid", "database_name"):
                value = None
            out[column.name] = value
        bases = {"database": db["db_uid"], "server": db["server_uid"],
                 "storage": db["server_uid"] if db["adapter"].storage_level == "server" else db["db_uid"]}
        for column, (entity, parts, scope) in result.keys.items():
            rows = zip(*(frame[p] for p in parts)) if len(frame) else []
            out[column] = [None if any(_missing(v) for v in values) else uid(entity, bases[scope], *values)
                           for values in rows]
        for column in result.names:
            out[column] = frame[column].to_numpy()
        out[model.DETAILS.name] = _details(frame[extras]) if extras else None
        return out.reset_index(drop=True)

    # --- run log ----------------------------------------------------------------------------
    def add_log(self, db, result, filters, status, rows=None, seconds=None, message=None):
        entry = {"collected_utc": self.collected_utc}
        entry.update({label: db["labels"].get(label) for label in self.labels})
        entry.update({"dss_connection": db["connection"], "engine": db.get("engine"),
                      "server_uid": db.get("server_uid"), "db_uid": db.get("db_uid"),
                      "unique_name": db.get("unique_name"), "database_name": db.get("database_name"),
                      "instance": db.get("instance"), "role": db.get("role"), "result": result,
                      "filters": _describe(filters), "status": status, "rows": rows,
                      "seconds": seconds, "message": message})
        self.log.append(entry)

    def error(self, db, result, filters, exc):
        self.add_log(db, result, filters, "ERROR", message=_short(exc))
        logger.warning("dbmon %s failed on %s: %s", result, db["connection"], _short(exc))

    def log_frame(self):
        return pd.DataFrame(self.log, columns=["collected_utc"] + self.labels + LOG_COLUMNS)

    def problems(self):
        errors = ["%s (%s): %s" % (e["dss_connection"], e["result"], e["message"])
                  for e in self.log if e["status"] == "ERROR"]
        return "; ".join(errors) or "no database reached"


# --- helpers ---------------------------------------------------------------------------------
def _conform(fact, df, engine):
    """Check an adapter result against the contract and give its columns their kinds."""
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    result = model.RESULTS[fact]
    missing = [c for c in result.required if c not in df.columns]
    if missing:
        raise _Fatal("%s adapter: %s lacks contract column(s) %s" % (engine, fact, ", ".join(missing)))
    clash = [c for c in df.columns if c in model.reserved()]
    if clash:
        raise _Fatal("%s adapter: %s returns reserved column(s) %s" % (engine, fact, ", ".join(clash)))
    for column in result.columns:
        if column.computed or column.name not in df.columns:
            continue
        values = df[column.name]
        if column.kind in (model.NUMBER, model.INTEGER):
            df[column.name] = pd.to_numeric(values, errors="coerce")
        elif column.kind == model.DATE:
            df[column.name] = _dates(values)
        else:
            df[column.name] = [None if _missing(v) else str(v) for v in values]
    return df


_MIXED = {"format": "mixed"} if int(pd.__version__.split(".")[0]) >= 2 else {}


def _dates(values):
    """Datetimes, time-zone aware ones converted to UTC and made naive. pandas 2+ needs
    format="mixed", or values with and without fractional seconds turn into NaT."""
    parsed = pd.to_datetime(pd.Series(values), errors="coerce", utc=True, **_MIXED)
    return parsed.dt.tz_convert(None)


def _details(extras):
    """Engine columns of each row as a JSON object, None when all are empty."""
    out = []
    for record in extras.to_dict("records"):
        values = {k: _plain(v) for k, v in record.items() if not _missing(v)}
        out.append(json.dumps(values, sort_keys=True, ensure_ascii=False) if values else None)
    return out


def _plain(value):
    """A JSON-friendly scalar."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _filters(names, schemas=None, table=None, tablespaces=None):
    """Filters of each result; a filter that no result accepts is an error."""
    unknown = [n for n in names if n not in model.RESULTS]
    if unknown:
        raise ValueError("unknown result(s) %s, expected: %s" % (", ".join(unknown), ", ".join(model.RESULTS)))
    given = {}
    if schemas is not None:
        given["schemas"] = _names(schemas, "schemas")
    if tablespaces is not None:
        given["tablespaces"] = _names(tablespaces, "tablespaces")
    if table is not None:
        given["table"] = _names([table], "table")[0]
    per_result = {n: {k: v for k, v in given.items() if k in model.RESULTS[n].filters} for n in names}
    unused = set(given) - {k for f in per_result.values() for k in f}
    if unused:
        raise ValueError("filter(s) %s apply to none of: %s" % (", ".join(sorted(unused)), ", ".join(names)))
    return per_result


def _names(values, what):
    if isinstance(values, str):
        values = [values]
    names = []
    for v in values:
        if not isinstance(v, str) or not v.strip() or len(v) > 128 or "\x00" in v:
            raise ValueError("%s must be names of 1 to 128 characters, got %r" % (what, v))
        names.append(v)
    return list(dict.fromkeys(names))


def _targets(connections):
    """[{"connection", "engine", "labels"}] from the connections argument."""
    if isinstance(connections, pd.DataFrame):
        items = connections.to_dict("records")
    elif isinstance(connections, (str, dict)):
        items = [connections]
    else:
        items = list(connections or [])
    targets, reserved = [], model.reserved()
    for item in items:
        entry = {"connection": item} if isinstance(item, str) else dict(item)
        name = _text(entry.pop("connection", None))
        engine = _text(entry.pop("engine", None)) or None
        if not name:
            raise ValueError("every target needs a connection name, got %r" % (item,))
        labels = {str(k): (None if _missing(v) else v) for k, v in entry.items()}
        clash = sorted(set(labels) & (reserved | {"adapter"}))
        if clash:
            raise ValueError("label(s) %s are reserved column names" % ", ".join(clash))
        targets.append({"connection": name, "engine": engine, "labels": labels})
    if not targets:
        raise ValueError("no connection given")
    return targets


def _executor(target):
    return SQLExecutor2(connection=target["connection"])


def _check_snapshot_partition(dimension):
    """Refuse to write today's state into a past partition."""
    variables = getattr(dataiku, "dku_flow_variables", None) or {}
    target = variables.get("DKU_DST_" + dimension)
    if target is None:
        return
    today = {datetime.now().strftime("%Y-%m-%d"), datetime.now(timezone.utc).strftime("%Y-%m-%d")}
    if target not in today:
        raise RuntimeError("Refusing to build partition %s=%s: only today's partition can be "
                           "collected." % (dimension, target))


def _describe(filters):
    parts = ["%s=%s" % (k, ",".join(v) if isinstance(v, list) else v) for k, v in sorted(filters.items())]
    return " ".join(parts) or None


def _missing(value):
    """None, NaN, NaT or pd.NA."""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _text(value):
    return "" if _missing(value) else str(value).strip()


def _short(exc):
    return " ".join(str(exc).split())[:2000]
