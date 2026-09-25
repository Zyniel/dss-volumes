"""Optional use-case (UC) layer on top of dbmon. It reads contract columns only
(schema_name, tablespace_name, family), so it works for every engine.

A naming convention says how the schemas and tablespaces of a UC are named. The
default one: a UC owns the schemas <UC>_DSSWORK and <UC>_DSSOUT, each with a default
tablespace <schema>_DATA and an extra one <schema>_INDEX, in upper case.

    import dbmon
    from dbmon import uc

    schemas = uc.schemas("SALES")                              # one UC
    objs = uc.enrich(dbmon.objects(conns, schemas=schemas))    # uc, schema_layer, ts_role, placement
    tabs = uc.enrich(dbmon.tables(conns, schemas=schemas))
    tabs = tabs.merge(uc.table_placement(objs), on="table_uid", how="left")

Every UC: list the schemas once, keep those that read as a UC.
    s = uc.enrich(dbmon.schemas(conns))
    schemas = sorted(s.loc[s["uc"].notna(), "schema_name"].unique())

PostgreSQL schemas are usually lower case, and its tablespaces rarely follow a convention:
    pg = uc.Convention(case="lower", tablespaces=False)
    pg.schemas("sales")    # ['sales_dsswork', 'sales_dssout']
"""
import re

import numpy as np

DATA_FAMILIES = ("TABLE", "LOB", "LOBINDEX")


class Convention(object):
    """How the schemas and tablespaces of a use case are named.

    layers       schema suffixes, one schema per layer
    roles        tablespace suffixes after the schema name; the first one is the default
    case         "upper", "lower" or "keep": case of the names built by schemas() and
                 tablespaces(). Reading names (enrich) ignores case.
    tablespaces  False when tablespaces do not follow the convention: enrich() then
                 reads schemas only, without ts_* columns or placement
    """

    def __init__(self, layers=("DSSWORK", "DSSOUT"), roles=("DATA", "INDEX"), case="upper",
                 tablespaces=True):
        self.layers, self.roles, self.case, self.with_tablespaces = tuple(layers), tuple(roles), case, tablespaces
        alt = lambda values: "|".join(re.escape(v) for v in values)
        self.schema_rx = "^(.+)_(%s)$" % alt(self.layers)
        self.tablespace_rx = "^(.+)_(%s)_(%s)$" % (alt(self.layers), alt(self.roles))

    def schemas(self, uc):
        """Schemas of a UC."""
        return [self._cased("%s_%s" % (_name(uc), layer)) for layer in self.layers]

    def tablespaces(self, uc):
        """Tablespaces of a UC."""
        return [self._cased("%s_%s_%s" % (_name(uc), layer, role))
                for layer in self.layers for role in self.roles]

    def enrich(self, df):
        """A copy of a dbmon result with its UC reading.

        schema_name      -> uc, schema_layer
        tablespace_name  -> ts_uc, ts_layer, ts_role, ts_schema (schema it belongs to)
        objects (family) -> placement: OK, INDEX_IN_DATA_TS (index created without a
                            TABLESPACE clause), DATA_IN_INDEX_TS, FOREIGN_TS (tablespace of
                            another schema, or outside the convention)
        """
        out = df.copy()
        if "schema_name" in out.columns:
            parts = out["schema_name"].astype(str).str.extract(self.schema_rx, flags=re.IGNORECASE)
            out["uc"], out["schema_layer"] = parts[0], parts[1].str.upper()
        if self.with_tablespaces and "tablespace_name" in out.columns:
            parts = out["tablespace_name"].astype(str).str.extract(self.tablespace_rx, flags=re.IGNORECASE)
            out["ts_uc"], out["ts_layer"], out["ts_role"] = parts[0], parts[1].str.upper(), parts[2].str.upper()
            out["ts_schema"] = parts[0].str.cat(parts[1], sep="_")
            if {"family", "schema_name"} <= set(out.columns):
                same = _flag(out["ts_schema"].str.upper() == out["schema_name"].astype(str).str.upper())
                data = _flag(out["family"].isin(DATA_FAMILIES))
                default, extra = self.roles[0].upper(), self.roles[-1].upper()
                out["placement"] = np.select(
                    [~same,
                     data & _flag(out["ts_role"] == extra),
                     _flag(out["family"] == "INDEX") & _flag(out["ts_role"] == default)],
                    ["FOREIGN_TS", "DATA_IN_INDEX_TS", "INDEX_IN_DATA_TS"], default="OK")
        return out

    def table_placement(self, objects):
        """Placement issues of each table from the objects result: table_uid, placement_issues."""
        obj = objects if "placement" in objects.columns else self.enrich(objects)
        issues = obj[(obj["placement"] != "OK") & obj["table_uid"].notna()]
        return (issues.groupby("table_uid")["placement"]
                .agg(lambda s: ",".join(sorted(set(s))))
                .rename("placement_issues").reset_index())

    def _cased(self, text):
        return text.upper() if self.case == "upper" else text.lower() if self.case == "lower" else text


def _name(uc):
    text = str(uc).strip()
    if not re.match(r"^[A-Za-z][A-Za-z0-9_$#]*$", text):
        raise ValueError("uc must be an identifier, got %r" % (uc,))
    return text


def _flag(series):
    """Boolean numpy array, missing values read as False (pandas 1.x to 3.x)."""
    return series.fillna(False).astype(bool).to_numpy()


DEFAULT = Convention()
schemas = DEFAULT.schemas
tablespaces = DEFAULT.tablespaces
enrich = DEFAULT.enrich
table_placement = DEFAULT.table_placement
