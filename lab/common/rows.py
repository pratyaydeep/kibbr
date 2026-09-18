"""One dataset, parsed once, in the shape the storage scripts need.

Every script from 02 on wants the same thing: rows grouped by template, each template
carrying its own column list and the folded type of every column - which is exactly what
a TrieDB leaf holds. Parsing is the slow part in Python, so it happens here once and the
result is handed round.

The column type is folded *per template*, not globally, because that is where TrieDB
folds it: `LeafMeta.columns` is per leaf. A key that is Datetime under one template and
String under another is two columns with two types, and flattening that distinction here
would quietly answer a different question than the one being asked.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from .frontend import (
    BOOL,
    DATETIME,
    FLOAT64,
    INT64,
    IP,
    STRING,
    UUID,
    DEFAULT_RULE,
    parse_line,
)

# `src/store/mod.rs`: the column a day partition is cut on. A row whose `ts` is not a
# readable Datetime goes to `undated/`, and this mirrors that rule exactly.
TS_COLUMN = "ts"
UNDATED = "undated"


@dataclass
class Template:
    tid: int
    keys: list = field(default_factory=list)  # sorted; promoted keys included
    types: dict = field(default_factory=dict)  # key -> folded ColumnType or None
    promoted: tuple = ()
    fingerprint: str = ""
    rows: list = field(default_factory=list)  # RowRef indices into Dataset.rows

    @property
    def n(self) -> int:
        return len(self.rows)


@dataclass
class Parsed:
    """The whole dataset: a row list, and the templates indexing into it."""

    name: str
    note: str
    raw_bytes: int = 0
    # Parallel arrays, one entry per row.
    tid: list = field(default_factory=list)
    day: list = field(default_factory=list)  # "YYYY-MM-DD" or "undated"
    ts_ns: list = field(default_factory=list)  # None where there is no usable `ts`
    raws: list = field(default_factory=list)  # tuple, in the template's key order
    vals: list = field(default_factory=list)  # tuple, this row's own typed values
    lab_id: list = field(default_factory=list)
    templates: dict = field(default_factory=dict)

    @property
    def n_rows(self) -> int:
        return len(self.tid)

    def column_values(self, tmpl: Template, key: str, rows: list):
        """This template's column, cast to the column's folded type.

        Where the row's own type differs from the folded one, the raw text is what the
        column keeps: a column that widened to String holds the text that arrived, not a
        rendering of the value the ladder happened to build for that row.
        """
        i = tmpl.keys.index(key)
        ty = tmpl.types[key]
        out = []
        for r in rows:
            raw = self.raws[r][i]
            val = self.vals[r][i]
            # `vals` is None exactly where the ladder was never run - the key was absent,
            # or its JSON value was null. Both are `ColValue::Null` in a column, and
            # falling back to `raws` here would store the literal text "null" instead.
            if val is None:
                out.append(None)
            elif ty == STRING or ty in (UUID, IP):
                out.append(raw)
            elif ty == FLOAT64:
                out.append(None if val is None else float(val))
            elif ty is None:
                out.append(None)
            else:
                out.append(val)
        return out


def _day_of(ns) -> str:
    if ns is None:
        return UNDATED
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


def load(ds, rule=DEFAULT_RULE, limit: int | None = None) -> Parsed:
    """Parse a dataset into templates. One pass to fold types, one to lay out the rows."""
    from . import datasets

    out = Parsed(ds.name, ds.note)
    # Pass 1: key sets and the type join, per template.
    type_acc: dict = defaultdict(lambda: defaultdict(lambda: None))
    seen: dict = {}
    parsed_cache = []
    for n, (lab_id, raw) in enumerate(datasets.iter_payloads(ds.path)):
        if limit is not None and n >= limit:
            break
        p = parse_line(raw, rule)
        out.raw_bytes += len(raw)
        tid = p.template_id
        if tid not in seen:
            seen[tid] = Template(
                tid=tid,
                keys=list(p.keys),
                promoted=tuple(sorted((k, p.values[k]) for k in p.promoted)),
                fingerprint=p.fingerprint,
            )
        acc = type_acc[tid]
        for key in p.keys:
            acc[key] = _join(acc[key], p.column_type(key))
        parsed_cache.append((lab_id, p))

    for tid, tmpl in seen.items():
        tmpl.types = {k: type_acc[tid][k] for k in tmpl.keys}
    out.templates = seen

    # Pass 2: lay the rows out in each template's key order.
    for lab_id, p in parsed_cache:
        tmpl = seen[p.template_id]
        raws = tuple(p.values.get(k) for k in tmpl.keys)
        vals = tuple(
            p.values[k] if k in p.promoted else (p.typed[k].value if k in p.typed else None)
            for k in tmpl.keys
        )
        ts_ns = None
        if tmpl.types.get(TS_COLUMN) == DATETIME:
            ts_ns = vals[tmpl.keys.index(TS_COLUMN)]
        idx = len(out.tid)
        out.tid.append(p.template_id)
        out.ts_ns.append(ts_ns)
        out.day.append(_day_of(ts_ns))
        out.raws.append(raws)
        out.vals.append(vals)
        out.lab_id.append(lab_id)
        tmpl.rows.append(idx)
    return out


def _join(a, b):
    from .frontend import join_types

    return join_types(a, b)


# --- arrow ---------------------------------------------------------------------

_ARROW = {
    STRING: "string",
    UUID: "string",  # section 6.2 stores both as Str; script 05 tests 16 raw bytes
    IP: "string",
    INT64: "int64",
    FLOAT64: "double",
    BOOL: "bool",
    DATETIME: "timestamp",
    None: "string",  # a column that has only ever held nulls has no ColumnType
}


def arrow_type(ty):
    import pyarrow as pa

    kind = _ARROW[ty]
    if kind == "timestamp":
        return pa.timestamp("ns", tz="UTC")
    return {
        "string": pa.string(),
        "int64": pa.int64(),
        "double": pa.float64(),
        "bool": pa.bool_(),
    }[kind]


def arrow_array(values, ty):
    import pyarrow as pa

    at = arrow_type(ty)
    if ty == DATETIME:
        # Built as int64 then cast: rung 2 saturates at i64::MAX for a thirteen-digit
        # value near the top of the range, and pyarrow will not take that through its
        # datetime path.
        return pa.array(values, type=pa.int64()).cast(at, safe=False)
    return pa.array(values, type=at)
