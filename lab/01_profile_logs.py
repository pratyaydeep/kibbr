#!/usr/bin/env python3
"""What do the logs actually look like - templates, columns, skew and time.

Counts and shapes only. Everything here is derived from `common/frontend.py`, which
`00_frontend_agree.py` has certified against the binary, so a template id in this table
is the template id kibbr would assign.

Run over every dataset that exists and printed side by side, because the point of the
table is the comparison: if the generator's shape does not resemble `real`, every later
result is a statement about the generator.
"""

from __future__ import annotations

import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, measure
from lab.common.rows import time_source
from lab.common.frontend import DATETIME, fold_types, parse_line

# What `src/store/mod.rs` calls DEFAULT_TS_COLUMN. A template whose time column is named
# anything else has no min/max to skip on, so its rows land in `undated/` and a
# time-range search reads them all.
TS_COLUMN = "ts"


class Profile:
    """One dataset, walked once."""

    def __init__(self, name: str, note: str):
        self.name = name
        self.note = note
        self.rows = 0
        self.raw_bytes = 0
        self.template_rows: Counter = Counter()
        self.template_keys: dict = {}
        self.template_promoted: dict = {}
        self.template_bytes: Counter = Counter()
        # Per key name, rolled up across every template that holds it.
        self.col_types: dict = defaultdict(list)
        self.col_distinct: dict = defaultdict(set)
        self.col_present: Counter = Counter()
        self.col_json_null: Counter = Counter()
        self.col_bytes: Counter = Counter()
        self.col_templates: dict = defaultdict(set)
        # Days spanned, per Datetime column. Pooling them into one set would let
        # `order_id` - ten digits, not a timestamp, and rung 2 claims it anyway - spread
        # the dataset over 180 years and hide the real span of `ts`.
        self.day_sets: dict = defaultdict(set)
        # The binary's rule now: each template's time column, chosen on its first line
        # from `[schema] time_columns`, and the rows that have no Datetime there.
        self.template_time: dict = {}
        self.rows_on_arrival = 0

    def feed(self, raw: bytes) -> None:
        p = parse_line(raw)
        self.rows += 1
        self.raw_bytes += len(raw)
        tid = p.template_id
        self.template_rows[tid] += 1
        self.template_bytes[tid] += len(raw)
        if tid not in self.template_keys:
            self.template_keys[tid] = tuple(p.keys)
            self.template_promoted[tid] = tuple(
                sorted((k, p.values[k]) for k in p.promoted)
            )
            self.template_time[tid] = time_source(p)
        tc = self.template_time[tid]
        own = p.typed.get(tc) if tc is not None else None
        if own is None or own.ty != DATETIME:
            self.rows_on_arrival += 1

        for key in p.keys:
            raw_value = p.values[key]
            ty = p.column_type(key)
            self.col_types[key].append(ty)
            self.col_present[key] += 1
            self.col_templates[key].add(tid)
            self.col_bytes[key] += len(raw_value.encode())
            if len(self.col_distinct[key]) < 2_000_000:
                self.col_distinct[key].add(raw_value)
            if ty is None:
                self.col_json_null[key] += 1
            if ty == DATETIME and key in p.typed:
                t = p.typed[key].value
                if t is not None:
                    self.day_sets[key].add(t // 1_000_000_000 // 86400)

    # -- derived ---------------------------------------------------------------

    @property
    def templates(self) -> int:
        return len(self.template_rows)

    def rows_per_template(self) -> tuple:
        counts = sorted(self.template_rows.values())
        if not counts:
            return (0, 0, 0, 0)
        p90 = counts[min(len(counts) - 1, int(0.90 * len(counts)))]
        return (counts[0], int(statistics.median(counts)), p90, counts[-1])

    def top10_share(self) -> float:
        top = sum(c for _, c in self.template_rows.most_common(10))
        return 100.0 * top / self.rows if self.rows else 0.0

    def small_templates(self) -> int:
        return sum(1 for c in self.template_rows.values() if c < 100)

    def promoted_groups(self) -> Counter:
        """How many templates share each set of promoted values.

        This is the ceiling on merging: two templates with different promoted values
        cannot merge under any rule that keeps the fingerprint honest, so a group of one
        is a template that merging can never collapse.
        """
        groups: Counter = Counter()
        for tid in self.template_rows:
            groups[self.template_promoted[tid]] += 1
        return groups

    def column_table(self) -> list:
        out = []
        for key, types in self.col_types.items():
            folded = fold_types(types)
            present = self.col_present[key]
            distinct = len(self.col_distinct[key])
            out.append(
                {
                    "name": key,
                    "type": folded if folded is not None else "Null",
                    "distinct": distinct,
                    "distinct_over_rows": distinct / present if present else 0.0,
                    "avg_len": self.col_bytes[key] / present if present else 0.0,
                    "raw_bytes": self.col_bytes[key],
                    "present": present,
                    # What a WIDE layout pays: the column is absent on every row of every
                    # template that does not hold it.
                    "null_share_wide": 100.0 * (self.rows - present) / self.rows,
                    # An explicit JSON null among the rows that do hold the key.
                    "null_share_json": 100.0 * self.col_json_null[key] / present
                    if present
                    else 0.0,
                    "templates": len(self.col_templates[key]),
                }
            )
        out.sort(key=lambda c: -c["raw_bytes"])
        return out

    def widest_column(self) -> str:
        spans = {k: len(v) for k, v in self.day_sets.items()}
        return max(spans, key=lambda k: spans[k]) if spans else "-"

    def time_shape(self) -> dict:
        """Which templates a time-range search cannot skip.

        Three buckets: a template with a `ts` column typed Datetime can be skipped on its
        row-group min/max; one whose time column is named something else, or typed
        anything else, cannot; one with no Datetime column at all certainly cannot.
        """
        no_time = set()
        wrong_name = set()
        good = set()
        folded_per_template: dict = defaultdict(dict)
        # Fold each template's own column types, which is the type its leaf would carry.
        for tid, keys in self.template_keys.items():
            for key in keys:
                folded_per_template[tid][key] = None
        # Second pass is avoided by folding the global column type per key, then asking
        # per template - a key's type is folded over the whole dataset in kibbr too
        # only within one template, so this over-reports widening at worst. The datasets
        # here have no key that is Datetime in one template and String in another.
        global_types = {k: fold_types(v) for k, v in self.col_types.items()}
        for tid, keys in self.template_keys.items():
            dt_keys = [k for k in keys if global_types.get(k) == DATETIME]
            if not dt_keys:
                no_time.add(tid)
            elif TS_COLUMN in dt_keys:
                good.add(tid)
            else:
                wrong_name.add(tid)

        unskippable = no_time | wrong_name
        rows = sum(self.template_rows[t] for t in unskippable)
        rbytes = sum(self.template_bytes[t] for t in unskippable)
        spans = {k: len(v) for k, v in self.day_sets.items()}
        widest = max(spans, key=lambda k: spans[k]) if spans else "-"
        with_col = sum(1 for v in self.template_time.values() if v is not None)
        return {
            "days spanned by `ts`": spans.get(TS_COLUMN, 0),
            "days spanned by every Datetime column pooled": len(
                set().union(*self.day_sets.values()) if self.day_sets else set()
            ),
            # The label is fixed and the column is named under the table: a label naming
            # it differed per dataset as soon as one had another widest column, and the
            # side-by-side table is keyed on the label.
            "days spanned by the widest Datetime column": spans.get(widest, 0),
            "templates_with_ts": len(good),
            "templates_no_time_column": len(no_time),
            "templates_time_not_named_ts": len(wrong_name),
            "unskippable_templates": len(unskippable),
            "unskippable_rows": rows,
            "unskippable_row_share": 100.0 * rows / self.rows if self.rows else 0.0,
            "unskippable_bytes": rbytes,
            "unskippable_byte_share": 100.0 * rbytes / self.raw_bytes
            if self.raw_bytes
            else 0.0,
            # Under `[schema] time_columns`, which the binary uses now, the question is
            # not which templates a search cannot skip but which rows had to be stamped
            # with their arrival instead of their own time.
            "templates_with_time_column (list rule)": with_col,
            "templates_without (list rule)": len(self.template_time) - with_col,
            "rows_on_arrival_time (list rule)": self.rows_on_arrival,
            "rows_on_arrival_time_share (list rule)": 100.0 * self.rows_on_arrival / self.rows
            if self.rows
            else 0.0,
        }


def build(name: str, size: str) -> Profile:
    ds = datasets.ensure(name, size)
    prof = Profile(ds.name, ds.note)
    for _, raw in datasets.iter_payloads(ds.path):
        prof.feed(raw)
    return prof


def main() -> int:
    args = measure.parse_args("01_profile_logs").parse_args()
    res = measure.Results("01_profile_logs", args.dataset)
    measure.print_machine()

    names = datasets.available(args.size)
    profs = [build(n, args.size) for n in names]

    # -- templates, side by side ------------------------------------------------
    rows = []
    fields = [
        ("rows", lambda p: p.rows),
        ("raw bytes", lambda p: p.raw_bytes),
        ("templates", lambda p: p.templates),
        ("rows/template min", lambda p: p.rows_per_template()[0]),
        ("rows/template median", lambda p: p.rows_per_template()[1]),
        ("rows/template p90", lambda p: p.rows_per_template()[2]),
        ("rows/template max", lambda p: p.rows_per_template()[3]),
        ("top-10 share of rows %", lambda p: round(p.top10_share(), 2)),
        ("templates with <100 rows", lambda p: p.small_templates()),
        ("distinct promoted-value sets", lambda p: len(p.promoted_groups())),
        (
            "templates in the biggest promoted group",
            lambda p: max(p.promoted_groups().values()) if p.promoted_groups() else 0,
        ),
        (
            "promoted groups holding one template",
            lambda p: sum(1 for v in p.promoted_groups().values() if v == 1),
        ),
        ("distinct column names", lambda p: len(p.col_types)),
    ]
    for label, fn in fields:
        rows.append([label] + [fn(p) for p in profs])
        for p in profs:
            res.add(p.name, label, fn(p), "count", p.note, dataset=p.name)
    measure.table(rows, headers=["metric"] + names, title="shape, side by side")

    # -- time -------------------------------------------------------------------
    tshapes = {p.name: p.time_shape() for p in profs}
    keys = list(next(iter(tshapes.values())).keys())
    trows = [[k] + [round(tshapes[n][k], 2) for n in names] for k in keys]
    measure.table(
        trows,
        headers=["metric"] + names,
        title="time: what a range search cannot skip",
    )
    print("widest Datetime column: " + ", ".join(f"{p.name} `{p.widest_column()}`" for p in profs))
    for p in profs:
        for k, v in tshapes[p.name].items():
            res.add(
                p.name,
                k,
                round(v, 4),
                "percent" if "share" in k else "count",
                "a template whose time column is not named `ts` has no row-group "
                "min/max to skip on: its rows land in `undated/` and a time-range "
                "search reads every one",
                dataset=p.name,
            )

    # -- promoted value groups --------------------------------------------------
    for p in profs:
        groups = p.promoted_groups()
        body = []
        for pv, count in groups.most_common(10):
            label = ", ".join(f"{k}={v}" for k, v in pv) or "(nothing promoted)"
            share = 100.0 * sum(
                p.template_rows[t]
                for t in p.template_rows
                if p.template_promoted[t] == pv
            ) / p.rows
            body.append([label[:48], count, round(share, 2)])
        measure.table(
            body,
            headers=["promoted values", "templates sharing it", "% of rows"],
            title=f"{p.name}: promoted-value groups (top 10) - the ceiling on merging",
        )

    # -- columns ----------------------------------------------------------------
    for p in profs:
        cols = p.column_table()
        body = [
            [
                c["name"][:34],
                c["type"],
                c["templates"],
                c["distinct"],
                round(c["distinct_over_rows"], 4),
                round(c["avg_len"], 1),
                c["raw_bytes"],
                round(100.0 * c["raw_bytes"] / p.raw_bytes, 2),
                round(c["null_share_wide"], 2),
                round(c["null_share_json"], 2),
            ]
            for c in cols[:15]
        ]
        measure.table(
            body,
            headers=[
                "column",
                "type",
                "tmpl",
                "distinct",
                "distinct/rows",
                "avg bytes",
                "raw bytes",
                "% of raw",
                "% null (wide)",
                "% null (json)",
            ],
            title=f"{p.name}: 15 columns holding the most raw bytes",
        )
        for c in cols:
            res.add(
                f"column:{c['name']}",
                "raw_bytes",
                c["raw_bytes"],
                "bytes",
                f"type={c['type']} distinct={c['distinct']} "
                f"distinct/rows={c['distinct_over_rows']:.4f} "
                f"avg_len={c['avg_len']:.1f} null_wide={c['null_share_wide']:.2f}% "
                f"null_json={c['null_share_json']:.2f}% templates={c['templates']}",
                dataset=p.name,
            )

    if "real" not in names:
        print(
            "\nNo `real` dataset: LAB_REAL_LOGS is unset, so the generator's shape is "
            "unchecked against real logs. Every later result is a statement about the "
            "generator until it is."
        )

    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
