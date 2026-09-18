#!/usr/bin/env python3
"""Which merging rule - and what each one costs.

Section 5 says "merge if the key sets overlap by at least a threshold" and leaves the
threshold, what it is measured against, and whether promoted values matter as open
questions. This script answers them by running each candidate over real lines and
counting: templates, nulls, type widening, order dependence, and the rows a search has to
scan once templates are bigger.

Merging is a trade, not a win. It cuts the template count, which is what makes the file
count of script 02 and 03 survivable; it pays for that in null cells and in rows scanned.
Both halves are here.

Every rule's line-to-template mapping is written to `lab/out/04/` so script 02 can write
the merged layouts and measure them as real Parquet files.
"""

from __future__ import annotations

import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, measure, merge, rows as rowlib
from lab.common.frontend import promoted_keys_rule, STRING

RULES = [
    ("NONE", {}),
    ("J70", {"threshold": 0.70}),
    ("J80", {"threshold": 0.80}),
    ("J90", {"threshold": 0.90}),
    ("J80_GROWN", {"threshold": 0.80, "match_grown": True}),
    ("J80_SPEC", {"threshold": 0.80, "respect_promoted": False}),
    ("PER_EVENT", {"per_event": True}),
]
CAPS = [100, 500, 2_000]
N_SEARCHES = 20
N_SHUFFLES = 5
SEED = 20240115

PROMOTION_CHOICES = [
    ("nothing", ()),
    ("event", ("event",)),
    ("event+service", ("event", "service")),
    ("binary", None),  # whatever src/flatten.rs compiles in
]


# --- searches -------------------------------------------------------------------


def build_searches(P: rowlib.Parsed) -> list:
    """20 searches of the form "one promoted value, plus one optional key is present".

    The key has to be optional - present on some templates of that promoted value and
    absent on others - or the search is answered by the trie walk alone and says nothing
    about what merging costs.
    """
    by_pv: dict = defaultdict(lambda: [Counter(), 0])
    for t in P.templates.values():
        slot = by_pv[t.promoted]
        slot[1] += 1
        for k in t.keys:
            slot[0][k] += 1

    pool = []
    for pv, (counts, n_templates) in by_pv.items():
        for key, seen in counts.items():
            if 0 < seen < n_templates:
                pool.append((pv, key, seen))
    pool.sort(key=lambda x: (str(x[0]), x[1]))
    rng = random.Random(SEED)
    # Spread across promoted values rather than letting the busiest one take all twenty.
    grouped: dict = defaultdict(list)
    for pv, key, seen in pool:
        grouped[pv].append((pv, key))
    out = []
    pvs = sorted(grouped, key=str)
    i = 0
    while len(out) < N_SEARCHES and pvs:
        pv = pvs[i % len(pvs)]
        if grouped[pv]:
            out.append(grouped[pv].pop(rng.randrange(len(grouped[pv]))))
        else:
            pvs = [p for p in pvs if grouped[p]]
            if not pvs:
                break
            continue
        i += 1
    return out


def search_scan(result: merge.Result, searches: list) -> tuple:
    """Rows a search must scan, and the rows that actually match.

    A leaf is scanned whole: if a group holds any row with the promoted value and its
    grown key set contains the key, every row in it is read. That is the price of a
    bigger template, and it is what "merging makes searches worse" means in rows.
    """
    scanned = 0
    for pv, key in searches:
        for g in result.groups.values():
            if pv in g.promoted_seen and key in g.grown_keys:
                scanned += g.n
    return scanned


def search_matching(P: rowlib.Parsed, searches: list) -> int:
    """Rows that genuinely satisfy each search - the floor no layout can go below."""
    total = 0
    for pv, key in searches:
        for t in P.templates.values():
            if t.promoted == pv and key in t.types:
                total += t.n
    return total


# --- order dependence ------------------------------------------------------------


def order_dependence(P: rowlib.Parsed, name: str, kw: dict) -> dict:
    """Five shuffles of the same lines. Run 1 is the reference.

    A row "moved" if its group in run k is not the group that holds the plurality of its
    run-1 groupmates. Comparing group ids directly would be meaningless - they are handed
    out in arrival order - so the partitions are matched before they are compared.
    """
    rng = random.Random(SEED)
    runs = []
    for i in range(N_SHUFFLES):
        order = list(range(P.n_rows))
        rng.shuffle(order)
        runs.append(merge.run(P, rule_name=name, order=order, **kw))

    counts = [len(r.groups) for r in runs]
    ref = runs[0]
    moved_shares = []
    for other in runs[1:]:
        partner: dict = {}
        tally: dict = defaultdict(Counter)
        for r in range(P.n_rows):
            tally[ref.assign[r]][other.assign[r]] += 1
        for g, c in tally.items():
            partner[g] = c.most_common(1)[0][0]
        moved = sum(
            1 for r in range(P.n_rows) if other.assign[r] != partner[ref.assign[r]]
        )
        moved_shares.append(100.0 * moved / P.n_rows)

    return {
        "template_count_min": min(counts),
        "template_count_max": max(counts),
        "template_count_spread": max(counts) - min(counts),
        "moved_share_pct_max": max(moved_shares) if moved_shares else 0.0,
        "moved_share_pct_mean": sum(moved_shares) / len(moved_shares)
        if moved_shares
        else 0.0,
    }


# --- main --------------------------------------------------------------------------


def main() -> int:
    args = measure.parse_args("04_merge_rules").parse_args()
    ds = datasets.ensure(args.dataset, args.size)
    res = measure.Results("04_merge_rules", ds.name)
    measure.print_machine()
    print(f"\ndataset {ds.name}: {ds.lines:,} lines")

    P = rowlib.load(ds)
    searches = build_searches(P)
    floor = search_matching(P, searches)
    outdir = measure.fresh_dir("04", ds.name)

    # -- the rules -------------------------------------------------------------
    results: dict = {}
    body = []
    for name, kw in RULES:
        r = merge.run(P, rule_name=name, **kw)
        results[name] = r
        st = r.stats(P.n_rows)
        scanned = search_scan(r, searches)
        od = order_dependence(P, name, kw)
        body.append(
            [
                name,
                st["templates"],
                st["rows_per_template_min"],
                st["rows_per_template_median"],
                st["rows_per_template_p90"],
                st["rows_per_template_max"],
                round(st["null_cell_share_pct"], 2),
                st["columns_widened_to_string"],
                od["template_count_spread"],
                round(od["moved_share_pct_max"], 2),
                scanned,
                round(scanned / floor, 2) if floor else 0.0,
            ]
        )
        for k, v in list(st.items()) + list(od.items()):
            res.add(name, k, round(v, 4) if isinstance(v, float) else v,
                    "percent" if "pct" in k or "share" in k else "count")
        res.add(name, "search_rows_scanned", scanned, "rows",
                f"{len(searches)} searches of 'one promoted value plus one optional key "
                f"is present'; {floor:,} rows actually match, so this is the scan cost")
        res.add(name, "search_scan_amplification", round(scanned / floor, 4) if floor else 0,
                "ratio", "rows scanned divided by rows that match")

        mapping = {P.lab_id[i]: r.assign[i] for i in range(P.n_rows)}
        (outdir / f"mapping_{name}.json").write_text(json.dumps(mapping))

    measure.table(
        body,
        headers=[
            "rule", "templates", "min", "median", "p90", "max", "null cell %",
            "cols->String", "count spread", "moved % max", "rows scanned", "x floor",
        ],
        title=f"{ds.name}: merge rules (order dependence over {N_SHUFFLES} shuffles)",
    )

    multi = sum(
        1
        for k, v in _types_per_key(P).items()
        if len(v) > 1
    )
    print(
        f"\n  `cols->String` is 0 for every rule because no key in `{ds.name}` ever "
        f"carries two types ({multi} such keys). The type join has nothing to widen "
        "here; that is a fact about the generator, not a verdict on merging. A real "
        "`N/A` in a numeric field is what would move this column, so it needs `real`."
    )
    res.add("all", "keys_with_more_than_one_type", multi, "count",
            "the type join can only widen a column where one key carries two types; at "
            "zero, every columns->String figure above is vacuous")

    # -- J80_SPEC's own question -----------------------------------------------
    spec = results["J80_SPEC"].stats(P.n_rows)
    print(
        f"\n  J80_SPEC (section 5.3 as written, ignoring promoted values) puts "
        f"{spec['rows_in_mixed_promoted_group']:,} rows "
        f"({100.0 * spec['rows_in_mixed_promoted_group'] / P.n_rows:.1f}%) into a "
        f"template whose promoted value differs from their own, across "
        f"{spec['mixed_promoted_groups']} such templates."
    )

    # -- promotion ---------------------------------------------------------------
    prows = []
    for label, keys in PROMOTION_CHOICES:
        rule = None if keys is None else promoted_keys_rule(keys)
        Pp = rowlib.load(ds, rule=rule) if rule else rowlib.load(ds)
        for rname, kw in (("NONE", {}), ("J80", {"threshold": 0.80})):
            r = merge.run(Pp, rule_name=rname, **kw)
            st = r.stats(Pp.n_rows)
            ev = _key_search(Pp, r, "event")
            sv = _key_search(Pp, r, "service")
            prows.append(
                [
                    label,
                    rname,
                    st["templates"],
                    st["rows_per_template_median"],
                    st["rows_per_template_max"],
                    ev,
                    sv,
                ]
            )
            res.add(f"promote={label}/{rname}", "templates", st["templates"], "count")
            res.add(f"promote={label}/{rname}", "rows_scanned_event_search", ev, "rows",
                    "one value of `event`; a promoted key is one trie edge, an "
                    "unpromoted one is a scan of every template holding the column")
            res.add(f"promote={label}/{rname}", "rows_scanned_service_search", sv, "rows")
    measure.table(
        prows,
        headers=[
            "promoted keys", "rule", "templates", "rows/tmpl median", "max",
            "rows scanned: event=X", "rows scanned: service=Y",
        ],
        title="what promoting a key buys - and what promoting the wrong one costs",
    )
    print(
        "  `binary` and `event` are the same rule: src/flatten.rs compiles in "
        "PROMOTED_KEYS = [\"event\"]. The two rows agreeing is a check, not a finding."
    )

    # -- template cap ---------------------------------------------------------------
    crows = []
    for cap in CAPS:
        r = merge.run(P, rule_name=f"J80_cap{cap}", threshold=0.80, cap=cap)
        left = next((g for g in r.groups.values() if g.is_leftover), None)
        if left is None:
            crows.append([cap, len(r.groups), 0, 0.0, 0, "-"])
            res.add(f"J80 cap={cap}", "leftover_rows", 0, "rows",
                    "the cap was never reached")
            continue
        cells = left.n * len(left.grown_keys)
        nulls = 100.0 * (cells - left.n_cells_present) / cells if cells else 0.0
        crows.append(
            [
                cap,
                len(r.groups),
                left.n,
                round(100.0 * left.n / P.n_rows, 2),
                len(left.grown_keys),
                round(nulls, 2),
            ]
        )
        for k, v in [
            ("leftover_rows", left.n),
            ("leftover_row_share_pct", round(100.0 * left.n / P.n_rows, 3)),
            ("leftover_columns", len(left.grown_keys)),
            ("leftover_null_cell_share_pct", round(nulls, 3)),
        ]:
            res.add(f"J80 cap={cap}", k, v, "percent" if "pct" in k else "count",
                    "lines that would have created a template past the cap go into one "
                    "shared leftover template, which is never itself a merge target")
        mapping = {P.lab_id[i]: r.assign[i] for i in range(P.n_rows)}
        (outdir / f"mapping_J80_cap{cap}.json").write_text(json.dumps(mapping))
    measure.table(
        crows,
        headers=[
            "cap", "templates", "leftover rows", "leftover % of rows",
            "leftover columns", "leftover null cell %",
        ],
        title="J80 with a hard cap on the template count",
    )

    # -- which two rules script 02 should write -------------------------------------
    none_amp = search_scan(results["NONE"], searches) / floor if floor else 1.0
    eligible = []
    for name, _kw in RULES:
        if name == "NONE":
            continue
        amp = search_scan(results[name], searches) / floor if floor else 0.0
        eligible.append((name, len(results[name].groups), amp))
    qualifying = [e for e in eligible if e[2] <= 2.0 * none_amp]
    qualifying.sort(key=lambda e: e[1])
    if len(qualifying) < 2:
        eligible.sort(key=lambda e: e[2])
        qualifying = (qualifying + [e for e in eligible if e not in qualifying])[:2]
    best = [e[0] for e in qualifying[:2]]
    (outdir / "best_rules.json").write_text(
        json.dumps(
            {
                "best_two": best,
                "criterion": "fewest templates among the rules whose search-scan "
                "amplification is at most twice NONE's; ties broken by amplification",
                "none_amplification": round(none_amp, 4),
                "candidates": [
                    {"rule": n, "templates": t, "amplification": round(a, 4)}
                    for n, t, a in eligible
                ],
            },
            indent=2,
        )
    )
    print(
        f"\n  best two for script 02 to write as Parquet: {', '.join(best)} "
        f"(criterion: fewest templates with scan amplification <= 2x NONE's "
        f"{none_amp:.2f}x). Mappings in {outdir}."
    )
    res.add("verdict", "best_two_rules", " ".join(best), "name",
            "written to lab/out/04/best_rules.json for script 02 to pick up")

    res.flush()
    return 0


def _types_per_key(P: rowlib.Parsed) -> dict:
    seen: dict = defaultdict(set)
    for t in P.templates.values():
        for k, v in t.types.items():
            seen[k].add(v)
    return seen


def _key_search(P: rowlib.Parsed, r: merge.Result, key: str) -> int:
    """Rows scanned to answer "this key equals one particular value".

    A promoted key is a trie edge: only the groups carrying that value are opened, and
    the value filter is already satisfied. An unpromoted one is a column predicate, so
    every group holding the column is scanned whole.
    """
    values = Counter()
    for t in P.templates.values():
        for pk, pv in t.promoted:
            if pk == key:
                values[pv] += t.n
    if values:
        target = values.most_common(1)[0][0]
        return sum(
            g.n
            for g in r.groups.values()
            if any((key, target) == pair for pvs in g.promoted_seen for pair in pvs)
        )
    return sum(g.n for g in r.groups.values() if key in g.grown_keys)


if __name__ == "__main__":
    sys.exit(main())
