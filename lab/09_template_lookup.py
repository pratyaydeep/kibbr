#!/usr/bin/env python3
"""Searches on keys that are NOT promoted. SHAPE ONLY.

A promoted key is a trie edge: the walk of section 4.2 goes straight to the leaves that
have it. Every other key is a column, and finding which templates even hold it is a
separate question - one the trie does not answer. There are two ways to answer it, and
the only thing that matters is how each grows:

  scan     ask every template whether its key set contains the key.  O(templates)
  index    keep a map from key to the templates holding it.          O(1) plus the answer

At 1,000 templates the difference is not worth a data structure. The question is what
happens at 100,000, which is where `uneven` already points (4,931 templates from 200,000
lines, and script 01 says 4,913 of them hold fewer than 100 rows).

Absolute times are deliberately not the finding - this is Python, and the constant factor
is meaningless. What is reported is the growth against the 1,000-template case, plus the
comparison counts, which are a property of the algorithm and not of the language.
"""

from __future__ import annotations

import random
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import measure

SIZES = [1_000, 10_000, 100_000]
VOCAB = 200  # distinct key names to draw from
KEYS_PER_TEMPLATE = (5, 20)
N_QUERIES = 200
RUNS = 3
SEED = 20240115
SHAPE = "shape only - growth against the 1,000-template case, not an absolute time"


def synthesise(n: int, rng: random.Random) -> list:
    """`n` templates over a shared vocabulary, Zipf-weighted.

    Zipf rather than uniform because that is what real key names do: `level` and `ts` are
    on nearly every template and `shard.leader` is on a handful. A uniform draw would give
    every key the same posting-list length and make the index look better than it is.
    """
    names = [f"k{i:03d}" for i in range(VOCAB)]
    weights = [1.0 / (i + 1) for i in range(VOCAB)]
    out = []
    for _ in range(n):
        size = rng.randint(*KEYS_PER_TEMPLATE)
        keys = set()
        while len(keys) < size:
            keys.add(rng.choices(names, weights=weights, k=1)[0])
        out.append(frozenset(keys))
    return out


def build_index(templates: list) -> dict:
    idx: dict = defaultdict(list)
    for i, keys in enumerate(templates):
        for k in keys:
            idx[k].append(i)
    return idx


def main() -> int:
    measure.parse_args("09_template_lookup").parse_args()
    res = measure.Results("09_template_lookup", "synthetic")
    measure.print_machine()
    print(f"\nSHAPE ONLY. {N_QUERIES} queries, median of {RUNS} runs.")

    rng = random.Random(SEED)
    names = [f"k{i:03d}" for i in range(VOCAB)]
    queries = [rng.choice(names) for _ in range(N_QUERIES)]

    body = []
    base_scan = base_index = None
    for n in SIZES:
        templates = synthesise(n, random.Random(SEED + n))

        def scan() -> float:
            t0 = time.perf_counter()
            for q in queries:
                [i for i, keys in enumerate(templates) if q in keys]
            return time.perf_counter() - t0

        t_scan = statistics.median(scan() for _ in range(RUNS))

        idx = build_index(templates)

        def lookup() -> float:
            t0 = time.perf_counter()
            for q in queries:
                idx.get(q, ())
            return time.perf_counter() - t0

        t_index = statistics.median(lookup() for _ in range(RUNS))

        t_build = statistics.median(
            _time(lambda: build_index(templates)) for _ in range(RUNS)
        )
        entries = sum(len(v) for v in idx.values())
        comparisons = N_QUERIES * n
        answers = sum(len(idx.get(q, ())) for q in queries)

        if base_scan is None:
            base_scan, base_index = t_scan, t_index
        body.append(
            [
                n,
                comparisons,
                round(t_scan / base_scan, 2),
                round(t_index / base_index, 2),
                round(t_scan / t_index, 1),
                entries,
                answers,
                round(t_build * 1000, 1),
            ]
        )
        for metric, value, unit in [
            ("scan_comparisons", comparisons, "count"),
            ("scan_growth_vs_1000", round(t_scan / base_scan, 4), "ratio"),
            ("index_growth_vs_1000", round(t_index / base_index, 4), "ratio"),
            ("scan_over_index", round(t_scan / t_index, 3), "ratio"),
            ("index_entries", entries, "count"),
            ("rows_of_answer", answers, "count"),
            ("index_build_ms", round(t_build * 1000, 3), "ms"),
        ]:
            res.add(f"{n} templates", metric, value, unit, SHAPE)

    measure.table(
        body,
        headers=[
            "templates", "scan comparisons", "scan growth", "index growth",
            "scan / index", "index entries", "answer size", "index build ms",
        ],
        title="finding which templates hold an unpromoted key",
    )
    print(
        "\n  The scan column grows with the template count because that is what it is:\n"
        "  one comparison per template per query. The index column is flat - a dict\n"
        "  lookup does not care how many templates there are - and its cost moves to\n"
        "  build time and to memory, both of which grow with the total key entries, not\n"
        "  with the query rate."
    )
    last = body[-1]
    linear = last[0] / SIZES[0]
    if last[2] > 1.3 * linear:
        print(
            f"\n  The scan is worse than linear: {SIZES[-1] // SIZES[0]}x the templates "
            f"costs {last[2]:.0f}x the time, not {linear:.0f}x. The template list stops "
            "fitting in cache, so each comparison starts paying for a memory fetch as "
            "well. Whatever the constant factor is in Rust, that bend is the shape, and "
            "it bends the wrong way."
        )
        res.add("verdict", "scan_superlinearity", round(last[2] / linear, 3), "ratio",
                f"scan growth {last[2]:.1f}x against the linear {linear:.0f}x expected "
                f"from {SIZES[0]:,} to {SIZES[-1]:,} templates")
    res.flush()
    return 0


def _time(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


if __name__ == "__main__":
    sys.exit(main())
