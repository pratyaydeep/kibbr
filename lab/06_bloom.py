#!/usr/bin/env python3
"""Is a bloom filter per row group worth its bytes?

The columns that matter are the id-shaped ones - distinct values over rows above 0.5 -
because those are what a search names and what min/max statistics are useless for. A
random UUID column has a min and a max that span nearly the whole space, so every row
group survives the statistics filter and gets read. That is the gap a bloom filter is
supposed to close, and this script measures both sides of it: bytes added, and row groups
skipped.

Also over words, for text columns whose values are long enough that a substring search is
a plausible query: one filter per row group over the lower-cased words, 10 bits per
distinct word.
"""

from __future__ import annotations

import csv
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import bloom, datasets, measure, rows as rowlib
from lab.common.frontend import STRING, UUID

CHUNK = 8_192
BITS = [8, 10, 16]
WORD_BITS = 10
N_PROBES = 1_000
N_WORD_PROBES = 200
ID_THRESHOLD = 0.5  # distinct / rows above this is an id, not a dimension
LONG_TEXT_BYTES = 20
SEED = 20240115


def best_total_from_05(dataset: str):
    """Script 05's BEST_PER_CHUNK, so the filter's bytes can be quoted as a share of it."""
    if not measure.RESULTS_CSV.exists():
        return None
    best = None
    with measure.RESULTS_CSV.open() as fh:
        for r in csv.DictReader(fh):
            if (
                r["script"] == "05_encodings"
                and r["dataset"] == dataset
                and r["variant"] == "BEST_PER_CHUNK arrival"
                and r["metric"] == "bytes"
            ):
                if best is None or r["when"] > best[0]:
                    best = (r["when"], int(float(r["value"])))
    return best[1] if best else None


def row_groups(P: rowlib.Parsed):
    """(template, row indices) per row group - chunks of 8,192 rows, as in script 05."""
    for t in P.templates.values():
        ordered = sorted(t.rows)
        for s in range(0, len(ordered), CHUNK):
            yield t, ordered[s : s + CHUNK]


def id_columns(P: rowlib.Parsed) -> list:
    """Columns whose distinct/rows is above the threshold, biggest first."""
    distinct: dict = defaultdict(set)
    present: Counter = Counter()
    nbytes: Counter = Counter()
    types: dict = {}
    for t in P.templates.values():
        for i, key in enumerate(t.keys):
            types[key] = t.types[key]
            for r in t.rows:
                v = P.vals[r][i]
                if v is None:
                    continue
                present[key] += 1
                s = str(P.raws[r][i])
                nbytes[key] += len(s.encode())
                if len(distinct[key]) < 1_000_000:
                    distinct[key].add(s)
    out = []
    for key, n in present.items():
        ratio = len(distinct[key]) / n if n else 0.0
        if ratio > ID_THRESHOLD:
            out.append((key, types[key], ratio, n, nbytes[key], distinct[key]))
    out.sort(key=lambda c: -c[4])
    return out


def long_text_columns(P: rowlib.Parsed) -> list:
    present: Counter = Counter()
    nbytes: Counter = Counter()
    for t in P.templates.values():
        for i, key in enumerate(t.keys):
            if t.types[key] != STRING:
                continue
            for r in t.rows:
                if P.vals[r][i] is None:
                    continue
                present[key] += 1
                nbytes[key] += len(str(P.raws[r][i]).encode())
    out = [
        (k, nbytes[k] / present[k], nbytes[k])
        for k in present
        if present[k] and nbytes[k] / present[k] > LONG_TEXT_BYTES
    ]
    out.sort(key=lambda c: -c[2])
    return out


def measure_column(P: rowlib.Parsed, key: str, present_values: set, bits: int, rng):
    """Build one filter per row group holding this column, then probe it.

    Reported against the same denominator for both filters: the row groups that hold the
    column at all. A row group that does not have the column is not a candidate and
    skipping it is free.
    """
    groups = []
    for t, idxs in row_groups(P):
        if key not in t.types:
            continue
        i = t.keys.index(key)
        vals = [str(P.raws[r][i]) for r in idxs if P.vals[r][i] is not None]
        if not vals:
            continue
        groups.append((bloom.Filter(vals, bits), min(vals), max(vals), set(vals)))
    if not groups:
        return None

    pool = sorted(present_values)
    hits = rng.sample(pool, min(N_PROBES, len(pool)))
    # Misses are held inside the column's own global [min, max]. A synthetic id that
    # falls outside that range is one min/max statistics rule out for free, which would
    # flatter the statistics and answer a question nobody asks: a real query names an id
    # that exists somewhere in the system and asks which row group has it.
    glo, ghi = pool[0], pool[-1]
    seen = set(pool)
    misses = []
    rejected = 0
    attempts = 0
    while len(misses) < N_PROBES and attempts < N_PROBES * 200:
        attempts += 1
        cand = _synthetic(hits[attempts % len(hits)], rng)
        if cand in seen:
            continue
        if not (glo <= cand <= ghi):
            rejected += 1
            continue
        misses.append(cand)
    if not misses:
        return None

    out = {
        "row_groups": len(groups),
        "filter_bytes": sum(g[0].nbytes for g in groups),
        "k": groups[0][0].k,
        "misses_generated": len(misses),
        "misses_rejected_out_of_range": rejected,
    }
    for label, probes in (("present", hits), ("absent", misses)):
        h1, h2 = bloom.hashes(probes)
        bloom_survive = np.zeros(len(probes), dtype=np.int64)
        stat_survive = np.zeros(len(probes), dtype=np.int64)
        truly_in = np.zeros(len(probes), dtype=np.int64)
        lo_hi = [(g[1], g[2]) for g in groups]
        for gi, (filt, lo, hi, exact) in enumerate(groups):
            bloom_survive += filt.probe_many(h1, h2)
            for pi, p in enumerate(probes):
                if lo <= p <= hi:
                    stat_survive[pi] += 1
                if p in exact:
                    truly_in[pi] += 1
        n = len(groups)
        out[f"{label}_bloom_skipped_pct"] = 100.0 * float(
            np.mean((n - bloom_survive) / n)
        )
        out[f"{label}_minmax_skipped_pct"] = 100.0 * float(
            np.mean((n - stat_survive) / n)
        )
        out[f"{label}_ideal_skipped_pct"] = 100.0 * float(np.mean((n - truly_in) / n))
    return out


def _synthetic(like: str, rng) -> str:
    """A value of the same shape that is not in the column.

    The first two characters are kept, which is what usually lands the result inside the
    column's global range; the caller rejects the ones that still fall outside.
    """
    hexd = "0123456789abcdef"
    out = list(like[:2])
    for ch in like[2:]:
        if ch in "-.:@":
            out.append(ch)
        elif ch.isdigit():
            out.append(rng.choice("0123456789"))
        elif ch in hexd or ch.lower() in hexd:
            out.append(rng.choice(hexd))
        elif ch.isalpha():
            out.append(rng.choice("abcdefghijklmnopqrstuvwxyz"))
        else:
            out.append(ch)
    return "".join(out)


def main() -> int:
    args = measure.parse_args("06_bloom").parse_args()
    ds = datasets.ensure(args.dataset, args.size)
    res = measure.Results("06_bloom", ds.name)
    measure.print_machine()
    print(f"\ndataset {ds.name}: {ds.lines:,} lines")

    P = rowlib.load(ds)
    rng = random.Random(SEED)
    best05 = best_total_from_05(ds.name)
    if best05 is None:
        print("  script 05 has not run for this dataset: filter bytes are given "
              "absolutely, with no share of the best total")

    ids = id_columns(P)
    print(f"  id-shaped columns (distinct/rows > {ID_THRESHOLD}): {len(ids)}")

    body = []
    for key, ty, ratio, n, nbytes, values in ids[:6]:
        for bits in BITS:
            m = measure_column(P, key, values, bits, random.Random(SEED))
            if m is None:
                print(
                    f"  {key} @ {bits} bits: skipped - no in-range miss could be "
                    "generated for this column's value shape, so a miss rate measured "
                    "on it would be measuring the generator instead of the filter"
                )
                continue
            share = 100.0 * m["filter_bytes"] / best05 if best05 else None
            body.append(
                [
                    key[:26],
                    ty,
                    round(ratio, 3),
                    bits,
                    m["k"],
                    m["row_groups"],
                    m["filter_bytes"],
                    round(share, 2) if share is not None else "-",
                    round(m["present_bloom_skipped_pct"], 2),
                    round(m["present_minmax_skipped_pct"], 2),
                    round(m["present_ideal_skipped_pct"], 2),
                    round(m["absent_bloom_skipped_pct"], 2),
                    round(m["absent_minmax_skipped_pct"], 2),
                ]
            )
            for k2, v in m.items():
                res.add(f"{key}@{bits}bits", k2, round(v, 4) if isinstance(v, float) else v,
                        "percent" if "pct" in k2 else ("bytes" if "bytes" in k2 else "count"),
                        f"distinct/rows {ratio:.3f}; {N_PROBES} ids that exist and "
                        f"{N_PROBES} that do not; the denominator is the row groups "
                        "holding the column")
            if share is not None:
                res.add(f"{key}@{bits}bits", "share_of_best_total_pct", round(share, 4),
                        "percent", f"against script 05's BEST_PER_CHUNK ({best05:,} bytes)")
    measure.table(
        body,
        headers=[
            "id column", "type", "dist/rows", "bits", "k", "row groups", "filter bytes",
            "% of best", "hit: bloom skip%", "hit: min/max%", "hit: ideal%",
            "miss: bloom skip%", "miss: min/max%",
        ],
        title="bloom filter per row group on id columns",
    )

    # -- words --------------------------------------------------------------------
    texts = long_text_columns(P)
    print(f"\n  text columns averaging over {LONG_TEXT_BYTES} bytes: {len(texts)}")
    wbody = []
    for key, avg, total in texts[:4]:
        groups = []
        df: Counter = Counter()
        for t, idxs in row_groups(P):
            if key not in t.types:
                continue
            i = t.keys.index(key)
            words = set()
            for r in idxs:
                if P.vals[r][i] is None:
                    continue
                words.update(str(P.raws[r][i]).lower().split())
            if not words:
                continue
            groups.append((bloom.Filter(sorted(words), WORD_BITS), words))
            for w in words:
                df[w] += 1
        if not groups:
            continue
        ranked = [w for w, _ in df.most_common()]
        # With fewer distinct words than probes, "common" and "rare" are the same set
        # and the two columns must not be read as a comparison.
        half = min(N_WORD_PROBES, max(1, len(ranked) // 2))
        narrow = len(ranked) < 2 * N_WORD_PROBES
        common = ranked[:half]
        rare = ranked[-half:]
        never = [f"zzq{i}xnotaword" for i in range(N_WORD_PROBES)]
        fbytes = sum(g[0].nbytes for g in groups)
        row = [key[:26], round(avg, 1), len(groups),
               f"{len(df)}{' (narrow)' if narrow else ''}", fbytes,
               round(100.0 * fbytes / best05, 3) if best05 else "-"]
        for label, probes in (("common", common), ("rare", rare), ("never", never)):
            if not probes:
                row.append("-")
                continue
            h1, h2 = bloom.hashes(probes)
            survive = np.zeros(len(probes), dtype=np.int64)
            for filt, _words in groups:
                survive += filt.probe_many(h1, h2)
            pct = 100.0 * float(np.mean((len(groups) - survive) / len(groups)))
            row.append(round(pct, 2))
            res.add(f"words:{key}", f"{label}_skipped_pct", round(pct, 4), "percent",
                    f"{WORD_BITS} bits per distinct word, one filter per row group; "
                    f"{len(probes)} probe words"
                    + ("; this column has fewer distinct words than the probe set, so "
                       "`common` and `rare` are drawn from the same list and the two "
                       "figures are not a comparison" if narrow else ""))
        wbody.append(row)
        res.add(f"words:{key}", "filter_bytes", fbytes, "bytes",
                f"{len(groups)} row groups, {len(df):,} distinct words")
    measure.table(
        wbody,
        headers=[
            "text column", "avg bytes", "row groups", "distinct words", "filter bytes",
            "% of best", "common skip%", "rare skip%", "never skip%",
        ],
        title=f"word bloom filters, {WORD_BITS} bits per distinct word",
    )
    if any("narrow" in str(r[3]) for r in wbody):
        print(
            "  (narrow) marks a column with fewer distinct words than the 200-word probe "
            "set: its `common` and `rare` probes are the same words, so those two columns "
            "agree by construction. The generator draws labels from a fixed list of eight, "
            "so there is no long tail to find here - a real message field is what would "
            "separate the two."
        )

    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
