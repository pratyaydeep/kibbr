#!/usr/bin/env python3
"""What each encoding saves, per column and per column type.

Chunks of 8,192 rows, taken per template - which is the unit the segment format calls a
row group. Every encoder produces real bytes; nothing here is a size model.

The only speed figure is zstd's, because zstd is a C library doing the work. It is run
three times at each level and the median is reported. No other number in this script is
a rate, and none of them is a TrieDB throughput.

Two orders are measured throughout: rows as they arrived, and rows sorted inside each
chunk by the columns with the fewest distinct values first and then by time. The sort is
a property of the chunk, not of one column, so it is computed once per chunk and applied
to every column in it.
"""

from __future__ import annotations

import gzip as gzlib
import json
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, encodings as enc, measure, rows as rowlib
from lab.common.frontend import FLOAT64, IP, STRING, UUID

CHUNK = 8_192
ZSTD_LEVELS = [1, 3, 9, 19]
TIMING_RUNS = 3
TOP_TEXT_COLUMNS = 5


def chunk_order(P: rowlib.Parsed, t, idxs: list) -> list:
    """Rows sorted by the lowest-cardinality columns first, then by time.

    Low cardinality first is what makes a run-length or dictionary encoding pay: it puts
    equal values next to each other. Time last keeps the row group's min/max tight, which
    is what a time-range skip needs - sorting by time first would undo the clustering,
    and not sorting by time at all would widen every row group to the whole day.
    """
    card = []
    for i, key in enumerate(t.keys):
        distinct = len({P.raws[r][i] for r in idxs})
        card.append((distinct, i))
    card.sort()
    cols = [i for _, i in card]
    ts_i = t.keys.index(t.time_col) if t.time_col is not None else None

    def key_of(r):
        base = tuple("" if P.raws[r][i] is None else str(P.raws[r][i]) for i in cols)
        return base + ((P.ts_ns[r] or 0,) if ts_i is not None else ())

    return sorted(idxs, key=key_of)


def column_values(P: rowlib.Parsed, t, i: int, idxs: list, ty) -> list:
    """This column's values for these rows, in the column's folded type."""
    out = []
    for r in idxs:
        raw, val = P.raws[r][i], P.vals[r][i]
        # `vals` is None exactly where the ladder was never run: the key is absent, or
        # its JSON value was null. Both are a null cell. Reaching for `raws` here would
        # encode the four-character text "null" as if it were a value.
        if raw is None or val is None:
            out.append(None)
        elif ty in (STRING, UUID, IP) or ty is None:
            out.append(raw)
        elif ty == FLOAT64:
            out.append(float(val))
        else:
            out.append(val)
    return out


def charged(sizes: dict, names: list) -> dict:
    """Every encoder's cost for this chunk, falling back to plain where it does not apply.

    Without the fallback each encoder's column would sum over a different set of chunks:
    `dict` skips the high-cardinality ones, `const` skips everything that varies, and the
    totals stop being comparable - `dict` on UUIDs would read as a thousandfold win when
    what it really did was decline to encode all but a handful of chunks. Charging plain
    where an encoder declines is also what a real implementation does.
    """
    out = {}
    for n in names:
        if n in sizes:
            out[n] = sizes[n]
        elif n.endswith("+zstd3"):
            out[n] = sizes["plain+zstd3"]
        else:
            out[n] = sizes["plain"]
    return out


def main() -> int:
    ap = measure.parse_args("05_encodings")
    ap.add_argument(
        "--merge",
        metavar="RULE",
        help="chunk the templates one of 04's rules produces (e.g. J80) instead of the "
        "unmerged ones; recorded under the dataset name with [RULE] after it",
    )
    args = ap.parse_args()
    ds = datasets.ensure(args.dataset, args.size)
    # Its own label, or the merged run would replace the unmerged one wherever the newest
    # run per (script, dataset) is what counts - RESULTS.md and script 06 both.
    label = f"{ds.name}[{args.merge}]" if args.merge else ds.name
    res = measure.Results("05_encodings", label)
    measure.print_machine()

    P = rowlib.load(ds)
    if args.merge:
        f = measure.OUT / "04" / ds.name / f"mapping_{args.merge}.json"
        if not f.exists():
            sys.exit(f"{f} not found: run 04_merge_rules.py --dataset {ds.name} first")
        n_before = len(P.templates)
        P = rowlib.merged(P, json.loads(f.read_text()))
        print(f"\n{args.merge}: {n_before:,} templates merged into {len(P.templates):,}")
    print(f"\ndataset {label}: {ds.lines:,} lines, chunks of {CHUNK:,} rows per template")
    res.add("units", "templates chunked", len(P.templates), "count")

    # by (order, type, encoder) -> bytes; and per column for the top-10 table
    totals: dict = defaultdict(lambda: defaultdict(int))
    per_column: dict = defaultdict(lambda: defaultdict(int))
    col_type: dict = {}
    best_per_chunk: dict = {"arrival": 0, "sorted": 0}
    applied: dict = defaultdict(int)   # encoder -> plain bytes of the chunks it applied to
    plain_seen = 0
    ALL = [n for n, _ in enc.ENCODERS] + [f"{n}+zstd3" for n, _ in enc.ENCODERS]
    dict_scope: dict = {"per_chunk": 0, "per_file": 0}
    plain_blobs: list = []
    file_dicts: dict = defaultdict(lambda: defaultdict(list))

    for t in P.templates.values():
        rows_sorted = sorted(t.rows)
        for start in range(0, len(rows_sorted), CHUNK):
            arrival = rows_sorted[start : start + CHUNK]
            ordered = chunk_order(P, t, arrival)
            for order_name, idxs in (("arrival", arrival), ("sorted", ordered)):
                for i, key in enumerate(t.keys):
                    ty = t.types[key]
                    label = ty if ty is not None else "Null"
                    col_type[key] = label
                    values = column_values(P, t, i, idxs, ty)
                    sizes = enc.encode_all(values, ty)
                    full = charged(sizes, ALL)
                    for name, n in full.items():
                        totals[order_name][(label, name)] += n
                        if order_name == "arrival":
                            per_column[key][name] += n
                    if order_name == "arrival":
                        plain_seen += sizes["plain"]
                        for name in ALL:
                            if name in sizes:
                                applied[name] += sizes["plain"]
                    best = min(sizes.values())
                    best_per_chunk[order_name] += best
                    if order_name == "arrival":
                        plain_blobs.append(enc.enc_plain(values, ty))
                        # A word list per chunk, against one per file for the same column.
                        d = enc.enc_dict(values, ty)
                        dict_scope["per_chunk"] += (
                            len(d) if d is not None else sizes["plain"]
                        )
                        file_dicts[(t.tid, key)]["values"].extend(values)
                        file_dicts[(t.tid, key)]["ty"] = ty

    for (tid, key), slot in file_dicts.items():
        ty = slot["ty"]
        d = enc.enc_dict(slot["values"], ty)
        dict_scope["per_file"] += (
            len(d) if d is not None else len(enc.enc_plain(slot["values"], ty))
        )

    # -- bytes per column type per variant -------------------------------------
    types_seen = sorted({k[0] for k in totals["arrival"]})
    encoders = sorted({k[1] for k in totals["arrival"]})
    for order_name in ("arrival", "sorted"):
        body = []
        for label in types_seen:
            row = [label]
            for e in encoders:
                v = totals[order_name].get((label, e))
                row.append(v if v else "-")
            body.append(row)
        measure.table(
            body,
            headers=["column type"] + encoders,
            title=f"bytes per column type per encoding, rows in {order_name} order",
        )
        for label in types_seen:
            for e in encoders:
                v = totals[order_name].get((label, e))
                if v:
                    res.add(f"{order_name}/{label}", e, v, "bytes",
                            f"chunks of {CHUNK:,} rows, one column type")

    cov = [
        [n, applied[n], round(100.0 * applied[n] / plain_seen, 2)]
        for n in ALL
        if not n.endswith("+zstd3")
    ]
    measure.table(
        cov,
        headers=["encoder", "plain bytes of the chunks it applied to", "% of all bytes"],
        title="how much of the data each encoder actually applies to",
    )
    for n, b, pct in cov:
        res.add(f"coverage/{n}", "applies_to_pct_of_plain_bytes", pct, "percent",
                "chunks where the encoder declines are charged plain in the tables above")

    # -- zstd levels, and the one speed figure ---------------------------------
    corpus = b"".join(plain_blobs)
    zrows = []
    for level in ZSTD_LEVELS:
        times = []
        size = 0
        for _ in range(TIMING_RUNS):
            t0 = time.perf_counter()
            out = enc.zstd_bytes(corpus, level)
            times.append(time.perf_counter() - t0)
            size = len(out)
        med = statistics.median(times)
        mbps = (len(corpus) / 1024 / 1024) / med
        zrows.append([level, size, round(len(corpus) / size, 3), round(mbps, 1)])
        res.add(f"zstd level {level}", "bytes", size, "bytes",
                "one zstd call over every plain chunk concatenated")
        res.add(f"zstd level {level}", "compress_MBps", round(mbps, 2), "MB/s",
                f"median of {TIMING_RUNS} runs; zstd is a C library and this is its "
                "speed, not TrieDB's")
    measure.table(
        zrows,
        headers=["zstd level", "bytes", "ratio over plain", "MB/s (C library)"],
        title=f"zstd over the whole plain corpus ({measure.human(len(corpus))})",
    )

    # -- word list scope --------------------------------------------------------
    measure.table(
        [
            ["one word list per chunk", dict_scope["per_chunk"]],
            ["one word list per file (column)", dict_scope["per_file"]],
        ],
        headers=["word list scope", "bytes"],
        title="does a word list want to be per chunk or per file",
    )
    for k, v in dict_scope.items():
        res.add(f"wordlist {k}", "bytes", v, "bytes",
                "a per-file word list stores each distinct value once for the whole "
                "column; a per-chunk one repeats it, but indexes into a smaller list")

    # -- the ten biggest columns ------------------------------------------------
    ranked = sorted(per_column.items(), key=lambda kv: -kv[1]["plain"])[:10]
    body = []
    for key, sizes in ranked:
        plain = sizes["plain"]
        best_name = min(sizes, key=lambda n: sizes[n])
        body.append(
            [
                key[:32],
                col_type[key],
                plain,
                best_name,
                sizes[best_name],
                round(sizes[best_name] / plain, 4),
            ]
        )
        res.add(f"column:{key}", "plain_bytes", plain, "bytes", f"type {col_type[key]}")
        res.add(f"column:{key}", "best_encoding", best_name, "name",
                f"{sizes[best_name]:,} bytes, {sizes[best_name] / plain:.4f} of plain")
    measure.table(
        body,
        headers=["column", "type", "plain bytes", "best encoding", "bytes", "of plain"],
        title="the 10 biggest columns and what helps each",
    )

    # -- the CLP probe -----------------------------------------------------------
    text_cols = [
        (k, s["plain"])
        for k, s in sorted(per_column.items(), key=lambda kv: -kv[1]["plain"])
        if col_type[k] == STRING
    ][:TOP_TEXT_COLUMNS]
    crows = []
    for key, plain in text_cols:
        best = min(per_column[key].values())
        clp_total = 0
        for t in P.templates.values():
            if key not in t.types:
                continue
            i = t.keys.index(key)
            rows_sorted = sorted(t.rows)
            for start in range(0, len(rows_sorted), CHUNK):
                idxs = rows_sorted[start : start + CHUNK]
                values = column_values(P, t, i, idxs, t.types[key])
                split = enc.clp_split(values)
                if split is None:
                    clp_total += min(enc.encode_all(values, t.types[key]).values())
                else:
                    clp_total += min(
                        split.total,
                        min(enc.encode_all(values, t.types[key]).values()),
                    )
        crows.append(
            [key[:32], plain, best, clp_total, round(clp_total / best, 4) if best else 0]
        )
        res.add(f"column:{key}", "clp_split_bytes", clp_total, "bytes",
                f"fixed part through a word list, each changing part its own column; "
                f"{clp_total / best:.4f} of the best encoding above")
    measure.table(
        crows,
        headers=["text column", "plain", "best above", "CLP split", "CLP / best"],
        title=f"the CLP idea on the {TOP_TEXT_COLUMNS} text columns with the most bytes",
    )

    # -- the full stack ----------------------------------------------------------
    raw = P.raw_bytes
    gz = len(gzlib.compress(b"\n".join(p for _, p in datasets.iter_payloads(ds.path)), 9))
    srows = []
    for order_name in ("arrival", "sorted"):
        b = best_per_chunk[order_name]
        srows.append(
            [
                f"BEST_PER_CHUNK, {order_name} order",
                b,
                round(b / raw, 4),
                round(b / gz, 3),
            ]
        )
        res.add(f"BEST_PER_CHUNK {order_name}", "bytes", b, "bytes",
                "the smallest applicable encoding for every chunk of every column")
        res.add(f"BEST_PER_CHUNK {order_name}", "ratio_to_raw", round(b / raw, 5), "ratio")
        res.add(f"BEST_PER_CHUNK {order_name}", "ratio_to_gzip9", round(b / gz, 5), "ratio")
    plain_total = sum(totals["arrival"][(l, "plain")] for l in types_seen)
    srows.insert(0, ["plain (segment format)", plain_total, round(plain_total / raw, 4),
                     round(plain_total / gz, 3)])
    res.add("plain", "bytes", plain_total, "bytes", "`.col` plain chunk sizes, no codec")
    measure.table(
        srows,
        headers=["variant", "bytes", "of raw NDJSON", "of gzip -9"],
        title=f"the full stack (raw {raw:,} B, gzip -9 {gz:,} B)",
    )
    gain = 100.0 * (best_per_chunk["arrival"] - best_per_chunk["sorted"]) / best_per_chunk["arrival"]
    print(f"\n  sorting rows inside each chunk saves {gain:.2f}% of the best total.")
    res.add("verdict", "sort_saving_pct", round(gain, 3), "percent",
            "BEST_PER_CHUNK sorted against BEST_PER_CHUNK in arrival order")

    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
