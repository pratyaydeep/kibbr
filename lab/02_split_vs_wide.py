#!/usr/bin/env python3
"""Does splitting by template help or hurt compression?

The same lines written every way TrieDB could write them, measured on real files under
`lab/out/`. Two numbers for every layout: `st_size`, the bytes the format spends, and
`st_blocks * 512`, what the filesystem charges. The gap between them is the tiny-file
waste, and on a per-template layout it is not a rounding error - it is the answer.

SHARED_DAY exists to separate the two effects. It writes each template's rows as their
own Parquet buffer and concatenates the buffers into one file per day, so it measures
per-template *bytes* with none of the per-file block rounding. PER_TEMPLATE minus
SHARED_DAY is the cost of the file count alone.

Nothing here reports a rate. gzip and zstd are C libraries and script 08 times them; this
script only counts what they produced.
"""

from __future__ import annotations

import io
import json
import struct
import subprocess
import zlib
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, measure, rows as rowlib
from lab.common.frontend import fold_types

ROW_GROUP_SIZES = [1_000, 8_192, 122_880]
DEFAULT_RG = 8_192
ZSTD_LEVEL = 3  # the parquet codec level, held constant so layout is what varies
WINDOW_SECONDS = 300  # the 5-minute search window


# --- groups: what one Parquet file holds --------------------------------------


@dataclass
class Group:
    name: str
    keys: list
    types: dict
    rows: list


def groups_none(P: rowlib.Parsed) -> list:
    """One group per template - what TrieDB does with merging off."""
    return [
        Group(str(t.tid), list(t.keys), dict(t.types), list(t.rows))
        for t in P.templates.values()
    ]


def groups_from_mapping(P: rowlib.Parsed, mapping: dict) -> list:
    """One group per merged template, from a `lab/out/04/` mapping of lab_id -> group."""
    buckets: dict = defaultdict(list)
    for i, lab_id in enumerate(P.lab_id):
        g = mapping.get(lab_id)
        if g is None:
            return []
        buckets[g].append(i)
    out = []
    for g, idxs in buckets.items():
        keys: list = []
        types: dict = {}
        for i in idxs:
            t = P.templates[P.tid[i]]
            for k in t.keys:
                if k not in types:
                    keys.append(k)
                    types[k] = t.types[k]
                else:
                    types[k] = fold_types([types[k], t.types[k]])
        out.append(Group(str(g), sorted(keys), types, idxs))
    return out


def group_union(P: rowlib.Parsed) -> Group:
    """Every column in the dataset, types folded across all templates - the WIDE schema."""
    keys: list = []
    types: dict = {}
    for t in P.templates.values():
        for k in t.keys:
            if k not in types:
                keys.append(k)
                types[k] = t.types[k]
            else:
                types[k] = fold_types([types[k], t.types[k]])
    return Group("wide", sorted(keys), types, list(range(P.n_rows)))


# --- arrow tables ---------------------------------------------------------------


def table_for(P: rowlib.Parsed, g: Group, idxs: list) -> pa.Table:
    """One Arrow table for these rows under this group's schema.

    A row whose template does not hold a column contributes a null there, which is what
    a merged template or a WIDE layout has to store and what `null bitmaps` cost in the
    segment format: one bit per row, ahead of any chunk with a row missing.
    """
    arrays = []
    for key in g.keys:
        ty = g.types[key]
        col = []
        for r in idxs:
            t = P.templates[P.tid[r]]
            if key not in t.types:
                col.append(None)
                continue
            i = t.keys.index(key)
            raw, val = P.raws[r][i], P.vals[r][i]
            col.append(_cast(raw, val, ty))
        arrays.append(rowlib.arrow_array(col, ty))
    return pa.table(arrays, names=g.keys)


def _cast(raw, val, ty):
    from lab.common.frontend import FLOAT64, IP, STRING, UUID

    # `val` is None exactly where the type ladder was never run: the key is absent on
    # this row, or its JSON value was null. Both are Null in a column. Falling through
    # to `raw` would write the four-character string "null" as a value.
    if raw is None or val is None:
        return None
    if ty in (STRING, UUID, IP) or ty is None:
        return raw
    if ty == FLOAT64:
        return float(val)
    return val


def _write_parquet(table: pa.Table, dest, rg: int) -> None:
    pq.write_table(
        table,
        dest,
        compression="zstd",
        compression_level=ZSTD_LEVEL,
        use_dictionary=True,
        row_group_size=rg,
        write_statistics=True,
        version="2.6",
    )


# --- layouts --------------------------------------------------------------------


def layout_raw(P: rowlib.Parsed, ds, out: Path) -> Path:
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, "raw")
    dest = d / "logs.ndjson"
    with open(dest, "wb") as fh:
        for _, payload in datasets.iter_payloads(ds.path):
            fh.write(payload + b"\n")
    return d


def layout_gzip(raw_dir: Path, out: Path) -> Path:
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, "gzip9")
    src = raw_dir / "logs.ndjson"
    with open(d / "logs.ndjson.gz", "wb") as fh:
        subprocess.run(["gzip", "-9", "-c", str(src)], stdout=fh, check=True)
    return d


def layout_zstd(raw_dir: Path, out: Path) -> Path:
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, "zstd19")
    src = raw_dir / "logs.ndjson"
    cctx = zstd.ZstdCompressor(level=19)
    with open(src, "rb") as fin, open(d / "logs.ndjson.zst", "wb") as fout:
        cctx.copy_stream(fin, fout)
    return d


def layout_wide(P: rowlib.Parsed, out: Path, rg: int) -> Path:
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, f"wide_rg{rg}")
    g = group_union(P)
    _write_parquet(table_for(P, g, g.rows), d / "all.parquet", rg)
    return d


def layout_per_group(
    P: rowlib.Parsed, groups: list, out: Path, rg: int, tag: str, per_day: bool
) -> Path:
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, tag)
    for g in groups:
        if per_day:
            by_day: dict = defaultdict(list)
            for r in g.rows:
                by_day[P.day[r]].append(r)
            for day, idxs in by_day.items():
                sub = d / day
                sub.mkdir(exist_ok=True)
                _write_parquet(table_for(P, g, idxs), sub / f"{g.name}.parquet", rg)
        else:
            _write_parquet(table_for(P, g, g.rows), d / f"{g.name}.parquet", rg)
    return d


SHARED_MAGIC = b"TRDBSHRD"


def layout_shared_day(
    P: rowlib.Parsed, groups: list, out: Path, rg: int, tag: str
) -> Path:
    """One container file per day; each template's Parquet buffer concatenated into it.

    Per-template bytes with no per-file block rounding. The footer is real - 8 bytes of
    template id, 8 of offset, 8 of length each - because a container nothing can index
    into is not a layout, it is a number.
    """
    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, tag)
    per_day: dict = defaultdict(list)
    for g in groups:
        by_day: dict = defaultdict(list)
        for r in g.rows:
            by_day[P.day[r]].append(r)
        for day, idxs in by_day.items():
            per_day[day].append((g, idxs))

    for day, members in per_day.items():
        entries = []
        with open(d / f"{day}.shard", "wb") as fh:
            fh.write(SHARED_MAGIC)
            for g, idxs in members:
                buf = io.BytesIO()
                _write_parquet(table_for(P, g, idxs), buf, rg)
                blob = buf.getvalue()
                # Not `hash()`: Python randomises string hashing per process, and
                # this lab's rule is that the same input gives the same output. The
                # footer entry is eight bytes either way, so only reproducibility is at
                # stake - which is reason enough.
                ident = zlib.crc32(g.name.encode()) & 0xFFFFFFFFFFFFFFFF
                entries.append((ident, fh.tell(), len(blob)))
                fh.write(blob)
            for tid, off, length in entries:
                fh.write(struct.pack("<QQQ", tid, off, length))
            fh.write(struct.pack("<I", len(entries)))
            fh.write(SHARED_MAGIC)
    return d


# --- the 5-minute window --------------------------------------------------------


def window_cost(P: rowlib.Parsed, out: Path, rg: int) -> dict:
    """Bytes a 5-minute search over the busiest template must read, at this row group size.

    Row groups whose `[min ts, max ts]` overlaps the window are read whole; the rest are
    skipped on statistics alone. Bigger row groups compress better and skip worse, and
    this is the second half of that trade.

    Only meaningful because the dataset is in timestamp order - a shuffled template gives
    every row group the full day's range and skips nothing. The `uneven` dataset is
    sorted by time for exactly this reason, and a real ingest is too.
    """
    dated = [
        t
        for t in P.templates.values()
        if any(P.ts_ns[r] is not None for r in t.rows)
    ]
    if not dated:
        return {}
    tmpl = max(dated, key=lambda t: t.n)
    idxs = [r for r in tmpl.rows if P.ts_ns[r] is not None]
    idxs.sort(key=lambda r: P.ts_ns[r])

    d = measure.fresh_dir(*out.relative_to(measure.OUT).parts, f"window_rg{rg}")
    dest = d / "busy.parquet"
    g = Group(str(tmpl.tid), list(tmpl.keys), dict(tmpl.types), idxs)
    _write_parquet(table_for(P, g, idxs), dest, rg)

    # The window starts at the template's median timestamp, floored to five minutes:
    # deterministic, and inside the busy part of the day rather than at an edge.
    mid = P.ts_ns[idxs[len(idxs) // 2]]
    lo = (mid // 1_000_000_000 // WINDOW_SECONDS) * WINDOW_SECONDS
    hi = lo + WINDOW_SECONDS

    md = pq.ParquetFile(dest).metadata
    ts_col = tmpl.keys.index(tmpl.time_col)
    total = 0
    touched = 0
    groups_touched = 0
    matching_rows = sum(1 for r in idxs if lo <= P.ts_ns[r] // 1_000_000_000 < hi)
    for i in range(md.num_row_groups):
        size = sum(
            md.row_group(i).column(c).total_compressed_size
            for c in range(md.row_group(i).num_columns)
        )
        total += size
        st = md.row_group(i).column(ts_col).statistics
        if st is None or not st.has_min_max:
            touched += size
            groups_touched += 1
            continue
        g_lo = st.min.timestamp()
        g_hi = st.max.timestamp()
        if g_hi >= lo and g_lo < hi:
            touched += size
            groups_touched += 1
    return {
        "template": tmpl.tid,
        "rows": len(idxs),
        "row_groups": md.num_row_groups,
        "groups_touched": groups_touched,
        "file_bytes": total,
        "bytes_read": touched,
        "read_share_pct": 100.0 * touched / total if total else 0.0,
        "matching_rows": matching_rows,
        "rows_scanned": sum(
            md.row_group(i).num_rows
            for i in range(md.num_row_groups)
            if _overlaps(md, i, ts_col, lo, hi)
        ),
    }


def _overlaps(md, i, ts_col, lo, hi) -> bool:
    st = md.row_group(i).column(ts_col).statistics
    if st is None or not st.has_min_max:
        return True
    return st.max.timestamp() >= lo and st.min.timestamp() < hi


# --- merge mappings from script 04 ----------------------------------------------


def merge_mappings(dataset: str) -> list:
    """The best two merge rules from script 04, if it has been run on this dataset."""
    d = measure.OUT / "04" / dataset
    best = d / "best_rules.json"
    if not best.exists():
        return []
    names = json.loads(best.read_text())["best_two"]
    out = []
    for name in names:
        f = d / f"mapping_{name}.json"
        if f.exists():
            out.append((name, json.loads(f.read_text())))
    return out


# --- main ------------------------------------------------------------------------


def main() -> int:
    args = measure.parse_args("02_split_vs_wide").parse_args()
    ds = datasets.ensure(args.dataset, args.size)
    res = measure.Results("02_split_vs_wide", ds.name)
    measure.print_machine()
    print(f"\ndataset {ds.name}: {ds.lines:,} lines")

    P = rowlib.load(ds)
    out = measure.OUT / "02" / ds.name
    base = groups_none(P)

    built: list = []

    def record(label: str, path: Path, note: str = "") -> None:
        u = measure.disk_use(path)
        built.append((label, u, note))

    raw_dir = layout_raw(P, ds, out)
    record("raw NDJSON", raw_dir)
    record("gzip -9", layout_gzip(raw_dir, out), "C zlib, level 9")
    record("zstd -19", layout_zstd(raw_dir, out), "C zstd, level 19")

    raw_bytes = measure.disk_use(raw_dir).bytes
    gz_bytes = measure.disk_use(out / "gzip9").bytes

    for rg in ROW_GROUP_SIZES:
        record(f"WIDE rg={rg:,}", layout_wide(P, out, rg))
    for rg in ROW_GROUP_SIZES:
        record(
            f"PER_TEMPLATE rg={rg:,}",
            layout_per_group(P, base, out, rg, f"per_template_rg{rg}", False),
        )
    record(
        f"PER_TEMPLATE_DAY rg={DEFAULT_RG:,}",
        layout_per_group(P, base, out, DEFAULT_RG, "per_template_day", True),
        "what TrieDB does today",
    )
    for rg in ROW_GROUP_SIZES:
        record(
            f"SHARED_DAY rg={rg:,}",
            layout_shared_day(P, base, out, rg, f"shared_day_rg{rg}"),
            "per-template bytes without per-file block rounding",
        )

    for name, mapping in merge_mappings(ds.name):
        groups = groups_from_mapping(P, mapping)
        if not groups:
            print(f"  merge rule {name}: mapping does not cover this dataset, skipped")
            continue
        record(
            f"PER_TEMPLATE[{name}] rg={DEFAULT_RG:,}",
            layout_per_group(P, groups, out, DEFAULT_RG, f"pt_{name}", False),
            f"{len(groups)} merged templates, from script 04",
        )
        record(
            f"SHARED_DAY[{name}] rg={DEFAULT_RG:,}",
            layout_shared_day(P, groups, out, DEFAULT_RG, f"sd_{name}"),
            f"{len(groups)} merged templates, from script 04",
        )
    if not merge_mappings(ds.name):
        print("  script 04 has not run: no merged-template layouts in this table")

    body = []
    for label, u, note in built:
        body.append(
            [
                label,
                u.bytes,
                u.blocks,
                u.files,
                round(u.waste_pct, 1),
                round(u.bytes / raw_bytes, 4),
                round(u.blocks / raw_bytes, 4),
                round(u.bytes / gz_bytes, 3),
            ]
        )
        res.add(label, "bytes", u.bytes, "bytes", note)
        res.add(label, "disk_blocks", u.blocks, "bytes", note)
        res.add(label, "files", u.files, "count", note)
        res.add(label, "ratio_to_raw_bytes", round(u.bytes / raw_bytes, 5), "ratio", note)
        res.add(
            label, "ratio_to_raw_blocks", round(u.blocks / raw_bytes, 5), "ratio", note
        )
        res.add(label, "ratio_to_gzip9", round(u.bytes / gz_bytes, 5), "ratio", note)

    measure.table(
        body,
        headers=[
            "layout",
            "bytes",
            "disk blocks",
            "files",
            "% wasted",
            "bytes/raw",
            "blocks/raw",
            "bytes/gzip9",
        ],
        title=f"{ds.name}: every layout, real files under lab/out/02/{ds.name}",
    )

    # -- the other side of the row group trade ---------------------------------
    wrows = []
    for rg in ROW_GROUP_SIZES:
        w = window_cost(P, out, rg)
        if not w:
            continue
        wrows.append(
            [
                f"{rg:,}",
                w["row_groups"],
                w["groups_touched"],
                w["file_bytes"],
                w["bytes_read"],
                round(w["read_share_pct"], 2),
                w["matching_rows"],
                w["rows_scanned"],
            ]
        )
        for k, v in w.items():
            if k == "template":
                continue
            res.add(
                f"window rg={rg}",
                k,
                round(v, 4) if isinstance(v, float) else v,
                "bytes" if "bytes" in k else "count",
                f"5-minute window over the busiest dated template ({w['template']}); "
                "row groups are skipped on their ts statistics",
            )
    measure.table(
        wrows,
        headers=[
            "row group",
            "row groups",
            "touched",
            "file bytes",
            "bytes read",
            "% of file read",
            "rows wanted",
            "rows scanned",
        ],
        title="a 5-minute search over the busiest template - the cost of a big row group",
    )

    # -- the plain statement ----------------------------------------------------
    storage = [(l, u) for l, u, _ in built if l != "raw NDJSON"]
    by_bytes = min(storage, key=lambda x: x[1].bytes)
    by_blocks = min(storage, key=lambda x: x[1].blocks)
    pt = next(
        (u for l, u, _ in built if l == f"PER_TEMPLATE rg={DEFAULT_RG:,}"), None
    )
    gz = next(u for l, u, _ in built if l == "gzip -9")

    print("\n=== the answer ===")
    print(f"  smallest by bytes:       {by_bytes[0]} at {measure.human(by_bytes[1].bytes)}")
    print(
        f"  smallest by disk blocks: {by_blocks[0]} at "
        f"{measure.human(by_blocks[1].blocks)}"
    )
    if pt:
        verdict = "beats" if pt.bytes < gz.bytes else "loses to"
        print(
            f"  PER_TEMPLATE {verdict} gzip -9 on bytes "
            f"({measure.human(pt.bytes)} vs {measure.human(gz.bytes)}, "
            f"{pt.bytes / gz.bytes:.2f}x)"
        )
        bverdict = "beats" if pt.blocks < gz.blocks else "loses to"
        print(
            f"  PER_TEMPLATE {bverdict} gzip -9 once the filesystem is charged "
            f"({measure.human(pt.blocks)} vs {measure.human(gz.blocks)}, "
            f"{pt.blocks / gz.blocks:.2f}x) across {pt.files:,} files"
        )
        res.add(
            "verdict",
            "per_template_vs_gzip9_bytes",
            round(pt.bytes / gz.bytes, 4),
            "ratio",
            f"below 1 means per-template Parquet spends fewer bytes than gzip -9; "
            f"the blocks ratio is {pt.blocks / gz.blocks:.4f} over {pt.files:,} files",
        )
    res.add("verdict", "smallest_by_bytes", by_bytes[1].bytes, "bytes", by_bytes[0])
    res.add("verdict", "smallest_by_blocks", by_blocks[1].blocks, "bytes", by_blocks[0])

    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
