#!/usr/bin/env python3
"""Disk-dominated shapes. Every number here is SHAPE ONLY.

These are the measurements where the work is genuinely done by the disk or by a C
library, so a time means something. What it does not mean is a TrieDB throughput: the
loop around each call is Python, the machine is one laptop, and the figures are here to
show how a curve bends, not to predict a server. Every row says so.

Each timing runs three times and the median is reported.

One macOS-specific finding is worth the extra column: `fsync(2)` on Darwin returns once
the data reaches the drive's write cache, not the platter. A database that wants a real
durability barrier has to call `fcntl(F_FULLFSYNC)`, and the gap between the two is
measured here rather than assumed.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import platform
import shutil
import statistics
import sys
import time
from pathlib import Path

import zstandard as zstd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, measure, rows as rowlib

RECORD = b"x" * 200
N_RECORDS = 3_000
BATCHES = [1, 10, 100, 500]
WAL_BATCHES = [1, 10, 100, 500]
FILE_COUNTS = [1_000, 10_000, 100_000]
RUNS = 3
SSD_MS = 0.1
S3_MS = 15.0
SHAPE = "shape only - one laptop, Python around the call; not a TrieDB throughput"

HAS_FULLFSYNC = platform.system() == "Darwin" and hasattr(fcntl, "F_FULLFSYNC")


def _load_03():
    """Script 03's simulation and layouts, loaded by path.

    Registered in `sys.modules` before it is executed: `@dataclass` looks its own module
    up by name while the class body is being processed, and a module that is not there
    yet fails with an unhelpful AttributeError. The name starts with a digit, so a plain
    import cannot reach it.
    """
    name = "lab_layout_model"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).resolve().parent / "03_layout_model.py"
    )
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


def median_of(fn, runs: int = RUNS) -> float:
    return statistics.median(fn() for _ in range(runs))


# --- 1. flush batching ------------------------------------------------------------


def flush_batching(out: Path) -> list:
    rows = []
    modes = [("fsync", False)] + ([("F_FULLFSYNC", True)] if HAS_FULLFSYNC else [])
    for mode, full in modes:
        for batch in BATCHES:
            path = out / f"wal_{mode}_{batch}.bin"

            def once() -> float:
                with open(path, "wb") as fh:
                    fd = fh.fileno()
                    t0 = time.perf_counter()
                    for i in range(N_RECORDS):
                        fh.write(RECORD)
                        if (i + 1) % batch == 0:
                            fh.flush()
                            if full:
                                fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
                            else:
                                os.fsync(fd)
                    fh.flush()
                    if full:
                        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
                    else:
                        os.fsync(fd)
                    return time.perf_counter() - t0

            med = median_of(once)
            path.unlink(missing_ok=True)
            rows.append([mode, batch, round(med * 1000, 2), int(N_RECORDS / med)])
    return rows


# --- 2. WAL compression -----------------------------------------------------------


def wal_compression(ds) -> list:
    lines = [p for _, p in datasets.iter_payloads(ds.path)]
    lines = lines[:50_000]
    cctx = zstd.ZstdCompressor(level=3)
    rows = []
    for batch in WAL_BATCHES:
        raw = 0
        comp = 0

        def once() -> float:
            nonlocal raw, comp
            raw = comp = 0
            t0 = time.perf_counter()
            for s in range(0, len(lines), batch):
                blob = b"\n".join(lines[s : s + batch])
                raw += len(blob)
                comp += len(cctx.compress(blob))
            return time.perf_counter() - t0

        med = median_of(once)
        rows.append(
            [batch, raw, comp, round(comp / raw, 4), round((raw / 1024 / 1024) / med, 1)]
        )
    return rows


# --- 3. finding files -------------------------------------------------------------


def finding_files(out: Path) -> list:
    rows = []
    for n in FILE_COUNTS:
        d = out / f"many_{n}"
        d.mkdir(parents=True, exist_ok=True)
        names = [f"seg_{i:08d}.col" for i in range(n)]
        for name in names:
            (d / name).touch()
        manifest = out / f"manifest_{n}.txt"
        manifest.write_text("\n".join(names))

        listing = median_of(lambda: _time(lambda: os.listdir(d)))
        reading = median_of(lambda: _time(lambda: manifest.read_text().split("\n")))
        rows.append(
            [
                n,
                round(listing * 1000, 3),
                round(reading * 1000, 3),
                round(listing / reading, 1) if reading else 0,
            ]
        )
        shutil.rmtree(d)
    return rows


def _time(fn) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


# --- 4. scattered versus grouped ---------------------------------------------------


def scattered_vs_grouped(ds, P) -> dict:
    """The one-template search over the shared-file layout, before and after grouping.

    "Before" is the order the row groups were written in: a template's row groups sit
    wherever its lines happened to arrive, interleaved with every other template's.
    "After" is the same bytes with each template's row groups laid next to each other, so
    consecutive ones collapse into one range. Nothing else changes - same bytes, same
    files, same row groups. Only the ordering.
    """
    m = _load_03()
    costs = m.build_costs(P)
    sim = m.simulate(P, costs, rate=12, wait=300, tick_closes=False)
    lay = m.assign(sim, costs, "shared_day_limit", limit=1024**3)

    from collections import Counter

    day_hist = Counter(d for d in P.day if d != rowlib.UNDATED)
    day = day_hist.most_common(1)[0][0] if day_hist else rowlib.UNDATED
    on_day: Counter = Counter()
    for r in range(P.n_rows):
        if P.day[r] == day:
            on_day[P.tid[r]] += 1
    tid = on_day.most_common(1)[0][0]

    before = after = 0
    groups = 0
    for key, rgs in lay.files.items():
        if lay.day_of[key] != day:
            continue
        hits = [i for i, rg in enumerate(rgs) if rg.tid == tid]
        groups += len(hits)
        if not hits:
            continue
        before += 1 + sum(1 for a, b in zip(hits, hits[1:]) if b != a + 1)
        # Grouped: every row group of this template is contiguous, so one range per file.
        after += 1
    return {
        "day": day,
        "template": tid,
        "row_groups_of_template": groups,
        "ranges_before": before,
        "ranges_after": after,
        "ssd_ms_before": before * SSD_MS,
        "ssd_ms_after": after * SSD_MS,
        "s3_ms_before": before * S3_MS,
        "s3_ms_after": after * S3_MS,
    }


# --- main ---------------------------------------------------------------------------


def main() -> int:
    args = measure.parse_args("08_io_shapes").parse_args()
    ds = datasets.ensure(args.dataset, args.size)
    res = measure.Results("08_io_shapes", ds.name)
    measure.print_machine()
    out = measure.fresh_dir("08")
    print(f"\nevery figure below is SHAPE ONLY; median of {RUNS} runs")

    fb = flush_batching(out)
    measure.table(
        fb,
        headers=["durability call", "records per flush", "total ms", "records/sec"],
        title=f"flush batching: {N_RECORDS:,} records of {len(RECORD)} bytes",
    )
    for mode, batch, ms, rps in fb:
        res.add(f"{mode} batch={batch}", "records_per_sec", rps, "records/s",
                f"{SHAPE}; the disk does this work. "
                + ("F_FULLFSYNC is Darwin's real barrier to the platter"
                   if mode == "F_FULLFSYNC"
                   else "fsync(2) on Darwin returns at the drive's write cache, not the "
                        "platter"))
        res.add(f"{mode} batch={batch}", "total_ms", ms, "ms", SHAPE)
    if HAS_FULLFSYNC:
        f1 = next(r for r in fb if r[0] == "fsync" and r[1] == 1)
        f2 = next(r for r in fb if r[0] == "F_FULLFSYNC" and r[1] == 1)
        print(
            f"\n  A per-record fsync(2) does {f1[3]:,} records/sec; the same loop with "
            f"F_FULLFSYNC does {f2[3]:,}. The expected 40k-vs-300k lines/sec gap between "
            "an fsync per line and one per 500-line WAL batch is about batching, but on "
            "this platform which call is used moves it by "
            f"{f1[3] / max(f2[3], 1):.0f}x on its own."
        )

    wc = wal_compression(ds)
    measure.table(
        wc,
        headers=["lines per batch", "raw bytes", "zstd 3 bytes", "ratio", "MB/s (C lib)"],
        title="WAL compression: zstd 3 over batches of whole lines",
    )
    for batch, raw, comp, ratio, mbps in wc:
        res.add(f"wal batch={batch}", "compression_ratio", ratio, "ratio",
                f"{SHAPE}; zstd is a C library, so the MB/s is its own: {mbps}")
        res.add(f"wal batch={batch}", "zstd_MBps", mbps, "MB/s", SHAPE)
    best = min(wc, key=lambda r: r[3])
    (out / "wal_ratio.json").write_text(
        json.dumps({"batch": best[0], "ratio": best[3], "level": 3})
    )
    print(f"  best WAL batch is {best[0]} lines at {best[3]:.4f}; script 03 reads this "
          f"from {out / 'wal_ratio.json'}")

    ff = finding_files(out)
    measure.table(
        ff,
        headers=["files in folder", "listdir ms", "manifest read ms", "listdir / manifest"],
        title="finding files: listing a directory against reading one manifest",
    )
    for n, lms, mms, ratio in ff:
        res.add(f"{n} files", "listdir_ms", lms, "ms", f"{SHAPE}; the filesystem does this")
        res.add(f"{n} files", "manifest_read_ms", mms, "ms", SHAPE)
        res.add(f"{n} files", "listdir_over_manifest", ratio, "ratio", SHAPE)

    P = rowlib.load(ds)
    sg = scattered_vs_grouped(ds, P)
    measure.table(
        [
            ["separate byte ranges", sg["ranges_before"], sg["ranges_after"]],
            ["SSD at 0.1 ms/range", round(sg["ssd_ms_before"], 1), round(sg["ssd_ms_after"], 1)],
            ["S3 at 15 ms/range", round(sg["s3_ms_before"], 1), round(sg["s3_ms_after"], 1)],
        ],
        headers=["", "scattered (as written)", "grouped by template"],
        title=f"one-template search over the shared-day file, {sg['day']}, template "
              f"{sg['template']} ({sg['row_groups_of_template']} row groups)",
    )
    for k, v in sg.items():
        if k in ("day", "template"):
            continue
        res.add("scattered vs grouped", k, v,
                "ms" if k.endswith("_ms_before") or k.endswith("_ms_after") else "count",
                f"{SHAPE}; a range count multiplied by a per-seek constant, not a timed "
                f"read. Day {sg['day']}, template {sg['template']}")

    shutil.rmtree(out / "many_1000", ignore_errors=True)
    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
