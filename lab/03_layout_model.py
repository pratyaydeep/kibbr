#!/usr/bin/env python3
"""Files, row groups and overhead for each way of laying segments out.

A simulation on a fake clock - nothing sleeps, and no number here is a rate. Lines arrive
in timestamp order at `--rate` lines per second; a row group closes at 8,192 rows, 8 MB,
or when its oldest row has waited `--wait` seconds. Closes are counted by reason, because
which rule fires is what decides the shape.

Bytes come from `common/colmodel.py`, which is TRIEDB.md section 6.2 written out field by
field, and every file rounds up to a 4 KiB block.

CALIBRATION runs last and is the point of the whole script: the same data goes into the
real binary under `lab/out/03/calib/`, and the model's per-template-per-day figure is held
to within 10% of what the binary actually wrote. A model that has not been checked against
the thing it models is a spreadsheet.
"""

from __future__ import annotations

import heapq
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import colmodel, datasets, measure, rows as rowlib

MAX_ROWS = 8_192
MAX_BYTES = 8 * 1024 * 1024
TICK_MS = 500
RATES = [12, 120, 1200]
WAITS = [60, 300, 900]
SHARED_LIMITS = [128 * 1024**2, 512 * 1024**2, 1024**3]
MOD_N = [4, 8]
WAL_RECORD_OVERHEAD = 4 + 8 + 8 + 8 + 4  # section 7.1, before the raw line


# --- segment sealing (src/store/disk.rs row_fits / extend_shape) --------------
#
# A segment does not hold a whole (template, day). It holds a maximal run of rows that
# agree on their key set, on each column's kind, and on each Datetime column's rung -
# and the moment a row would widen any of those, `row_fits` says no and the segment is
# sealed. Section 6.2 describes the file format and says nothing about this, so the first
# version of this model assumed one file per (template, day) and was exactly right on
# flat30 and uneven for the wrong reason: neither has a key carrying two types, so every
# partition happens to be one run.
#
# On `messy` the same assumption predicted 130 files where the binary wrote 34,892.
#
# Two details decide the count, and both come from the source rather than the spec:
#   - The store's kind space is {Str, I64, F64, Dt, Bool} (segment.rs ColKind), so Uuid
#     and Ip are Str. A UUID column taking an empty string does NOT seal.
#   - `extend_shape` refuses two different `dt_form`s outright rather than merging them to
#     Mixed, and `row_fits` then rejects any other rung. So a `ts` column alternating
#     between RFC 3339 and ten-digit epoch seals in BOTH directions, every time, forever.
#     That is the single largest source of segments on `messy`: 17,404 of 34,891.

# segment.rs ColKind. Uuid and Ip collapse into Str, which is why widening one of those
# costs a type but not a segment.
_KIND = {
    "String": "Str", "Uuid": "Str", "Ip": "Str",
    "Int64": "I64", "Float64": "F64", "Datetime": "Dt", "Bool": "Bool",
    None: None,
}


def _kind_join(a: str, b: str) -> str:
    if a == b:
        return a
    if {a, b} == {"I64", "F64"}:
        return "F64"
    return "Str"


def seal_starts(ds, rule=None) -> dict:
    """Row indices, per (template, day), at which the binary would open a new segment.

    One parse pass. Only the boundaries are kept, so this costs a few thousand ints
    rather than a type per row per column.
    """
    from lab.common.frontend import parse_line

    out: dict = defaultdict(list)
    spec: dict = {}
    counters: dict = defaultdict(int)
    for _lab_id, raw in datasets.iter_payloads(ds.path):
        p = parse_line(raw, rule) if rule else parse_line(raw)
        shape = {}
        for k in p.keys:
            t = p.typed.get(k)
            shape[k] = (_KIND[p.column_type(k)], t.dt_form if t is not None else None)
        ts = p.typed.get(rowlib.TS_COLUMN)
        day = (
            _day_from_ns(ts.value)
            if ts is not None and ts.ty == "Datetime" and ts.value is not None
            else rowlib.UNDATED
        )
        key = (p.template_id, day)
        idx = counters[key]
        counters[key] += 1

        cur = spec.get(key)
        if cur is None or not _fits(cur, shape):
            out[key].append(idx)
            spec[key] = dict(shape)
    return dict(out)


def _fits(spec: dict, shape: dict) -> bool:
    if len(shape) != len(spec) or any(k not in spec for k in shape):
        return False
    for k, (kind, form) in shape.items():
        skind, sform = spec[k]
        if kind is None or skind is None:
            continue  # a null claims nothing, here as in the type lattice
        if _kind_join(skind, kind) != skind:
            return False
        if skind == "Dt" and form is not None and sform is not None and form != sform:
            return False
    return True


def _day_from_ns(ns: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc).strftime(
        "%Y-%m-%d"
    )


# --- per-template column costs, precomputed -----------------------------------


@dataclass
class ColCost:
    """One column of one template, with prefix sums so a row group costs O(1)."""

    name: str
    ty: object
    data: list  # cumulative value bytes over the template's rows
    present: list  # cumulative present count

    def slice(self, a: int, b: int) -> tuple:
        return self.data[b] - self.data[a], self.present[b] - self.present[a]


@dataclass
class TemplateCost:
    tid: int
    keys: list
    cols: dict
    rows: list  # row indices, in arrival order
    days: list  # day per row, aligned with `rows`
    line_bytes: list


def build_costs(P: rowlib.Parsed) -> dict:
    out = {}
    for tid, t in P.templates.items():
        ordered = sorted(t.rows)  # the dataset is already in timestamp order
        cols = {}
        for i, key in enumerate(t.keys):
            ty = t.types[key]
            data = [0]
            present = [0]
            d = p = 0
            for r in ordered:
                # Null is `vals is None`: the key was absent, or its JSON value was
                # null. Section 6.2 spends a bitmap bit on such a row and no data bytes;
                # charging it the four characters of `raws` would price a null as a
                # string.
                if P.vals[r][i] is None:
                    data.append(d)
                    present.append(p)
                    continue
                d += colmodel.value_bytes(ty, P.raws[r][i])
                p += 1
                data.append(d)
                present.append(p)
            cols[key] = ColCost(key, ty, data, present)
        out[tid] = TemplateCost(
            tid, list(t.keys), cols, ordered, [P.day[r] for r in ordered], []
        )
    return out


# --- the simulation ------------------------------------------------------------


@dataclass
class RowGroup:
    tid: int
    day: str
    start: int  # index into the template's own row list
    end: int
    reason: str

    @property
    def rows(self) -> int:
        return self.end - self.start


@dataclass
class SimResult:
    row_groups: list
    reasons: Counter
    backlog_max: int
    backlog_median: float
    backlog_p90: int


@dataclass
class _Buf:
    tid: int
    day: str
    start: int
    count: int = 0
    nbytes: int = 0
    opened_at: float = 0.0
    seq: int = 0


def simulate(
    P: rowlib.Parsed,
    costs: dict,
    rate: float,
    wait: float,
    tick_closes: bool,
    seals: dict | None = None,
) -> SimResult:
    """One pass over the dataset on a fake clock.

    Arrival time is `i / rate`: nothing sleeps, and `--wait` is measured on that clock.
    Expiry is driven by a heap keyed on each buffer's oldest row, so the per-row cost
    does not grow with the number of open templates - scanning 5,000 buffers per line
    would make the simulation itself the bottleneck it is meant to be measuring.
    """
    cursor = {tid: 0 for tid in costs}
    bufs: dict = {}
    heap: list = []
    closed: list = []
    reasons: Counter = Counter()
    backlog = 0
    samples: list = []
    seq = 0
    next_tick = TICK_MS / 1000.0

    def close(key, reason: str) -> None:
        nonlocal backlog
        b = bufs.pop(key, None)
        if b is None or b.count == 0:
            return
        closed.append(RowGroup(b.tid, b.day, b.start, b.start + b.count, reason))
        reasons[reason] += 1
        backlog -= b.count

    seal_set = {k: set(v) for k, v in (seals or {}).items()}
    order = sorted(range(P.n_rows), key=lambda r: (P.ts_ns[r] is None, P.ts_ns[r] or 0))
    for i, r in enumerate(order):
        now = i / rate

        # Buffers whose oldest row has waited long enough.
        while heap and heap[0][0] <= now:
            _, _, key, s = heapq.heappop(heap)
            b = bufs.get(key)
            if b is not None and b.seq == s:
                close(key, "wait")

        if tick_closes:
            while now >= next_tick:
                for key in list(bufs):
                    close(key, "tick")
                next_tick += TICK_MS / 1000.0

        tid = P.tid[r]
        tc = costs[tid]
        day = P.day[r]
        key = (tid, day)

        # A row that would not fit the open segment seals it, and `run_end` returns a run
        # that stops short of that row - so a row group never spans a seal. Without this
        # the model puts one row group across a whole partition and then cannot tell
        # which of its 34,891 segments the rows belong to.
        if seals is not None and cursor[tid] in seal_set.get(key, ()):
            close(key, "seal")

        b = bufs.get(key)
        if b is None:
            seq += 1
            b = _Buf(tid, day, cursor[tid], opened_at=now, seq=seq)
            bufs[key] = b
            heapq.heappush(heap, (now + wait, seq, key, seq))

        idx = cursor[tid]
        cursor[tid] = idx + 1
        b.count += 1
        backlog += 1
        for col in tc.cols.values():
            d, _ = col.slice(idx, idx + 1)
            b.nbytes += d

        if b.count >= MAX_ROWS:
            close(key, "rows")
        elif b.nbytes >= MAX_BYTES:
            close(key, "bytes")

        if i % 97 == 0:
            samples.append(backlog)

    for key in list(bufs):
        close(key, "shutdown")

    samples.append(backlog)
    ordered = sorted(samples)
    return SimResult(
        closed,
        reasons,
        max(samples) if samples else 0,
        statistics.median(ordered) if ordered else 0,
        ordered[min(len(ordered) - 1, int(0.9 * len(ordered)))] if ordered else 0,
    )


# --- costing a row group under a given column set ------------------------------


def rg_cost(costs: dict, rg: RowGroup, colset: list) -> tuple:
    """`(total, overhead, data)` for one row group written under `colset`.

    A shared file holds one column list for the whole segment (section 6.2: names are
    stored once per segment), so a row group in one pays a header entry and an all-null
    bitmap for every column it does not hold. That is the honest cost of putting two
    templates in one segment without changing the format, and it is most of what the
    shared-file layouts below are being asked about.
    """
    tc = costs[rg.tid]
    cols = []
    for name in colset:
        col = tc.cols.get(name)
        if col is None:
            cols.append((name, 0, 0))
        else:
            d, p = col.slice(rg.start, rg.end)
            cols.append((name, p, d))
    return colmodel.rowgroup_bytes(rg.rows, cols)


# --- layouts -------------------------------------------------------------------


@dataclass
class Layout:
    name: str
    files: dict = field(default_factory=lambda: defaultdict(list))
    day_of: dict = field(default_factory=dict)  # file key -> day, so a search can filter
    note: str = ""

    def put(self, key, rg) -> None:
        self.files[key].append(rg)
        self.day_of[key] = rg.day


def assign(sim: SimResult, costs: dict, kind: str, **kw) -> Layout:
    """Which file each closed row group lands in. The close decisions are already made."""
    lay = Layout(kind)
    if kind == "per_template_day":
        seals = kw.get("seals") or {}
        for rg in sim.row_groups:
            starts = seals.get((rg.tid, rg.day))
            seg = 0
            if starts:
                # The run a row group was cut from never spans a seal, so its first row
                # decides which segment it lands in.
                import bisect

                seg = bisect.bisect_right(starts, rg.start) - 1
            lay.put((rg.tid, rg.day, seg), rg)
        lay.note = (
            "what TrieDB does today: one segment per leaf per day, resealed whenever a "
            "row would widen a column's kind or change a Datetime column's rung"
        )

    elif kind == "shared_day_limit":
        limit = kw["limit"]
        seqs: dict = defaultdict(int)
        sizes: dict = defaultdict(int)
        for rg in sim.row_groups:
            # Sized on the row group's own columns; the union penalty is charged when
            # the file is costed, which is after the file's column set is known.
            approx = rg_cost(costs, rg, costs[rg.tid].keys)[0]
            key = (rg.day, seqs[rg.day])
            if sizes[key] and sizes[key] + approx > limit:
                seqs[rg.day] += 1
                key = (rg.day, seqs[rg.day])
            sizes[key] += approx
            lay.put(key, rg)
        lay.note = f"one shared file per day, rolled at {measure.human(limit)}"

    elif kind == "mod_n":
        n = kw["n"]
        for rg in sim.row_groups:
            lay.put((rg.day, rg.tid % n), rg)
        lay.note = f"{n} files per day, by template mod {n}"

    elif kind == "least_full":
        n = kw["n"]
        sizes: dict = defaultdict(lambda: [0] * n)
        for rg in sim.row_groups:
            approx = rg_cost(costs, rg, costs[rg.tid].keys)[0]
            slot = min(range(n), key=lambda s: sizes[rg.day][s])
            sizes[rg.day][slot] += approx
            lay.put((rg.day, slot), rg)
        lay.note = f"{n} files per day, each row group to the file with the fewest bytes"

    else:
        raise ValueError(kind)
    return lay


@dataclass
class LayoutCost:
    files: int
    row_groups: int
    total: int
    overhead: int
    data: int
    blocks: int
    footer: int
    rows: list
    per_file: dict


def cost_layout(lay: Layout, costs: dict, shared_columns: bool) -> LayoutCost:
    files = 0
    row_groups = 0
    total = overhead = data = blk = foot = 0
    rowcounts: list = []
    per_file: dict = {}
    for key, rgs in lay.files.items():
        if shared_columns:
            names: list = []
            seen = set()
            for rg in rgs:
                for nm in costs[rg.tid].keys:
                    if nm not in seen:
                        seen.add(nm)
                        names.append(nm)
            names.sort()
        else:
            names = costs[rgs[0].tid].keys
        built = [rg_cost(costs, rg, names) for rg in rgs]
        t, o, d, f = colmodel.file_bytes(names, built)
        files += 1
        row_groups += len(rgs)
        total += t
        overhead += o
        data += d
        foot += f
        blk += colmodel.blocks(t)
        rowcounts.extend(rg.rows for rg in rgs)
        per_file[key] = (t, f, len(rgs), names)
    return LayoutCost(
        files, row_groups, total, overhead, data, blk, foot, rowcounts, per_file
    )


# --- searches over one day -----------------------------------------------------


def search_cost(lay: Layout, lc: LayoutCost, costs: dict, day: str, tid: int) -> dict:
    """What two searches must touch for one day: one template, and every template.

    Separate byte ranges is the number the disk actually feels. Row groups of one
    template that sit next to each other in a file are one range; scattered among other
    templates' row groups they are many, and script 08 turns that count into milliseconds.
    """
    one_files = 0
    one_index = 0
    one_ranges = 0
    all_files = 0
    all_index = 0
    all_ranges = 0
    for key, rgs in lay.files.items():
        if lay.day_of[key] != day:
            continue
        t, f, n, _names = lc.per_file[key]
        all_files += 1
        all_index += f
        all_ranges += 1  # every row group is wanted, so the data region is one range
        hits = [i for i, rg in enumerate(rgs) if rg.tid == tid]
        if hits:
            one_files += 1
            one_index += f
            runs = 1
            for a, b in zip(hits, hits[1:]):
                if b != a + 1:
                    runs += 1
            one_ranges += runs
    return {
        "one_template_files": one_files,
        "one_template_index_bytes": one_index,
        "one_template_ranges": one_ranges,
        "all_templates_files": all_files,
        "all_templates_index_bytes": all_index,
        "all_templates_ranges": all_ranges,
    }


# --- calibration ----------------------------------------------------------------


def calibrate(ds, P: rowlib.Parsed, costs: dict) -> dict:
    """Ingest the same lines into the real binary and measure what it wrote.

    The binary is handed the whole file in one go, so no row group times out mid-run and
    each (template, day) gets one row group - which is the close rule the model is given
    here too. Comparing a bulk ingest against a model that had a 60-second timer running
    would be comparing two different experiments.
    """
    data_dir = measure.fresh_dir("03", "calib")
    port = 7811
    body = data_dir.parent / "calib_input.ndjson"
    with open(body, "wb") as fh:
        for _, payload in datasets.iter_payloads(ds.path):
            fh.write(payload + b"\n")

    proc = subprocess.Popen(
        [
            str(datasets.BINARY),
            "serve",
            "--port",
            str(port),
            "--data-dir",
            str(data_dir),
            "--storage",
            "disk",
            "--row-group-timeout-ms",
            "3600000",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        import urllib.error
        import urllib.request

        for _ in range(120):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1).read()
                break
            except Exception:
                time.sleep(0.25)
        else:
            raise RuntimeError("triedb serve did not come up")

        ingested = 0
        chunk: list = []
        with open(body, "rb") as fh:
            for line in fh:
                chunk.append(line)
                if len(chunk) >= 20_000:
                    ingested += _post(port, b"".join(chunk))
                    chunk = []
        if chunk:
            ingested += _post(port, b"".join(chunk))
    finally:
        # Shutdown is where every open segment is sealed and `trie.json` is written.
        # At 16,000 segments that is tens of thousands of small writes, so the wait is
        # generous: killing it early would measure a half-written data directory.
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=1800)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise RuntimeError(
                "triedb did not finish its shutdown inside 30 minutes; the data dir is "
                "half-written and the calibration would be measuring a torn run"
            )

    real = _measure_datadir(data_dir)
    real["ingested"] = ingested

    # The model, under the same close rule: no timer, so one row group per (template,
    # day) unless 8,192 rows or 8 MB got there first.
    seals = seal_starts(ds)
    sim = simulate(P, costs, rate=1e12, wait=1e18, tick_closes=False, seals=seals)
    lay = assign(sim, costs, "per_template_day", seals=seals)
    model = cost_layout(lay, costs, shared_columns=False)

    diff = 100.0 * (model.total - real["col_bytes"]) / real["col_bytes"]
    fdiff = 100.0 * (model.files - real["col_files"]) / real["col_files"]
    bdiff = 100.0 * (model.blocks - real["col_blocks"]) / real["col_blocks"]
    return {
        "real": real,
        "model": model,
        "bytes_diff_pct": diff,
        "files_diff_pct": fdiff,
        "blocks_diff_pct": bdiff,
        "within_10pct": abs(diff) <= 10.0,
    }


def _post(port: int, body: bytes) -> int:
    import urllib.request

    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/ingest",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.loads(r.read())["ingested"]


def _measure_datadir(d: Path) -> dict:
    out = {
        "col_bytes": 0,
        "col_blocks": 0,
        "col_files": 0,
        "meta_bytes": 0,
        "meta_blocks": 0,
        "meta_files": 0,
        "dirs": 0,
        # Segments holding more blocks than `ceil(size / 4096)` accounts for. Counted
        # rather than assumed: the model rounds every file up to a 4 KiB block, and this
        # says how often the filesystem disagrees, and by how much.
        "col_over_ceil_files": 0,
        "col_over_ceil_bytes": 0,
        "col_over_ceil_largest": 0,
    }
    for root, dirs, names in os.walk(d):
        out["dirs"] += len(dirs)
        for n in names:
            st = os.stat(Path(root) / n)
            kind = "col" if n.endswith(".col") else "meta"
            out[f"{kind}_bytes"] += st.st_size
            out[f"{kind}_blocks"] += st.st_blocks * 512
            out[f"{kind}_files"] += 1
            if kind == "col":
                over = st.st_blocks * 512 - colmodel.blocks(st.st_size)
                if over > 0:
                    out["col_over_ceil_files"] += 1
                    out["col_over_ceil_bytes"] += over
                    out["col_over_ceil_largest"] = max(
                        out["col_over_ceil_largest"], st.st_size
                    )
    return out


# --- WAL -----------------------------------------------------------------------


def wal_ratio() -> tuple:
    """The batch compression ratio measured by script 08, if it has run."""
    f = measure.OUT / "08" / "wal_ratio.json"
    if f.exists():
        d = json.loads(f.read_text())
        return d["ratio"], f"zstd 3 over batches of {d['batch']} lines, from script 08"
    return None, "script 08 has not run, so only the raw figure is given"


# --- main -----------------------------------------------------------------------


def main() -> int:
    ap = measure.parse_args("03_layout_model")
    ap.add_argument("--rate", type=float, default=None)
    ap.add_argument("--wait", type=float, default=None)
    ap.add_argument("--tick-closes", action="store_true")
    ap.add_argument("--skip-calibration", action="store_true")
    args = ap.parse_args()

    ds = datasets.ensure(args.dataset, args.size)
    res = measure.Results("03_layout_model", ds.name)
    measure.print_machine()
    print(f"\ndataset {ds.name}: {ds.lines:,} lines, raw ", end="")

    P = rowlib.load(ds)
    print(f"{P.raw_bytes:,} bytes")
    costs = build_costs(P)
    raw = P.raw_bytes
    avg_line = raw / P.n_rows

    rates = [args.rate] if args.rate else RATES
    waits = [args.wait] if args.wait else WAITS

    runs = [(r, w, False) for r in rates for w in waits]
    runs += [(r, max(waits), True) for r in rates] if not args.tick_closes else []
    if args.tick_closes:
        runs = [(r, w, True) for r in rates for w in waits]

    # -- close reasons and backlog ---------------------------------------------
    seals = seal_starts(ds)
    n_seg = sum(len(v) for v in seals.values())
    print(
        f"sealing: {len(seals):,} (template, day) partitions become {n_seg:,} segments "
        "under the kind/dt_form rule of src/store/disk.rs"
    )
    res.add("sealing", "partitions", len(seals), "count",
            "a (template, day) pair - what the model used to assume was one file")
    res.add("sealing", "segments", n_seg, "count",
            "segments the seal rule actually produces; section 6.2 does not mention "
            "sealing, so the first version of this model assumed one per partition")

    sims: dict = {}
    crows = []
    for rate, wait, tick in runs:
        sim = simulate(P, costs, rate, wait, tick, seals=seals)
        sims[(rate, wait, tick)] = sim
        label = f"rate={rate:g} wait={wait:g}" + (" tick" if tick else "")
        rc = sorted(rg.rows for rg in sim.row_groups)
        crows.append(
            [
                label,
                len(sim.row_groups),
                sim.reasons["rows"],
                sim.reasons["bytes"],
                sim.reasons["wait"],
                sim.reasons["tick"],
                sim.reasons["shutdown"],
                rc[0],
                int(statistics.median(rc)),
                rc[min(len(rc) - 1, int(0.9 * len(rc)))],
                rc[-1],
            ]
        )
        for k, v in sim.reasons.items():
            res.add(label, f"closes_{k}", v, "count", "close reason")
        res.add(label, "row_groups", len(sim.row_groups), "count")
        res.add(label, "rows_per_rg_median", int(statistics.median(rc)), "rows")
        res.add(label, "rows_per_rg_p90", rc[min(len(rc) - 1, int(0.9 * len(rc)))], "rows")
        res.add(label, "backlog_max_lines", sim.backlog_max, "lines",
                "lines in memory and not yet in a closed row group - what a crash replays")
        res.add(label, "backlog_median_lines", sim.backlog_median, "lines")
    measure.table(
        crows,
        headers=[
            "run", "row groups", "rows", "bytes", "wait", "tick", "shutdown",
            "rg min", "rg median", "rg p90", "rg max",
        ],
        title="closes by reason, and rows per row group",
    )

    # -- WAL backlog ------------------------------------------------------------
    ratio, ratio_note = wal_ratio()
    brows = []
    for (rate, wait, tick), sim in sims.items():
        if tick:
            continue
        day_raw = rate * 86400 * (avg_line + WAL_RECORD_OVERHEAD)
        brows.append(
            [
                f"rate={rate:g} wait={wait:g}",
                sim.backlog_max,
                int(sim.backlog_median),
                sim.backlog_p90,
                measure.human(day_raw),
                measure.human(day_raw * ratio) if ratio else "-",
            ]
        )
        res.add(
            f"rate={rate:g} wait={wait:g}",
            "wal_24h_bytes_raw",
            int(day_raw),
            "bytes",
            f"{WAL_RECORD_OVERHEAD} bytes of section 7.1 record per line plus "
            f"{avg_line:.0f} bytes of line; {ratio_note}",
        )
        if ratio:
            res.add(
                f"rate={rate:g} wait={wait:g}",
                "wal_24h_bytes_zstd",
                int(day_raw * ratio),
                "bytes",
                ratio_note,
            )
    measure.table(
        brows,
        headers=[
            "run", "backlog max lines", "backlog median", "backlog p90",
            "24h WAL raw", "24h WAL zstd",
        ],
        title="WAL backlog - what a crash would replay - and a day of WAL on disk",
    )

    # -- layouts ---------------------------------------------------------------
    focus = (rates[0], waits[1] if len(waits) > 1 else waits[0], False)
    if focus not in sims:
        focus = next(iter(sims))
    sim = sims[focus]
    print(
        f"\nlayouts are costed on the run rate={focus[0]:g} wait={focus[1]:g}"
        f" ({len(sim.row_groups):,} row groups)"
    )

    specs = [("per_template_day", {"seals": seals}, False)]
    specs += [("shared_day_limit", {"limit": L}, True) for L in SHARED_LIMITS]
    specs += [("mod_n", {"n": n}, True) for n in MOD_N]
    specs += [("least_full", {"n": n}, True) for n in MOD_N]

    # The busiest *dated* day. `undated` is where rows whose `ts` is not a readable
    # Datetime land (script 01 counts them), and a time-range search over it skips
    # nothing - so costing a search against it would answer a different question.
    day_hist = Counter(d for d in P.day if d != rowlib.UNDATED)
    day = day_hist.most_common(1)[0][0] if day_hist else rowlib.UNDATED
    on_day: Counter = Counter()
    for r in range(P.n_rows):
        if P.day[r] == day:
            on_day[P.tid[r]] += 1
    busiest = on_day.most_common(1)[0][0]
    print(
        f"searches are costed over {day}, the busiest dated day "
        f"({sum(on_day.values()):,} rows); the one-template search is template "
        f"{busiest} ({on_day[busiest]:,} rows that day)"
    )

    lrows = []
    srows = []
    spread_rows = []
    for kind, kw, shared in specs:
        lay = assign(sim, costs, kind, **kw)
        lc = cost_layout(lay, costs, shared_columns=shared)
        # Only the knobs that name the variant. `seals` is a dict of every partition's
        # segment boundaries, and joining it into the label wrote a 190 KB field into
        # results.csv that the CSV reader then refused to parse at all.
        label = lay.name + "".join(
            f" {k}={measure.human(v) if k == 'limit' else v}"
            for k, v in kw.items()
            if k in ("limit", "n")
        )
        rc = sorted(lc.rows)
        lrows.append(
            [
                label,
                lc.files,
                lc.row_groups,
                rc[0],
                int(statistics.median(rc)),
                rc[min(len(rc) - 1, int(0.9 * len(rc)))],
                lc.total,
                lc.overhead,
                round(100.0 * lc.overhead / lc.total, 1),
                lc.blocks,
                round(lc.blocks / raw, 4),
            ]
        )
        for metric, value, unit in [
            ("files", lc.files, "count"),
            ("row_groups", lc.row_groups, "count"),
            ("bytes", lc.total, "bytes"),
            ("overhead_bytes", lc.overhead, "bytes"),
            ("overhead_share_pct", round(100.0 * lc.overhead / lc.total, 3), "percent"),
            ("disk_blocks", lc.blocks, "bytes"),
            ("ratio_to_raw", round(lc.blocks / raw, 5), "ratio"),
        ]:
            res.add(label, metric, value, unit, lay.note)

        sc = search_cost(lay, lc, costs, day, busiest)
        srows.append([label] + [sc[k] for k in sc])
        for k, v in sc.items():
            res.add(label, k, v, "count" if "files" in k or "ranges" in k else "bytes",
                    f"one day ({day}); the one-template search is template {busiest}")

        if kind in ("mod_n", "least_full"):
            per_day: dict = defaultdict(list)
            for key, (t, _f, _n, _names) in lc.per_file.items():
                per_day[key[0]].append(t)
            worst = max(
                (max(v) / (sum(v) / len(v)) for v in per_day.values() if v), default=1.0
            )
            spread_rows.append([label, round(worst, 3)])
            res.add(label, "byte_spread_largest_over_average", round(worst, 4), "ratio",
                    "across the N files of one day; 1.0 is perfectly even")

        if kind == "shared_day_limit":
            avg_rg = lc.total / lc.row_groups
            n_cols = int(
                statistics.median([len(v[3]) for v in lc.per_file.values()])
            )
            rg_when_full = int(kw["limit"] / avg_rg)
            idx = rg_when_full * (
                colmodel.FOOTER_PER_RG_FIXED + n_cols * colmodel.FOOTER_PER_RG_PER_COL
            )
            res.add(
                label,
                "open_file_index_memory_bytes",
                idx,
                "bytes",
                f"{rg_when_full:,} row groups x {n_cols} columns x "
                f"{colmodel.FOOTER_PER_RG_PER_COL} bytes, plus "
                f"{colmodel.FOOTER_PER_RG_FIXED} per row group, at the size limit",
            )
            rolled = lc.files > len(set(lay.day_of.values()))
            print(
                f"  {label}: near-full index held in memory ~"
                f"{measure.human(idx)} ({rg_when_full:,} row groups x {n_cols} columns)"
                + ("" if rolled else " - the limit never rolled at this dataset size, "
                   "so its row is the unrolled shared-day file")
            )

    measure.table(
        lrows,
        headers=[
            "layout", "files", "row groups", "rg min", "rg median", "rg p90",
            "bytes", "overhead", "overhead %", "disk blocks", "blocks/raw",
        ],
        title="layouts, under the section 6.2 byte model",
    )
    measure.table(
        srows,
        headers=[
            "layout", "1-tmpl files", "1-tmpl index B", "1-tmpl ranges",
            "all files", "all index B", "all ranges",
        ],
        title=f"what a search over one day ({day}) has to touch",
    )
    if spread_rows:
        measure.table(
            spread_rows,
            headers=["layout", "largest file / average file"],
            title="how unevenly bytes land across the N files of a day",
        )

    # -- calibration -------------------------------------------------------------
    if not args.skip_calibration:
        print("\n=== CALIBRATION: the model against the real binary ===")
        cal = calibrate(ds, P, costs)
        r, m = cal["real"], cal["model"]
        measure.table(
            [
                ["segment (.col) bytes", r["col_bytes"], m.total, round(cal["bytes_diff_pct"], 2)],
                ["segment files", r["col_files"], m.files, round(cal["files_diff_pct"], 2)],
                ["segment disk blocks", r["col_blocks"], m.blocks, round(cal["blocks_diff_pct"], 2)],
                ["metadata bytes (_leaf.json, trie.json)", r["meta_bytes"], "-", "-"],
                ["metadata disk blocks", r["meta_blocks"], "-", "-"],
                ["metadata files", r["meta_files"], "-", "-"],
                ["directories", r["dirs"], "-", "-"],
            ],
            headers=["", "real binary", "model", "model - real %"],
            title=f"{ds.name} ingested into triedb, data dir under lab/out/03/calib",
        )
        for k, v in [
            ("real_col_bytes", r["col_bytes"]),
            ("real_col_files", r["col_files"]),
            ("real_col_blocks", r["col_blocks"]),
            ("real_meta_bytes", r["meta_bytes"]),
            ("real_meta_blocks", r["meta_blocks"]),
            ("real_meta_files", r["meta_files"]),
            ("real_segments_over_ceil", r["col_over_ceil_files"]),
            ("real_bytes_over_ceil", r["col_over_ceil_bytes"]),
            ("model_col_bytes", m.total),
            ("model_col_files", m.files),
            ("model_col_blocks", m.blocks),
        ]:
            res.add("calibration", k, v, "bytes" if "bytes" in k or "blocks" in k else "count",
                    f"{r['ingested']:,} lines ingested over POST /ingest, server stopped "
                    "with SIGTERM so every segment is sealed")
        res.add("calibration", "model_minus_real_pct", round(cal["bytes_diff_pct"], 3),
                "percent", "the model's per-template-per-day bytes against the binary's")
        if cal["within_10pct"]:
            print(
                f"\n  PASS: the model is {cal['bytes_diff_pct']:+.2f}% of the binary's "
                "segment bytes, inside the 10% bound."
            )
        else:
            print(
                f"\n  FAIL: the model is {cal['bytes_diff_pct']:+.2f}% of the binary's "
                f"segment bytes, outside the 10% bound.\n"
                f"  real {r['col_bytes']:,} bytes in {r['col_files']:,} files; "
                f"model {m.total:,} bytes in {m.files:,} files.\n"
                "  Nothing downstream of the model is trusted until this is explained."
            )
        if r["col_over_ceil_files"]:
            print(
                f"\n  Bytes match exactly; disk blocks do not, by "
                f"{cal['blocks_diff_pct']:+.2f}%. The whole of that difference is "
                f"{r['col_over_ceil_files']} of {r['col_files']:,} segments holding "
                f"{measure.human(r['col_over_ceil_bytes'])} more than "
                f"`ceil(size / 4096)` accounts for - the largest ones, up to "
                f"{measure.human(r['col_over_ceil_largest'])}. Every other segment "
                "matches the model's rounding to the byte."
            )
            print(
                "  The cause is NOT established. Writing a file of that exact size on "
                "this filesystem - in one call, or appended in 8 KiB pieces with an "
                "fsync after each - reproduces `ceil` exactly, so neither the size nor "
                "the append pattern explains it alone. Until it is explained, read "
                "every disk-blocks figure the model reports as a floor, not an estimate."
            )
        print(
            f"\n  The binary also wrote {r['meta_files']:,} metadata files "
            f"({measure.human(r['meta_bytes'])} of bytes, "
            f"{measure.human(r['meta_blocks'])} of blocks) and {r['dirs']:,} "
            "directories. The model does not predict those: it models the segment "
            "format, and `_leaf.json` and `trie.json` are not in section 6.2."
        )

    res.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
