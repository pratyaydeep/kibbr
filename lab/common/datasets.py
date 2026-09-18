"""The inputs, built under `lab/data/` and reproducible from a seed.

Four shapes, because the questions need different ones:

  flat30  what `triedb gen-logs` makes by default - 200,000 lines over 31 days.
  oneday  the same lines, timestamps squeezed into one day, so a per-day layout is
          measured without the day count doing the work.
  uneven  one day, and the top 10 templates holding about 80% of the rows, which is what
          real log traffic looks like and what a flat generator will not produce.
  big     the uneven shape at 2,000,000 lines, built only for `--size big`.
  real    whatever `LAB_REAL_LOGS` points at. Every script works without it.

`_lab_id`
---------
The generator has no such field, so it is inserted here, textually, as the first member:

    {"_lab_id":"uneven-0000000042", ...the original line...}

Inserted as text and removed as text, so `payload()` returns the generator's bytes back
exactly. Every measurement in this lab runs on the payload, never on the line with the id
in it - an extra key on every line would be an extra column in every template, would
shift every merge overlap ratio by one key in both numerator and denominator, and would
add its own bytes to every size figure. The id is bookkeeping; it is not data.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from .measure import DATA, LAB

BINARY = LAB.parent / "target" / "release" / "triedb"

# The generator's time fields, by leaf segment name. `order_id` is also ten digits and is
# deliberately NOT here: rung 2 mistaking it for a timestamp is a fact about the data we
# are measuring, not one to paper over.
TIME_KEYS = ("ts", "ts_unix", "ts_ms")
# Extra names a real NDJSON file is likely to use.
REAL_TIME_KEYS = TIME_KEYS + ("@timestamp", "timestamp", "time", "eventTime")

# The single day everything one-day is squeezed into: 2024-01-15, the date section 5.3's
# worked example uses.
DAY0 = 1705276800  # 2024-01-15T00:00:00Z
DAY_SECS = 86400


@dataclass
class Dataset:
    name: str
    path: Path
    lines: int
    note: str


def _run(args: list, stdout=None) -> None:
    subprocess.run(args, stdout=stdout, check=True)


def _require_binary() -> None:
    if not BINARY.exists():
        sys.exit(
            f"{BINARY} not found. Build it first:  cargo build --release\n"
            "(this lab reads the binary; it never writes outside lab/)"
        )


# --- _lab_id ------------------------------------------------------------------


def _with_lab_id(raw: bytes, name: str, index: int) -> bytes:
    """Insert `_lab_id` as the first member, textually, so removal is exact."""
    body = raw.strip()
    ident = f'"_lab_id":"{name}-{index:010d}"'.encode()
    if body == b"{}":
        return b"{" + ident + b"}"
    assert body.startswith(b"{"), body[:40]
    return b"{" + ident + b"," + body[1:]


def payload(line: bytes) -> tuple[str, bytes]:
    """Split a dataset line back into `(lab_id, original bytes)`.

    The inverse of `_with_lab_id`, by the same textual route - no re-serialization, so
    the bytes handed back are the generator's own.
    """
    body = line.rstrip(b"\n")
    assert body.startswith(b'{"_lab_id":"'), body[:40]
    end = body.index(b'"', 12)
    ident = body[12:end].decode()
    rest = body[end + 1 :]
    if rest.startswith(b","):
        return ident, b"{" + rest[1:]
    return ident, b"{}"  # the empty object case


def iter_payloads(path: Path):
    """`(lab_id, payload bytes)` for every line of a dataset."""
    with open(path, "rb") as fh:
        for line in fh:
            if line.strip():
                yield payload(line)


# --- timestamp squeezing ------------------------------------------------------


def _time_value(key: str, v):
    """Seconds since the epoch for one time field, or None if it does not read as one."""
    leaf = key.rsplit(".", 1)[-1]
    if leaf not in REAL_TIME_KEYS:
        return None
    if isinstance(v, str):
        from .frontend import _parse_rfc3339

        ns = _parse_rfc3339(v)
        return ns / 1e9 if ns is not None else None
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    s = str(int(v))
    if len(s) == 10:
        return float(v)
    if len(s) == 13:
        return v / 1000.0
    return None


def _walk_times(obj, out: list, path: str = "") -> None:
    for k, v in obj.items():
        full = f"{path}.{k}" if path else k
        if isinstance(v, dict):
            _walk_times(v, out, full)
        else:
            t = _time_value(full, v)
            if t is not None:
                out.append((obj, k, v, t))


def _rewrite(obj, mapper) -> None:
    """Rewrite every time field in place, keeping each one's textual form.

    The form has to survive: `ts_unix` must stay exactly ten ASCII digits and `ts_ms`
    exactly thirteen, or rung 2 of the type ladder stops seeing a timestamp and the
    dataset quietly changes what it is testing.
    """
    found: list = []
    _walk_times(obj, found)
    for parent, key, old, t in found:
        new_t = mapper(t)
        if isinstance(old, str):
            from datetime import datetime, timezone

            dt = datetime.fromtimestamp(int(new_t), tz=timezone.utc)
            parent[key] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif len(str(int(old))) == 13:
            parent[key] = int(new_t * 1000)
        else:
            parent[key] = int(new_t)


def _squeeze_to_one_day(objs: list) -> str:
    """Map every timestamp linearly onto DAY0..DAY0+86399. Returns a note."""
    times: list = []
    for o in objs:
        found: list = []
        _walk_times(o, found)
        times.extend(t for *_, t in found)
    if not times:
        return "no time fields found; timestamps untouched"
    lo, hi = min(times), max(times)
    span = hi - lo

    def mapper(t: float) -> float:
        frac = 0.0 if span == 0 else (t - lo) / span
        return DAY0 + frac * (DAY_SECS - 1)

    for o in objs:
        _rewrite(o, mapper)
    return (
        f"generator has no one-day flag; timestamps rewritten in Python - every ts/"
        f"ts_unix/ts_ms mapped linearly from its {span / 86400:.1f}-day span onto "
        f"2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits)"
    )


def _dump(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()


# --- builders -----------------------------------------------------------------


def _gen(lines: int, templates: int, seed: int, dest: Path) -> None:
    _require_binary()
    with open(dest, "wb") as fh:
        _run(
            [
                str(BINARY),
                "gen-logs",
                "--lines",
                str(lines),
                "--templates",
                str(templates),
                "--seed",
                str(seed),
            ],
            stdout=fh,
        )


def _template_ids(path: Path) -> list:
    """One template id per line, from the binary - the oracle, not the Python port."""
    _require_binary()
    out = subprocess.run(
        [str(BINARY), "parse", str(path), "--json"],
        capture_output=True,
        check=True,
    ).stdout
    return [json.loads(line)["template_id"] for line in out.splitlines() if line.strip()]


def _write(name: str, raws, note: str) -> Dataset:
    dest = DATA / f"{name}.ndjson"
    DATA.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(dest, "wb") as fh:
        for i, raw in enumerate(raws):
            fh.write(_with_lab_id(raw, name, i) + b"\n")
            n += 1
    return Dataset(name, dest, n, note)


def _build_flat30(size: str) -> Dataset:
    lines = 200_000
    tmp = DATA / "_flat30.raw"
    _gen(lines, 12, 42, tmp)
    raws = [ln.rstrip(b"\n") for ln in open(tmp, "rb") if ln.strip()]
    tmp.unlink()
    return _write(
        "flat30",
        raws,
        f"triedb gen-logs --lines {lines} --templates 12 --seed 42, unmodified; "
        "31 days, flat across templates",
    )


def _build_oneday(size: str) -> Dataset:
    flat = ensure("flat30", size)
    objs = [json.loads(p) for _, p in iter_payloads(flat.path)]
    note = _squeeze_to_one_day(objs)
    return _write(
        "oneday",
        [_dump(o) for o in objs],
        "flat30's lines, " + note + "; line order unchanged, so oneday differs from "
        "flat30 in timestamps and nothing else",
    )


def _uneven_weights(n_templates: int, total: int) -> list:
    """Row counts per template: the top 10 hold ~80%, the rest share the remaining 20%.

    Zipf inside each half - a flat 8% each for the top ten would be a different shape
    from the one real traffic has, where the single busiest template dwarfs the tenth.
    """
    top = min(10, n_templates)
    head = [1.0 / (i + 1) for i in range(top)]
    head = [0.80 * total * w / sum(head) for w in head]
    tail_n = n_templates - top
    # With ten or fewer templates there is no tail; the head already holds every one
    # of them, and the rounding below puts the remaining 20% back on the busiest.
    tail = []
    if tail_n:
        tail = [1.0 / (i + 1) ** 0.5 for i in range(tail_n)]
        tail = [0.20 * total * w / sum(tail) for w in tail]
    counts = [max(1, int(round(w))) for w in head + tail]
    # Settle the rounding on the busiest template, so the total is exact.
    counts[0] += total - sum(counts)
    return counts


def _build_uneven(size: str, name: str = "uneven", total: int | None = None) -> Dataset:
    import random

    total = total or (2_000_000 if size == "big" else 200_000)
    # A pool wide enough that the tail templates still have distinct lines to give.
    pool_lines = max(3 * total // 2, 300_000)
    tmp = DATA / f"_{name}.pool"
    _gen(pool_lines, 60, 7, tmp)
    ids = _template_ids(tmp)
    lines = [ln.rstrip(b"\n") for ln in open(tmp, "rb") if ln.strip()]
    tmp.unlink()

    by_template: dict = {}
    for tid, raw in zip(ids, lines):
        by_template.setdefault(tid, []).append(raw)
    # Busiest first; ties by template id, so the ordering does not depend on dict order.
    order = sorted(by_template, key=lambda t: (-len(by_template[t]), t))
    counts = _uneven_weights(len(order), total)

    rng = random.Random(1315)
    picked: list = []
    reused = 0
    for tid, want in zip(order, counts):
        available = by_template[tid]
        if want <= len(available):
            picked.extend(rng.sample(available, want))
        else:
            picked.extend(available)
            extra = rng.choices(available, k=want - len(available))
            picked.extend(extra)
            reused += len(extra)

    objs = [json.loads(p) for p in picked]
    squeeze_note = _squeeze_to_one_day(objs)
    # Time order, because that is the order an ingest actually sees and script 03
    # simulates. Sorting is done on the squeezed times, so it is stable.
    keyed = []
    for o in objs:
        found: list = []
        _walk_times(o, found)
        keyed.append((min((t for *_, t in found), default=DAY0), o))
    keyed.sort(key=lambda kv: kv[0])

    dup_pct = 100.0 * reused / total
    return _write(
        name,
        [_dump(o) for _, o in keyed],
        f"generator cannot skew template shares, so built by sampling: pool of "
        f"{pool_lines:,} lines at --templates 60 --seed 7, grouped by the binary's "
        f"template_id, {len(order)} templates drawn to a Zipf profile where the top 10 "
        f"hold 80% of {total:,} rows (seed 1315); {squeeze_note}; sorted by timestamp; "
        f"{dup_pct:.2f}% of rows are repeats of a pool line, drawn where a tail template "
        f"had fewer distinct lines than its quota"
    )


def _build_big(size: str) -> Dataset:
    return _build_uneven("big", name="big", total=2_000_000)


def _build_real(size: str) -> Dataset:
    src = os.environ.get("LAB_REAL_LOGS")
    if not src or not Path(src).exists():
        raise FileNotFoundError("LAB_REAL_LOGS is unset or does not point at a file")
    raws = []
    skipped = 0
    with open(src, "rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                skipped += 1
                continue
            if not isinstance(obj, dict):
                skipped += 1
                continue
            raws.append(line)
    return _write(
        "real",
        raws,
        f"LAB_REAL_LOGS={src}, taken verbatim; {skipped} lines skipped as non-object "
        "or invalid JSON",
    )


BUILDERS = {
    "flat30": _build_flat30,
    "oneday": _build_oneday,
    "uneven": _build_uneven,
    "big": _build_big,
    "real": _build_real,
}

_NOTES = DATA / "notes.json"


def _load_notes() -> dict:
    if _NOTES.exists():
        return json.loads(_NOTES.read_text())
    return {}


def ensure(name: str, size: str = "small", rebuild: bool = False) -> Dataset:
    """Build `name` if it is not already under `lab/data/`, and return it."""
    if name == "big" or (name == "uneven" and size == "big"):
        name = "big"
    dest = DATA / f"{name}.ndjson"
    notes = _load_notes()
    if dest.exists() and not rebuild and name in notes:
        n = sum(1 for _ in open(dest, "rb"))
        return Dataset(name, dest, n, notes[name])
    ds = BUILDERS[name](size)
    notes[name] = ds.note
    DATA.mkdir(parents=True, exist_ok=True)
    _NOTES.write_text(json.dumps(notes, indent=2))
    return ds


def available(size: str = "small") -> list:
    """Every dataset that can be built here - `real` only when the env var points at one."""
    names = ["flat30", "oneday", "uneven"]
    if size == "big":
        names.append("big")
    src = os.environ.get("LAB_REAL_LOGS")
    if src and Path(src).exists():
        names.append("real")
    return names


def resolve(name: str, size: str) -> Dataset:
    return ensure(name, size)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="*", default=None)
    ap.add_argument("--size", default="small")
    ap.add_argument("--rebuild", action="store_true")
    a = ap.parse_args()
    for nm in a.names or available(a.size):
        d = ensure(nm, a.size, a.rebuild)
        print(f"{d.name:8} {d.lines:>9,} lines  {d.path}")
        print(f"         note: {d.note}")
