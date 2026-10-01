"""The inputs, built under `lab/data/` and reproducible from a seed.

Five shapes, because the questions need different ones:

  flat30  what `kibbr gen-logs` makes by default - 200,000 lines over 31 days.
  farm    `kibbr gen-logs --profile farm`: one simulated day of a ten-service estate,
          in time order, uneven across templates, with request ids, users and addresses
          that recur across services. 200,000 lines, or 2,000,000 as `farm_big`.
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

# `LAB_KIBBR` points the lab at a binary built elsewhere - a worktree of this branch has
# no build of its own, and the farm profile exists only in newer builds.
BINARY = Path(os.environ.get("LAB_KIBBR") or LAB.parent / "target" / "release" / "kibbr")

# The generator's time fields, by leaf segment name. `order_id` is also ten digits and is
# deliberately NOT here: rung 2 mistaking it for a timestamp is a fact about the data we
# are measuring, not one to paper over.
TIME_KEYS = ("ts", "ts_unix", "ts_ms")
# Extra names a real NDJSON file is likely to use.
REAL_TIME_KEYS = TIME_KEYS + ("@timestamp", "timestamp", "time", "eventTime")

# The single day everything one-day is squeezed into: 2024-01-15, the date on every line
# of testdata/sample_logs.ndjson, the fixture tests/query.rs and tests/trieql.rs load.
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
        f"kibbr gen-logs --lines {lines} --templates 12 --seed 42, unmodified; "
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


def _build_farm(size: str, name: str = "farm") -> Dataset:
    lines = 2_000_000 if size == "big" else 200_000
    _require_binary()
    DATA.mkdir(parents=True, exist_ok=True)
    tmp = DATA / f"_{name}.raw"
    with open(tmp, "wb") as fh:
        _run(
            [str(BINARY), "gen-logs", "--profile", "farm", "--lines", str(lines), "--seed", "42"],
            stdout=fh,
        )
    # Streamed: at 2M lines the file is about a gigabyte. Not squeezed, not sorted - the
    # generator already writes one day in time order, and `_rewrite` would drop the
    # milliseconds every farm timestamp carries.
    with open(tmp, "rb") as fh:
        ds = _write(
            name,
            (ln.rstrip(b"\n") for ln in fh if ln.strip()),
            f"kibbr gen-logs --profile farm --lines {lines:,} --seed 42, unmodified; one "
            "simulated day in time order, ten services, each with its own time field "
            "(@timestamp, time, ts, ts_ms), a request's id recurring across services, "
            "0.5% dirty values on five named keys",
        )
    tmp.unlink()
    return ds


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




# --- messy: type inconsistency, which the shipped generator never produces ------------
#
# NOT real logs. Hand-authored here, and every figure derived from it is a statement
# about these rates and nothing else. It exists because `gen-logs` is too well behaved to
# answer one question the lab needs: a column's type is the join of every type its key
# has shown and a column never narrows, so one bad value pins a column to String for the
# life of the template. Across flat30, oneday, uneven, sample.ndjson and
# testdata/query-timing/1m.ndjson, zero keys carry two types - so `columns widened to
# String` came back 0 for every merge rule, which is a fact about the generator rather
# than a verdict on merging.
#
# The faults below are the ones production logs actually have, at rates in the range
# real streams show. Each is listed so a reader can scale the result or disagree with it.
MESSY_FAULTS = {
    # key                  rate    what arrives instead
    "duration_ms_na":     (0.012, '"N/A" where a timeout left no duration'),
    "duration_ms_null":   (0.004, "JSON null, same field"),
    "user_id_legacy":     (0.020, '"u-4821" from a pre-migration writer'),
    "order_id_alpha":     (0.010, '"AX-99213" - and the clean value is ten digits, so '
                                  "rung 2 calls the rest of them Datetime"),
    "http_status_empty":  (0.005, 'empty string where the upstream never answered'),
    "retry_stringy":      (0.015, '"1" / "0" instead of true / false'),
    "bytes_sent_float":   (0.030, "a float in an otherwise integer column - the one "
                                  "fault that widens to Float64 rather than String"),
    "error_code_symbol":  (0.025, '"E_TIMEOUT" in an integer code column'),
    "tags_scalar":        (0.010, "a bare string where the field is otherwise an array"),
    "ts_epoch":           (0.050, "ten-digit epoch seconds where the field is otherwise "
                                  "RFC 3339 - still Datetime, but a different dt_form, "
                                  "the per-column byte that exists to record which "
                                  "form a date took"),
    "trace_id_empty":     (0.008, "empty string in a UUID column"),
    "client_ip_unknown":  (0.006, '"unknown" in an IP column'),
}

_MESSY_SERVICES = {
    "auth": ("login_ok", "login_failed", "token_refresh"),
    "payment": ("charged", "declined", "refunded"),
    "search": ("query", "no_results"),
    "cart": ("item_added", "item_removed", "checkout"),
    "shipping": ("label_created", "dispatched"),
    "notify": ("email_sent", "push_sent"),
    "inventory": ("reserved", "restocked"),
    "gateway": ("rate_limited", "upstream_error"),
}
_MESSY_LEVELS = ("info", "info", "info", "warn", "error", "debug")
_MESSY_REGIONS = ("us-east-1", "eu-west-1", "ap-south-1")


def _build_messy(size: str) -> Dataset:
    import random
    from datetime import datetime, timezone

    total = 2_000_000 if size == "big" else 200_000
    rng = random.Random(90210)
    rate = {k: v[0] for k, v in MESSY_FAULTS.items()}
    hit = {k: 0 for k in MESSY_FAULTS}

    def maybe(name: str) -> bool:
        if rng.random() < rate[name]:
            hit[name] += 1
            return True
        return False

    raws = []
    for i in range(total):
        service = rng.choice(list(_MESSY_SERVICES))
        event = rng.choice(_MESSY_SERVICES[service])
        secs = DAY0 + int(rng.random() * (DAY_SECS - 1))

        o = {}
        # `ts` is the partition column, so its form is the one that matters most.
        if maybe("ts_epoch"):
            o["ts"] = secs
        else:
            o["ts"] = datetime.fromtimestamp(secs, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        o["level"] = rng.choice(_MESSY_LEVELS)
        o["service"] = service
        o["event"] = event
        o["region"] = rng.choice(_MESSY_REGIONS)

        if maybe("user_id_legacy"):
            o["user_id"] = f"u-{rng.randint(1000, 99999)}"
        else:
            o["user_id"] = rng.randint(1000, 999999)

        if maybe("trace_id_empty"):
            o["trace_id"] = ""
        else:
            o["trace_id"] = str(
                __import__("uuid").UUID(int=rng.getrandbits(128), version=4)
            )

        if maybe("client_ip_unknown"):
            o["client"] = {"ip": "unknown"}
        else:
            o["client"] = {
                "ip": f"{rng.randint(1,223)}.{rng.randint(0,255)}."
                      f"{rng.randint(0,255)}.{rng.randint(1,254)}"
            }

        if maybe("duration_ms_na"):
            o["duration_ms"] = "N/A"
        elif maybe("duration_ms_null"):
            o["duration_ms"] = None
        else:
            o["duration_ms"] = round(rng.random() * 5000, 2)

        if maybe("bytes_sent_float"):
            o["bytes_sent"] = round(rng.random() * 100000, 1)
        else:
            o["bytes_sent"] = rng.randint(0, 100000)

        if maybe("retry_stringy"):
            o["retry"] = rng.choice(("1", "0"))
        else:
            o["retry"] = rng.random() < 0.15

        if maybe("tags_scalar"):
            o["tags"] = rng.choice(("auth", "billing", "internal"))
        else:
            o["tags"] = rng.sample(["auth", "billing", "internal", "beta", "slow"],
                                   rng.randint(1, 3))

        # Optional fields, so merging has key sets that differ by one or two keys.
        if service == "payment" or rng.random() < 0.3:
            if maybe("order_id_alpha"):
                o["order_id"] = f"AX-{rng.randint(10000, 99999)}"
            else:
                o["order_id"] = rng.randint(1_000_000_000, 9_999_999_999)
        if rng.random() < 0.45:
            status_empty = maybe("http_status_empty")
            o["http"] = {
                "method": rng.choice(("GET", "POST", "PUT", "DELETE")),
                "status": "" if status_empty else rng.choice((200, 201, 400, 404, 500)),
            }
        if event in ("login_failed", "declined", "upstream_error") or rng.random() < 0.1:
            if maybe("error_code_symbol"):
                o["error"] = {"code": rng.choice(("E_TIMEOUT", "E_REFUSED", "E_AUTH"))}
            else:
                o["error"] = {"code": rng.randint(400, 599)}

        raws.append(_dump(o))

    faults = "; ".join(
        f"{k} {100.0 * hit[k] / total:.2f}% ({hit[k]:,} lines)" for k in MESSY_FAULTS
    )
    return _write(
        "messy",
        raws,
        "HAND-AUTHORED BY THE LAB, NOT REAL LOGS. 200,000 lines over one day, 8 services "
        "and 20 event types, written to carry the type inconsistency that `gen-logs` "
        "never produces - across flat30, oneday, uneven and testdata, zero keys carry "
        "two types, so every `columns widened to String` figure was vacuous. Injected "
        f"fault rates, as realised: {faults}. Every number derived from this dataset is a "
        "statement about these rates and about nothing else."
    )


BUILDERS = {
    "flat30": _build_flat30,
    "oneday": _build_oneday,
    "uneven": _build_uneven,
    "big": _build_big,
    "messy": _build_messy,
    "farm": _build_farm,
    # Its own name: `ensure` keys the cache on the name, so a big farm under `farm` would
    # hand back the small file.
    "farm_big": lambda size: _build_farm("big", "farm_big"),
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
    if name == "farm" and size == "big":
        name = "farm_big"
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
    names = ["flat30", "oneday", "uneven", "messy", "farm"]
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
