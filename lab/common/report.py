"""Regenerate `lab/RESULTS.md` from `lab/results/results.csv`.

One section per script, newest run only. `results.csv` is append-only, so an older run's
rows stay on disk for a diff; they just stop being what RESULTS.md says. A script's rows
all carry the one timestamp `Results.flush` stamped, which is what makes "newest run"
well defined rather than a per-row guess.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

from tabulate import tabulate

from .measure import LAB, RESULTS_CSV, machine_info

OUT_MD = LAB / "RESULTS.md"

# The order the lab was built in, which is the order the findings depend on each other.
SCRIPT_ORDER = [
    "00_frontend_agree",
    "01_profile_logs",
    "02_split_vs_wide",
    "03_layout_model",
    "04_merge_rules",
    "05_encodings",
    "06_bloom",
    "07_vs_others",
    "08_io_shapes",
    "09_template_lookup",
]

TITLES = {
    "00_frontend_agree": "Frontend agreement - the Python port against the binary",
    "01_profile_logs": "What the logs look like",
    "02_split_vs_wide": "Splitting by template: does it help or hurt compression",
    "03_layout_model": "Files, row groups and overhead per layout",
    "04_merge_rules": "Which merging rule",
    "05_encodings": "What each encoding saves",
    "06_bloom": "Is a bloom filter per row group worth its bytes",
    "07_vs_others": "The same logs in other systems",
    "08_io_shapes": "Disk-dominated shapes",
    "09_template_lookup": "Searches on keys that are not promoted",
}


def load() -> list:
    if not RESULTS_CSV.exists():
        return []
    with RESULTS_CSV.open() as fh:
        return list(csv.DictReader(fh))


def newest_per_script(rows: list) -> dict:
    """Newest run per (script, dataset), not per script.

    A script run twice with `--dataset flat30` and `--dataset uneven` produces two runs
    that answer different questions; keying on the script alone would let the second
    erase the first.
    """
    latest: dict = {}
    for r in rows:
        k = (r["script"], r["dataset"])
        if k not in latest or r["when"] > latest[k]:
            latest[k] = r["when"]
    kept = defaultdict(list)
    for r in rows:
        if r["when"] == latest[(r["script"], r["dataset"])]:
            kept[r["script"]].append(r)
    return kept


def _fmt(v: str) -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    if f == int(f) and abs(f) < 1e15:
        return f"{int(f):,}"
    return f"{f:,.4g}"


def _pick(kept, script, dataset, variant=None, metric=None):
    """One value out of the newest run, or None. Findings are read from the CSV, never
    written by hand - a headline that can drift from the table under it is worse than no
    headline at all."""
    for r in kept.get(script, []):
        if r["dataset"] != dataset:
            continue
        if variant is not None and r["variant"] != variant:
            continue
        if metric is not None and r["metric"] != metric:
            continue
        return r
    return None


def findings(kept: dict) -> list:
    """The headline, derived from the rows below it."""
    out: list = []

    agree = [r for r in kept.get("00_frontend_agree", []) if r["metric"] == "disagreements"]
    if agree:
        bad = sum(int(float(r["value"])) for r in agree)
        out.append(
            f"- **The Python frontend agrees with the binary**: {bad} disagreements over "
            f"{len(agree)} datasets. Every template id and inferred type below is the one "
            "TrieDB would assign."
            if bad == 0
            else f"- **The Python frontend DISAGREES with the binary on {bad} points.** "
            "Nothing below is trusted."
        )

    for ds in ("flat30", "uneven"):
        r = _pick(kept, "03_layout_model", ds, "calibration", "model_minus_real_pct")
        if r:
            out.append(
                f"- **The byte model is calibrated on `{ds}`**: it predicts "
                f"{float(r['value']):+.2f}% of the segment bytes the real binary wrote "
                "(bound: 10%)."
            )

    for ds in ("uneven", "flat30", "oneday"):
        small = _pick(kept, "02_split_vs_wide", ds, "verdict", "smallest_by_bytes")
        vs = _pick(kept, "02_split_vs_wide", ds, "verdict", "per_template_vs_gzip9_bytes")
        ptd = _pick(kept, "02_split_vs_wide", ds, "PER_TEMPLATE_DAY rg=8,192",
                    "ratio_to_raw_bytes")
        if small and vs:
            ratio = float(vs["value"])
            verb = "beats" if ratio < 1 else "loses to"
            line = (
                f"- **`{ds}`**: the smallest layout is {small['note']} at "
                f"{int(float(small['value'])):,} bytes. Per-template Parquet {verb} "
                f"gzip -9 by {ratio:.2f}x on bytes"
            )
            if ptd:
                line += (
                    f"; what TrieDB does today (one Parquet per template per day) costs "
                    f"{float(ptd['value']):.2f}x of raw NDJSON"
                )
            out.append(line + ".")

    return out


def render() -> str:
    rows = load()
    kept = newest_per_script(rows)

    parts = [
        "# TrieDB experiment lab - results",
        "",
        "Measurements, not opinions. Every size figure comes from a real file under",
        "`lab/out/`, counted twice: `st_size` for the bytes a format spends and",
        "`st_blocks * 512` for what the filesystem charges. Nothing here ships.",
        "",
        "**Speed figures appear only where a C library or the disk did the work**",
        "(zstd, fsync, file reads), and each says so. No throughput number measured over",
        "Python code is a TrieDB number.",
        "",
        "## Machine",
        "",
    ]
    info = machine_info()
    parts.append(tabulate(sorted(info.items()), headers=["", ""], tablefmt="simple"))
    parts.append("")

    found = findings(kept)
    if found:
        parts.append("## Findings so far")
        parts.append("")
        parts.extend(found)
        parts.append("")

    if not kept:
        parts.append("_No results yet._")
        return "\n".join(parts) + "\n"

    ordered = [s for s in SCRIPT_ORDER if s in kept]
    ordered += sorted(s for s in kept if s not in SCRIPT_ORDER)

    parts.append("## Contents")
    parts.append("")
    for script in ordered:
        parts.append(f"- [{script} - {TITLES.get(script, '')}](#{script.replace('_', '-')})")
    parts.append("")

    for script in ordered:
        srows = kept[script]
        when = max(r["when"] for r in srows)
        sha = srows[0]["git_sha"]
        parts.append(f"## {script}")
        parts.append("")
        parts.append(f"_{TITLES.get(script, '')}_")
        parts.append("")
        parts.append(f"Run {when}, git `{sha}`, {len(srows)} rows.")
        parts.append("")

        notes = []
        for ds in sorted({r["dataset"] for r in srows}):
            dsr = [r for r in srows if r["dataset"] == ds]
            body = [
                [r["variant"], r["metric"], _fmt(r["value"]), r["unit"]] for r in dsr
            ]
            parts.append(f"### `{script}` / dataset `{ds}`")
            parts.append("")
            parts.append(
                tabulate(body, headers=["variant", "metric", "value", "unit"],
                         tablefmt="github")
            )
            parts.append("")
            for r in dsr:
                if r["note"] and r["note"] not in notes:
                    notes.append(r["note"])
        if notes:
            parts.append("**Notes**")
            parts.append("")
            for n in notes:
                parts.append(f"- {n}")
            parts.append("")

    return "\n".join(parts) + "\n"


def write() -> Path:
    OUT_MD.write_text(render())
    return OUT_MD


if __name__ == "__main__":
    p = write()
    print(f"wrote {p} ({p.stat().st_size:,} bytes)")
