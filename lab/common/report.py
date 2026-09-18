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
    latest: dict = {}
    for r in rows:
        script = r["script"]
        if script not in latest or r["when"] > latest[script]:
            latest[script] = r["when"]
    kept = defaultdict(list)
    for r in rows:
        if r["when"] == latest[r["script"]]:
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
        when = srows[0]["when"]
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
