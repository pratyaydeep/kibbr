#!/usr/bin/env python3
"""Run the whole lab, then regenerate RESULTS.md.

The order is not the numbering, because three scripts feed each other:

    04 writes the merge mappings that 02 turns into real Parquet files
    08 writes the WAL compression ratio that 03 quotes for a day of WAL
    05 writes the best-encoding total that 06 quotes its filter bytes against

Running them in numeric order would leave those three cross-references empty on a fresh
checkout and filled on the second run, which is a worse thing than an odd-looking order.

00 is a gate, not a step. If the Python frontend disagrees with the binary, every
template id and inferred type downstream is suspect, so the run stops there.

07 is not here: it needs Docker, and this has to run without it. Run `bash
lab/07_vs_others.sh` separately.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

LAB = Path(__file__).resolve().parent
PY = LAB / ".venv" / "bin" / "python"

# (script, [datasets]) - None means the script takes no --dataset that matters.
PLAN = [
    ("00_frontend_agree", ["uneven"]),
    ("01_profile_logs", ["uneven"]),
    ("04_merge_rules", ["uneven", "flat30", "messy"]),
    ("08_io_shapes", ["uneven"]),
    ("02_split_vs_wide", ["uneven", "flat30", "oneday", "messy"]),
    ("03_layout_model", ["uneven", "flat30", "messy"]),
    ("05_encodings", ["uneven", "flat30", "messy"]),
    ("06_bloom", ["uneven", "flat30", "messy"]),
    ("09_template_lookup", [None]),
]

# 00 and 01 walk every dataset that exists on their own, so one invocation covers them.
SELF_SWEEPING = {"00_frontend_agree", "01_profile_logs"}


def run(script: str, dataset, size: str, extra: list) -> tuple:
    cmd = [str(PY), str(LAB / f"{script}.py")]
    if dataset is not None:
        cmd += ["--dataset", dataset]
    cmd += ["--size", size] + extra
    label = f"{script}" + (f" --dataset {dataset}" if dataset else "")
    print(f"\n{'=' * 78}\n>>> {label}\n{'=' * 78}", flush=True)
    t0 = time.perf_counter()
    r = subprocess.run(cmd, cwd=LAB.parent)
    return label, r.returncode, time.perf_counter() - t0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_all")
    ap.add_argument("--size", choices=["small", "big"], default="small")
    ap.add_argument("--only", default=None,
                    help="comma-separated script prefixes, e.g. 02,05")
    ap.add_argument("--skip-calibration", action="store_true",
                    help="skip script 03's ingest into the real binary")
    args = ap.parse_args()

    if not PY.exists():
        print(f"{PY} not found. Create the venv and install lab/requirements.txt.")
        return 2

    only = {p.strip() for p in args.only.split(",")} if args.only else None
    results = []
    t_start = time.perf_counter()

    for script, dsets in PLAN:
        if only and not any(script.startswith(p) for p in only):
            continue
        targets = dsets[:1] if script in SELF_SWEEPING else dsets
        for ds in targets:
            extra = []
            if script == "03_layout_model" and args.skip_calibration:
                extra.append("--skip-calibration")
            label, code, took = run(script, ds, args.size, extra)
            results.append((label, code, took))
            if code != 0:
                if script == "00_frontend_agree":
                    print(
                        "\n00_frontend_agree failed. The Python port does not agree with "
                        "the binary, so nothing downstream would mean anything. Stopping."
                    )
                    _summary(results, time.perf_counter() - t_start)
                    return 1
                print(f"\n!!! {label} exited {code}; continuing with the rest.")

    print(f"\n{'=' * 78}\n>>> regenerating RESULTS.md\n{'=' * 78}", flush=True)
    r = subprocess.run([str(PY), "-m", "lab.common.report"], cwd=LAB.parent)
    results.append(("report", r.returncode, 0.0))

    _summary(results, time.perf_counter() - t_start)
    print(
        "\n07_vs_others was not run: it needs Docker. Run it separately with\n"
        "  bash lab/07_vs_others.sh --dataset uneven"
    )
    return 0 if all(c == 0 for _, c, _ in results) else 1


def _summary(results: list, total: float) -> None:
    print(f"\n{'=' * 78}\nsummary\n{'=' * 78}")
    for label, code, took in results:
        mark = "ok  " if code == 0 else f"FAIL({code})"
        print(f"  {mark}  {took:7.1f}s  {label}")
    print(f"\n  total {total / 60:.1f} min")


if __name__ == "__main__":
    sys.exit(main())
