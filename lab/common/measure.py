"""Disk accounting, machine facts and the results.csv sink.

Every size figure in this lab comes from a real file under `lab/out/`. Two numbers are
always reported: `st_size`, the bytes a format actually spends, and `st_blocks * 512`,
what the filesystem charges for them. The gap between the two IS the finding whenever a
layout makes many small files, so neither number is allowed to stand alone.
"""

from __future__ import annotations

import csv
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tabulate import tabulate

LAB = Path(__file__).resolve().parent.parent
RESULTS_CSV = LAB / "results" / "results.csv"
OUT = LAB / "out"
DATA = LAB / "data"

RESULT_FIELDS = [
    "when",
    "git_sha",
    "script",
    "dataset",
    "variant",
    "metric",
    "value",
    "unit",
    "note",
]


# ---------------------------------------------------------------- disk accounting


@dataclass
class DiskUse:
    """What a set of files costs, counted both ways."""

    bytes: int
    blocks: int  # st_blocks * 512, the charge the filesystem actually makes
    files: int

    @property
    def waste(self) -> int:
        return self.blocks - self.bytes

    @property
    def waste_pct(self) -> float:
        return 100.0 * self.waste / self.blocks if self.blocks else 0.0


def disk_use(path: Path | str) -> DiskUse:
    """Bytes, disk blocks and file count for one file or a whole folder."""
    path = Path(path)
    total_bytes = 0
    total_blocks = 0
    files = 0
    if path.is_file():
        st = os.stat(path)
        return DiskUse(st.st_size, st.st_blocks * 512, 1)
    for root, _dirs, names in os.walk(path):
        for name in names:
            st = os.stat(Path(root) / name)
            total_bytes += st.st_size
            total_blocks += st.st_blocks * 512
            files += 1
    return DiskUse(total_bytes, total_blocks, files)


def fresh_dir(*parts: str) -> Path:
    """An empty directory under `lab/out/`. Emptied first, so a rerun measures one run."""
    import shutil

    d = OUT.joinpath(*parts)
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    return d


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.1f}{unit}" if unit != "B" else f"{int(n):,}B"
        n /= 1024
    return f"{n:.1f}GB"


# ---------------------------------------------------------------- machine facts


def git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=LAB.parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _sysctl(key: str) -> str:
    try:
        return subprocess.run(
            ["sysctl", "-n", key], capture_output=True, text=True, check=True
        ).stdout.strip()
    except Exception:
        return "?"


def machine_info() -> dict[str, str]:
    """CPU, cores, filesystem and disk type, as far as the platform will say."""
    info = {
        "platform": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "python": platform.python_version(),
    }
    if platform.system() == "Darwin":
        info["cpu"] = _sysctl("machdep.cpu.brand_string")
        info["cores"] = _sysctl("hw.ncpu")
        info["memory"] = human(int(_sysctl("hw.memsize") or 0))
    else:
        info["cpu"] = platform.processor() or "?"
        info["cores"] = str(os.cpu_count())
        info["memory"] = "?"

    # Filesystem holding lab/out, and whether it is rotational.
    try:
        df = subprocess.run(
            ["df", "-T" if platform.system() != "Darwin" else "-Y", str(OUT)],
            capture_output=True,
            text=True,
        ).stdout.strip().splitlines()
        info["filesystem"] = df[-1].split()[1] if len(df) > 1 else "?"
    except Exception:
        info["filesystem"] = "?"
    info["disk"] = "SSD (internal, assumed)" if platform.system() == "Darwin" else "?"
    info["alloc_block"] = f"{measured_alloc_block()} bytes (measured)"
    return info


def measured_alloc_block() -> int:
    """What one byte on disk actually costs, measured rather than asked for.

    `statvfs.f_bsize` is the preferred I/O size, which on APFS is 1 MiB and is not the
    allocation unit - reporting it would put the tiny-file waste out by 256x. Writing a
    one-byte file and reading its `st_blocks` back gives the number that matters.
    """
    import tempfile

    OUT.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=OUT) as fh:
        fh.write(b"x")
        fh.flush()
        os.fsync(fh.fileno())
        return os.stat(fh.name).st_blocks * 512


# ---------------------------------------------------------------- results sink


def append_results(rows: list[dict]) -> None:
    """Append rows to results.csv, writing the header if the file is new."""
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    new = not RESULTS_CSV.exists()
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    sha = git_sha()
    with RESULTS_CSV.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RESULT_FIELDS)
        if new:
            w.writeheader()
        for row in rows:
            out = {k: "" for k in RESULT_FIELDS}
            out.update(row)
            out["when"] = when
            out["git_sha"] = sha
            w.writerow(out)


class Results:
    """Collects result rows for one script run, then writes them all at once."""

    def __init__(self, script: str, dataset: str):
        self.script = script
        self.dataset = dataset
        self.rows: list[dict] = []

    def add(
        self,
        variant: str,
        metric: str,
        value,
        unit: str,
        note: str = "",
        dataset: str | None = None,
    ) -> None:
        self.rows.append(
            {
                "script": self.script,
                "dataset": dataset or self.dataset,
                "variant": variant,
                "metric": metric,
                "value": value,
                "unit": unit,
                "note": note,
            }
        )

    def flush(self) -> None:
        if self.rows:
            append_results(self.rows)
        print(f"\n[{self.script}] wrote {len(self.rows)} rows to lab/results/results.csv")


def table(rows, headers, title: str = "", floatfmt=".3f") -> None:
    if title:
        print(f"\n=== {title} ===")
    print(tabulate(rows, headers=headers, tablefmt="simple", floatfmt=floatfmt))


def print_machine() -> None:
    info = machine_info()
    print("\n=== machine ===")
    for k, v in info.items():
        print(f"  {k:12} {v}")


def parse_args(script: str):
    """The two flags every script takes."""
    import argparse

    p = argparse.ArgumentParser(prog=script)
    p.add_argument("--dataset", default="uneven")
    p.add_argument("--size", choices=["small", "big"], default="small")
    return p


def eprint(*a):
    print(*a, file=sys.stderr)
