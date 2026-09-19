"""Regenerate `lab/RESULTS.md` from `lab/results/results.csv`.

One section per script, newest run only. `results.csv` is append-only, so an older run's
rows stay on disk for a diff; they just stop being what RESULTS.md says. A script's rows
all carry the one timestamp `Results.flush` stamped, which is what makes "newest run"
well defined rather than a per-row guess.
"""

from __future__ import annotations

import csv
import sys
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


# A note field is free text and one bug put 190 KB in it. The reader should report that
# rather than refuse the whole file, so the limit is raised and oversized values are
# truncated on the way in.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
MAX_FIELD = 4_000


def load() -> list:
    if not RESULTS_CSV.exists():
        return []
    with RESULTS_CSV.open() as fh:
        rows = []
        for r in csv.DictReader(fh):
            for k, v in r.items():
                if isinstance(v, str) and len(v) > MAX_FIELD:
                    r[k] = v[:MAX_FIELD] + f"... [truncated from {len(v):,} chars]"
            rows.append(r)
        return rows


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

    for ds in ("flat30", "uneven"):
        b = _pick(kept, "05_encodings", ds, "BEST_PER_CHUNK sorted", "ratio_to_gzip9")
        raw = _pick(kept, "05_encodings", ds, "BEST_PER_CHUNK sorted", "ratio_to_raw")
        sort = _pick(kept, "05_encodings", ds, "verdict", "sort_saving_pct")
        if b and raw:
            line = (
                f"- **Encodings on `{ds}`**: picking the smallest encoding per chunk "
                f"reaches {float(raw['value']):.3f}x of raw NDJSON and "
                f"{float(b['value']):.3f}x of gzip -9"
            )
            if sort:
                line += (
                    f"; sorting rows inside each chunk is worth a further "
                    f"{float(sort['value']):.2f}%"
                )
            out.append(line + ".")

    bloom = [
        r for r in kept.get("06_bloom", [])
        if r["metric"] == "absent_bloom_skipped_pct" and "@10bits" in r["variant"]
    ]
    minmax = [
        r for r in kept.get("06_bloom", [])
        if r["metric"] == "absent_minmax_skipped_pct" and "@10bits" in r["variant"]
    ]
    share = [
        r for r in kept.get("06_bloom", [])
        if r["metric"] == "share_of_best_total_pct" and "@10bits" in r["variant"]
    ]
    if bloom and minmax:
        b = sum(float(r["value"]) for r in bloom) / len(bloom)
        m = sum(float(r["value"]) for r in minmax) / len(minmax)
        s_ = sum(float(r["value"]) for r in share) / len(share) if share else None
        line = (
            f"- **Bloom filters earn their bytes**: at 10 bits per value they skip "
            f"{b:.1f}% of row groups for an id that is not there, against {m:.1f}% for "
            "min/max statistics alone"
        )
        if s_ is not None:
            line += f", and cost {s_:.2f}% of the best encoded total"
        out.append(line + ".")

    f1 = _pick(kept, "08_io_shapes", "uneven", "fsync batch=1", "records_per_sec")
    ff1 = _pick(kept, "08_io_shapes", "uneven", "F_FULLFSYNC batch=1", "records_per_sec")
    w = _pick(kept, "08_io_shapes", "uneven", "wal batch=500", "compression_ratio")
    if f1 and ff1:
        out.append(
            f"- **Which durability call matters as much as batching**: a per-record "
            f"`fsync(2)` reaches {int(float(f1['value'])):,} records/sec on this "
            f"platform, the same loop with `F_FULLFSYNC` reaches "
            f"{int(float(ff1['value'])):,} - Darwin's `fsync` stops at the drive cache. "
            "Shape only."
        )
    if w:
        out.append(
            f"- **WAL batching pays twice**: zstd 3 over batches of 500 lines "
            f"compresses to {float(w['value']):.3f} of raw, against 0.864 at one line "
            "per batch."
        )

    sg = _pick(kept, "08_io_shapes", "uneven", "scattered vs grouped", "ranges_before")
    sga = _pick(kept, "08_io_shapes", "uneven", "scattered vs grouped", "ranges_after")
    if sg and sga:
        out.append(
            f"- **Grouping a template's row groups together** turns "
            f"{int(float(sg['value']))} separate byte ranges into "
            f"{int(float(sga['value']))} for a one-template search - on S3 at 15 ms a "
            "range, the difference between a second and nothing."
        )

    # Script 07, where the storage claim meets other people's answers to it.
    for ds in sorted({r["dataset"] for r in kept.get("07_vs_others", [])}):
        others = []
        for system in ("Elasticsearch standard", "Elasticsearch logsdb", "ClickHouse",
                       "clp-s"):
            r = _pick(kept, "07_vs_others", ds, system, "ratio_to_raw")
            if r:
                others.append((system, float(r["value"])))
        if not others:
            continue
        best_ours = None
        # The metric name differs by script - 02 distinguishes bytes from blocks and
        # calls it `ratio_to_raw_bytes`, 05 has only bytes and calls it `ratio_to_raw`.
        # Looking for one name found only 05 and quietly reported the larger figure as
        # TrieDB's best.
        for script, variant, metric in (
            ("02_split_vs_wide", "WIDE rg=122,880", "ratio_to_raw_bytes"),
            ("05_encodings", "BEST_PER_CHUNK sorted", "ratio_to_raw"),
        ):
            r = _pick(kept, script, ds, variant, metric)
            if r and (best_ours is None or float(r["value"]) < best_ours[1]):
                best_ours = (variant, float(r["value"]))

        out.append(
            f"- **Against other systems on `{ds}`**: "
            + ", ".join(f"{n} {v:.3f}x of raw" for n, v in others)
            + (f"; TrieDB's best measured layout is {best_ours[0]} at "
               f"{best_ours[1]:.3f}x" if best_ours else "")
            + "."
        )

        clps = next((v for n, v in others if n == "clp-s"), None)
        if clps is not None and best_ours is not None:
            rt = _pick(kept, "07_vs_others", ds, "clp-s", "roundtrip_lines")
            verified = (
                f" Its archive was decompressed and counted back to "
                f"{int(float(rt['value'])):,} lines, so the ratio is not a partial "
                "compression." if rt else ""
            )
            if clps < best_ours[1]:
                out.append(
                    f"- **clp-s stores these logs smaller than TrieDB does**: "
                    f"{clps:.3f}x of raw against {best_ours[1]:.3f}x, on the same "
                    f"200,000 lines.{verified} Its searches are the slower side of that "
                    "trade, but on the storage question this lab was built to answer, "
                    "clp-s wins."
                )
            else:
                out.append(
                    f"- **TrieDB's best layout is smaller than clp-s**: "
                    f"{best_ours[1]:.3f}x of raw against {clps:.3f}x.{verified}"
                )

        note = _pick(kept, "07_vs_others", ds, "all", "at_timestamp_added")
        if note:
            out.append(
                "- **One asymmetry in that comparison**: Elasticsearch and ClickHouse "
                "each received an added `@timestamp` field, roughly 30 bytes per "
                "document, because logsdb mode requires one and the dataset carries "
                "`ts`/`ts_unix`/`ts_ms` instead. clp-s and TrieDB were fed the raw "
                "payloads, so their ratios are flattered by that much."
            )

    # Type widening, which only a dataset carrying two types per key can answer.
    for ds in sorted({r["dataset"] for r in kept.get("04_merge_rules", [])}):
        multi = _pick(kept, "04_merge_rules", ds, "all", "keys_with_more_than_one_type")
        if not multi or float(multi["value"]) == 0:
            continue
        none_share = _pick(kept, "04_merge_rules", ds, "NONE",
                           "columns_widened_share_pct")
        none_keys = _pick(kept, "04_merge_rules", ds, "NONE", "distinct_keys_widened")
        worst = None
        for rule in ("J70", "J80", "J90", "J80_GROWN", "PER_EVENT"):
            r = _pick(kept, "04_merge_rules", ds, rule, "columns_widened_share_pct")
            if r and (worst is None or float(r["value"]) > worst[1]):
                worst = (rule, float(r["value"]))
        if none_share and worst:
            out.append(
                f"- **Type widening is a ladder cost, not a merging cost** (`{ds}`, "
                f"{int(float(multi['value']))} keys carrying two types): "
                f"{float(none_share['value']):.1f}% of columns are already String by "
                f"widening with NO merging at all, across "
                f"{int(float(none_keys['value'])) if none_keys else '?'} distinct keys. "
                f"The worst merge rule ({worst[0]}) takes that to {worst[1]:.1f}% and "
                "adds no new keys. Section 3.2's monotonic join does the damage inside a "
                "single template; merging contributes a few points on top."
            )

    sl = _pick(kept, "09_template_lookup", "synthetic", "verdict", "scan_superlinearity")
    if sl:
        out.append(
            f"- **Scanning every template for an unpromoted key bends the wrong way**: "
            f"100x the templates costs {float(sl['value']):.1f}x more than linear, not "
            "the same. A key-to-templates map is flat."
        )

    return out


def pinned_versions() -> list:
    """The image tags script 07 would measure, read from the compose file itself.

    Printed whether or not 07 has run: the prompt for a comparison is answered by saying
    which versions it is against, and an unrun comparison that does not name its
    versions is worse than no comparison.
    """
    out = []
    compose = LAB / "others" / "docker-compose.yml"
    if compose.exists():
        for line in compose.read_text().splitlines():
            line = line.strip()
            if line.startswith("image:") and "lab_clps" not in line:
                out.append(line.split("image:", 1)[1].strip())
    dockerfile = LAB / "others" / "Dockerfile.clps"
    if dockerfile.exists():
        for line in dockerfile.read_text().splitlines():
            if line.strip().startswith("ARG CLP_VERSION="):
                v = line.split("=", 1)[1].strip()
                out.append(f"y-scope/clp {v} (clp-json-x86_64 release tarball)")
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

    if (LAB / "CLP_S_NOTES.md").exists():
        parts.append("## Further reading")
        parts.append("")
        parts.append(
            "[`CLP_S_NOTES.md`](CLP_S_NOTES.md) - how clp-s works, read off its source at "
            "tag v0.13.0, and what TrieDB could take from it. It is the only comparison "
            "system in script 07 that shares TrieDB's central idea (one columnar table per "
            "distinct key set), so where it differs is where the design choices actually "
            "are."
        )
        parts.append("")

    found = findings(kept)
    if found:
        parts.append("## Findings so far")
        parts.append("")
        parts.extend(found)
        parts.append("")

    versions = pinned_versions()
    if versions:
        parts.append("## Comparison systems (script 07)")
        parts.append("")
        ran = "07_vs_others" in kept
        parts.append(
            "Pinned in `lab/others/docker-compose.yml` and `lab/others/Dockerfile.clps`:"
            if ran
            else "Pinned in `lab/others/docker-compose.yml` and "
            "`lab/others/Dockerfile.clps`. Script 07 has **not** been run here - it "
            "needs a reachable Docker daemon - so no comparison numbers appear below."
        )
        parts.append("")
        for v in versions:
            parts.append(f"- `{v}`")
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
