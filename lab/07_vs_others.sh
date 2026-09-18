#!/usr/bin/env bash
# The same logs in other systems. Needs Docker; skips cleanly without it.
#
# Every image tag is pinned in lab/others/docker-compose.yml and written into
# results.csv, so RESULTS.md says which versions it measured. A comparison against
# "latest" is a comparison against nothing.
#
# Sizes here are exact and are the point. The timings are labelled
# "rough, one machine, default settings" and mean it: one laptop, no tuning of any
# system including TrieDB's, containers competing for the same disk, and clp-s running
# under x86_64 emulation on Apple Silicon. They say which order of magnitude, nothing
# finer.
set -uo pipefail

LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$LAB_DIR")"
PY="$LAB_DIR/.venv/bin/python"
COMPOSE_FILE="$LAB_DIR/others/docker-compose.yml"
DATASET="${DATASET:-uneven}"
SIZE="${SIZE:-small}"
# Comma-separated: es, clickhouse, clps. Each is started, measured and stopped on its
# own, so one can be re-run without paying for the others.
SYSTEMS="${SYSTEMS:-es,clickhouse,clps}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset) DATASET="$2"; shift 2 ;;
    --size)    SIZE="$2";    shift 2 ;;
    --systems) SYSTEMS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

skip() {
  echo "07_vs_others: SKIPPED - $1"
  echo "  This script is the only one in the lab that needs Docker. Everything else"
  echo "  runs without it, and run_all.py does not call this one."
  exit 0
}

command -v docker >/dev/null 2>&1 || skip "no docker binary on PATH"
docker info >/dev/null 2>&1 || skip "the Docker daemon is not reachable (start Docker Desktop and retry)"
[[ -x "$PY" ]] || skip "lab/.venv is missing; create it first"

# One system at a time, not all three: the Docker VM on this machine holds under 4 GB,
# and three JVM/columnar servers sharing it would measure the OOM killer. Each system is
# started, measured and stopped by the Python block below.
echo "07_vs_others: building the clp-s image (x86_64 under emulation; this is slow)"
docker compose -f "$COMPOSE_FILE" build clps || echo "  clp-s image build failed; it will be recorded as a failure"

cleanup() {
  echo "07_vs_others: tearing down"
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Match the `image:` line, not the service-name line. `/elasticsearch:/` also matches
# the key `  elasticsearch:` two lines above, whose last field is the key itself - which
# is how "elasticsearch:" with no version ended up recorded as the version.
ES_VER=$(docker compose -f "$COMPOSE_FILE" config 2>/dev/null \
          | awk '/image:.*elasticsearch/{print $NF; exit}' | tr -d '"')
CH_VER=$(docker compose -f "$COMPOSE_FILE" config 2>/dev/null \
          | awk '/image:.*clickhouse-server/{print $NF; exit}' | tr -d '"')

cd "$REPO_DIR"
ES_IMAGE="$ES_VER" CH_IMAGE="$CH_VER" DATASET="$DATASET" SIZE="$SIZE" \
  SYSTEMS="$SYSTEMS" \
  "$PY" -u - <<'PYEOF'
import os
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from lab.common import datasets, measure, others_load as O

COMPOSE = ["docker", "compose", "-f", str(Path("lab/others/docker-compose.yml"))]


def compose_up(svc: str) -> bool:
    import subprocess

    print(f"\n  starting {svc} ...", flush=True)
    r = subprocess.run(COMPOSE + ["up", "-d", svc], capture_output=True)
    if r.returncode != 0:
        print(f"  {svc}: up failed - {r.stderr.decode()[:300]}")
        return False
    return True


def compose_stop(svc: str) -> None:
    import subprocess

    subprocess.run(COMPOSE + ["stop", "-t", "30", svc], capture_output=True)
    subprocess.run(COMPOSE + ["rm", "-f", "-v", svc], capture_output=True)
    print(f"  stopped {svc}, memory returned to the VM")


dataset = os.environ["DATASET"]
size = os.environ["SIZE"]
wanted = {w.strip() for w in os.environ.get("SYSTEMS", "es,clickhouse,clps").split(",")}
versions = {
    "elasticsearch": os.environ.get("ES_IMAGE", "?"),
    "clickhouse": os.environ.get("CH_IMAGE", "?"),
    "clp-s": "y-scope/clp v0.13.0, clp-s built from source for the host architecture (no prebuilt binary or image is published)",
}
ROUGH = ("rough, one machine, default settings - one laptop, no tuning of any system, "
         "containers sharing one disk")

res = measure.Results("07_vs_others", dataset)
measure.print_machine()
print("\npinned versions:")
for k, v in versions.items():
    print(f"  {k:15} {v}")
    res.add(k, "version", v, "tag", "pinned in lab/others/docker-compose.yml")

ds, objs, raw_bytes, event, ident_field, ident = O.load_dataset(dataset, size)
print(f"\n{ds.name}: {len(objs):,} documents, {raw_bytes:,} raw bytes")
print(f"  probes: event={event!r}  {ident_field}={ident!r}")
res.add("raw NDJSON", "bytes", raw_bytes, "bytes", "the denominator for every ratio")
res.add("all", "at_timestamp_added", 1, "flag",
        "`@timestamp` is added to every document on the way into Elasticsearch and "
        "ClickHouse - logsdb mode requires one and the dataset has `ts`/`ts_unix`/"
        "`ts_ms` instead. Those bytes are not in the raw NDJSON denominator.")

rows = []


def record(system, size_bytes, load_s, searches, note=""):
    ratio = size_bytes / raw_bytes if raw_bytes else 0
    rows.append([system, size_bytes, round(ratio, 4), round(load_s, 1)]
                + [round(searches.get(k), 3) if searches.get(k) is not None else "fail"
                   for k in ("event value", "level = ERROR", "one id", "newest 500")])
    res.add(system, "bytes_on_disk", size_bytes, "bytes", note)
    res.add(system, "ratio_to_raw", round(ratio, 5), "ratio", note)
    res.add(system, "load_seconds", round(load_s, 2), "s", ROUGH)
    for k, v in searches.items():
        res.add(system, f"search_{k}_seconds",
                round(v, 4) if v is not None else -1, "s", ROUGH)


# -- Elasticsearch ---------------------------------------------------------------
res.add("Elasticsearch", "heap", "1g", "setting",
        "-Xms1g -Xmx1g, not an Elasticsearch default: the Docker VM here holds under "
        "4 GB and 07 runs one system at a time")
if "es" not in wanted:
    print("\n  elasticsearch: not in --systems, skipped")
elif compose_up("elasticsearch") and O.wait_for(f"{O.ES}/_cluster/health"):
    for index, logsdb in (("triedb_standard", False), ("triedb_logsdb", True)):
        label = f"Elasticsearch {'logsdb' if logsdb else 'standard'}"
        try:
            O.es_create(index, logsdb)
            took = O.es_load(index, objs)
            size_b = O.es_size(index)
            searches = O.es_searches(index, event, ident_field, ident)
            record(label, size_b, took, searches,
                   "after _forcemerge to one segment and _flush")
        except Exception as e:
            print(f"  {label}: FAILED - {e}")
            res.add(label, "status", "failed", "text", str(e)[:200])
else:
    print("  Elasticsearch never became healthy; skipped")
    res.add("Elasticsearch", "status", "unreachable", "text", "")
if "es" in wanted:
    compose_stop("elasticsearch")

# -- ClickHouse -------------------------------------------------------------------
if "clickhouse" not in wanted:
    print("\n  clickhouse: not in --systems, skipped")
elif compose_up("clickhouse") and O.wait_for(f"{O.CH}/ping"):
    try:
        took = O.ch_load(objs)
        size_b = O.ch_size()
        searches = O.ch_searches(event, ident_field, ident)
        record("ClickHouse", size_b, took, searches,
               "sum(bytes_on_disk) over active parts after OPTIMIZE TABLE FINAL")
    except Exception as e:
        print(f"  ClickHouse: FAILED - {e}")
        res.add("ClickHouse", "status", "failed", "text", str(e)[:200])
else:
    print("  ClickHouse never became healthy; skipped")
    res.add("ClickHouse", "status", "unreachable", "text", "")
if "clickhouse" in wanted:
    compose_stop("clickhouse")

# -- clp-s --------------------------------------------------------------------------
try:
    if "clps" not in wanted:
        raise RuntimeError("not in --systems, skipped")
    if not compose_up("clps"):
        raise RuntimeError("clp-s container would not start")
    payload_file = measure.OUT / "07" / "input.ndjson"
    payload_file.parent.mkdir(parents=True, exist_ok=True)
    with open(payload_file, "wb") as fh:
        for _, p in datasets.iter_payloads(ds.path):
            fh.write(p + b"\n")
    took = O.clps_load(payload_file)
    size_b = O.clps_size()
    vlines, vbytes, vok = O.clps_verify(len(objs))
    print(f"  clp-s round-trip: {vlines:,} lines, {vbytes:,} bytes back "
          f"({'matches the input' if vok else 'DOES NOT MATCH THE INPUT'})")
    res.add("clp-s", "roundtrip_lines", vlines, "lines",
            f"archive decompressed and counted; {len(objs):,} went in. A ratio is this "
            "script's headline number, so the archive is expanded rather than trusted.")
    res.add("clp-s", "roundtrip_ok", 1 if vok else 0, "flag", "")
    if not vok:
        raise RuntimeError(
            f"clp-s archive holds {vlines:,} lines, not {len(objs):,} - its size is not "
            "comparable and is not recorded"
        )
    searches = O.clps_searches(event, ident)
    record("clp-s", size_b, took, searches,
           "archive directory size, round-trip verified; clp-s is fed the raw payloads "
           "with no @timestamp added, unlike Elasticsearch and ClickHouse")
except Exception as e:
    print(f"  clp-s: FAILED - {e}")
    res.add("clp-s", "status", "failed", "text", str(e)[:200])
finally:
    if "clps" in wanted:
        compose_stop("clps")

# -- TrieDB's own best, from scripts 02 and 05 --------------------------------------
import csv
best = {}
if measure.RESULTS_CSV.exists():
    with measure.RESULTS_CSV.open() as fh:
        for r in csv.DictReader(fh):
            if r["dataset"] != dataset:
                continue
            key = (r["script"], r["variant"], r["metric"])
            if key in (("02_split_vs_wide", "WIDE rg=122,880", "bytes"),
                       ("05_encodings", "BEST_PER_CHUNK sorted", "bytes")):
                if key not in best or r["when"] > best[key][0]:
                    best[key] = (r["when"], int(float(r["value"])))
for (script, variant, _m), (_when, value) in best.items():
    rows.append([f"TrieDB {variant} ({script})", value,
                 round(value / raw_bytes, 4), "-", "-", "-", "-", "-"])

measure.table(
    rows,
    headers=["system", "bytes on disk", "of raw", "load s", "event", "level=ERROR",
             "one id", "newest 500"],
    title=f"{ds.name}: the same logs in other systems "
          f"(timings: {ROUGH})",
)
res.flush()
PYEOF
