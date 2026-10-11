#!/usr/bin/env bash
# `kibbr bench`, run inside the image, on a farm day of seed 42.
#
# The bench is the one in the binary (src/speed/): it makes the day with `gen-logs
# --profile farm --seed 42`, loads it once, and measures servers that are children of the
# same binary. This script is what is around it: it builds the image, runs the bench in a
# container, brings the result out, and writes it as a table with a `shed` column.
#
# **Two images, one binary.** `kibbr` is the image that is deployed: distroless, and
# with no `ps` and no `df`, which the bench runs and will not start without. `kibbr:bench`
# is the Dockerfile's `bench` target - Debian's slim base with procps - holding the same
# file: both copy /out/kibbr out of one build stage, and this script checks that the two
# are the same bytes before it measures one and reports the size of the other.
#
#   bench/run.sh                               # this laptop: 200,000 lines
#   bench/run.sh --lines 1000000 --threads 8   # the cloud box
#
# What it writes, under bench/results/, named for the UTC date (and `-2`, `-3` for a
# second and third run of one day - a result is never written over):
#
#   <date>.md     the table, and what it was measured on
#   <date>.json   the bench's own result, every sample in it
#   <date>.log    the bench's raw log
#
# **The loaded day and every server's copy of it are in a Docker volume, not a bind
# mount.** On Docker Desktop a bind mount is a file server between two kernels, and the
# number would be its speed. The volume is `kibbr-bench`; it keeps the loaded day, so a
# second run does not load it again. `docker volume rm kibbr-bench` gives the space back.
#
# **The numbers are this image's on this machine's Docker**, which on a laptop is a
# Linux VM with some of the cores and memory. They are not comparable with
# testdata/bench/*.json, which were measured on macOS without one, and the bench says so
# if asked to compare them.
#
#   --lines N       lines in the loaded day and in the live stream     [200000]
#   --threads T     `kibbr bench --threads T`: the scan threads of every server measured.
#                   Refused if the bench in the image has no such flag (an image built
#                   before it had one, under --no-build) - it is not dropped, since a
#                   result headed "8 threads" that used 4 is worse than none.
#   --runs R        as `kibbr bench --runs`                            [3]
#   --samples K     as `kibbr bench --samples`                         [100]
#   --unchecked     as `kibbr bench --unchecked`: not a measurement. For trying the script.
#   --image NAME    the deployed image's tag; the bench's is NAME-bench [kibbr]
#   --no-build      use the two images as they are
#   --volume NAME   the Docker volume                                   [kibbr-bench]

set -uo pipefail
cd "$(dirname "$0")/.."

LINES=200000
THREADS=""
RUNS=""
SAMPLES=""
UNCHECKED=""
IMAGE=kibbr
VOLUME=kibbr-bench
BUILD=yes
while [ $# -gt 0 ]; do
  case "$1" in
    --lines) LINES=$2; shift 2 ;;
    --threads) THREADS=$2; shift 2 ;;
    --runs) RUNS=$2; shift 2 ;;
    --samples) SAMPLES=$2; shift 2 ;;
    --image) IMAGE=$2; shift 2 ;;
    --volume) VOLUME=$2; shift 2 ;;
    --unchecked) UNCHECKED=yes; shift ;;
    --no-build) BUILD=no; shift ;;
    -h|--help) sed -n '2,45p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "bench/run.sh: unknown argument $1" >&2; exit 2 ;;
  esac
done

docker version --format '{{.Server.Version}}' >/dev/null 2>&1 ||
  { echo "bench/run.sh: no Docker daemon answers" >&2; exit 1; }

now() { python3 -c 'import time; print("%.3f" % time.time())'; }
elapsed() { python3 -c 'import sys, time; print("%.1f" % (time.time() - float(sys.argv[1])))' "$1"; }

BEGAN=$(now)
COMMIT=$(git rev-parse HEAD 2>/dev/null || echo unknown)
DIRTY=$(git diff --quiet HEAD -- src crates Cargo.toml Cargo.lock Dockerfile 2>/dev/null && echo no || echo yes)

# A tag of its own beside a name with one (`kibbr:0.1.0` -> `kibbr:0.1.0-bench`).
case "$IMAGE" in
  *:*) BENCH_IMAGE=$IMAGE-bench ;;
  *) BENCH_IMAGE=$IMAGE:bench ;;
esac
BUILD_S="-"
if [ "$BUILD" = yes ]; then
  T=$(now)
  docker build --build-arg "KIBBR_COMMIT=$COMMIT" -t "$IMAGE" . || exit 1
  # The build stage is cached by now: this adds the bench's base and copies the binary.
  docker build --target bench -t "$BENCH_IMAGE" . || exit 1
  BUILD_S=$(elapsed "$T")
fi
IMAGE_ID=$(docker image inspect --format '{{.Id}}' "$IMAGE") || exit 1
IMAGE_BYTES=$(docker image inspect --format '{{.Size}}' "$IMAGE")
docker image inspect "$BENCH_IMAGE" >/dev/null || exit 1

# One binary in both: the file copied out of each image and compared.
same_binary() {
  local dir a b
  dir=$(mktemp -d) || return 1
  a=$(docker create "$IMAGE") && docker cp "$a:/usr/local/bin/kibbr" "$dir/deployed" >/dev/null
  b=$(docker create "$BENCH_IMAGE") && docker cp "$b:/usr/local/bin/kibbr" "$dir/bench" >/dev/null
  # `-v`: each image declares a volume, and a container made from it has an unnamed one.
  docker rm -v "$a" "$b" >/dev/null 2>&1
  cmp -s "$dir/deployed" "$dir/bench"
  local same=$?
  rm -rf "$dir"
  return $same
}
same_binary || {
  echo "bench/run.sh: $IMAGE and $BENCH_IMAGE do not hold the same kibbr: build both from one" >&2
  echo "  checkout (run without --no-build)" >&2
  exit 1
}

# Asked of the image and not assumed: see --threads above.
ARGS=(bench --profile farm --lines "$LINES" --dir /var/lib/kibbr/bench)
if [ -n "$THREADS" ]; then
  docker run --rm "$BENCH_IMAGE" bench --help 2>/dev/null | grep -q -- '--threads' || {
    echo "bench/run.sh: the kibbr bench in $BENCH_IMAGE has no --threads: the servers it measures" >&2
    echo "  take the build's own scan threads (half the cores, at most 4), and no flag of" >&2
    echo "  this script reaches them. Run without --threads, or on a build whose bench has it." >&2
    exit 2
  }
  ARGS+=(--threads "$THREADS")
fi
[ -z "$RUNS" ] || ARGS+=(--runs "$RUNS")
[ -z "$SAMPLES" ] || ARGS+=(--samples "$SAMPLES")
[ -z "$UNCHECKED" ] || ARGS+=(--unchecked)

mkdir -p bench/results
DATE=$(date -u +%Y-%m-%d)
NAME=$DATE
n=1
while [ -e "bench/results/$NAME.md" ] || [ -e "bench/results/$NAME.json" ]; do
  n=$((n + 1)); NAME=$DATE-$n
done
# The result's name inside the volume: the time, so that it is no earlier run's.
INSIDE=/var/lib/kibbr/bench/results/$(date -u +%Y%m%dT%H%M%SZ)-$$
ARGS+=(--out "$INSIDE.json" --label "docker $NAME")

CONTAINER=kibbr-bench-$$
cleanup() { docker rm -f -v "$CONTAINER" >/dev/null 2>&1; }
trap cleanup EXIT

# `--stop-timeout 30` for the reason the Dockerfile gives; `--init` is not used - the
# bench waits for its own children.
docker create --name "$CONTAINER" --stop-timeout 30 -v "$VOLUME:/var/lib/kibbr" \
  "$BENCH_IMAGE" "${ARGS[@]}" >/dev/null || exit 1
T=$(now)
docker start -a "$CONTAINER"
CODE=$?
BENCH_S=$(elapsed "$T")
if [ "$CODE" != 0 ]; then
  echo "bench/run.sh: kibbr bench exited $CODE after ${BENCH_S}s; nothing was written" >&2
  exit 1
fi
docker cp "$CONTAINER:$INSIDE.json" "bench/results/$NAME.json" >/dev/null || exit 1
docker cp "$CONTAINER:$INSIDE.log" "bench/results/$NAME.log" >/dev/null || exit 1
WALL_S=$(elapsed "$BEGAN")

DOCKER=$(docker version --format '{{.Server.Version}} on {{.Server.Os}}/{{.Server.Arch}}')
DOCKER_HOST_FACTS=$(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes of memory, {{.OperatingSystem}}')

THREADS_SAID=${THREADS:-"none - the default scan threads of the build"}
NAME="$NAME" COMMIT="$COMMIT" DIRTY="$DIRTY" IMAGE="$IMAGE" BENCH_IMAGE="$BENCH_IMAGE" IMAGE_ID="$IMAGE_ID" IMAGE_BYTES="$IMAGE_BYTES" \
BUILD_S="$BUILD_S" BENCH_S="$BENCH_S" WALL_S="$WALL_S" DOCKER="$DOCKER" DOCKER_HOST_FACTS="$DOCKER_HOST_FACTS" \
THREADS="$THREADS_SAID" \
python3 - "bench/results/$NAME.json" "bench/results/$NAME.md" <<'PY' || exit 1
import json, os, sys

src, out = sys.argv[1], sys.argv[2]
env = os.environ
r = json.load(open(src))
runs = r.get("runs") or []
med = r.get("medians") or {}
per = r.get("per_run") or {}
flags = r.get("flags") or {}
machine = r.get("machine") or {}

def num(v, digits=None):
    if v is None:
        return "-"
    if digits is None:
        digits = 0 if abs(v) >= 1000 else 2 if abs(v) >= 10 else 3
    return f"{v:,.{digits}f}"

def each(values, digits=None):
    return " / ".join(num(v, digits) for v in values) if values else "-"

def total(values):
    """A sum of counts, or None when no run recorded one."""
    seen = [v for v in values if v is not None]
    return sum(seen) if seen else None

def shed(v):
    return "-" if v is None else f"{v:,}"

rows = []
# Ingest alone: bodies the server shed, as it counts them; the senders' count of 503s and
# 429s where a server did not say.
def ingest_shed(run):
    ingest = run.get("ingest") or {}
    said = (ingest.get("at_end") or {}).get("requests_shed")
    if said is not None:
        return said
    refused = ingest.get("refused") or {}
    return total([refused.get("503"), refused.get("429")])

rows.append(("ingest, 4 senders, lines/s", num(med.get("ingest_lines_per_s"), 0),
             each(per.get("ingest_lines_per_s"), 0), shed(total([ingest_shed(x) for x in runs]))))
for where, title in (("idle", "idle"), ("loaded", "beside 4 senders")):
    for q in ("S1", "S2", "S3", "S4"):
        label = ((r.get("queries") or {}).get(q) or {}).get("label", "")
        refused = total([((x.get(where) or {}).get(q) or {}).get("refused") for x in runs])
        for p in ("p50_ms", "p99_ms"):
            rows.append((f"{q} {label}, {title}, {p[:3]} ms",
                         num(((med.get(where) or {}).get(q) or {}).get(p)),
                         each((((per.get(where) or {}).get(q) or {}).get(p))),
                         shed(refused)))
    if where == "loaded":
        laps = [x.get("loaded") or {} for x in runs]
        bodies = total([total((lap.get("lap") or {}).get("requests_shed") or [None]) for lap in laps])
        rows.append(("ingest beside the queries, lines/s (median lap)", "",
                     each([lap.get("lines_per_s_median") for lap in laps], 0), shed(bodies)))
rows.append(("scan, rows per CPU-second", num(med.get("scan_rows_per_cpu_s"), 0),
             each(per.get("scan_rows_per_cpu_s"), 0),
             shed(total([((x.get("idle") or {}).get("SCAN") or {}).get("refused") for x in runs]))))
rows.append(("plan step at 10,000 templates, ms", num(med.get("plan_step_ms")),
             each([max((v for v in ((x.get("plan") or {}).get(p, {}).get("p50_ms")
                                    for p in ("P1", "P2", "P3", "P4")) if v is not None),
                       default=None) for x in runs]), "-"))

voided = sum(len(x.get("voided") or []) for x in runs)
seconds = [x.get("seconds") for x in runs]
argv = " ".join((r.get("server") or {}).get("argv") or [])
with open(out, "w") as f:
    w = lambda line="": f.write(line + "\n")
    w(f"# kibbr bench in Docker, {env['NAME']}")
    w()
    if not flags.get("checked", False):
        w("**UNCHECKED - not a measurement.** The bench's gates were off.")
        w()
    w("| measurement | median | each run | shed |")
    w("|---|---:|---:|---:|")
    for name, middle, runs_text, shed_text in rows:
        w(f"| {name} | {middle} | {runs_text} | {shed_text} |")
    w()
    w("`shed` is what was turned away while the row was measured, summed over the runs: for")
    w("the two ingest rows, the `/ingest` bodies the server answered `503` and told to come")
    w("back (`/stats` `ingest.requests_shed`); for a query row, the times that query was")
    w("refused before it was answered. `-` is a row nothing can be shed in. A number above 0")
    w("in an ingest row means the lines/s beside it is the rate of a server that was")
    w("shedding, and is not the rate it can take.")
    w()
    w("## What was measured")
    w()
    w(f"- `kibbr bench --profile farm --lines {flags.get('lines')}` - the farm day of seed "
      f"{flags.get('loaded_seed')} loaded once, the live stream of seed {flags.get('live_seed')}; "
      f"{flags.get('runs')} runs of {flags.get('samples')} samples, {flags.get('body_lines')} lines a body, "
      f"{flags.get('senders')} senders.")
    w(f"- commit `{env['COMMIT']}`"
      + ("" if env["DIRTY"] == "no" else ", **with uncommitted changes to the source**") + ".")
    w(f"- image `{env['IMAGE']}`, `{env['IMAGE_ID']}`, {int(env['IMAGE_BYTES']):,} bytes; "
      f"measured in `{env['BENCH_IMAGE']}`, which holds the same binary beside the `ps` and "
      f"`df` the bench runs; both built in {env['BUILD_S']} s.")
    w(f"- Docker {env['DOCKER']}: {env['DOCKER_HOST_FACTS']}.")
    said = lambda key: machine.get(key) if machine.get(key) is not None else "-"
    w(f"- inside the container: {said('os')} {said('os_version')}, {said('arch')}, "
      f"CPU model {said('cpu')}, {said('cores_logical')} cores, "
      f"{num(machine.get('memory_bytes'), 0)} bytes of memory.")
    w(f"- each server: `kibbr {argv}`.")
    w(f"- threads asked for: {env['THREADS']}.")
    w(f"- the runs took {each(seconds, 1)} s; {voided} unit(s) were thrown away and done again.")
    w(f"- this script, start to end: {env['WALL_S']} s, of which the bench {env['BENCH_S']} s.")
    w()
    w(f"The bench's own result is `{env['NAME']}.json` and its raw log `{env['NAME']}.log`, beside this file.")
PY

echo
cat "bench/results/$NAME.md"
echo
echo "bench/run.sh: ${WALL_S}s start to end (build ${BUILD_S}s, bench ${BENCH_S}s) - bench/results/$NAME.md"
