#!/usr/bin/env bash
# `/ingest` against a real log shipper - the only thing that can answer the two questions
# curl cannot: what does Vector do when the server dies mid-batch, and when it is too slow?
#
# curl proves the endpoint answers. It has no retry policy, no buffer, no adaptive
# concurrency and no opinion about a 503, so it cannot say whether the answers are ones a
# shipper acts on correctly - which is what putting TrieDB behind a shipper is betting on.
#
# Vector 0.37.1 is the pinned version. Downloaded into testdata/ and run from there:
# nothing installed, nothing on PATH, and the version is the one being claimed rather than
# whichever a machine happens to have. On arm64 it runs under Rosetta - 0.37.1 shipped no
# arm64 darwin build.
#
#   scripts/vector_check.sh
#
# Two experiments, in order:
#
#   A  restart      TrieDB is `kill -9`'d mid-batch and restarted. Vector must retry what
#                   it had not yet had acknowledged, and must not drop it.
#   B  slowdown     TrieDB is started with one ingest permit, so it sheds. Vector must back
#                   off - a falling concurrency limit and a bounded buffer - rather than
#                   queueing without limit.
#
# What A cannot test: durability. There is no write-ahead log, so a kill loses whatever had
# not reached a flush. A is about the shipper's half of the contract, not the server's.

set -uo pipefail
cd "$(dirname "$0")/.."

VERSION=0.37.1
DIR=testdata/vector
RUN=$DIR/run
BIN=$DIR/vector-x86_64-apple-darwin/bin/vector
TRIEDB=./target/release/triedb
PORT=7700
METRICS=127.0.0.1:9598
LINES=${LINES:-20000}

# Rosetta on arm64, nothing on x86_64. `arch` with a matching arch is a no-op, so this is
# one command either way rather than two paths to keep in step.
RUNNER=()
[ "$(uname -m)" = "arm64" ] && RUNNER=(arch -x86_64)

mkdir -p "$RUN"

# ── The tools ──────────────────────────────────────────────────────────────────────

if [ ! -x "$BIN" ]; then
  echo "downloading vector $VERSION"
  curl -sL -o "$DIR/vector.tar.gz" \
    "https://github.com/vectordotdev/vector/releases/download/v$VERSION/vector-$VERSION-x86_64-apple-darwin.tar.gz" \
    || { echo "download failed"; exit 1; }
  tar xzf "$DIR/vector.tar.gz" -C "$DIR"
fi
"${RUNNER[@]}" "$BIN" --version || exit 1

# Release, not debug: B is about the server being slow, and a debug build is slow for
# reasons that have nothing to do with the permit it is testing.
cargo build --release --quiet || exit 1

# ── The corpus ─────────────────────────────────────────────────────────────────────
#
# Written here rather than by `gen-logs`, for one field: `seq`. Counting duplicates after a
# retry needs each line to be distinguishable from every other line, and a generated log
# corpus is deliberately repetitive.

python3 - "$LINES" > "$RUN/corpus.full" <<'PY'
import json, random, sys
random.seed(7)
levels = ["INFO", "WARN", "ERROR"]
services = ["checkout", "auth", "search"]
for i in range(int(sys.argv[1])):
    print(json.dumps({
        "seq": i,
        "batch": i // 500,
        "level": random.choice(levels),
        "service": random.choice(services),
        "msg": "request handled",
    }))
PY

# ── Helpers ────────────────────────────────────────────────────────────────────────

triedb_up() {  # $@ = extra serve flags
  $TRIEDB serve --port $PORT "$@" >/dev/null 2>>"$RUN/triedb.log" &
  echo $! > "$RUN/triedb.pid"
  for _ in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$PORT/health" >/dev/null && return 0
    sleep 0.2
  done
  echo "triedb did not come up"; exit 1
}

# `$1` is a dotted path into /stats, e.g. `ingest.lines_ingested`. An argument rather than
# text interpolated into the program: a path carrying quotes would otherwise close the
# python string it was substituted into, which is how the first version of this read 0 for
# every value and made the kill land after the corpus had already shipped.
stat_of() {
  curl -s "http://127.0.0.1:$PORT/stats" | python3 -c '
import sys, json
node = json.load(sys.stdin)
for key in sys.argv[1].split("."):
    node = node[key]
print(node)' "$1" 2>/dev/null || echo 0
}
metric()  { curl -s "http://$METRICS/metrics" | grep -F "$1" | grep -v '^#' ; }
sum_of()  { metric "$1" | awk '{s+=$(NF-1)} END {printf "%d\n", s}'; }

cleanup() { pkill -f "bin/vector" 2>/dev/null; pkill -f "triedb serve" 2>/dev/null; }
trap cleanup EXIT

# ══ A - restart mid-batch ══════════════════════════════════════════════════════════

echo
echo "════ A. kill -9 mid-batch, restart ════"
cleanup; sleep 1
rm -rf "$RUN/data" "$RUN/triedb.log"; mkdir -p "$RUN/data"

cat > "$RUN/restart.toml" <<EOF
data_dir = "$PWD/$RUN/data"

[sources.logs]
type    = "file"
include = ["$PWD/$RUN/corpus.ndjson"]

# The file source hands the sink a line of text in \`.message\`; without this the body would
# be 20,000 JSON strings rather than 20,000 JSON documents.
[transforms.unwrap]
type   = "remap"
inputs = ["logs*"]
source = '. = parse_json!(.message)'

[sources.metrics]
type = "internal_metrics"

[sinks.prom]
type    = "prometheus_exporter"
inputs  = ["metrics"]
address = "$METRICS"

# The sink a deployment points at TrieDB, with two changes:
#   compression        turned on - the point of the exercise
#   encoding/framing   \`codec = "ndjson"\`, the obvious spelling, does not load on 0.37.1;
#                      the codec is \`json\` and the newlines come from \`framing\`.
[sinks.triedb]
type               = "http"
inputs             = ["unwrap"]
uri                = "http://127.0.0.1:$PORT/ingest"
encoding.codec     = "json"
framing.method     = "newline_delimited"
compression        = "gzip"
batch.max_events   = 500
batch.timeout_secs = 1
healthcheck.uri    = "http://127.0.0.1:$PORT/health"
EOF

# The corpus is *appended to* rather than written whole, because a complete file is shipped
# in well under a second and there is no mid-stream for a kill to land in. 2,500 lines a
# second is a log file being written to, which is what Vector is pointed at in production.
: > "$RUN/corpus.ndjson"
rm -f "$RUN"/chunk.*
triedb_up
"${RUNNER[@]}" "$BIN" --config "$RUN/restart.toml" >"$RUN/vector.log" 2>&1 &
sleep 1

split -l 500 "$RUN/corpus.full" "$RUN/chunk."
(
  for chunk in "$RUN"/chunk.*; do
    cat "$chunk" >> "$RUN/corpus.ndjson"
    sleep 0.2
  done
) &
FEEDER=$!

# Kill it partway, not at the start and not at the end: a batch has to be in flight for
# this to be testing anything.
for _ in $(seq 1 100); do
  read_so_far=$(stat_of ingest.lines_ingested)
  [ "$read_so_far" -gt $((LINES / 5)) ] && break
  sleep 0.1
done

sent_at_kill=$(sum_of 'vector_component_sent_events_total{component_id="triedb"')
echo "  before the kill:  triedb ingested $read_so_far, vector had $sent_at_kill acknowledged"

kill -9 "$(cat "$RUN/triedb.pid")" 2>/dev/null
echo "  kill -9 sent; leaving the server down for 8s"
sleep 8

sent_down=$(sum_of 'vector_component_sent_events_total{component_id="triedb"')
errors_down=$(sum_of 'vector_component_errors_total{')
echo "  while down:       vector acknowledged $sent_down total, errors $errors_down"

triedb_up
echo "  restarted, empty by design - the rows it held are gone (storage is memory)"
wait $FEEDER 2>/dev/null
echo "  the whole corpus has now been written to the file"

# Drained when the count stops moving.
last=-1
for _ in $(seq 1 120); do
  now=$(stat_of ingest.lines_ingested)
  [ "$now" = "$last" ] && [ "$now" -gt 0 ] && break
  last=$now
  sleep 1
done

echo
echo "── vector, after the restart ──"
metric 'vector_component_sent_events_total{component_id="triedb"'
metric 'vector_component_errors_total{'
metric 'vector_http_client_responses_total{component_id="triedb"'
metric 'vector_buffer_events{component_id="triedb"'
metric 'vector_component_discarded_events_total{component_id="triedb"'

echo
echo "── triedb, after the restart ──"
curl -s "http://127.0.0.1:$PORT/stats" | python3 -m json.tool | grep -A 8 '"ingest"'

echo
echo "── duplicates ──"
# A batch holding more than the 500 lines it was built from is a batch that landed twice.
# Retrying a batch the server had already stored is the price of at-least-once delivery,
# and the only way to see it is to have made every line distinguishable in the first place.
curl -s -X POST -d '{"q":"COUNT LOG {} GROUP BY batch"}' "http://127.0.0.1:$PORT/query" \
  > "$RUN/batches.json"
python3 - "$RUN/batches.json" <<'PY'
import json, sys
answer = json.load(open(sys.argv[1]))
groups = answer.get("groups") or []
total = sum(g["count"] for g in groups)
over = [g for g in groups if g["count"] > 500]
print(f"  {len(groups)} batches present, {total} rows in them")
print(f"  {len(over)} batch(es) hold more than the 500 lines they were built from")
for g in over[:5]:
    print(f"    {g}")
PY

echo
echo "── vector's own account of the outage ──"
grep -aE "Retrying after (error|response)|HTTP error" "$RUN/vector.log" | head -4

echo
echo "── triedb log ──"
cat "$RUN/triedb.log"

# ══ B - artificial slowdown ════════════════════════════════════════════════════════
#
# Two runs, because the first one has a finding in it that looks like a failure.
#
#   B1  Vector with its own adaptive concurrency, against a server with one ingest permit.
#       Little is shed, and that is adaptive concurrency working rather than the shed
#       failing: Vector holds few requests in flight, so it rarely asks for a second
#       permit. B1 proves the slow server does not make Vector queue without bound.
#
#   B2  the same, with `request.concurrency` pinned above the permit count, which is what a
#       fleet of shippers arriving at one server looks like from the server's side. This is
#       where the 503s happen, and where "backs off rather than dropping" is checkable.

run_slow() {  # $1 = label, $2 = concurrency setting, $3 = seconds, $4 = source count
  cleanup; sleep 1
  rm -rf "$RUN/data" "$RUN/triedb.log"; mkdir -p "$RUN/data"

  # One `demo_logs` at full rate tops out around what TrieDB ingests, so with a single
  # source neither side is ever behind and there is nothing to shed. The slowdown has to be
  # *relative*: N sources feeding one sink is the shape of a DaemonSet, where the server is
  # one process and the senders are one per node.
  sources=""
  for i in $(seq 1 "$4"); do
    sources="$sources
[sources.logs$i]
type     = \"demo_logs\"
format   = \"json\"
interval = 0.0
"
  done

  cat > "$RUN/slow.toml" <<EOF
data_dir = "$PWD/$RUN/data"

SOURCES

[transforms.unwrap]
type   = "remap"
inputs = ["logs*"]
source = '. = parse_json!(.message)'

[sources.metrics]
type = "internal_metrics"

[sinks.prom]
type    = "prometheus_exporter"
inputs  = ["metrics"]
address = "$METRICS"

# Big batches, so one request holds the single permit long enough for the next to meet it.
[sinks.triedb]
type               = "http"
inputs             = ["unwrap"]
uri                = "http://127.0.0.1:$PORT/ingest"
encoding.codec     = "json"
framing.method     = "newline_delimited"
compression        = "gzip"
batch.max_events   = 20000
batch.timeout_secs = 1
request.concurrency = $2
healthcheck.uri    = "http://127.0.0.1:$PORT/health"
EOF

  python3 - "$RUN/slow.toml" "$sources" <<'PY'
import sys
path, sources = sys.argv[1], sys.argv[2]
# Read fully before opening for write: `open(w)` truncates, and doing both in one
# expression leaves an empty file and a Vector that starts with no sources at all.
text = open(path).read()
open(path, "w").write(text.replace("SOURCES", sources))
PY

  triedb_up --ingest-concurrency 1 --max-body-bytes 67108864
  "${RUNNER[@]}" "$BIN" --config "$RUN/slow.toml" >"$RUN/vector-slow.log" 2>&1 &
  echo
  echo "════ $1 ════"
  echo "  one ingest permit, request.concurrency = $2, $4 source(s), running ${3}s"
  sleep "$3"

  echo
  echo "── vector ──"
  metric 'vector_http_client_responses_total{component_id="triedb"'
  metric 'vector_component_sent_events_total{component_id="triedb"'
  metric 'vector_component_discarded_events_total{'
  metric 'vector_component_errors_total{'
  metric 'vector_buffer_events{'
  metric 'vector_adaptive_concurrency_in_flight_sum'
  metric 'vector_adaptive_concurrency_in_flight_count'
  echo "  retries logged: $(grep -acE "Retrying after (error|response)" "$RUN/vector-slow.log")"
  grep -aE "Retrying after response" "$RUN/vector-slow.log" | head -2

  echo
  echo "── triedb ──"
  curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import sys,json;print(" ",json.load(sys.stdin)["ingest"])'
}

run_slow "B1. Vector's own adaptive concurrency" '"adaptive"' 40 8
run_slow "B2. eight concurrent senders, one permit" 8 40 8
