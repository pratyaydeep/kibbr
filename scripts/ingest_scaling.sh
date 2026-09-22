#!/usr/bin/env bash
# Does a second sender buy anything?
#
# As built, no. One global lock is taken for writing before `/ingest` has split the body
# into lines, so every body's parse - which is most of the work - runs one at a time, and
# the rate is flat from 1 sender to 8. The fix moves the parse out of the lock; the tick
# and the queries keep the lock they have, and splitting that is later work.
#
# Three things this measures around rather than through:
#
# - **The sender is one Python process holding one connection.** A `curl` per body spends
#   more on spawning a process than on the body, and that cost overlaps across senders -
#   which reads as scaling that is not there.
# - **The server is warmed first.** The first body a fresh data directory sees creates
#   several hundred leaf directories and fsyncs a catalog record for each. On disk that is
#   six times the cost of a warm body, and paying it serially in the 1-sender run while
#   the 4-sender run splits it four ways is the same false scaling by another route.
# - **A fresh server and data directory per sender count**, so the template count and the
#   leaves open are the same in each run.
#
#   LINES=2000 BODIES=20 scripts/ingest_scaling.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
LINES=${LINES:-2000}
BODIES=${BODIES:-20}
WARMUP=${WARMUP:-3}
GEN_SEED=${GEN_SEED:-42}
SENDERS=${SENDERS:-"1 2 4 8"}
# Above the largest sender count, so a body is never shed for want of a permit: this is a
# measurement of the lock, and a 503 would measure the admission control instead.
INGEST_CONCURRENCY=${INGEST_CONCURRENCY:-16}

cargo build --release --quiet || exit 1
CORES=$( (sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 1) )
echo "cores                $CORES"
echo "body                 $LINES lines, $BODIES per sender, $WARMUP to warm"

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

mkdir -p testdata/ingest-limits
INPUT=testdata/ingest-limits/${LINES}-seed${GEN_SEED}-batch.ndjson
[ -f "$INPUT" ] || "$BIN" gen-logs --lines "$LINES" --templates 12 --seed "$GEN_SEED" > "$INPUT"
# A bigger corpus for the parse measurement: 2,000 lines is over before the process has
# finished starting, so the small file would report the loader and not the parser.
PARSE_LINES=${PARSE_LINES:-20000}
PARSE_INPUT=testdata/ingest-limits/${PARSE_LINES}-seed${GEN_SEED}.ndjson
[ -f "$PARSE_INPUT" ] ||
  "$BIN" gen-logs --lines "$PARSE_LINES" --templates 12 --seed "$GEN_SEED" > "$PARSE_INPUT"

# One sender: `BODIES` bodies down one connection, reporting seconds and non-200s.
cat > "$WORK/sender.py" <<'PY'
import http.client, sys, time

host, port, path, n = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
body = open(path, "rb").read()
headers = {"Content-Type": "application/x-ndjson", "Content-Length": str(len(body))}
# One connection for every body: a fresh one per body would measure the TCP handshake.
conn = http.client.HTTPConnection(host, port, timeout=120)
bad = 0
start = time.time()
for _ in range(n):
    conn.request("POST", "/ingest", body, headers)
    response = conn.getresponse()
    response.read()
    if response.status != 200:
        bad += 1
print(f"{time.time() - start:.4f} {bad}")
PY

now() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c 'import sys, time; print(time.time() - float(sys.argv[1]))' "$1"; }

# `parse --json` on the same generator: the ceiling a single sender works against.
#
# Timed from inside one Python process. Bracketing the run with two `python3 -c` calls
# costs half a second of interpreter startup, which on a corpus this size is most of the
# number being reported. What is left in it is one fork and exec, so this is a floor under
# the parser rather than a measurement of it - it is here for scale, and nothing asserts
# against it.
PARSE_RATE=$(python3 -c '
import subprocess, sys, time
lines, binary, path = int(sys.argv[1]), sys.argv[2], sys.argv[3]
start = time.time()
subprocess.run([binary, "parse", path, "--json"],
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
print(int(lines / max(time.time() - start, 1e-9)))
' "$PARSE_LINES" "$BIN" "$PARSE_INPUT")
echo "parse alone          $PARSE_RATE lines/s ($PARSE_LINES lines)"
echo

PORT=""
start() {  # start <run directory>
  "$BIN" serve --port 0 --data-dir "$1/data" \
    --ingest-concurrency "$INGEST_CONCURRENCY" 2>"$1/serve.log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1/serve.log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$1/serve.log" >&2; return 1; fi
}

stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
}

printf '%-9s %-14s %-9s %-9s %s\n' senders lines/s speedup non-200 ingested
BASE_RATE=""
RESULTS="$WORK/results.txt"
: > "$RESULTS"

for n in $SENDERS; do
  RUN="$WORK/run-$n"
  mkdir -p "$RUN/data"
  start "$RUN" || exit 1

  # Warm: the templates, the leaf directories and the catalog records all exist after
  # this, so what follows is steady state.
  python3 "$WORK/sender.py" 127.0.0.1 "$PORT" "$INPUT" "$WARMUP" > /dev/null

  START=$(now)
  pids=()
  for s in $(seq 1 "$n"); do
    python3 "$WORK/sender.py" 127.0.0.1 "$PORT" "$INPUT" "$BODIES" > "$RUN/out-$s" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  ELAPSED=$(since "$START")

  BAD=$(cat "$RUN"/out-* 2>/dev/null | awk '{s += $2} END {print s + 0}')
  TOTAL=$((n * BODIES * LINES))
  # What the server says it took, against what was sent. A rate computed from lines
  # *sent* is not a rate: a store that cannot write answers every body in microseconds -
  # with a 200, until a tick has failed - and scores higher than a healthy one. Measured
  # here and asserted below.
  LANDED=$(curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingest"]["lines_ingested"])' \
    2>/dev/null || echo 0)
  RATE=$(python3 -c 'print(int('"$TOTAL"' / max('"$ELAPSED"', 1e-9)))')
  [ -z "$BASE_RATE" ] && BASE_RATE=$RATE
  SPEEDUP=$(python3 -c 'print("%.2fx" % ('"$RATE"' / max('"$BASE_RATE"', 1)))')
  # The warm-up bodies are in `lines_ingested` too, so the floor is the timed lines.
  WANT=$((TOTAL + WARMUP * LINES))
  printf '%-9s %-14s %-9s %-9s %s\n' "$n" "$RATE" "$SPEEDUP" "$BAD" "$LANDED/$WANT"
  echo "$n $RATE $BAD $LANDED $WANT" >> "$RESULTS"
  stop
done

ONE=$(awk '$1 == 1 {print $2}' "$RESULTS")
FOUR=$(awk '$1 == 4 {print $2}' "$RESULTS")
echo
if [ -z "$FOUR" ] || [ -z "$ONE" ]; then
  echo "no 1-sender and 4-sender pair to compare"
  exit 1
fi
RATIO=$(python3 -c 'print("%.2fx" % ('"$FOUR"' / max('"$ONE"', 1)))')
# A rate measured over bodies the server did not take is not a rate. Two ways that
# happens and both have to be closed: a body answered 503 comes back in microseconds, and
# a body answered 200 by a store that cannot write is faster still. `lines_ingested` is
# what the server says it actually took, so it is what the verdict rests on.
BAD_TOTAL=$(awk '{s += $3} END {print s + 0}' "$RESULTS")
if [ "$BAD_TOTAL" != "0" ]; then
  echo "FAIL $BAD_TOTAL body/bodies were not answered 200, so these rates measure refusals"
  exit 1
fi
SHORT=$(awk '$4 < $5 {print $1 " (" $4 " of " $5 ")"}' "$RESULTS")
if [ -n "$SHORT" ]; then
  echo "FAIL the server took fewer lines than were sent, so these rates measure nothing:"
  echo "$SHORT" | sed 's/^/  senders /'
  exit 1
fi
if [ "$CORES" -lt 4 ]; then
  echo "SKIP the 4-senders-beat-1-by-2x check needs 4 or more cores; this machine has $CORES ($RATIO)"
  exit 0
fi
if [ "$FOUR" -ge $((ONE * 2)) ]; then
  echo "PASS 4 senders reach $FOUR lines/s against $ONE for 1 - $RATIO"
  exit 0
fi
echo "FAIL 4 senders reach $FOUR lines/s against $ONE for 1 - $RATIO, want 2.00x"
exit 1
