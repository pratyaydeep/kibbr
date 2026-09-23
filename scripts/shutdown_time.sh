#!/usr/bin/env bash
# How long a clean stop takes with every row still in memory.
#
# Kubernetes sends SIGKILL 30 seconds after SIGTERM, and with no write-ahead log a row still
# buffered when the SIGKILL lands is gone. Measured when every template had a file of its
# own in every day: the final flush sealed one partition at a time with four flushes each,
# and 200,000 rows across 16,694 partitions took 349 seconds.
#
# The row group timeout is set past the length of the run, so no tick writes a row: every
# one reaches the disk through the final flush, which is the thing being timed.
#
# What this asserts:
#
#   1. /stats says every row is buffered before the stop, so the stop is what writes them.
#   2. SIGTERM to exit takes under BUDGET_SECONDS, and the exit code is 0.
#   3. A restart answers COUNT LOG {} = LINES: nothing was lost to go fast.
#
#   LINES=200000 BUDGET_SECONDS=10 scripts/shutdown_time.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/triedb}
LINES=${LINES:-200000}
GEN_SEED=${GEN_SEED:-42}
SENDERS=${SENDERS:-4}
BATCH=${BATCH:-2000}
BUDGET_SECONDS=${BUDGET_SECONDS:-10}
# How long to wait for the stop before giving up on it. Far past the budget: the number is
# worth having even when it fails.
STOP_TIMEOUT_SECONDS=${STOP_TIMEOUT_SECONDS:-1800}

# Another build can be measured by naming its binary; the default is built first.
[ "$BIN" != ./target/release/triedb ] || cargo build --release --quiet || exit 1

mkdir -p testdata/live-door
INPUT=testdata/live-door/${LINES}-seed${GEN_SEED}.ndjson
[ -s "$INPUT" ] || "$BIN" gen-logs --lines "$LINES" --seed "$GEN_SEED" > "$INPUT"

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# One sender: its share of the bodies, once, down one keep-alive connection.
cat > "$WORK/send.py" <<'PY'
import http.client, json, sys

port, path, batch, share, of = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
lines = open(path, "rb").read().split(b"\n")
if lines and lines[-1] == b"":
    lines.pop()
bodies = [b"\n".join(lines[i:i + batch]) + b"\n" for i in range(0, len(lines), batch)][share::of]
conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=900)
ingested = 0
for body in bodies:
    conn.request("POST", "/ingest", body, {"Content-Type": "application/x-ndjson"})
    response = conn.getresponse()
    text = response.read()
    if response.status != 200:
        print(f"ingest answered {response.status}: {text[:300]!r}", file=sys.stderr)
        sys.exit(1)
    ingested += json.loads(text)["ingested"]
print(ingested)
PY

stat_field() {  # stat_field <dotted path>
  curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(a)' "$1" 2>/dev/null
}

count_all() {
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

PORT=""
start() {  # start <log> [serve flags...]
  local log=$1
  shift
  "$BIN" serve --port 0 --data-dir "$WORK/db" "$@" 2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 1200); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$log" >&2; return 1; fi
}

now() { python3 -c 'import time; print(time.time())'; }

start "$WORK/run1.log" --row-group-timeout-ms 86400000 || exit 1
pids=()
for k in $(seq 0 $((SENDERS - 1))); do
  python3 "$WORK/send.py" "$PORT" "$INPUT" "$BATCH" "$k" "$SENDERS" > "$WORK/sent.$k" &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || { echo "a sender failed" >&2; exit 1; }; done
SENT=$(cat "$WORK"/sent.* | awk '{s += $1} END {print s + 0}')
BUFFERED=$(stat_field storage.buffered_rows)
HELD=$(stat_field storage.buffered_bytes)
RSS=$(ps -o rss= -p "$PID" | tr -d ' ')
echo "ingested             $SENT of $LINES lines"
echo "buffered before stop $BUFFERED rows, held $((HELD / 1048576))MB by the store's estimate, rss $((RSS / 1024))MB"

# 2. The stop, timed from the signal to the exit.
START=$(now)
kill -TERM "$PID"
STOPPED=no
for _ in $(seq 1 $((STOP_TIMEOUT_SECONDS * 20))); do
  kill -0 "$PID" 2>/dev/null || { STOPPED=yes; break; }
  sleep 0.05
done
ELAPSED=$(python3 -c 'import sys, time; print("%.2f" % (time.time() - float(sys.argv[1])))' "$START")
if [ "$STOPPED" = yes ]; then
  wait "$PID"
  CODE=$?
else
  CODE="(still running)"
fi
PID=""
echo "SIGTERM to exit      ${ELAPSED}s, exit code $CODE"
grep -E 'triedb: (final flush|close stores|exit)' "$WORK/run1.log" | sed 's/^/  /'
FILES=$(find "$WORK/db/data" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "data files           $FILES"

# 3. What a fresh process reads back.
start "$WORK/run2.log" || exit 1
AFTER=$(count_all)
echo "COUNT LOG {} restart $AFTER"
kill -TERM "$PID"; wait "$PID" 2>/dev/null; PID=""

echo
FAIL=0
[ "$BUFFERED" = "$LINES" ] ||
  { echo "FAIL $BUFFERED rows were buffered before the stop, want all $LINES"; FAIL=1; }
[ "$CODE" = "0" ] || { echo "FAIL the clean stop exited $CODE"; FAIL=1; }
[ "$AFTER" = "$LINES" ] || { echo "FAIL COUNT LOG {} is $AFTER after the restart, want $LINES"; FAIL=1; }
python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) < float(sys.argv[2]) else 1)' \
  "$ELAPSED" "$BUDGET_SECONDS" ||
  { echo "FAIL the stop took ${ELAPSED}s, want under ${BUDGET_SECONDS}s"; FAIL=1; }
if [ "$FAIL" = "0" ]; then
  echo "PASS $LINES buffered rows written and sealed in ${ELAPSED}s, and all $AFTER came back"
  exit 0
fi
exit 1
