#!/usr/bin/env bash
# No seal flapping, and no `mixed`. A template whose date column
# alternates between RFC 3339 and ten-digit epoch 1,000 times, ingested in bodies small
# enough that many row groups close, must end with at most 2 segment files in that
# partition; after a restart every row is still inside a window over its day and every
# `ts` comes back in the form it arrived in.
#
# Before M5.6b every change of `dt_form` sealed the segment, in either direction: 130
# partitions became 34,892 segment files on the real binary.
#
#   scripts/date_forms.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
LINES=${LINES:-1000}
BODIES=${BODIES:-20}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() { [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

PORT=""
start() {  # start <logfile> - its own file, so a restart cannot read the old banner's port
  local log=$1
  "$BIN" serve --port 0 --data-dir "$WORK/data" --flush-interval-ms 50 --row-group-timeout-ms 50 \
    2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 80); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  [ -n "$PORT" ] || { echo "server did not start:" >&2; cat "$log" >&2; return 1; }
  for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    sleep 0.1
  done
  echo "server did not answer /health:" >&2; cat "$log" >&2; return 1
}
ask() {  # ask <trieql>
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       -d "{\"q\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")}"
}

# One instant, two spellings, alternating. Same day either way, so one partition.
python3 - "$LINES" > "$WORK/input.ndjson" <<'PY'
import sys
n = int(sys.argv[1])
for i in range(n):
    ts = '"2024-01-15T10:30:00Z"' if i % 2 == 0 else '"1705314600"'
    print('{"event":"tick","ts":%s,"n":%d}' % (ts, i))
PY

start "$WORK/serve1.log" || exit 1
per=$((LINES / BODIES))
for b in $(seq 0 $((BODIES - 1))); do
  sed -n "$((b * per + 1)),$(((b + 1) * per))p" "$WORK/input.ndjson" > "$WORK/body.ndjson"
  code=$(curl -s -o "$WORK/last.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" \
         --data-binary "@$WORK/body.ndjson")
  [ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/last.json" >&2; exit 1; }
  # Long enough for the row group to close before the next body arrives.
  sleep 0.15
done

kill -TERM "$PID"; wait "$PID" 2>/dev/null; PID=""
files=$(find "$WORK/data" -path '*/2024-01-15/seg_*.col' | wc -l | tr -d ' ')
mixed=$(grep -c 'mixed' "$WORK/serve1.log" || true)

start "$WORK/serve2.log" || exit 1
status=0
count=$(ask 'COUNT LOG {"event":"tick"} BETWEEN "2024-01-15T00:00:00Z" AND "2024-01-16T00:00:00Z"' \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("count","?"))')
ask 'MATCH LOG {"event":"tick"} LIMIT 100000' | python3 -c '
import json, sys
rows = json.load(sys.stdin)["rows"]
got = sorted((r["n"], r["ts"]) for r in rows)
want = [(i, "2024-01-15T10:30:00Z" if i % 2 == 0 else "1705314600") for i in range(len(rows))]
bad = [(g, w) for g, w in zip(got, want) if g != w]
print("rows back: %d, first mismatch: %s" % (len(rows), bad[0] if bad else "none"))
sys.exit(1 if bad or not rows else 0)' || { echo "FAIL: a ts came back in a form it did not arrive in"; status=1; }

echo "$LINES lines in $BODIES bodies: $files segment file(s) in 2024-01-15, windowed count after restart $count"
[ "$files" -le 2 ] || { echo "FAIL: the alternating forms made $files segment files"; status=1; }
[ "$count" = "$LINES" ] || { echo "FAIL: the window over the day holds $count of $LINES rows"; status=1; }
[ "$mixed" -eq 0 ] || { echo "FAIL: a mixed chunk was written"; status=1; }
[ "$status" -eq 0 ] && echo "one seal, then text; every value back as it arrived"
exit $status
