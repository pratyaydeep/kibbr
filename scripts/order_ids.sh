#!/usr/bin/env bash
# What goes in comes back out. Five rows `"order_id":"45000001NN"`, one
# `AX-99213`, five more, each line flushed to its own row group. `MATCH` must return the
# eleven strings exactly as ingested.
#
# Before M5.6b the first five came back as `2112-08-07T08:02:0NZ`: rung 2 read the ten
# digits as a Datetime, the column widened to String on `AX-99213`, and the chunks already
# on disk were rendered as canonical RFC 3339 - `dt_form` was written and never read.
#
#   scripts/order_ids.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() { [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

# The row group timeout is floored at the flush interval, so 50ms is the shortest
# both can be; the sleep between lines is what puts each one in its own row group.
"$BIN" serve --port 0 --data-dir "$WORK/data" --flush-interval-ms 50 --row-group-timeout-ms 50 \
  2>"$WORK/serve.log" &
PID=$!
PORT=""
for _ in $(seq 1 80); do
  PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$WORK/serve.log" 2>/dev/null | head -1 | sed 's/.*://')
  [ -n "$PORT" ] && break
  kill -0 "$PID" 2>/dev/null || break
  sleep 0.1
done
[ -n "$PORT" ] || { echo "server did not start:" >&2; cat "$WORK/serve.log" >&2; exit 1; }

IDS=(4500000101 4500000102 4500000103 4500000104 4500000105 AX-99213
     4500000107 4500000108 4500000109 4500000110 4500000111)
for id in "${IDS[@]}"; do
  code=$(curl -s -o "$WORK/last.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" \
         --data-binary "{\"event\":\"order\",\"order_id\":\"$id\"}")
  [ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/last.json" >&2; exit 1; }
  sleep 0.2
done
sleep 0.3

printf '%s\n' "${IDS[@]}" | sort > "$WORK/want.txt"
curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
     -d '{"q":"MATCH LOG {\"event\":\"order\"}"}' \
  | python3 -c '
import json, sys
rows = json.load(sys.stdin)["rows"]
for r in rows:
    v = r.get("order_id")
    print(v if isinstance(v, str) else json.dumps(v))' | sort > "$WORK/got.txt"

files=$(find "$WORK/data" -name 'seg_*.col' | wc -l | tr -d ' ')
echo "11 lines, $files segment file(s)"
if diff "$WORK/want.txt" "$WORK/got.txt" > "$WORK/diff.txt"; then
  echo "eleven strings in, the same eleven out"
  exit 0
fi
echo "FAIL: MATCH returned strings that were never ingested (< ingested, > returned):"
cat "$WORK/diff.txt"
exit 1
