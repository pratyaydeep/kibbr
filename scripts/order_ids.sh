#!/usr/bin/env bash
# What goes in comes back out. Five rows of ten-digit ids, one `AX-99213`, five more,
# each line flushed to its own row group. `MATCH` must return the eleven strings exactly
# as ingested.
#
# Twice, over two sets of ids, because the epoch window puts them on different rungs and
# only one of them reaches the bug this exists for:
#
#   45000001NN   names 2112, outside the window, so it is an Int64 that widens to text
#   17053121NN   names 2024, inside it, so it is a Datetime that widens to text
#
# Before `dt_form` was read back, the dated set came back as `2024-01-15T09:50:0NZ`: the
# ten-digit rung read them as a Datetime, the column widened to String on `AX-99213`, and
# the chunks already on disk were rendered as canonical RFC 3339 - `dt_form` was written
# and never read. The undated set cannot show that, which is why both are here.
#
#   scripts/order_ids.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/kibbr

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

# One event value per set, so each is its own template with its own column.
round() { # round <event> <id>...
  local event=$1 id
  shift
  printf '%s\n' "$@" | sort > "$WORK/want.$event.txt"
  for id in "$@"; do
    code=$(curl -s -o "$WORK/last.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" \
           --data-binary "{\"event\":\"$event\",\"order_id\":\"$id\"}")
    [ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/last.json" >&2; exit 1; }
    sleep 0.2
  done
  sleep 0.3

  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       -d "{\"q\":\"MATCH LOG {\\\"event\\\":\\\"$event\\\"}\"}" \
    | python3 -c '
import json, sys
rows = json.load(sys.stdin)["rows"]
for r in rows:
    v = r.get("order_id")
    print(v if isinstance(v, str) else json.dumps(v))' | sort > "$WORK/got.$event.txt"

  local files
  files=$(find "$WORK/data" -name 'seg_*.col' | wc -l | tr -d ' ')
  echo "$event: $# lines, $files segment file(s) so far"
  if diff "$WORK/want.$event.txt" "$WORK/got.$event.txt" > "$WORK/diff.$event.txt"; then
    echo "$event: $# strings in, the same $# out"
    return 0
  fi
  echo "FAIL: $event: MATCH returned strings that were never ingested (< ingested, > returned):"
  cat "$WORK/diff.$event.txt"
  return 1
}

status=0
round undated 4500000101 4500000102 4500000103 4500000104 4500000105 AX-99213 \
              4500000107 4500000108 4500000109 4500000110 4500000111 || status=1
round dated   1705312101 1705312102 1705312103 1705312104 1705312105 AX-99213 \
              1705312107 1705312108 1705312109 1705312110 1705312111 || status=1
exit $status
