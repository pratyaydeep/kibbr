#!/usr/bin/env bash
# Every row has a time. Over 20,000 `--seed 42` lines, `COUNT LOG {}` must equal
# the same count over a window wide enough to hold everything, and no `undated` directory
# may exist once the rows are on disk.
#
# Before per-template time sources the time column was the literal name `ts`: 148 of 870
# templates had no time at all, their rows went to `undated`, and every time-range query
# skipped them.
#
#   NLINES=20000 scripts/check/time_columns.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/kibbr
NLINES=${NLINES:-20000}
GEN_SEED=${GEN_SEED:-42}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() { [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"

# Port 0 and the bound port read back off the banner, as store_agree.sh does.
"$BIN" serve --port 0 --data-dir "$WORK/data" 2>"$WORK/serve.log" &
PID=$!
PORT=""
for _ in $(seq 1 80); do
  PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$WORK/serve.log" 2>/dev/null | head -1 | sed 's/.*://')
  [ -n "$PORT" ] && break
  kill -0 "$PID" 2>/dev/null || break
  sleep 0.1
done
[ -n "$PORT" ] || { echo "server did not start:" >&2; cat "$WORK/serve.log" >&2; exit 1; }

code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
[ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/ingest.json" >&2; exit 1; }

count_of() {  # count_of <trieql>
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       -d "{\"q\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("count", "?"))'
}

all=$(count_of 'COUNT LOG {}')
windowed=$(count_of 'COUNT LOG {} BETWEEN "2020-01-01T00:00:00Z" AND "2030-01-01T00:00:00Z"')
arrival=$(curl -s "http://127.0.0.1:$PORT/templates" | python3 -c '
import json, sys
t = json.load(sys.stdin)["templates"]
print("%d of %d templates on arrival time" % (sum(1 for x in t if x.get("time_source") == "arrival"), len(t)))')

# A clean stop writes every buffered row out, so the directory check sees the partitions
# the rows were actually cut into rather than an empty leaf directory.
kill -TERM "$PID"; wait "$PID" 2>/dev/null; PID=""
undated=$(find "$WORK/data" -type d -name undated | wc -l | tr -d ' ')

echo "input: $NLINES lines, $arrival"
echo "COUNT LOG {}                                    $all"
echo "COUNT LOG {} BETWEEN 2020-01-01 AND 2030-01-01  $windowed"
echo "undated directories                             $undated"

status=0
[ "$all" = "$windowed" ] || { echo "FAIL: the window loses $((all - windowed)) rows"; status=1; }
[ "$undated" = "0" ] || { echo "FAIL: $undated undated director(ies) exist"; status=1; }
[ "$status" -eq 0 ] && echo "every row has a time"
exit $status
