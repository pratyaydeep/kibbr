#!/usr/bin/env bash
# What goes in comes back out, for the values the type ladder used to turn into another
# value: an instant past either end of what i64 nanoseconds hold, a second of 60, a tenth
# fraction digit, ten or thirteen digits with a leading zero - and the value either side
# of each of those edges.
#
# Each is ingested as a JSON string under a key of its own, so each is its own template
# with a column holding only it, and `MATCH LOG {}` must return every one as that same
# JSON string, character for character: once from the running server, and again after a
# restart has it read back from disk. A number, or a string with another character in it,
# is a difference.
#
# Before, `"9999-12-31T23:59:59Z"` came back as `2262-04-11T23:47:16.854775807Z`,
# `"2016-12-31T23:59:60Z"` as `2017-01-01T00:00:00Z`, `"...00.1234567891Z"` without its
# tenth digit, and `"0946684800"` as `2000-01-01T00:00:00Z`.
#
# Some still come back as another spelling of the same value, and fixing that is the
# store's job, not the ladder's. Digits with a leading zero are an `Int64`, and an
# `Int64` comes back as a JSON number without the zero: nothing records that the digits
# were a string or had one. Ten or thirteen digits read as a date come back as RFC 3339,
# because a column that stayed a date is printed that way; the rung that read them is
# kept, but only used once the column turns to text. Until the store prints back what it
# was given, this script lists those and exits 1.
#
#   scripts/round_trip.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() { [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

VALUES=(
  # Past either end of the nanosecond range, the two last instants it holds, and one
  # nanosecond past each.
  9999-12-31T23:59:59Z
  0001-01-01T00:00:00Z
  2262-04-11T23:47:16.854775807Z
  2262-04-11T23:47:16.854775808Z
  1677-09-21T00:12:43.145224192Z
  1677-09-21T00:12:43.145224191Z
  # A second of 60, with and without an offset, and either side of it.
  2016-12-31T23:59:59.999999999Z
  2016-12-31T23:59:60Z
  2016-12-31T23:59:60.999999999Z
  1990-12-31T15:59:60-08:00
  2024-01-15T10:30:59Z
  2024-01-15T10:30:60Z
  2024-01-15T10:30:61Z
  # Nine fraction digits, and ten.
  2024-01-15T10:30:00.123456789Z
  2024-01-15T10:30:00.1234567891Z
  2024-01-15T10:30:00.1234567890Z
  # A leading zero on ten and thirteen digits, either side of it, and a lone zero.
  0946684800
  0946684800000
  0999999999
  1000000000
  0999999999999
  1000000000000
  0
)

printf '%s\n' "${VALUES[@]}" | python3 -c '
import json, sys
for i, v in enumerate(sys.stdin.read().splitlines(), 1):
    print(json.dumps({f"k{i:02d}": v}))' > "$WORK/body.ndjson"

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
}

stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
}

ask() {  # ask <outfile>
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       -d '{"q":"MATCH LOG {}"}' > "$1"
}

start "$WORK/serve.log" || exit 1
code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" \
       --data-binary @"$WORK/body.ndjson")
[ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/ingest.json" >&2; exit 1; }
sleep 0.5
ask "$WORK/live.json"
stop
start "$WORK/serve2.log" || exit 1
ask "$WORK/restart.json"
stop

python3 - "$WORK/body.ndjson" "$WORK/live.json" "$WORK/restart.json" <<'EOF'
import json, sys

want = {}
for line in open(sys.argv[1]):
    want.update(json.loads(line))

differ = set()
for name, path in (("live", sys.argv[2]), ("after restart", sys.argv[3])):
    got = {}
    for row in json.load(open(path))["rows"]:
        got.update({k: v for k, v in row.items() if k in want})
    for key, value in want.items():
        if key not in got:
            print(f"{name}: {key} {json.dumps(value)} was not returned")
            differ.add(key)
        elif got[key] != value:
            print(f"{name}: {key} {json.dumps(value)} came back as {json.dumps(got[key])}")
            differ.add(key)

print(f"{len(want)} values, {len(differ)} differences")
sys.exit(1 if differ else 0)
EOF
