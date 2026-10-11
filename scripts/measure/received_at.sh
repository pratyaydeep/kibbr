#!/usr/bin/env bash
# The system column is not a key. Three checks:
#
# - the template count for 20,000 `--seed 42` lines is what it was before the system
#   column existed: 823, measured on a build without it - `\_received_at` is added after
#   fingerprinting, so it can neither make a template nor split one;
# - a line that carries its own `_received_at` key is ingested and returned unchanged, and
#   the system value is left out of that row;
# - a line with no time column at all comes back with a `_received_at` the store gave it.
#
#   scripts/measure/received_at.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/kibbr
NLINES=${NLINES:-20000}
GEN_SEED=${GEN_SEED:-42}
EXPECTED_TEMPLATES=${EXPECTED_TEMPLATES:-823}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() { [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"

"$BIN" serve --port 0 --storage memory 2>"$WORK/serve.log" &
PID=$!
PORT=""
for _ in $(seq 1 80); do
  PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$WORK/serve.log" 2>/dev/null | head -1 | sed 's/.*://')
  [ -n "$PORT" ] && break
  kill -0 "$PID" 2>/dev/null || break
  sleep 0.1
done
[ -n "$PORT" ] || { echo "server did not start:" >&2; cat "$WORK/serve.log" >&2; exit 1; }

post() { curl -s -o "$WORK/last.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "$1"; }
ask() {  # ask <trieql>
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       -d "{\"q\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1")}"
}

code=$(post "@$INPUT")
[ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/last.json" >&2; exit 1; }
templates=$(curl -s "http://127.0.0.1:$PORT/templates" | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')

own='{"event":"own_key","_received_at":"2019-05-05T05:05:05Z","n":1}'
none='{"event":"no_time_key","n":2}'
code=$(post "$own"); [ "$code" = "200" ] || { echo "ingest answered $code" >&2; exit 1; }
code=$(post "$none"); [ "$code" = "200" ] || { echo "ingest answered $code" >&2; exit 1; }

status=0
echo "input: $NLINES lines, $templates templates (expected $EXPECTED_TEMPLATES)"
[ "$templates" = "$EXPECTED_TEMPLATES" ] || { echo "FAIL: the template count moved"; status=1; }

ask 'MATCH LOG {"event":"own_key"}' | python3 -c '
import json, sys
rows = json.load(sys.stdin)["rows"]
row = rows[0] if rows else {}
print("own key      ", json.dumps(row, sort_keys=True))
ok = row.get("_received_at") == "2019-05-05T05:05:05Z" and sorted(row) == ["_received_at", "event", "n"]
sys.exit(0 if ok else 1)' || { echo "FAIL: the ingested _received_at did not come back unchanged"; status=1; }

ask 'MATCH LOG {"event":"no_time_key"}' | python3 -c '
import json, sys, re
rows = json.load(sys.stdin)["rows"]
row = rows[0] if rows else {}
print("no time key  ", json.dumps(row, sort_keys=True))
ok = bool(re.match(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d", str(row.get("_received_at"))))
sys.exit(0 if ok else 1)' || { echo "FAIL: a row with no time column has no _received_at"; status=1; }

[ "$status" -eq 0 ] && echo "the system column is not a key"
exit $status
