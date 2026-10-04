#!/usr/bin/env bash
# kill -9 must leave no orphan, and every closed row group must come back.
#
# Per round: start with a 200ms row group timeout, ingest 2,000 lines, wait 2s so every
# row group has closed, read /stats, kill -9, restart. The restart must print no orphan
# warning, and `COUNT LOG {}` must equal the rows /stats said were in closed row groups
# before the kill. Before the catalog a kill -9 lost the trie.json that
# only a clean stop writes, so every template created since the last clean stop became an
# orphan directory and the count came back 0.
#
# The rows are all in closed row groups before the kill, so this is about the catalog and
# the files. A kill with rows still in memory is scripts/crash_test.sh's: the write-ahead
# log puts those back.
#
#   ROUNDS=20 NLINES=2000 scripts/crash_templates.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/kibbr
ROUNDS=${ROUNDS:-20}
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-42}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"

PORT=""
start() {  # start <logfile> <data-dir>
  "$BIN" serve --port 0 --data-dir "$2" --row-group-timeout-ms 200 2>"$1" &
  PID=$!
  PORT=""
  # Up to 60s: a restart after kill -9 re-seals every unsealed segment first, one fsync
  # each, and with a segment per (template, day) that is hundreds of files.
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; cat "$1" >&2; return 1; fi
}

stat() {  # stat <port> <field>
  curl -s "http://127.0.0.1:$1/stats" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["storage"][sys.argv[1]])' "$2"
}

count_all() {  # count_all <port>
  curl -s -X POST "http://127.0.0.1:$1/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
    | python3 -c 'import json,sys; a=json.load(sys.stdin); print(a.get("count", "error: %s" % a))'
}

failed=0
for round in $(seq 1 "$ROUNDS"); do
  data="$WORK/data.$round"
  start "$WORK/run.$round.log" "$data" || exit 1
  code=$(curl -s -o /dev/null -w '%{http_code}' \
         -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
  [ "$code" = "200" ] || { echo "round $round: ingest answered $code" >&2; exit 1; }
  sleep 2
  closed=$(stat "$PORT" rows)
  buffered=$(stat "$PORT" buffered_rows)
  kill -KILL "$PID"; wait "$PID" 2>/dev/null; PID=""

  start "$WORK/restart.$round.log" "$data" || exit 1
  after=$(count_all "$PORT")
  kill -TERM "$PID"; wait "$PID" 2>/dev/null; PID=""

  orphans=$(grep -c "no template refers" "$WORK/restart.$round.log")
  verdict=ok
  if [ "$orphans" != "0" ] || [ "$after" != "$closed" ]; then verdict=FAIL; failed=$((failed + 1)); fi
  printf 'round %2d  closed %4s  buffered %4s  after restart %4s  orphan warnings %s  %s\n' \
    "$round" "$closed" "$buffered" "$after" "$orphans" "$verdict"
  [ "$verdict" = FAIL ] && grep -E "WARN" "$WORK/restart.$round.log" | head -3 | sed 's/^/    /'
done

echo
if [ "$failed" -eq 0 ]; then
  echo "$ROUNDS rounds: every closed row group survived kill -9, no orphans"
  exit 0
fi
echo "$failed of $ROUNDS rounds FAILED"
exit 1
