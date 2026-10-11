#!/usr/bin/env bash
# A store that cannot write refuses nothing and loses nothing.
#
# This used to assert the opposite - a 503 for any body holding a row the store could not
# take, and a non-zero exit - and had to: with no write-ahead log a 200 meant "buffered",
# and a row the store dropped was nowhere. A line is now in the log, flushed, before it is
# acknowledged, so a store that then cannot take its row has failed at nothing the sender
# needs to hear about. The row waits in memory and in the log, and is stored when the
# store can write again or by the next start's replay.
#
# What this does:
#
#   1. Starts a server with a 500 ms row group timeout and sends NLINES lines, BATCH to a
#      body; waits for `buffered_rows` to reach 0, so every row is in a data file.
#   2. `chmod 000` on every day directory under data/, while the server runs, and sends
#      the same lines again. Data files are sealed at SEGMENT_MAX_BYTES, which is small:
#      a file already open would go on taking rows through its descriptor whatever its
#      directory's mode, and the store would not fail at all. Sealed, the second copy
#      needs a new file in a directory that will not take one.
#   3. Asserts: every body of the second copy is answered 200; `/stats` shows
#      `write_failures` above 0 and the store degraded; `/health` says degraded; the
#      server is still running.
#   4. Stops the server with the directories still locked. The exit code is 0: nothing
#      acknowledged went with the process. The second copy is now in the log and nowhere
#      else.
#   5. `chmod 755`, restart: the start puts the second copy back from the log, and
#      `COUNT LOG {}` is twice NLINES - every line of both copies, once.
#
#   NLINES=2000 scripts/fault/leaf_unwritable.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/kibbr
NLINES=${NLINES:-2000}
BATCH=${BATCH:-100}
GEN_SEED=${GEN_SEED:-42}
SEGMENT_MAX_BYTES=${SEGMENT_MAX_BYTES:-1024}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
DATA="$WORK/data"
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  chmod -R u+rwx "$WORK" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL $*"; exit 1; }

PORT=""
start() {  # start <logfile>
  "$BIN" serve --port 0 --data-dir "$DATA" --row-group-timeout-ms 500 \
    --segment-max-bytes "$SEGMENT_MAX_BYTES" 2>"$1" &
  PID=$!
  PORT=""
  for _ in $(seq 1 300); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  [ -n "$PORT" ] || { tail -20 "$1"; fail "the server did not start"; }
}

stat() {  # stat <python expression over s, the /stats object>
  curl -s "http://127.0.0.1:$PORT/stats" | python3 -c "import json,sys; s=json.load(sys.stdin); print($1)"
}

# Sends the lines, BATCH to a body, and prints how many bodies were answered 200.
send() {
  local ok=0 code
  for body in "$WORK"/body.*; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --data-binary @"$body" "http://127.0.0.1:$PORT/ingest")
    [ "$code" = 200 ] && ok=$((ok + 1))
  done
  echo "$ok"
}

"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$WORK/lines.ndjson"
split -l "$BATCH" "$WORK/lines.ndjson" "$WORK/body."
BODIES=$(ls "$WORK"/body.* | wc -l | tr -d ' ')

start "$WORK/first.log"
first=$(send)
[ "$first" = "$BODIES" ] || fail "the first copy: $first of $BODIES bodies answered 200"
for _ in $(seq 1 100); do
  [ "$(stat 's["storage"]["buffered_rows"]')" = 0 ] && break
  sleep 0.1
done
[ "$(stat 's["storage"]["buffered_rows"]')" = 0 ] || fail "rows were still buffered after 10s"
before=$(stat 's["storage"]["write_failures"]')
echo "first copy           $first/$BODIES bodies answered 200, every row in a data file, write_failures $before"

chmod 000 "$DATA"/data/*/
echo "locked               $(ls "$DATA/data" | wc -l | tr -d ' ') day directories, chmod 000"

second=$(send)
for _ in $(seq 1 100); do
  [ "$(stat 's["storage"]["write_failures"]')" -gt "$before" ] && break
  sleep 0.1
done
after=$(stat 's["storage"]["write_failures"]')
degraded=$(stat 's["storage"]["degraded"]')
health=$(curl -s "http://127.0.0.1:$PORT/health" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
echo "second copy          $second/$BODIES bodies answered 200"
echo "store                write_failures $before -> $after, degraded $degraded; /health says $health"
[ "$second" = "$BODIES" ] || fail "a body was refused because the store could not write"
[ "$after" -gt "$before" ] || fail "the store's writes did not fail: nothing was tested"
[ "$degraded" = True ] || fail "/stats does not say the store cannot write"
[ "$health" = degraded ] || fail "/health does not say the store cannot write"
kill -0 "$PID" 2>/dev/null || fail "the server exited"
echo "server               still running"

# Stopped with the directories still locked: the second copy is in the log, and only there.
kill -TERM "$PID"
wait "$PID"
code=$?
PID=""
echo "clean stop           exit $code, directories still locked"
[ "$code" = 0 ] || fail "the shutdown exited $code, want 0: nothing acknowledged was lost"

chmod 755 "$DATA"/data/*/
start "$WORK/second.log"
replayed=$(stat 's["storage"]["wal"]["replay"]["applied"]')
count=$(curl -s --data '{"q": "COUNT LOG {}"}' -H 'Content-Type: application/json' \
  "http://127.0.0.1:$PORT/query" | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')
echo "unlocked, restarted  $replayed line(s) put back from the log, COUNT LOG {} = $count"
[ "$replayed" -gt 0 ] || fail "the restart put nothing back from the log"
[ "$count" = $((NLINES * 2)) ] || fail "COUNT LOG {} is $count, want $((NLINES * 2))"

echo "PASS a store that could not write refused nothing, and all $count lines are stored once"
