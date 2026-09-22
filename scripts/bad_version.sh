#!/usr/bin/env bash
# A segment whose header version this build does not write must stop the start.
#
# As built the server WARNs about it and serves on, answering `count 0` for the leaf that
# holds it - a guess about bytes it cannot read, delivered as an answer. A segment whose
# header version is not 2 is logged as an error naming the path, and the process exits
# non-zero, before the listener opens.
#
# What this asserts:
#
#   1. With byte 4 of one segment set to 3, `triedb serve` exits non-zero within 5s and
#      its stderr names that segment's path.
#   2. It exits that way, naming the segment, even when its port is already held by
#      another process - so the check ran before the listener was opened. A server that
#      bound first would fail on the port instead and never name the segment.
#   3. With the byte set back to 2, the server starts and every row is there.
#
#   NLINES=100 scripts/bad_version.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/triedb}
NLINES=${NLINES:-100}
GEN_SEED=${GEN_SEED:-42}
EXIT_WITHIN_S=${EXIT_WITHIN_S:-5}

cargo build --release --quiet || exit 1

WORK=$(mktemp -d)
PID=""
HOLDER=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  [ -n "$HOLDER" ] && kill -KILL "$HOLDER" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"
DATA="$WORK/data"

PORT=""
start() {  # start <logfile>
  "$BIN" serve --port 0 --data-dir "$DATA" 2>"$1" &
  PID=$!
  PORT=""
  for _ in $(seq 1 300); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$1" >&2; return 1; fi
}

STOP_CODE=""
stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  STOP_CODE=$?
  PID=""
}

count() {
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

# Start on a bad segment and wait for the process to end. `EXITED` is its code, or
# `running` if it was still up when the time ran out - and then it is killed and the count
# it was answering is kept, which is what the as-built server does with a leaf it cannot
# read.
EXITED=""
RUNNING_COUNT=""
start_expecting_refusal() {  # start_expecting_refusal <logfile> <port>
  "$BIN" serve --port "$2" --data-dir "$DATA" 2>"$1" &
  PID=$!
  EXITED="running"
  for _ in $(seq 1 $((EXIT_WITHIN_S * 10))); do
    if ! kill -0 "$PID" 2>/dev/null; then
      wait "$PID" 2>/dev/null
      EXITED=$?
      PID=""
      return
    fi
    sleep 0.1
  done
  PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
  [ -n "$PORT" ] && RUNNING_COUNT=$(count)
  kill -KILL "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
}

flip() {  # flip <file> <byte, as a printf escape>
  printf "$2" | dd of="$1" bs=1 seek=4 conv=notrunc 2>/dev/null
}

# ── A clean store ────────────────────────────────────────────────────────────────────
start "$WORK/run1.log" || exit 1
curl -s -o /dev/null -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT"
BEFORE=$(count)
stop
echo "ingested             $BEFORE row(s), exit $STOP_CODE"

SEGMENT=$(find "$DATA" -name 'seg_*.col' | sort | head -1)
[ -n "$SEGMENT" ] || { echo "FAIL no segment was written"; exit 1; }
echo "segment              ${SEGMENT#"$WORK"/}"

# ── Version 3 ────────────────────────────────────────────────────────────────────────
flip "$SEGMENT" '\x03'
echo "byte 4               $(xxd -s 4 -l 1 -p "$SEGMENT")"
start_expecting_refusal "$WORK/run2.log" 0
REFUSED=$EXITED
NAMED=no
grep -qF "$SEGMENT" "$WORK/run2.log" && NAMED=yes
ANSWERING=""
[ -n "$RUNNING_COUNT" ] && ANSWERING=", answering COUNT LOG {} = $RUNNING_COUNT"
echo "start                exit $REFUSED$ANSWERING"
echo "path named           $NAMED"
grep -F "$SEGMENT" "$WORK/run2.log" | head -2 | sed "s|[^ ]*$WORK/||g; s/^/  /"

# Before the listener: the same start with its port already held by something else. A
# check that ran after the bind would lose to the bind's own error. Deterministic, where a
# probe dialling the port would have to land inside however long the check takes.
HELD_PORT=""
python3 -c 'import socket, time
s = socket.socket()
s.bind(("127.0.0.1", 0))
s.listen(1)
print(s.getsockname()[1], flush=True)
time.sleep(60)' > "$WORK/holder" &
HOLDER=$!
for _ in $(seq 1 50); do
  HELD_PORT=$(head -1 "$WORK/holder" 2>/dev/null)
  [ -n "$HELD_PORT" ] && break
  sleep 0.1
done
RUNNING_COUNT=""
start_expecting_refusal "$WORK/run3.log" "$HELD_PORT"
HELD_EXIT=$EXITED
HELD_NAMED=no
grep -qF "$SEGMENT" "$WORK/run3.log" && HELD_NAMED=yes
HELD_BIND=no
grep -qiE 'cannot listen|in use' "$WORK/run3.log" && HELD_BIND=yes
kill -KILL "$HOLDER" 2>/dev/null
wait "$HOLDER" 2>/dev/null
HOLDER=""
echo "port $HELD_PORT held      exit $HELD_EXIT, path named $HELD_NAMED, bind error $HELD_BIND"

# ── Version 2 again ──────────────────────────────────────────────────────────────────
flip "$SEGMENT" '\x02'
RESTORED=no
if start "$WORK/run4.log"; then
  RESTORED=yes
  AFTER=$(count)
  stop
fi
echo "restored             started $RESTORED, COUNT LOG {} ${AFTER:-?}, exit ${STOP_CODE:-?}"

echo
FAIL=0
if [ "$REFUSED" = "running" ]; then
  echo "FAIL the server was still running ${EXIT_WITHIN_S}s after starting on a version 3 segment"; FAIL=1
elif [ "$REFUSED" = "0" ]; then
  echo "FAIL the server exited 0 on a version 3 segment"; FAIL=1
fi
[ "$NAMED" = "yes" ] || { echo "FAIL stderr did not name $SEGMENT"; FAIL=1; }
[ -n "$HELD_PORT" ] || { echo "FAIL could not hold a port for the before-the-listener check"; FAIL=1; }
if [ "$HELD_EXIT" = "running" ] || [ "$HELD_EXIT" = "0" ] || [ "$HELD_NAMED" != "yes" ] || [ "$HELD_BIND" != "no" ]; then
  echo "FAIL with its port held the start did not fail on the segment first (exit $HELD_EXIT, named $HELD_NAMED, bind error $HELD_BIND)"; FAIL=1
fi
[ "$RESTORED" = "yes" ] || { echo "FAIL the server did not start with the byte restored"; FAIL=1; }
[ "${AFTER:-}" = "$BEFORE" ] || { echo "FAIL COUNT LOG {} is ${AFTER:-nothing} after the restore, want $BEFORE"; FAIL=1; }
[ "${STOP_CODE:-}" = "0" ] || { echo "FAIL the stop after the restore exited ${STOP_CODE:-?}"; FAIL=1; }
if [ "$FAIL" = "0" ]; then
  echo "PASS a version 3 segment stopped the start before the listener, by path, and version 2 starts clean"
  exit 0
fi
exit 1
