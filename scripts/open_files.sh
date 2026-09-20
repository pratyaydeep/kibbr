#!/usr/bin/env bash
# A clean shutdown must not lose rows, however few file descriptors there are.
#
# 20,000 `--seed 42` lines are 823 templates over 31 days, so nearly every line opens a
# (template, day) partition of its own - about 20,000 of them. As built each partition
# holds an open segment file for the life of the process and nothing caps that, so the
# operating system's limit arrives first: the flush fails, the half-created file stays,
# every retry after it fails with `File exists`, the rows never leave memory, and the
# clean shutdown exits 0 without them. Of 20,000 accepted rows, 18,356, 12,940 and 10,625
# came back at three template counts.
#
# What this asserts, in order:
#
#   1. `buffered_rows` reaches 0 while the server is running. The rows were accepted, the
#      row group timeout has long expired, and there is nothing left for them to wait for.
#   2. The clean stop exits 0 - nothing was lost, so there is nothing to exit non-zero
#      about.
#   3. A restart answers `COUNT LOG {}` = 20,000.
#
# `ulimit -n` is set in this shell, so the server inherits it. The cap on open segments is
# set below it: a cap above the descriptor limit is not a cap.
#
#   NLINES=20000 FD_LIMIT=256 scripts/open_files.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
NLINES=${NLINES:-20000}
GEN_SEED=${GEN_SEED:-42}
FD_LIMIT=${FD_LIMIT:-256}
# Well under the descriptor limit: the listener, the accepted sockets, the catalog and
# stdio all want one too, and a cap that spends every descriptor on segments leaves none
# for the request that is being answered.
MAX_OPEN_SEGMENTS=${MAX_OPEN_SEGMENTS:-64}
# Floored at the flush interval (src/config.rs), so the effective timeout is 500ms. Named
# here as 200 because that is what the milestone's check asks for.
ROW_GROUP_TIMEOUT_MS=${ROW_GROUP_TIMEOUT_MS:-200}
# How long to wait for the buffer to drain. Every partition costs a footer and an fsync
# on the way out, and there are about 20,000 of them.
DRAIN_SECONDS=${DRAIN_SECONDS:-300}

cargo build --release --quiet || exit 1

ulimit -n "$FD_LIMIT" || { echo "could not set ulimit -n $FD_LIMIT" >&2; exit 1; }
echo "ulimit -n            $(ulimit -n)"
echo "max open segments    $MAX_OPEN_SEGMENTS"
echo "row group timeout    ${ROW_GROUP_TIMEOUT_MS}ms (floored at the 500ms flush interval)"

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT=testdata/m5_6c/${NLINES}-seed${GEN_SEED}.ndjson
mkdir -p testdata/m5_6c
[ -f "$INPUT" ] || "$BIN" gen-logs --lines "$NLINES" --templates 12 --seed "$GEN_SEED" > "$INPUT"

PORT=""
start() {  # start <logfile>
  TRIEDB__STORAGE__MAX_OPEN_SEGMENTS="$MAX_OPEN_SEGMENTS" \
    "$BIN" serve --port 0 --data-dir "$WORK/data" \
      --row-group-timeout-ms "$ROW_GROUP_TIMEOUT_MS" 2>"$1" &
  PID=$!
  PORT=""
  # Up to 120s: a restart reopens every segment file that was written, and there are
  # thousands of them.
  for _ in $(seq 1 1200); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$1" >&2; return 1; fi
}

# SIGTERM, and the exit code it produced. A shutdown that could not write every row says
# how many it lost and exits non-zero, so the code is the assertion.
STOP_CODE=""
stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  STOP_CODE=$?
  PID=""
}

stat_field() {  # stat_field <port> <dotted path>
  curl -s "http://127.0.0.1:$1/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(a)' "$2" 2>/dev/null
}

count_all() {  # count_all <port>
  curl -s -X POST "http://127.0.0.1:$1/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

echo
start "$WORK/run1.log" || exit 1

code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
[ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/ingest.json" >&2; exit 1; }
ACCEPTED=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["ingested"])' < "$WORK/ingest.json")
echo "ingested             $ACCEPTED of $NLINES lines"

# 1. The buffer must drain. Every row has been accepted and the row group timeout has
#    expired many times over, so a buffer that is still not empty is a flush that cannot
#    complete - which is the bug, whatever the count says afterwards.
DRAINED=no
LAST=""
for _ in $(seq 1 $((DRAIN_SECONDS * 2))); do
  LAST=$(stat_field "$PORT" storage.buffered_rows)
  [ "$LAST" = "0" ] && { DRAINED=yes; break; }
  sleep 0.5
done
FAILURES=$(stat_field "$PORT" storage.write_failures)
OPEN_STORES=$(stat_field "$PORT" storage.stores_open)
SEGMENTS=$(find "$WORK/data" -name 'seg_*.col' 2>/dev/null | wc -l | tr -d ' ')
echo "buffered_rows        $LAST (drained: $DRAINED)"
echo "write_failures       $FAILURES"
echo "stores open          $OPEN_STORES"
echo "segment files        $SEGMENTS"
[ -n "$(grep -c 'File exists' "$WORK/run1.log")" ] &&
  echo "File exists errors   $(grep -c 'File exists' "$WORK/run1.log")"

BEFORE=$(count_all "$PORT")
echo "COUNT LOG {} live    $BEFORE"

# 2. The clean stop.
stop
echo "shutdown exit code   $STOP_CODE"
grep -E 'final flush|FAILED|not written|exit ' "$WORK/run1.log" | sed 's/^/  /'

# 3. And what a fresh process reads back off the disk.
echo
start "$WORK/run2.log" || exit 1
AFTER=$(count_all "$PORT")
echo "COUNT LOG {} restart $AFTER"
stop

echo
FAIL=0
[ "$DRAINED" = "yes" ] || { echo "FAIL buffered_rows never reached 0 (last: $LAST)"; FAIL=1; }
[ "$STOP_CODE" = "0" ] || { echo "FAIL the clean stop exited $STOP_CODE"; FAIL=1; }
[ "$AFTER" = "$NLINES" ] || { echo "FAIL COUNT LOG {} is $AFTER after the restart, want $NLINES"; FAIL=1; }
if [ "$FAIL" = "0" ]; then
  echo "PASS $NLINES lines ingested under ulimit -n $FD_LIMIT, drained, and all $AFTER came back"
  exit 0
fi
exit 1
