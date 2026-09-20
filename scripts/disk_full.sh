#!/usr/bin/env bash
# A full disk must be loud, and it must not be total.
#
# As built it is both silent and total: every batch got a `200`, the server stayed up, the
# shutdown log said "90,000 rows written", and after a restart `COUNT LOG {}` was 0
# With no write-ahead log a `200` for a line the store cannot write is the lie, so from
# M5.6c the store says when it cannot write:
#
#   1. While the last flush tick failed, `POST /ingest` answers 503 with a `Retry-After`
#      and the last write error in the body, and `/health` reports `degraded`.
#   2. The server stays up. A disk that has gone away is not a reason to stop answering
#      queries about the rows that are already on it.
#   3. The shutdown exits non-zero and says how many rows it could not write.
#   4. Every row `/stats` reported as *written* before the disk filled is still there
#      after space is freed and the process restarted.
#
# The disk is a RAM disk so filling it is cheap and harmless. A ballast file takes most of
# it, which is what makes "free space" a single `rm` that leaves the data directory
# untouched - deleting rows to make room would be measuring the wrong thing.
#
#   DISK_MB=40 BALLAST_MB=20 scripts/disk_full.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
DISK_MB=${DISK_MB:-40}
BALLAST_MB=${BALLAST_MB:-20}
BATCH=${BATCH:-2000}
MAX_BATCHES=${MAX_BATCHES:-60}
GEN_SEED=${GEN_SEED:-42}
# Small, so the RAM disk fills on bytes rather than on descriptors - this script is about
# ENOSPC and nothing else.
MAX_OPEN_SEGMENTS=${MAX_OPEN_SEGMENTS:-32}
# Short, so a row group closes while the batches are still arriving. On the default 60s
# nothing reaches the disk during a run this size, the disk never fills, and the script
# measures a shutdown rather than a full disk.
ROW_GROUP_TIMEOUT_MS=${ROW_GROUP_TIMEOUT_MS:-500}
VOLUME=${VOLUME:-triedbfull}

case "$(uname -s)" in
  Darwin) PLATFORM=macos ;;
  Linux)  PLATFORM=linux ;;
  *) echo "SKIP no RAM disk recipe for $(uname -s)"; exit 0 ;;
esac

cargo build --release --quiet || exit 1

MOUNT=""
DEV=""
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  if [ "$PLATFORM" = macos ] && [ -n "$DEV" ]; then
    hdiutil detach "$DEV" -force >/dev/null 2>&1
  elif [ "$PLATFORM" = linux ] && [ -n "$MOUNT" ]; then
    sudo -n umount "$MOUNT" >/dev/null 2>&1
    rmdir "$MOUNT" >/dev/null 2>&1
  fi
}
trap cleanup EXIT

# ── The RAM disk ───────────────────────────────────────────────────────────────────
if [ "$PLATFORM" = macos ]; then
  # 512-byte sectors. `diskutil eraseVolume` mounts it without root; `mount -t hfs` does
  # not, which is the whole reason for going through diskutil.
  DEV=$(hdiutil attach -nomount "ram://$((DISK_MB * 2048))" 2>/dev/null | awk '{print $1}')
  [ -n "$DEV" ] || { echo "SKIP hdiutil could not attach a ${DISK_MB}MB RAM disk"; exit 0; }
  if ! diskutil eraseVolume HFS+ "$VOLUME" "$DEV" >/dev/null 2>&1; then
    echo "SKIP diskutil could not format the RAM disk"; exit 0
  fi
  MOUNT="/Volumes/$VOLUME"
else
  MOUNT=$(mktemp -d)
  if ! sudo -n mount -t tmpfs -o "size=${DISK_MB}m" tmpfs "$MOUNT" >/dev/null 2>&1; then
    rmdir "$MOUNT"
    echo "SKIP mounting a tmpfs needs sudo, and this shell has none without a password"
    exit 0
  fi
fi
[ -d "$MOUNT" ] || { echo "SKIP the RAM disk did not mount"; exit 0; }
echo "ram disk             $MOUNT (${DISK_MB}MB)"

WORK=$(mktemp -d)
trap '[ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null; wait 2>/dev/null; cleanup; rm -rf "$WORK"' EXIT

INPUT=testdata/m5_6c/${BATCH}-seed${GEN_SEED}-batch.ndjson
mkdir -p testdata/m5_6c
[ -f "$INPUT" ] || "$BIN" gen-logs --lines "$BATCH" --templates 12 --seed "$GEN_SEED" > "$INPUT"

mkdir -p "$MOUNT/data"
# The ballast. Everything left after it is what the database gets.
BALLAST="$MOUNT/ballast"
dd if=/dev/zero of="$BALLAST" bs=1m count="$BALLAST_MB" >/dev/null 2>&1
echo "ballast              $(du -m "$BALLAST" 2>/dev/null | cut -f1)MB"
echo "free for the data    $(df -m "$MOUNT" | tail -1 | awk '{print $4}')MB"

PORT=""
start() {  # start <logfile>
  TRIEDB__STORAGE__MAX_OPEN_SEGMENTS="$MAX_OPEN_SEGMENTS" \
    "$BIN" serve --port 0 --data-dir "$MOUNT/data" \
      --row-group-timeout-ms "$ROW_GROUP_TIMEOUT_MS" 2>"$1" &
  PID=$!
  PORT=""
  for _ in $(seq 1 600); do
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

stat_field() {  # stat_field <dotted path>
  curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(a if a is not None else "")' "$1" 2>/dev/null
}

echo
start "$WORK/run1.log" || exit 1

# ── Ingest until it will not take any more ─────────────────────────────────────────
#
# `written` is what /stats says has left the buffer: rows minus buffered_rows. That is the
# number the restart below has to return, because it is the number the server claimed was
# on the disk.
WRITTEN=0
SHED_CODE=""
SHED_BODY=""
SHED_RETRY=""
BATCHES=0

# **One batch, drained, before any of the rest.** The survival check at the end is only a
# check if some row reached the disk before it filled, and a disk small enough to fill on
# the first batch would make it vacuous. So the first batch is sent on its own and waited
# on: `buffered_rows` back to 0 with no write failure is the disk having taken it.
code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
[ "$code" = "200" ] || { echo "the first batch answered $code"; cat "$WORK/ingest.json"; }
for _ in $(seq 1 120); do
  rows=$(stat_field storage.rows)
  buffered=$(stat_field storage.buffered_rows)
  [ -n "$rows" ] && [ -n "$buffered" ] && WRITTEN=$((rows - buffered))
  [ "$buffered" = "0" ] && break
  sleep 0.5
done
echo "first batch written  $WRITTEN row(s), $(df -m "$MOUNT" | tail -1 | awk '{print $4}')MB free"

for n in $(seq 2 "$MAX_BATCHES"); do
  code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
         -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
  BATCHES=$n
  if [ "$code" = "503" ]; then
    SHED_CODE=$code
    SHED_BODY=$(cat "$WORK/ingest.json")
    SHED_RETRY=$(curl -s -D - -o /dev/null -X POST "http://127.0.0.1:$PORT/ingest" \
                 --data-binary "@$INPUT" | tr -d '\r' | grep -i '^retry-after:' | head -1)
    break
  fi
  [ "$code" = "200" ] || { echo "ingest answered $code"; cat "$WORK/ingest.json"; break; }
  # Give the tick a chance to run before reading what it managed to write.
  sleep 0.6
  rows=$(stat_field storage.rows)
  buffered=$(stat_field storage.buffered_rows)
  failures=$(stat_field storage.write_failures)
  [ -n "$rows" ] && [ -n "$buffered" ] && WRITTEN=$((rows - buffered))
  if [ -n "$failures" ] && [ "$failures" != "0" ]; then
    # The disk has started refusing. One more batch, which is the one that must be shed.
    code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
           -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
    if [ "$code" = "503" ]; then
      SHED_CODE=$code
      SHED_BODY=$(cat "$WORK/ingest.json")
      SHED_RETRY=$(curl -s -D - -o /dev/null -X POST "http://127.0.0.1:$PORT/ingest" \
                   --data-binary "@$INPUT" | tr -d '\r' | grep -i '^retry-after:' | head -1)
    else
      SHED_CODE=$code
      SHED_BODY=$(cat "$WORK/ingest.json")
    fi
    break
  fi
done

FAILURES=$(stat_field storage.write_failures)
LAST_ERROR=$(stat_field storage.last_write_error)
HEALTH=$(curl -s -o "$WORK/health.json" -w '%{http_code}' "http://127.0.0.1:$PORT/health")
HEALTH_BODY=$(cat "$WORK/health.json" 2>/dev/null)
ALIVE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/query" \
        -H 'Content-Type: application/json' --data '{"q": "COUNT LOG {}"}')

echo "batches sent         $BATCHES x $BATCH lines"
echo "write_failures       $FAILURES"
echo "last_write_error     $LAST_ERROR"
echo "rows written         $WRITTEN"
echo "ingest after failure $SHED_CODE"
echo "  body               $SHED_BODY"
echo "  ${SHED_RETRY:-Retry-After: (none)}"
echo "/health              $HEALTH $HEALTH_BODY"
echo "/query while full    $ALIVE"
echo "File exists errors   $(grep -c 'File exists' "$WORK/run1.log" 2>/dev/null)"

stop
# Kept, because the `stop` after the restart below overwrites it with that run's code.
FULL_DISK_CODE=$STOP_CODE
echo "shutdown exit code   $STOP_CODE"
grep -E 'final flush|FAILED|not written|could not|exit ' "$WORK/run1.log" | sed 's/^/  /'

# ── Free the space and read it back ────────────────────────────────────────────────
rm -f "$BALLAST"
echo
echo "ballast removed, free $(df -m "$MOUNT" | tail -1 | awk '{print $4}')MB"
start "$WORK/run2.log" || exit 1
AFTER=$(curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
        --data '{"q": "COUNT LOG {}"}' \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null)
RESTART_HEALTH=$(curl -s "http://127.0.0.1:$PORT/health")
echo "COUNT LOG {} restart $AFTER (want at least $WRITTEN)"
echo "/health after        $RESTART_HEALTH"
stop

echo
FAIL=0
[ "$SHED_CODE" = "503" ] || { echo "FAIL /ingest answered $SHED_CODE after the disk filled, want 503"; FAIL=1; }
case "$SHED_RETRY" in *[Rr]etry-[Aa]fter:*) ;; *) echo "FAIL the 503 carried no Retry-After"; FAIL=1 ;; esac
case "$HEALTH_BODY" in *degraded*) ;; *) echo "FAIL /health did not report degraded: $HEALTH_BODY"; FAIL=1 ;; esac
[ "$ALIVE" = "200" ] || { echo "FAIL the server stopped answering queries ($ALIVE)"; FAIL=1; }
if [ -z "$FULL_DISK_CODE" ] || [ "$FULL_DISK_CODE" = "0" ]; then
  echo "FAIL the shutdown on the full disk exited ${FULL_DISK_CODE:-?}, want non-zero"; FAIL=1
fi
[ "$STOP_CODE" = "0" ] || { echo "FAIL the shutdown after the disk came back exited $STOP_CODE"; FAIL=1; }
# The row-survival check is only a check if there were rows to survive. A run where
# nothing ever reached the disk would pass `AFTER >= 0` however badly the restart went.
if [ -z "$WRITTEN" ] || [ "$WRITTEN" -le 0 ]; then
  echo "FAIL /stats never reported a written row, so the survival check tested nothing"; FAIL=1
elif [ -z "$AFTER" ]; then
  echo "FAIL the restart answered nothing"; FAIL=1
elif [ "$AFTER" -lt "$WRITTEN" ]; then
  echo "FAIL $AFTER rows came back, but /stats reported $WRITTEN written before the disk filled"; FAIL=1
fi
# The retry-safety half of the milestone: a create that failed part way left a file that
# every later attempt at that number met with `File exists`.
EXISTS=$(grep -c 'File exists' "$WORK/run1.log" 2>/dev/null)
[ "$EXISTS" = "0" ] || { echo "FAIL $EXISTS retries answered \`File exists\`"; FAIL=1; }
if [ "$FAIL" = "0" ]; then
  echo "PASS the full disk was refused loudly, the server stayed up, the exit said so, and all $WRITTEN written rows came back"
  exit 0
fi
exit 1
