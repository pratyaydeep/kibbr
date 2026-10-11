#!/usr/bin/env bash
# A body of which nothing was taken is not a 200.
#
# A line of a template never seen before needs a record in catalog.log, written and
# flushed, before the line counts. On a full disk that write fails, the line is refused by
# number - `not ingested: catalog: <path>: <reason>` - and the body goes on: the next line
# that needs no record still lands. That is right for one line among many. For a body
# whose every line is such a line it used to be a `200` with `ingested: 0`, no
# `Retry-After` and two hundred errors, and a shipper that goes by the status dropped two
# hundred well-formed lines the server had none of. So:
#
#   1. A body of which no line was taken, and whose every refusal is a write this server
#      could not make, is answered 503 with a `Retry-After`. The per-line errors are still
#      in the body, each against its line, so the answer says which write and where.
#   2. That body was read to its end, so the connection is left open and the answer does
#      not say `Connection: close`.
#   3. The template the body needed was not made: `/stats` counts the templates it did.
#   4. The log's own refusal is as it was: a body of a template that exists, which needs
#      no record, reaches the write-ahead log, and a log that cannot write answers 503
#      with a `Retry-After` and its write error (scripts/fault/disk_full.sh, rule 2).
#   5. Nothing of either body is stored, on the full disk or after a restart, and both
#      are taken when they are sent again with space free: `COUNT LOG {}` is the lines
#      that were acknowledged, each once.
#
# **Which write meets the full disk first is a matter of bytes**, and the script arranges
# it. Filling the disk with the stream itself (scripts/fault/disk_full.sh) always fails the log
# first, and the door then refuses every body before it is read: a new template never
# reaches the catalog. Here the server is idle and its log healthy while a filler file
# takes every free block, and catalog.log has been grown, a template at a time, to end a
# few bytes short of a block boundary - so the next template's record needs a block that
# is not there, and it is the first write the next body asks for.
#
#   DISK_MB=10 BODY_LINES=200 scripts/measure/catalog_full_503.sh
#   BIN=$PWD/other/kibbr PORT=7700 scripts/measure/catalog_full_503.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
DISK_MB=${DISK_MB:-10}
# Under the thousand errors a response describes, so every refused line is in it by number.
BODY_LINES=${BODY_LINES:-200}
# How far short of a block boundary catalog.log is left: anything smaller than the records
# of one template (about five hundred bytes) does. 51 is what it was the day this was first seen.
SHORT=${SHORT:-51}
VOLUME=${VOLUME:-kibbrcat503}
# 0 is any free port, read back from the banner.
LISTEN=${PORT:-0}
# Short, so the rows sent while there is room reach their files and the log's checkpoint
# catches up before the disk is filled: nothing is then left to write, or to delete, that
# would move the free space under the bodies this is about.
ROW_GROUP_TIMEOUT_MS=${ROW_GROUP_TIMEOUT_MS:-200}

case "$(uname -s)" in
  Darwin) PLATFORM=macos ;;
  Linux)  PLATFORM=linux ;;
  *) echo "SKIP no RAM disk recipe for $(uname -s)"; exit 0 ;;
esac

[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1

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
  # This script fills the volume it is given to the last block, so it has to be the one
  # just made: a volume of this name that was already mounted would have sent the new one
  # to `/Volumes/<name> 1`, and the filler into somebody else's disk.
  if [ "$(df "$MOUNT" 2>/dev/null | tail -1 | awk '{print $1}')" != "$DEV" ]; then
    echo "SKIP $MOUNT is not the RAM disk just made ($DEV): a volume of that name was already mounted. If a run of this script that was killed left it, \`hdiutil detach \"$MOUNT\" -force\` clears it"
    MOUNT=""; exit 0
  fi
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

mkdir -p "$MOUNT/data"
CAT="$MOUNT/data/catalog.log"
FILLER="$MOUNT/filler"
# The size files are allocated in: what "a block that is not there" is measured in.
BLOCK=$(python3 -c 'import os,sys; print(os.statvfs(sys.argv[1]).f_frsize)' "$MOUNT" 2>/dev/null)
[ -n "$BLOCK" ] && [ "$BLOCK" -gt "$((SHORT * 4))" ] || { echo "SKIP the volume's block size reads as '${BLOCK:-nothing}'"; exit 0; }

free_blocks() {
  python3 -c 'import os,sys; print(os.statvfs(sys.argv[1]).f_bavail)' "$MOUNT" 2>/dev/null
}

# Every free block of the volume, into one file; prints the bytes it took. Smaller and
# smaller writes, so what is left at the end is not one byte.
fill() {
  python3 - "$FILLER" <<'PY'
import os, sys
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
taken = 0
for chunk in (65536, 4096, 512, 1):
    while True:
        try:
            taken += os.write(fd, b"\0" * chunk)
        except OSError:
            break
try:
    os.fsync(fd)
except OSError:
    pass
os.close(fd)
print(taken)
PY
}

# lines <first id> <how many> <key>: well-formed lines of one template, which <key> names.
lines() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib, json, sys
first, count, key = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
for i in range(first, first + count):
    print(json.dumps({"ts": "2026-10-04T00:%02d:%02dZ" % (i // 60 % 60, i % 60), "id": i,
                      "msg": hashlib.sha512(b"%d" % i).hexdigest(), key: i}, separators=(",", ":")))
PY
}

PORT=""
start() {  # start <logfile>
  # No compaction: the lines are dated days ago, and a day rebuilt while the disk is full
  # would write and delete files under the bodies this is about.
  "$BIN" serve --port "$LISTEN" --data-dir "$MOUNT/data" \
    --row-group-timeout-ms "$ROW_GROUP_TIMEOUT_MS" --compact-interval-ms 0 2>"$1" &
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

count() {
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
    --data '{"q": "COUNT LOG {}"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

acked() {  # acked <response file>: the lines a 200 said were ingested
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["ingested"])' "$1" 2>/dev/null || echo 0
}

# answer <body file> <tag>: POST the body, and then GET /health **on the same connection**
# if the server left it open. $WORK/<tag>.json is the answer, $WORK/<tag>.hdr its headers;
# CODE is its status and FRESH how many connections the GET had to open - 0 when it went
# out on the one the body came in on.
CODE=""
FRESH=""
answer() {
  curl -s -D "$WORK/$2.hdr" -o "$WORK/$2.json" -w '%{http_code}\n' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$1" \
       --next -s -o /dev/null -w '%{num_connects}\n' "http://127.0.0.1:$PORT/health" \
       > "$WORK/$2.codes" 2>/dev/null
  CODE=$(sed -n 1p "$WORK/$2.codes")
  FRESH=$(sed -n 2p "$WORK/$2.codes")
}

header() {  # header <tag> <name>: that header of the answer, or nothing
  tr -d '\r' < "$WORK/$1.hdr" 2>/dev/null | grep -i "^$2:" | head -1
}

# report <response file> <lines sent>: `ingested`, `ok`, how many errors, how many of
# them are the catalog's, and whether they name every line of the body, each once.
report() {
  python3 - "$1" "$2" <<'PY' 2>/dev/null || echo "none none 0 0 no"
import json, sys
answer, sent = json.load(open(sys.argv[1])), int(sys.argv[2])
errors = answer.get("errors") or []
catalog = [e for e in errors if str(e.get("msg", "")).startswith("not ingested: catalog: ")]
every = sorted(e.get("line") for e in errors) == list(range(1, sent + 1))
print(answer.get("ingested"), str(answer.get("ok")).lower(), len(errors), len(catalog), "yes" if every else "no")
PY
}

echo
start "$WORK/run1.log" || exit 1

# ── While there is room ────────────────────────────────────────────────────────────
#
# `ACKED` is every line a 200 said was ingested: the number `COUNT LOG {}` has to be,
# exactly, on the full disk and after the restart.
ACKED=0
lines 1 "$BODY_LINES" known > "$WORK/known1.ndjson"
# The known body sent on the full disk has to need a block of the log wherever its segment
# ends, so its frame is longer than a block: a line's `msg` is a digest, which packs to no
# less than 64 bytes, and a thirty-second of a block in lines is then two blocks of frame.
KNOWN_LINES=$((BLOCK / 32))
lines 100001 "$KNOWN_LINES" known > "$WORK/known2.ndjson"
lines 200001 "$BODY_LINES" never_seen > "$WORK/new.ndjson"

code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$WORK/known1.ndjson")
[ "$code" = "200" ] || { echo "the first body answered $code on a disk with room"; cat "$WORK/ingest.json"; }
[ "$code" = "200" ] && ACKED=$((ACKED + $(acked "$WORK/ingest.json")))

# **catalog.log to just short of a block boundary.** One line of a template of its own
# adds that template's records to the catalog, and a longer key adds a longer record, so
# two of them measure what a key's character costs and the rest are cut to fit.
catalog_size() { wc -c < "$CAT" 2>/dev/null | tr -d ' '; }
PADS=0
GREW=0
pad() {  # pad <key length>: one line of a template of its own; GREW is what the catalog grew by
  local before key code
  PADS=$((PADS + 1))
  before=$(catalog_size)
  key=$(python3 -c 'import sys; print(("pad%s_" % sys.argv[1]).ljust(int(sys.argv[2]), "x"))' "$PADS" "$1")
  code=$(curl -s -o "$WORK/pad.json" -w '%{http_code}' -X POST "http://127.0.0.1:$PORT/ingest" \
         --data-binary "{\"ts\":\"2026-10-04T00:00:00Z\",\"id\":0,\"msg\":\"pad\",\"$key\":1}")
  [ "$code" = "200" ] && ACKED=$((ACKED + $(acked "$WORK/pad.json")))
  GREW=$(( $(catalog_size) - before ))
}
pad 10; D10=$GREW
pad 40; D40=$GREW
PER=$(( (D40 - D10) / 30 )); [ "$PER" -ge 1 ] || PER=2
BASE=$(( D10 - PER * 10 ))
for _ in $(seq 1 200); do
  REM=$(( BLOCK - $(catalog_size) % BLOCK ))
  [ "$REM" -le "$((SHORT * 2))" ] && break
  L=$(( (REM - SHORT - BASE) / PER ))
  { [ "$L" -ge 8 ] && [ "$L" -le 400 ]; } || L=10
  pad "$L"
done
REM=$(( BLOCK - $(catalog_size) % BLOCK ))
echo "catalog.log          $(catalog_size) bytes, $REM short of a $BLOCK-byte block ($PADS one-line templates added; one of key length 10 is $D10 bytes)"

# Everything sent so far in its files and the checkpoint behind none of it, so nothing is
# left for the server to write or delete on its own.
for _ in $(seq 1 150); do
  [ "$(stat_field storage.buffered_rows)" = "0" ] \
    && [ "$(stat_field storage.wal.checkpoint)" = "$(stat_field storage.wal.last_seq)" ] && break
  sleep 0.2
done
echo "before the fill      $ACKED line(s) acknowledged, $(stat_field storage.buffered_rows) buffered, COUNT LOG {} $(count)"

# ── Every free block ───────────────────────────────────────────────────────────────
FILLED=$(fill)
sleep 0.3
# Once more, for a block the filesystem gave back after the first pass.
FILLED=$((FILLED + $(fill)))
FREE_BEFORE=$(free_blocks)
WAL_BEFORE=$(stat_field storage.wal.healthy)
TEMPLATES_BEFORE=$(stat_field trie.templates)
echo "filler               $FILLED bytes, $FREE_BEFORE block(s) free; wal healthy: $WAL_BEFORE; $TEMPLATES_BEFORE template(s)"

# ── The body this is about: every line needs a template the catalog cannot write ───
answer "$WORK/new.ndjson" new
NEW_CODE=$CODE
NEW_FRESH=$FRESH
NEW_RETRY=$(header new retry-after)
NEW_CLOSE=$(header new connection | grep -i close)
read -r NEW_INGESTED NEW_OK NEW_ERRORS NEW_CATALOG NEW_EVERY <<< "$(report "$WORK/new.json" "$BODY_LINES")"
FREE_AFTER=$(free_blocks)
WAL_AFTER=$(stat_field storage.wal.healthy)
TEMPLATES_AFTER=$(stat_field trie.templates)
echo "never-seen template  $NEW_CODE  ${NEW_RETRY:-Retry-After: (none)}  ${NEW_CLOSE:-no Connection: close}"
echo "  body               $(head -c 420 "$WORK/new.json" 2>/dev/null)..."
echo "  ingested           $NEW_INGESTED, ok $NEW_OK, $NEW_ERRORS error(s), $NEW_CATALOG of them \`not ingested: catalog:\`, every line named: $NEW_EVERY"
echo "  connection         the next request opened $NEW_FRESH new connection(s)"
echo "  after it           $FREE_AFTER block(s) free; wal healthy: $WAL_AFTER; $TEMPLATES_AFTER template(s)"

# ── And the log's refusal, as it was: a template that exists needs no record ───────
answer "$WORK/known2.ndjson" known
KNOWN_CODE=$CODE
KNOWN_RETRY=$(header known retry-after)
KNOWN_BODY=$(cat "$WORK/known.json" 2>/dev/null)
echo "known template       $KNOWN_CODE  ${KNOWN_RETRY:-Retry-After: (none)}"
echo "  body               $(printf '%s' "$KNOWN_BODY" | cut -c1-420)"

HEALTH_BODY=$(curl -s "http://127.0.0.1:$PORT/health")
COUNT_FULL=$(count)
echo "/health while full   $HEALTH_BODY"
echo "COUNT LOG {} full    $COUNT_FULL (want $ACKED)"
ACKED_FULL=$ACKED

# ── Space again: both bodies are sent again, as a shipper told 503 sends them ──────
rm -f "$FILLER"
echo
echo "filler removed, $(free_blocks) block(s) free"
for _ in $(seq 1 150); do
  [ "$(stat_field storage.wal.healthy)" = "True" ] && break
  sleep 0.1
done
AGAIN_NEW=$(curl -s -o "$WORK/again-new.json" -w '%{http_code}' \
            -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$WORK/new.ndjson")
[ "$AGAIN_NEW" = "200" ] && ACKED=$((ACKED + $(acked "$WORK/again-new.json")))
AGAIN_KNOWN=$(curl -s -o "$WORK/again-known.json" -w '%{http_code}' \
              -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$WORK/known2.ndjson")
[ "$AGAIN_KNOWN" = "200" ] && ACKED=$((ACKED + $(acked "$WORK/again-known.json")))
COUNT_AGAIN=$(count)
echo "sent again           never-seen $AGAIN_NEW, known $AGAIN_KNOWN; COUNT LOG {} $COUNT_AGAIN (want $ACKED)"
stop
# Kept, because the `stop` after the restart below overwrites it with that run's code.
FIRST_CODE=$STOP_CODE
echo "shutdown exit code   $STOP_CODE"

start "$WORK/run2.log" || exit 1
AFTER=$(count)
RESTART_HEALTH=$(curl -s "http://127.0.0.1:$PORT/health")
echo "COUNT LOG {} restart $AFTER (want $ACKED)"
echo "/health after        $RESTART_HEALTH"
stop

echo
FAIL=0
# What the body met has to be the catalog's write and nothing else, or the rest of this
# tested something other than it says.
[ "$REM" -le "$((SHORT * 2))" ] || { echo "FAIL catalog.log ended $REM bytes short of a block, not within $((SHORT * 2)): the next template's record may have fitted"; FAIL=1; }
[ "$FREE_BEFORE" = "0" ] || { echo "FAIL ${FREE_BEFORE:-?} block(s) were free when the body was sent: the disk was not full"; FAIL=1; }
[ "$WAL_BEFORE" = "True" ] || { echo "FAIL the write-ahead log was not healthy before the body ($WAL_BEFORE): the door would refuse it unread, and the catalog is never asked"; FAIL=1; }
# 1. Nothing taken, every refusal a write: 503, a Retry-After, and the errors by line.
[ "$NEW_CODE" = "503" ] || { echo "FAIL a body of $BODY_LINES well-formed lines of which none was taken was answered ${NEW_CODE:-nothing}, want 503: a shipper that goes by the status does not send it again"; FAIL=1; }
case "$NEW_RETRY" in [Rr]etry-[Aa]fter:*) ;; *) echo "FAIL that answer carried no Retry-After"; FAIL=1 ;; esac
[ "$NEW_INGESTED" = "0" ] || { echo "FAIL that answer says ingested: $NEW_INGESTED, want 0"; FAIL=1; }
[ "$NEW_OK" = "false" ] || { echo "FAIL that answer says ok: $NEW_OK, want false"; FAIL=1; }
if [ "$NEW_ERRORS" != "$BODY_LINES" ] || [ "$NEW_CATALOG" != "$BODY_LINES" ] || [ "$NEW_EVERY" != "yes" ]; then
  echo "FAIL the per-line errors are not all in the body: $NEW_ERRORS error(s), $NEW_CATALOG of them \`not ingested: catalog: ...\`, for $BODY_LINES lines (every line named once: $NEW_EVERY)"; FAIL=1
fi
# 2. The body was read, so the connection is still good.
[ -z "$NEW_CLOSE" ] || { echo "FAIL that answer says \`$NEW_CLOSE\` for a body that was read to its end"; FAIL=1; }
[ "$NEW_FRESH" = "0" ] || { echo "FAIL the request after it had to open ${NEW_FRESH:-?} connection(s): the server closed the one the body came in on"; FAIL=1; }
# 3. No template was made of it, and the log was not what failed.
[ "$TEMPLATES_AFTER" = "$TEMPLATES_BEFORE" ] || { echo "FAIL the trie holds $TEMPLATES_AFTER template(s) after the refusal and held $TEMPLATES_BEFORE before: the template exists in memory with no record on disk"; FAIL=1; }
[ "$WAL_AFTER" = "True" ] || { echo "FAIL the write-ahead log is unhealthy after a body it was never handed ($WAL_AFTER)"; FAIL=1; }
# 4. The log's refusal.
[ "$KNOWN_CODE" = "503" ] || { echo "FAIL a body of a known template on the full disk was answered ${KNOWN_CODE:-nothing}, want the log's 503"; FAIL=1; }
case "$KNOWN_BODY" in *"write-ahead log"*) ;; *) echo "FAIL that 503 does not name the write-ahead log: $KNOWN_BODY"; FAIL=1 ;; esac
case "$KNOWN_RETRY" in [Rr]etry-[Aa]fter:*) ;; *) echo "FAIL that 503 carried no Retry-After"; FAIL=1 ;; esac
case "$HEALTH_BODY" in *degraded*) ;; *) echo "FAIL /health did not report degraded once the log could not write: $HEALTH_BODY"; FAIL=1 ;; esac
# 5. Nothing of either was stored, and both land when sent again.
if [ -z "$ACKED_FULL" ] || [ "$ACKED_FULL" -le 0 ]; then
  echo "FAIL no line was acknowledged before the disk filled, so the counts test nothing"; FAIL=1
elif [ "$COUNT_FULL" != "$ACKED_FULL" ]; then
  echo "FAIL COUNT LOG {} on the full disk is ${COUNT_FULL:-nothing}, and $ACKED_FULL lines were acknowledged: a line of a refused body is stored"; FAIL=1
fi
[ "$AGAIN_NEW" = "200" ] || { echo "FAIL the never-seen template, sent again with space free, was answered $AGAIN_NEW"; FAIL=1; }
[ "$AGAIN_KNOWN" = "200" ] || { echo "FAIL the known template, sent again with space free, was answered $AGAIN_KNOWN"; FAIL=1; }
[ "$COUNT_AGAIN" = "$ACKED" ] || { echo "FAIL COUNT LOG {} is ${COUNT_AGAIN:-nothing} after both were sent again, and $ACKED lines were acknowledged"; FAIL=1; }
[ "$FIRST_CODE" = "0" ] || { echo "FAIL the shutdown after the disk came back exited ${FIRST_CODE:-?}, want 0"; FAIL=1; }
[ "$STOP_CODE" = "0" ] || { echo "FAIL the shutdown after the restart exited $STOP_CODE"; FAIL=1; }
if [ -z "$AFTER" ]; then
  echo "FAIL the restart answered nothing"; FAIL=1
elif [ "$AFTER" != "$ACKED" ]; then
  echo "FAIL $AFTER rows came back after the restart, and $ACKED lines were acknowledged"; FAIL=1
fi
[ -z "$RESTART_HEALTH" ] || case "$RESTART_HEALTH" in *'"status":"ok"'*) ;; *) echo "FAIL /health after the restart: $RESTART_HEALTH"; FAIL=1 ;; esac
if [ "$FAIL" = "0" ]; then
  echo "PASS a body of $BODY_LINES lines the catalog could not write a template for was answered 503 with a Retry-After and its $BODY_LINES errors by line, the connection stayed open, the log's own 503 followed, nothing of either was stored, and all $ACKED acknowledged lines came back, once each"
  exit 0
fi
exit 1
