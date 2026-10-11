#!/usr/bin/env bash
# A day directory the start cannot read is refused for what it is: unreadable. Not gone.
#
# A start that could not search a day directory used to say of the file in it "named by
# manifest.log and not on disk; its rows are gone" - twice, and with no error from the
# system - when the file was whole and every row came back once the directory could be
# read. Refusing the start is right; the reason given was false. "Not on disk" is for a
# file whose directory was read and does not hold it.
#
# What this asserts:
#
#   1. NLINES lines of one day are ingested and the server is stopped cleanly: one day
#      directory under data/, holding the day's file.
#   2. With that directory at `chmod 000`, `kibbr serve` exits 1 within EXIT_WITHIN_S
#      seconds, and its stderr
#        - names the directory, for itself and not only as the start of the file's path,
#        - carries the system's error, `Permission denied`,
#        - says the file `was not read`, and names it,
#        - does not say `not on disk` or `rows are gone`,
#        - says all of it once: the directory, the file and `Permission denied` are each
#          there one time, on one line, and no `caused by:` line says any of it again.
#      `kibbr load` and `kibbr compact`, run with no server on the same directory, open
#      it the same way and are held to the same.
#   3. The refused starts left the day's file as it was, byte for byte.
#   4. After `chmod 755` the server starts and `COUNT LOG {}` is NLINES.
#   5. With the directory readable and the file removed, the start is refused for a file
#      that is not on disk - that text once, and no `Permission denied`.
#
# The directory's mode is put back by the exit trap, so a run that fails part way leaves
# nothing that cannot be deleted. Run as root it tests nothing - root reads through mode
# 000 - and says so.
#
#   NLINES=50 scripts/fault/start_unreadable_day.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
NLINES=${NLINES:-50}
EXIT_WITHIN_S=${EXIT_WITHIN_S:-5}

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

FAIL=0
fail() { echo "FAIL $*"; FAIL=1; }

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

# A command that is expected to refuse. `EXITED` is its code, or `running` if it was still
# up when the time ran out - and then it is killed.
EXITED=""
expecting_refusal() {  # expecting_refusal <logfile> <kibbr arguments...>
  local log=$1
  shift
  "$BIN" "$@" >/dev/null 2>"$log" &
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
  kill -KILL "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
}

times() {  # times <fixed string> <file>: how many times the string is in the file
  { grep -oF -- "$1" "$2" || true; } | wc -l | tr -d ' '
}

shown() {  # shown: standard input, without the scratch path, indented
  sed "s|[^ ]*$WORK/||g; s/^/  /"
}

# What one command said of a day directory it could not search, held to step 2.
unread_once() {  # unread_once <what ran> <exit code> <stderr file>
  local what=$1 code=$2 err=$3
  local lines dir file denied there unread gone caused
  lines=$({ grep -F -- "$DAY" "$err" || true; } | wc -l | tr -d ' ')
  # The file's path begins with the directory's: the directory named for itself is every
  # time its path is there, less the times it is only the start of something under it.
  dir=$(( $(times "$DAY" "$err") - $(times "$DAY/" "$err") ))
  file=$(times "$SEGMENT" "$err")
  denied=$(times 'Permission denied' "$err")
  there=$({ grep -F -- "$DAY" "$err" || true; } | { grep -oF 'Permission denied' || true; } | wc -l | tr -d ' ')
  unread=$(times 'was not read' "$err")
  gone=$(( $(times 'rows are gone' "$err") + $(times 'not on disk' "$err") ))
  caused=$(times 'caused by:' "$err")
  echo "chmod 000, $what"
  echo "  exit $code; the directory is named $dir time(s) and the file $file, on $lines line(s); \`Permission denied\` $denied time(s), $there on that line; \`was not read\` $unread; \`not on disk\`/\`rows are gone\` $gone; \`caused by:\` $caused line(s)"
  shown < "$err"

  if [ "$code" = "running" ]; then
    fail "$what was still running ${EXIT_WITHIN_S}s after starting on a day directory at chmod 000"
  elif [ "$code" != "1" ]; then
    fail "$what on a day directory at chmod 000 exited $code, want 1"
  fi
  [ "$dir" -ge 1 ] || fail "$what did not name the directory $DAY: its path is there only as the start of the file's"
  [ "$denied" -ge 1 ] || fail "$what does not carry the system's error: no \`Permission denied\`"
  [ "$unread" -ge 1 ] && [ "$file" -ge 1 ] || fail "$what does not say which file was not read: \`was not read\` $unread time(s), the file named $file time(s)"
  [ "$gone" = "0" ] || fail "$what says the file is not on disk or its rows are gone; the file is there, whole, and was not read"
  if [ "$dir" -gt 1 ] || [ "$file" -gt 1 ] || [ "$denied" -gt 1 ] || [ "$unread" -gt 1 ] || [ "$lines" -gt 1 ] \
     || [ "$caused" != "0" ] || { [ "$denied" = "1" ] && [ "$there" != "1" ]; }; then
    fail "$what does not say the refusal once: the directory $dir time(s), the file $file, \`Permission denied\` $denied ($there on the line naming the directory), \`was not read\` $unread, on $lines line(s), with $caused \`caused by:\` line(s)"
  fi
}

# ── One day, in a data file ──────────────────────────────────────────────────────────
for i in $(seq 1 "$NLINES"); do
  printf '{"uid":"u%d","ts":"2024-03-01T00:%02d:%02dZ","n":%d}\n' "$i" $((i / 60 % 60)) $((i % 60)) "$i"
done > "$WORK/lines.ndjson"

start "$WORK/run1.log" || exit 1
SENT=$(curl -s -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$WORK/lines.ndjson" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["ingested"])' 2>/dev/null)
BEFORE=$(count)
stop
echo "ingested             ${SENT:-?} of $NLINES line(s), COUNT LOG {} = ${BEFORE:-?}, exit $STOP_CODE"
[ "${BEFORE:-}" = "$NLINES" ] || { echo "FAIL COUNT LOG {} was ${BEFORE:-nothing} before anything was done, want $NLINES"; exit 1; }
[ "$STOP_CODE" = "0" ] || { echo "FAIL the first stop exited $STOP_CODE"; exit 1; }

DAYS=$(find "$DATA/data" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
DAY=$(find "$DATA/data" -mindepth 1 -maxdepth 1 -type d | sort | head -1)
SEGMENT=$(find "$DATA/data" -name 'seg_*.col' | sort | head -1)
[ "$DAYS" = "1" ] && [ -n "$SEGMENT" ] || { echo "FAIL want one day directory holding a data file, found $DAYS director(ies)"; exit 1; }
SUM_BEFORE=$(cksum < "$SEGMENT")
echo "day directory        ${DAY#"$WORK"/}"
echo "data file            ${SEGMENT#"$WORK"/}, $(wc -c < "$SEGMENT" | tr -d ' ') byte(s)"

# ── The directory at 000 ─────────────────────────────────────────────────────────────
chmod 000 "$DAY"
if ls "$DAY" >/dev/null 2>&1; then
  echo "FAIL chmod 000 did not stop this user reading ${DAY#"$WORK"/} - run as root? Nothing was tested"
  exit 1
fi
expecting_refusal "$WORK/run2.log" serve --port 0 --data-dir "$DATA"
unread_once "kibbr serve" "$EXITED" "$WORK/run2.log"
expecting_refusal "$WORK/load.log" load --data-dir "$DATA" "$WORK/lines.ndjson"
unread_once "kibbr load" "$EXITED" "$WORK/load.log"
expecting_refusal "$WORK/compact.log" compact --day "$(basename "$DAY")" --data-dir "$DATA"
unread_once "kibbr compact" "$EXITED" "$WORK/compact.log"

chmod 755 "$DAY"
SUM_AFTER=$(cksum < "$SEGMENT" 2>/dev/null)
SAME=no; [ "$SUM_BEFORE" = "$SUM_AFTER" ] && SAME=yes
echo "data file            unchanged by the refused starts: $SAME"

# ── The directory at 755 again ───────────────────────────────────────────────────────
RESTORED=no
AFTER=""
STOP_CODE=""
if start "$WORK/run3.log"; then
  RESTORED=yes
  AFTER=$(count)
  stop
fi
echo "chmod 755, start     started $RESTORED, COUNT LOG {} = ${AFTER:-?}, exit ${STOP_CODE:-?}"

# ── The file truly absent, its directory readable ────────────────────────────────────
# Looked for again: the start above may have left the day in another file than it found.
SEGMENT=$(find "$DATA/data" -name 'seg_*.col' | sort | head -1)
rm -f "$SEGMENT"
expecting_refusal "$WORK/run4.log" serve --port 0 --data-dir "$DATA"
ABSENT_EXIT=$EXITED
ABSENT="$WORK/run4.log"
ABSENT_NAMED=$(times "$SEGMENT" "$ABSENT")
ABSENT_SAID=$(times 'not on disk' "$ABSENT")
ABSENT_GONE=$(times 'rows are gone' "$ABSENT")
ABSENT_DENIED=$(times 'Permission denied' "$ABSENT")
ABSENT_CAUSED=$(times 'caused by:' "$ABSENT")
echo "file removed, start  exit $ABSENT_EXIT; the file is named $ABSENT_NAMED time(s); \`not on disk\` $ABSENT_SAID time(s), \`rows are gone\` $ABSENT_GONE; \`Permission denied\` $ABSENT_DENIED time(s); \`caused by:\` $ABSENT_CAUSED line(s)"
{ grep -F -- "$SEGMENT" "$ABSENT" || true; } | shown

echo
[ "$SAME" = "yes" ] || fail "a refused start changed the data file it said it could not read"
[ "$RESTORED" = "yes" ] || fail "the server did not start after chmod 755"
[ "${AFTER:-}" = "$NLINES" ] || fail "COUNT LOG {} is ${AFTER:-nothing} after chmod 755, want $NLINES"
[ "${STOP_CODE:-}" = "0" ] || fail "the stop after chmod 755 exited ${STOP_CODE:-?}"
if [ "$ABSENT_EXIT" != "1" ] || [ "$ABSENT_NAMED" != "1" ] || [ "$ABSENT_SAID" != "1" ] || [ "$ABSENT_GONE" != "1" ] \
   || [ "$ABSENT_DENIED" != "0" ] || [ "$ABSENT_CAUSED" != "0" ]; then
  fail "a file truly absent from a readable directory: exit $ABSENT_EXIT (want 1), named $ABSENT_NAMED time(s) (want 1), \`not on disk\` $ABSENT_SAID time(s) (want 1), \`rows are gone\` $ABSENT_GONE (want 1), \`Permission denied\` $ABSENT_DENIED time(s) (want 0), \`caused by:\` $ABSENT_CAUSED line(s) (want 0)"
fi
if [ "$FAIL" = "0" ]; then
  echo "PASS an unreadable day directory stopped serve, load and compact by name with the system's error, once; all $AFTER rows were there after chmod 755; and only a file truly absent is called not on disk"
  exit 0
fi
exit 1
