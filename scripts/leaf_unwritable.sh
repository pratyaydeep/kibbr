#!/usr/bin/env bash
# A row the store could not take must not be counted as ingested.
#
# As built, when a leaf's directory cannot be opened, `StoreManager::append` records the
# failure and drops the row - and `/ingest` still answers `200` with that line counted in
# `ingested`, and the clean stop exits 0. `/health` goes degraded, which is the one place
# that said anything. With no write-ahead log a `200` for a row that is nowhere is the lie.
#
# What this asserts:
#
#   1. With one `leaf_*` directory `chmod 000` across a restart, re-sending the same body
#      answers `503` with a `Retry-After`.
#   2. `ingested` is short by exactly the rows that leaf held after the first run, and
#      every one of those lines is in `errors`, by number, with the store error as the
#      reason.
#   3. `/health` reports `degraded`, and still does after several flush ticks: the leaf
#      holds no buffer for a tick to fail on, and a tick that cleared it would let the
#      shipper's retry in to store the body's other lines a second time.
#   4. That retry - the same body again - is refused with a `503` and stores nothing.
#   5. The clean stop exits non-zero.
#   6. With the directory given back, the same body answers `200`, and `COUNT LOG {}`
#      holds the rows of all three bodies minus the dropped ones - the dropped rows were
#      not stored after all, and the counted ones were.
#
#   NLINES=2000 scripts/leaf_unwritable.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-42}

if [ "$(id -u)" = "0" ]; then
  echo "SKIP running as root, and chmod 000 does not stop root"
  exit 0
fi

cargo build --release --quiet || exit 1

WORK=$(mktemp -d)
PID=""
LOCKED=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  [ -n "$LOCKED" ] && chmod 755 "$LOCKED" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"
DATA="$WORK/data"

PORT=""
start() {  # start <logfile> [serve flags...]
  local logfile=$1
  shift
  "$BIN" serve --port 0 --data-dir "$DATA" "$@" 2>"$logfile" &
  PID=$!
  PORT=""
  for _ in $(seq 1 300); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$logfile" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$logfile" >&2; return 1; fi
}

STOP_CODE=""
stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  STOP_CODE=$?
  PID=""
}

INGEST_CODE=""
INGEST_RETRY=""
ingest() {  # ingest <response file>
  INGEST_CODE=$(curl -s -D "$WORK/headers" -o "$1" -w '%{http_code}' \
                -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
  INGEST_RETRY=$(tr -d '\r' < "$WORK/headers" | grep -i '^retry-after:' | head -1)
}

field() {  # field <json file> <key>
  python3 -c 'import json,sys; v = json.load(open(sys.argv[1])).get(sys.argv[2]); print("" if v is None else v)' "$1" "$2" 2>/dev/null
}

count() {
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

# ── Run 1: a clean ingest and a clean stop, so every leaf is on disk ─────────────────
start "$WORK/run1.log" || exit 1
ingest "$WORK/run1.json"
RUN1_CODE=$INGEST_CODE
RUN1_INGESTED=$(field "$WORK/run1.json" ingested)
curl -s "http://127.0.0.1:$PORT/templates" > "$WORK/templates.json"
stop
RUN1_EXIT=$STOP_CODE
echo "run 1                $RUN1_CODE, ingested $RUN1_INGESTED, exit $RUN1_EXIT"

# The leaf to lock: the one holding the most rows, so the dropped lines are a real share of
# the body. Its row count now is what the second run has to report as dropped.
read -r LEAF_ID LEAF_ROWS < <(python3 -c 'import json,sys
t = max(json.load(open(sys.argv[1]))["templates"], key=lambda t: (t["rows"], -t["template_id"]))
print(t["template_id"], t["rows"])' "$WORK/templates.json")
LOCKED="$DATA/leaf_$LEAF_ID"
[ -d "$LOCKED" ] || { echo "FAIL $LOCKED is not a directory"; exit 1; }
chmod 000 "$LOCKED"
echo "locked               leaf_$LEAF_ID, holding $LEAF_ROWS row(s)"

# ── Run 2: the same body, with that leaf unreachable ─────────────────────────────────
start "$WORK/run2.log" || exit 1
ingest "$WORK/run2.json"
RUN2_CODE=$INGEST_CODE
RUN2_RETRY=$INGEST_RETRY
STORED=$(count)
# Three flush intervals, so a tick that would clear `degraded` has had its chance.
sleep 1.5
HEALTH_BODY=$(curl -s "http://127.0.0.1:$PORT/health")
ingest "$WORK/retry.json"
RETRY_CODE=$INGEST_CODE
AFTER_RETRY=$(count)
RUN2_INGESTED=$(field "$WORK/run2.json" ingested)
# Every error is checked, not sampled: its line number is in range and not repeated, and
# its reason is the store's.
read -r ERRORS STORE_ERRORS DISTINCT IN_RANGE FIRST_MSG < <(python3 -c 'import json,sys
r = json.load(open(sys.argv[1]))
errs = r.get("errors", [])
store = [e for e in errs if e["msg"].startswith("not ingested: store: ")]
lines = [e["line"] for e in errs]
in_range = all(1 <= n <= int(sys.argv[2]) for n in lines)
first = errs[0]["msg"].replace(" ", "_") if errs else "-"
print(len(errs), len(store), len(set(lines)), "yes" if in_range else "no", first)' \
  "$WORK/run2.json" "$NLINES")
OMITTED=$(field "$WORK/run2.json" errors_omitted)
stop
RUN2_EXIT=$STOP_CODE
echo "run 2                $RUN2_CODE, ingested $RUN2_INGESTED, ${RUN2_RETRY:-Retry-After: (none)}"
echo "  errors             $ERRORS listed, $STORE_ERRORS with a store reason, $DISTINCT distinct lines${OMITTED:+, $OMITTED omitted}"
echo "  first reason       ${FIRST_MSG//_/ }"
echo "  /health            $HEALTH_BODY"
echo "  retry              $RETRY_CODE, COUNT LOG {} $STORED before and $AFTER_RETRY after"
echo "  exit               $RUN2_EXIT"
grep -E '^triedb: .*(exit [0-9]|refused|dropped)' "$WORK/run2.log" | sed 's/^/  /'

# ── Run 3: the directory given back ──────────────────────────────────────────────────
chmod 755 "$LOCKED"
LOCKED=""
start "$WORK/run3.log" || exit 1
ingest "$WORK/run3.json"
RUN3_CODE=$INGEST_CODE
RUN3_INGESTED=$(field "$WORK/run3.json" ingested)
RUN3_COUNT=$(count)
stop
RUN3_EXIT=$STOP_CODE
WANT_COUNT=$((RUN1_INGESTED + RUN2_INGESTED + RUN3_INGESTED))
echo "run 3                $RUN3_CODE, ingested $RUN3_INGESTED, COUNT LOG {} $RUN3_COUNT (want $WANT_COUNT), exit $RUN3_EXIT"

echo
FAIL=0
[ "$RUN1_CODE" = "200" ] && [ "$RUN1_INGESTED" = "$NLINES" ] && [ "$RUN1_EXIT" = "0" ] \
  || { echo "FAIL run 1 was not clean: $RUN1_CODE, $RUN1_INGESTED, exit $RUN1_EXIT"; FAIL=1; }
[ "$LEAF_ROWS" -gt 0 ] 2>/dev/null || { echo "FAIL the locked leaf held no rows, so nothing was tested"; FAIL=1; }
[ "$RUN2_CODE" = "503" ] || { echo "FAIL run 2 answered $RUN2_CODE, want 503"; FAIL=1; }
case "$RUN2_RETRY" in *[Rr]etry-[Aa]fter:*) ;; *) echo "FAIL the 503 carried no Retry-After"; FAIL=1 ;; esac
if [ -z "$RUN2_INGESTED" ] || [ "$RUN2_INGESTED" -ge "$NLINES" ]; then
  echo "FAIL run 2 counted ${RUN2_INGESTED:-nothing} ingested, want below $NLINES"; FAIL=1
elif [ "$RUN2_INGESTED" != "$((NLINES - LEAF_ROWS))" ]; then
  echo "FAIL run 2 counted $RUN2_INGESTED ingested, want $NLINES - $LEAF_ROWS"; FAIL=1
fi
# `errors` is capped at a thousand (MAX_REPORTED_ERRORS in src/serve.rs) and the rest are
# counted in `errors_omitted`. At the default NLINES the locked leaf is far below it, and
# every dropped line has to be listed.
CAP=1000
WANT_LISTED=$(( LEAF_ROWS < CAP ? LEAF_ROWS : CAP ))
[ "$ERRORS" = "$WANT_LISTED" ] || { echo "FAIL $ERRORS error(s) listed, want $WANT_LISTED of the $LEAF_ROWS dropped"; FAIL=1; }
[ "$((ERRORS + ${OMITTED:-0}))" = "$LEAF_ROWS" ] || { echo "FAIL $ERRORS listed and ${OMITTED:-0} omitted, want the $LEAF_ROWS dropped"; FAIL=1; }
[ "$STORE_ERRORS" = "$ERRORS" ] || { echo "FAIL $((ERRORS - STORE_ERRORS)) error(s) without a store reason"; FAIL=1; }
[ "$DISTINCT" = "$ERRORS" ] && [ "$IN_RANGE" = "yes" ] || { echo "FAIL the error line numbers repeat or fall outside 1..$NLINES"; FAIL=1; }
case "$HEALTH_BODY" in *degraded*) ;; *) echo "FAIL /health did not report degraded after three ticks: $HEALTH_BODY"; FAIL=1 ;; esac
[ "$RETRY_CODE" = "503" ] || { echo "FAIL the retry answered $RETRY_CODE, want 503"; FAIL=1; }
[ -n "$STORED" ] && [ "$STORED" = "$AFTER_RETRY" ] || { echo "FAIL the retry stored rows: COUNT LOG {} went from $STORED to $AFTER_RETRY"; FAIL=1; }
if [ -z "$RUN2_EXIT" ] || [ "$RUN2_EXIT" = "0" ]; then
  echo "FAIL the stop after dropped rows exited ${RUN2_EXIT:-?}, want non-zero"; FAIL=1
fi
[ "$RUN3_CODE" = "200" ] && [ "$RUN3_INGESTED" = "$NLINES" ] \
  || { echo "FAIL with the directory back, ingest answered $RUN3_CODE with $RUN3_INGESTED ingested"; FAIL=1; }
[ "$RUN3_COUNT" = "$WANT_COUNT" ] || { echo "FAIL COUNT LOG {} is $RUN3_COUNT, want $WANT_COUNT"; FAIL=1; }
[ "$RUN3_EXIT" = "0" ] || { echo "FAIL the clean stop after the directory came back exited $RUN3_EXIT"; FAIL=1; }
if [ "$FAIL" = "0" ]; then
  echo "PASS $LEAF_ROWS dropped row(s) were refused by line with a 503, not counted, and the exit said so"
  exit 0
fi
exit 1
