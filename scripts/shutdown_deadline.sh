#!/usr/bin/env bash
# A stop is over inside the thirty seconds Kubernetes gives it, whatever its clients do.
#
# Kubernetes sends SIGKILL thirty seconds after its SIGTERM. A stop used to wait for every
# request in flight for as long as each took, and two kinds of request take as long as
# they like (.dev/BLACKBOX_FINDINGS.md, 3 and 4): one whose client sent its headers and
# then nothing - the stop tracked the client's socket to the second, 45 s, 150 s, 240 s -
# and a query nobody had bounded, `MATCH LOG {}` with `--max-rows 0`, which ran for a
# minute or two more and kept its thread, its core and every row it had fetched. Behind
# either the final flush, the checkpoint and trie.json waited, and the SIGKILL arrived
# first. Nothing acknowledged was lost - it is in the write-ahead log - but no stop was
# ever a clean one.
#
# `[api] shutdown_deadline`, 25 seconds, bounds the wait: what is still in flight then is
# answered 503 and let go, a query's scan is told to stop and does, and the wind-down of
# The shutdown order runs in the five seconds that are left.
#
# RUNS times, each on its own copy of one store of QUERY_LINES rows:
#
#   1. a server is started with `--max-rows 0` and a row group timeout past the length of
#      the run, and BUFFERED lines are sent through /ingest: every one of them is in the
#      log and in a buffer, and in no row group;
#   2. a client sends the headers of an /ingest body and never the body, and keeps its
#      socket; a second connects and sends nothing at all; a third asks `MATCH LOG {}`;
#   3. SIGTERM, at t=0.
#
# What this asserts, each run:
#
#   a. every buffered row was buffered before the stop, so the stop is what writes them;
#   b. the deadline found both requests in flight - the held body and the query. A query
#      that finished before it is a store too small for this machine, not a pass: the run
#      is INCONCLUSIVE (exit 2) and says to raise QUERY_LINES;
#   c. the query's scan stopped: the server says so, within STOP_SECONDS of the deadline;
#   d. the wind-down ran, in order: the final flush wrote the BUFFERED rows, then the
#      checkpoint, then trie.json;
#   e. the process was gone by GRACE_SECONDS after the signal, and exited 0;
#   f. a restart had nothing to read back from the log, and `COUNT LOG {}` is
#      QUERY_LINES + BUFFERED: every buffered row, and nothing twice.
#
# A line per run: when the deadline fired, when the scan stopped, when the process was
# gone, its exit code, the rows the final flush wrote, what the restart replayed and what
# it counts.
#
# The store is loaded once through the bulk door and kept under testdata/shutdown-deadline/
# (scratch, ignored): 2,500,000 lines are 590 MB of NDJSON and 430 MB of store, and about
# a minute and a half to load. A run takes a copy of it - a clone, where the volume makes
# them - and removes the copy when it ends. **The server holds every row the query has
# fetched until the deadline, which on this store is several gigabytes**: that is the
# finding, and the reason the scan has to stop.
#
#   RUNS=5 scripts/shutdown_deadline.sh
#   QUERY_LINES=1000000 BUFFERED=20000 RUNS=1 scripts/shutdown_deadline.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/kibbr}
RUNS=${RUNS:-5}
QUERY_LINES=${QUERY_LINES:-2500000}
BUFFERED=${BUFFERED:-100000}
GEN_SEED=${GEN_SEED:-42}
SENDERS=${SENDERS:-4}
BATCH=${BATCH:-2000}
# What kills the process next, and what the stop is held to.
GRACE_SECONDS=${GRACE_SECONDS:-30}
# How long after the deadline the scan may take to say it stopped.
STOP_SECONDS=${STOP_SECONDS:-2}
# How long the clients are given to be in flight before the signal.
SETTLE_SECONDS=${SETTLE_SECONDS:-1}
# How long to wait for the stop before giving up on it: the number is worth having even
# when it fails, and the query alone was two minutes before there was a deadline.
STOP_TIMEOUT_SECONDS=${STOP_TIMEOUT_SECONDS:-600}
# Flags for the server under test, beyond the ones this script sets: `--shutdown-deadline
# 0s` shows what a stop was before there was one.
SERVE_FLAGS=${SERVE_FLAGS:-}

# Another build can be measured by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1

mkdir -p testdata/shutdown-deadline
STREAM=testdata/shutdown-deadline/${QUERY_LINES}-seed${GEN_SEED}.ndjson
EXTRA=testdata/shutdown-deadline/${BUFFERED}-seed$((GEN_SEED + 1)).ndjson
BASE=testdata/shutdown-deadline/base-${QUERY_LINES}-seed${GEN_SEED}

[ -s "$EXTRA" ] || "$BIN" gen-logs --lines "$BUFFERED" --seed $((GEN_SEED + 1)) > "$EXTRA" || exit 1
# `loaded` is written last: a base without it is a load that was interrupted.
if [ ! -f "$BASE/loaded" ]; then
  [ ! -e "$BASE" ] || { echo "$BASE is there and was not finished: move it away and run again" >&2; exit 1; }
  [ -s "$STREAM" ] || "$BIN" gen-logs --lines "$QUERY_LINES" --seed "$GEN_SEED" > "$STREAM" || exit 1
  echo "loading $QUERY_LINES lines into $BASE, once"
  "$BIN" load --data-dir "$BASE" "$STREAM" 2>"$BASE.load.log" >/dev/null ||
    { echo "the load failed:" >&2; tail -5 "$BASE.load.log" >&2; exit 1; }
  : > "$BASE/loaded"
fi

WORK=$(mktemp -d)
PID=""
CLIENTS=()
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  for c in ${CLIENTS[@]+"${CLIENTS[@]}"}; do kill -KILL "$c" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# One sender: its share of the bodies, once, down one keep-alive connection.
cat > "$WORK/send.py" <<'PY'
import http.client, json, sys

port, path, batch, share, of = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
lines = open(path, "rb").read().split(b"\n")
if lines and lines[-1] == b"":
    lines.pop()
bodies = [b"\n".join(lines[i:i + batch]) + b"\n" for i in range(0, len(lines), batch)][share::of]
conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=900)
ingested = 0
for body in bodies:
    conn.request("POST", "/ingest", body, {"Content-Type": "application/x-ndjson"})
    response = conn.getresponse()
    text = response.read()
    if response.status != 200:
        print(f"ingest answered {response.status}: {text[:300]!r}", file=sys.stderr)
        sys.exit(1)
    ingested += json.loads(text)["ingested"]
print(ingested)
PY

# A client that keeps a socket and does not finish: `body` sends the headers of an /ingest
# body of a thousand bytes and ten of them; `idle` sends nothing. Each prints the first
# line it is answered with, or how the server let it go, and when.
cat > "$WORK/hold.py" <<'PY'
import socket, sys, time

mode, port, seconds = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
began = time.time()
s = socket.create_connection(("127.0.0.1", port))
if mode == "body":
    s.sendall(b"POST /ingest HTTP/1.1\r\nHost: kibbr\r\nContent-Type: application/x-ndjson\r\n"
              b"Content-Length: 1000\r\n\r\n" + b'{"held":1}')
print("holding", flush=True)
s.settimeout(seconds)
try:
    got = s.recv(4096)
    said = got.split(b"\r\n")[0].decode("latin-1") if got else "closed with no answer"
except socket.timeout:
    said = "nothing in %.0fs" % seconds
except OSError as e:
    said = "closed: %s" % e
print("%s after %.2fs" % (said, time.time() - began), flush=True)
PY

stat_field() {  # stat_field <dotted path>
  curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(a)' "$1" 2>/dev/null
}

count_all() {
  curl -s -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       --data '{"q": "COUNT LOG {}"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null
}

PORT=""
start() {  # start <data dir> <log> [serve flags...]
  local dir=$1 log=$2
  shift 2
  # Each line of the log stamped as it arrives, for the timeline: seconds since the epoch.
  "$BIN" serve --port 0 --data-dir "$dir" "$@" 2> >(python3 -u -c 'import sys, time
for line in sys.stdin:
    sys.stdout.write("%.3f %s" % (time.time(), line))' > "$log") &
  PID=$!
  PORT=""
  # Up to ten minutes: a start after a stop that did not finish reads the log back first.
  for _ in $(seq 1 6000); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$log" >&2; return 1; fi
}

now() { python3 -c 'import time; print("%.3f" % time.time())'; }
# since <t0> <log> <pattern>: seconds from t0 to the first line of the log that matches.
since() {
  python3 - "$1" "$2" "$3" <<'PY'
import re, sys
t0, path, pattern = float(sys.argv[1]), sys.argv[2], re.compile(sys.argv[3])
for line in open(path, errors="replace"):
    stamp, _, rest = line.partition(" ")
    if pattern.search(rest):
        print("%.2f" % (float(stamp) - t0))
        break
else:
    print("-")
PY
}
under() { python3 -c 'import sys; sys.exit(0 if float(sys.argv[1]) <= float(sys.argv[2]) else 1)' "$1" "$2" 2>/dev/null; }

WANT=$((QUERY_LINES + BUFFERED))
echo "store $QUERY_LINES rows, $BUFFERED buffered at the signal; the process must be gone ${GRACE_SECONDS}s after it"
echo
PASSED=0
INCONCLUSIVE=0
for run in $(seq 1 "$RUNS"); do
  DB=$WORK/db
  rm -rf "$DB"
  cp -Rc "$BASE" "$DB" 2>/dev/null || cp -R "$BASE" "$DB" || exit 1
  rm -f "$DB/loaded"
  LOG=$WORK/run$run.log

  # shellcheck disable=SC2086
  start "$DB" "$LOG" --max-rows 0 --row-group-timeout-ms 86400000 $SERVE_FLAGS || exit 1
  pids=()
  for k in $(seq 0 $((SENDERS - 1))); do
    python3 "$WORK/send.py" "$PORT" "$EXTRA" "$BATCH" "$k" "$SENDERS" > "$WORK/sent.$k" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || { echo "a sender failed" >&2; exit 1; }; done
  HELD=$(stat_field storage.buffered_rows)

  # The three clients, in flight before the signal.
  python3 "$WORK/hold.py" body "$PORT" "$STOP_TIMEOUT_SECONDS" > "$WORK/body.out" 2>&1 &
  CLIENTS=($!)
  python3 "$WORK/hold.py" idle "$PORT" "$STOP_TIMEOUT_SECONDS" > "$WORK/idle.out" 2>&1 &
  CLIENTS+=($!)
  curl -s -o /dev/null -w '%{http_code} after %{time_total}s' -m "$STOP_TIMEOUT_SECONDS" \
       -X POST "http://127.0.0.1:$PORT/query" -H 'Content-Type: application/json' \
       --data '{"q": "MATCH LOG {}"}' > "$WORK/query.out" 2>/dev/null &
  CLIENTS+=($!)
  sleep "$SETTLE_SECONDS"

  T0=$(now)
  kill -TERM "$PID"
  GONE=no
  for _ in $(seq 1 $((STOP_TIMEOUT_SECONDS * 20))); do
    kill -0 "$PID" 2>/dev/null || { GONE=yes; break; }
    sleep 0.05
  done
  EXIT_AT=$(python3 -c 'import sys, time; print("%.2f" % (time.time() - float(sys.argv[1])))' "$T0")
  if [ "$GONE" = yes ]; then wait "$PID"; CODE=$?; else CODE="(still running)"; kill -KILL "$PID" 2>/dev/null; fi
  PID=""
  # The log's writer ends with the server's stderr; the clients with their sockets.
  sleep 0.3
  for c in "${CLIENTS[@]}"; do kill -KILL "$c" 2>/dev/null; done
  wait 2>/dev/null
  CLIENTS=()

  CUT_AT=$(since "$T0" "$LOG" 'shutdown deadline - ')
  CUT_OF=$(grep -o 'shutdown deadline - [0-9]* request' "$LOG" | grep -o '[0-9][0-9]*' | head -1)
  STOP_AT=$(since "$T0" "$LOG" 'query stopped - ')
  FLUSH_AT=$(since "$T0" "$LOG" 'final flush - ')
  CHECK_AT=$(since "$T0" "$LOG" 'WAL checkpoint - ')
  TRIE_AT=$(since "$T0" "$LOG" 'trie\.json - ')
  FLUSHED=$(grep -o 'final flush - [0-9,]* row' "$LOG" | grep -o '[0-9][0-9,]*' | head -1 | tr -d ,)

  # What a fresh process reads back.
  start "$DB" "$WORK/again$run.log" || exit 1
  REPLAYED=$(stat_field storage.wal.replay.records)
  AFTER=$(count_all)
  kill -TERM "$PID"; wait "$PID" 2>/dev/null; PID=""

  printf 'run %d  deadline +%ss, %s in flight  scan stopped +%ss  flush +%ss  checkpoint +%ss  trie.json +%ss  gone +%ss  exit %s  flushed %s  replayed %s  COUNT %s\n' \
    "$run" "$CUT_AT" "${CUT_OF:--}" "$STOP_AT" "$FLUSH_AT" "$CHECK_AT" "$TRIE_AT" "$EXIT_AT" "$CODE" "${FLUSHED:--}" "${REPLAYED:--}" "${AFTER:--}"
  printf '       held body: %s; idle socket: %s; query: %s\n' \
    "$(tail -1 "$WORK/body.out")" "$(tail -1 "$WORK/idle.out")" "$(cat "$WORK/query.out")"

  FAIL=0
  [ "$HELD" = "$BUFFERED" ] ||
    { echo "  FAIL $HELD rows were buffered before the stop, want all $BUFFERED"; FAIL=1; }
  if [ "${CUT_OF:-0}" != 2 ]; then
    if [ "$CUT_AT" = "-" ] || [ "${CUT_OF:-0}" = 1 ]; then
      echo "  INCONCLUSIVE the deadline found ${CUT_OF:-no} request(s) in flight, want 2: the query finished first. Raise QUERY_LINES"
      INCONCLUSIVE=1
    else
      echo "  FAIL the deadline found $CUT_OF request(s) in flight, want 2"; FAIL=1
    fi
  fi
  if [ "$STOP_AT" = "-" ]; then
    echo "  FAIL the server never said the query's scan stopped"; FAIL=1
  elif [ "$CUT_AT" != "-" ]; then
    under "$STOP_AT" "$(python3 -c 'import sys; print(float(sys.argv[1]) + float(sys.argv[2]))' "$CUT_AT" "$STOP_SECONDS")" ||
      { echo "  FAIL the scan stopped at +${STOP_AT}s, more than ${STOP_SECONDS}s after the deadline at +${CUT_AT}s"; FAIL=1; }
  fi
  if [ "$FLUSH_AT" = "-" ] || [ "$CHECK_AT" = "-" ] || [ "$TRIE_AT" = "-" ]; then
    echo "  FAIL the wind-down did not run: final flush +$FLUSH_AT, WAL checkpoint +$CHECK_AT, trie.json +$TRIE_AT"; FAIL=1
  else
    { under "$FLUSH_AT" "$CHECK_AT" && under "$CHECK_AT" "$TRIE_AT"; } ||
      { echo "  FAIL the wind-down ran out of order: flush +$FLUSH_AT, checkpoint +$CHECK_AT, trie.json +$TRIE_AT"; FAIL=1; }
  fi
  [ "${FLUSHED:-}" = "$BUFFERED" ] ||
    { echo "  FAIL the final flush wrote ${FLUSHED:-no} rows, want the $BUFFERED buffered"; FAIL=1; }
  [ "$CODE" = "0" ] || { echo "  FAIL the stop exited $CODE"; FAIL=1; }
  under "$EXIT_AT" "$GRACE_SECONDS" ||
    { echo "  FAIL the process was gone ${EXIT_AT}s after the signal, want within ${GRACE_SECONDS}s"; FAIL=1; }
  [ "$REPLAYED" = "0" ] ||
    { echo "  FAIL the restart read $REPLAYED record(s) back from the log, want 0: the stop was not a clean one"; FAIL=1; }
  [ "$AFTER" = "$WANT" ] ||
    { echo "  FAIL COUNT LOG {} is $AFTER after the restart, want $WANT"; FAIL=1; }
  if [ "$FAIL" = 0 ] && [ "$INCONCLUSIVE" = 0 ]; then
    PASSED=$((PASSED + 1))
  else
    echo "  the stop's log:"; sed 's/^[0-9.]* /    /' "$LOG" | grep -v '^    *$' | tail -14
  fi
done

echo
if [ "$PASSED" = "$RUNS" ]; then
  echo "PASS $PASSED of $RUNS: gone within ${GRACE_SECONDS}s of the signal, exit 0, and all $WANT rows counted after the restart"
  exit 0
fi
echo "FAIL $PASSED of $RUNS passed"
[ "$INCONCLUSIVE" = 0 ] || exit 2
exit 1
