#!/usr/bin/env bash
# Do queries and ingest get in each other's way?
#
# The four standard searches, on an idle server and while four senders ingest at full
# speed, and the ingest rate with and without those searches running beside it.
#
# Measured on the build that had one lock over every leaf of every template - taken for
# writing by each ingest body, and by the flush tick across the encode, the write and the
# fsync of every row group it closed; taken for reading by each query for the whole of its
# run - ingest fell from 44,800 lines a second to 1,645 with the searches running, and the
# count by a promoted value went from 33ms to 1.1s.
#
# The searches, over the seed-42 corpus:
#
#   S1  one template's worth, by a promoted value - `event` is in the fingerprint
#   S2  many templates, by a plain column - `level` is in most of them
#   S3  one id - a UUID column whose every value differs; line 1's trace id
#   S4  the newest 500 rows across everything
#
# One run, on a fresh copy of the same 1,000,000-row data directory:
#
#   idle                each search ITER times, nothing else running
#   ingest alone        four senders for LOAD_SECONDS, looping over the first 300,000 lines
#   ingest + searches   the same four senders, and each search in turn for a quarter of
#                       the window
#
# RUNS runs; every number printed is the median over them. The data directory is built
# once, by this script, through `/ingest`, and kept in testdata/live-door/ - delete it to
# rebuild. A run's server is killed rather than stopped: what it holds is not needed, and
# a clean stop would time the final flush instead of the searches.
#
# PASS when ingest with the searches running keeps more than half its rate without them.
#
#   RUNS=3 ITER=20 LOAD_SECONDS=32 scripts/search_under_load.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/triedb}
RUNS=${RUNS:-3}
ITER=${ITER:-20}
LOAD_SECONDS=${LOAD_SECONDS:-32}
SENDERS=${SENDERS:-4}
BATCH=${BATCH:-2000}
GEN_SEED=${GEN_SEED:-42}
CORPUS_LINES=${CORPUS_LINES:-1000000}
LOAD_LINES=${LOAD_LINES:-300000}
# How long the corpus build may take to reach disk. Its rows close on the 60-second row
# group timeout, so the wait is at least that.
DRAIN_SECONDS=${DRAIN_SECONDS:-1800}

S1='COUNT LOG {"event":"login_ok"}'
S2='COUNT LOG {"level":"error"}'
S3='MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}'
S4='MATCH LOG {} LIMIT 500'

# Another build can be measured by naming its binary; the default is built first.
[ "$BIN" != ./target/release/triedb ] || cargo build --release --quiet || exit 1

DIR=testdata/live-door
mkdir -p "$DIR"
INPUT=$DIR/${CORPUS_LINES}-seed${GEN_SEED}.ndjson
LOAD=$DIR/${LOAD_LINES}-seed${GEN_SEED}.ndjson
CORPUS=$DIR/corpus-${CORPUS_LINES}-seed${GEN_SEED}
[ -s "$INPUT" ] || "$BIN" gen-logs --lines "$CORPUS_LINES" --seed "$GEN_SEED" > "$INPUT"
[ -s "$LOAD" ] || head -n "$LOAD_LINES" "$INPUT" > "$LOAD"

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# The load: `send` loops over its share of the bodies for a number of seconds, `once` sends
# its share of them once, `search` runs one query for a count or for a number of seconds.
# One keep-alive connection per process: a connection per request would measure the TCP
# handshake, and a thread per sender in one process would put the senders behind one GIL.
cat > "$WORK/load.py" <<'PY'
import http.client, json, sys, time

def connect(port):
    return http.client.HTTPConnection("127.0.0.1", int(port), timeout=900)

def bodies(path, batch):
    lines = open(path, "rb").read().split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return [b"\n".join(lines[i:i + batch]) + b"\n" for i in range(0, len(lines), batch)]

def post(conn, path, body, kind):
    conn.request("POST", path, body, {"Content-Type": kind})
    response = conn.getresponse()
    return response.status, response.read()

def ingest(port, path, batch, share, of, seconds):
    all_bodies = bodies(path, int(batch))
    # A file of fewer bodies than senders leaves a sender none of its own; it sends them all.
    mine = all_bodies[int(share)::int(of)] or all_bodies
    conn = connect(port)
    ingested = shed = bad = requests = 0
    start = time.monotonic()
    deadline = start + float(seconds) if seconds else None
    at = 0
    while True:
        if deadline is None and at >= len(mine):
            break
        if deadline is not None and time.monotonic() >= deadline:
            break
        body = mine[at % len(mine)]
        status, text = post(conn, "/ingest", body, "application/x-ndjson")
        requests += 1
        if status == 200:
            ingested += json.loads(text)["ingested"]
            at += 1
        elif status == 503:
            # Shed or refused: the body is sent again, as a shipper would.
            shed += 1
            time.sleep(0.05)
        else:
            bad += 1
            at += 1
    print(json.dumps({"ingested": ingested, "requests": requests, "shed": shed, "bad": bad,
                      "seconds": time.monotonic() - start}))

def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p * (len(values) - 1))))] if values else 0

def search(port, q, n, seconds):
    conn = connect(port)
    body = json.dumps({"q": q}).encode()
    client, server = [], []
    answer = {}
    deadline = time.monotonic() + float(seconds) if float(seconds) > 0 else None
    while (deadline is None and len(client) < int(n)) or \
          (deadline is not None and time.monotonic() < deadline):
        start = time.perf_counter()
        status, text = post(conn, "/query", body, "application/json")
        client.append((time.perf_counter() - start) * 1000)
        if status != 200:
            print(json.dumps({"error": status, "body": text[:300].decode(errors="replace")}))
            sys.exit(1)
        answer = json.loads(text)
        server.append(answer["meta"]["duration_ms"])
    meta = answer.get("meta", {})
    print(json.dumps({
        "n": len(client),
        "client_p50": pct(client, 0.50), "client_p99": pct(client, 0.99),
        "client_max": max(client) if client else 0,
        "server_p50": pct(server, 0.50), "server_p99": pct(server, 0.99),
        "count": answer.get("count"), "rows_scanned": meta.get("rows_scanned"),
        "files_opened": meta.get("files_opened"), "row_groups_read": meta.get("row_groups_read"),
    }))

mode = sys.argv[1]
if mode == "send":
    ingest(*sys.argv[2:8])
elif mode == "once":
    ingest(*sys.argv[2:7], 0)
elif mode == "search":
    search(*sys.argv[2:6])
PY

field() {  # field <json file> <key>
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

stat_field() {  # stat_field <dotted path>
  curl -s "http://127.0.0.1:$PORT/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(a)' "$1" 2>/dev/null
}

PORT=""
start() {  # start <data dir> <log>
  "$BIN" serve --port 0 --data-dir "$1" 2>"$2" &
  PID=$!
  PORT=""
  # A start on the corpus opens every data file's footer first; give it a minute.
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$2" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$2" >&2; return 1; fi
}

# `send` or `once` from every sender at once; the lines they landed, and the seconds the
# slowest took. Writes <prefix>.json.
senders() {  # senders <mode> <file> <seconds> <prefix>
  local pids=() k
  for k in $(seq 0 $((SENDERS - 1))); do
    if [ "$1" = send ]; then
      python3 "$WORK/load.py" send "$PORT" "$2" "$BATCH" "$k" "$SENDERS" "$3" > "$4.$k" &
    else
      python3 "$WORK/load.py" once "$PORT" "$2" "$BATCH" "$k" "$SENDERS" > "$4.$k" &
    fi
    pids+=($!)
  done
  for k in "${pids[@]}"; do wait "$k"; done
  python3 - "$4" "$SENDERS" > "$4.json" <<'PY'
import json, sys
parts = [json.load(open(f"{sys.argv[1]}.{k}")) for k in range(int(sys.argv[2]))]
lines = sum(p["ingested"] for p in parts)
seconds = max(p["seconds"] for p in parts)
print(json.dumps({"lines": lines, "seconds": seconds, "lines_per_second": lines / seconds,
                  "shed": sum(p["shed"] for p in parts), "bad": sum(p["bad"] for p in parts)}))
PY
}

# ── The corpus: built once, through /ingest, and every row on disk ──────────────────

if [ ! -f "$CORPUS.done" ]; then
  echo "building $CORPUS from $CORPUS_LINES lines - once"
  rm -rf "$CORPUS"
  start "$CORPUS" "$CORPUS.log" || exit 1
  senders once "$INPUT" 0 "$WORK/corpus"
  echo "  ingested $(field "$WORK/corpus.json" lines) lines in $(printf '%.1f' "$(field "$WORK/corpus.json" seconds)")s"
  DRAINED=no
  for _ in $(seq 1 $((DRAIN_SECONDS * 2))); do
    [ "$(stat_field storage.buffered_rows)" = "0" ] && { DRAINED=yes; break; }
    sleep 0.5
  done
  [ "$DRAINED" = yes ] || { echo "the corpus never reached disk in ${DRAIN_SECONDS}s" >&2; exit 1; }
  kill -TERM "$PID"; wait "$PID"; CODE=$?; PID=""
  [ "$CODE" = 0 ] || { echo "the corpus server exited $CODE" >&2; tail -20 "$CORPUS.log" >&2; exit 1; }
  touch "$CORPUS.done"
fi
echo "corpus               $CORPUS ($(find "$CORPUS/data" -type f | wc -l | tr -d ' ') data files, $(du -sh "$CORPUS" | cut -f1))"
echo "load                 $SENDERS senders, $BATCH-line bodies over $LOAD, ${LOAD_SECONDS}s"
echo "cores                $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
echo

# ── The runs ─────────────────────────────────────────────────────────────────────────

searches() {  # searches <label> <per-search seconds, or 0 for ITER times> <run>
  local s q
  for s in S1 S2 S3 S4; do
    q=${!s}
    python3 "$WORK/load.py" search "$PORT" "$q" "$ITER" "$2" > "$WORK/$3-$1-$s.json" || {
      echo "$s failed: $(cat "$WORK/$3-$1-$s.json")" >&2
      return 1
    }
  done
}

for run in $(seq 1 "$RUNS"); do
  RUN_DIR=$WORK/run-$run
  mkdir -p "$RUN_DIR"
  cp -R "$CORPUS" "$RUN_DIR/db"
  start "$RUN_DIR/db" "$RUN_DIR/serve.log" || exit 1

  searches idle 0 "$run" || exit 1
  senders send "$LOAD" "$LOAD_SECONDS" "$WORK/$run-alone"

  senders send "$LOAD" "$LOAD_SECONDS" "$WORK/$run-with" &
  LOADER=$!
  sleep 1
  searches loaded $(((LOAD_SECONDS - 4) / 4)) "$run" || exit 1
  wait "$LOADER"

  RSS=$(ps -o rss= -p "$PID" | tr -d ' ')
  BUFFERED=$(stat_field storage.buffered_rows)
  printf 'run %s: ingest alone %s lines/s, with searches %s lines/s; S1 idle %.0fms, loaded %.0fms; rss %sMB, %s rows buffered\n' \
    "$run" \
    "$(printf '%.0f' "$(field "$WORK/$run-alone.json" lines_per_second)")" \
    "$(printf '%.0f' "$(field "$WORK/$run-with.json" lines_per_second)")" \
    "$(field "$WORK/$run-idle-S1.json" client_p50)" \
    "$(field "$WORK/$run-loaded-S1.json" client_p50)" \
    "$((RSS / 1024))" "$BUFFERED"
  kill -KILL "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""
  rm -rf "$RUN_DIR/db"
done

# ── The medians ──────────────────────────────────────────────────────────────────────

echo
python3 - "$WORK" "$RUNS" <<'PY'
import json, statistics, sys
work, runs = sys.argv[1], int(sys.argv[2])
def load(name):
    return [json.load(open(f"{work}/{r}-{name}.json")) for r in range(1, runs + 1)]
def median(name, key):
    return statistics.median(x[key] for x in load(name))

print(f"{'':4} {'idle p50':>10} {'idle p99':>10} {'loaded p50':>11} {'loaded p99':>11} "
      f"{'loaded/idle':>12}   rows scanned idle, loaded   (client ms, median of {runs})")
for s in ["S1", "S2", "S3", "S4"]:
    idle50, idle99 = median(f"idle-{s}", "client_p50"), median(f"idle-{s}", "client_p99")
    load50, load99 = median(f"loaded-{s}", "client_p50"), median(f"loaded-{s}", "client_p99")
    scanned = (load(f"idle-{s}")[0]["rows_scanned"], load(f"loaded-{s}")[0]["rows_scanned"])
    print(f"{s:4} {idle50:10.1f} {idle99:10.1f} {load50:11.1f} {load99:11.1f} "
          f"{load50 / max(idle50, 1e-9):11.1f}x   {scanned[0]:,}, {scanned[1]:,}")
alone = median("alone", "lines_per_second")
with_searches = median("with", "lines_per_second")
shed = sum(x["shed"] for x in load("alone") + load("with"))
print()
print(f"ingest alone         {alone:,.0f} lines/s")
print(f"ingest + searches    {with_searches:,.0f} lines/s  ({with_searches / alone:.2f} of alone)")
if shed:
    print(f"bodies answered 503  {shed}")
print()
if with_searches > alone / 2:
    print(f"PASS ingest keeps {with_searches / alone:.2f} of its rate with the searches running")
else:
    print(f"FAIL ingest keeps {with_searches / alone:.2f} of its rate with the searches running, want more than 0.50")
    sys.exit(1)
PY
