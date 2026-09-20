#!/usr/bin/env bash
# Deleting trie.json must change no answer.
#
# Ingest 2,000 `--seed 42` lines, stop cleanly, record three queries, delete trie.json,
# restart, run them again. The two answer sets must be byte-identical. Before the catalog
# every query came back 0 after the delete: trie.json was the only thing
# on disk that could rebuild a trie, so losing it made every row unreachable.
#
#   NLINES=2000 GEN_SEED=42 scripts/lost_trie.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-42}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -TERM "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT"

# Port 0 and the bound port read back off the banner, as store_agree.sh does. Sets PID
# and PORT in this shell, so the server is this shell's child and `wait` means something.
PORT=""
start() {  # start <logfile>
  "$BIN" serve --port 0 --data-dir "$WORK/data" 2>"$1" &
  PID=$!
  PORT=""
  # Up to 60s: a restart after kill -9 re-seals every unsealed segment first, one fsync
  # each, and on the M5.5 layout that is hundreds of files.
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; cat "$1" >&2; return 1; fi
}

stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
}

QUERIES=(
  'COUNT LOG {}'
  'COUNT LOG {"event": "login_failed"}'
  'COUNT LOG {"level": "error"}'
)

ask() {  # ask <port> <outfile>
  : > "$2"
  for q in "${QUERIES[@]}"; do
    body=$(python3 -c 'import json,sys; print(json.dumps({"q": sys.argv[1]}))' "$q")
    answer=$(curl -s -X POST "http://127.0.0.1:$1/query" \
               -H 'Content-Type: application/json' --data "$body" \
             | python3 -c 'import json,sys; a=json.load(sys.stdin); print(json.dumps({k: a.get(k) for k in ("exists","count")}, sort_keys=True))')
    printf '%-40s %s\n' "$q" "$answer" >> "$2"
  done
}

start "$WORK/run1.log" || exit 1
code=$(curl -s -o "$WORK/ingest.json" -w '%{http_code}' \
       -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$INPUT")
[ "$code" = "200" ] || { echo "ingest answered $code" >&2; cat "$WORK/ingest.json" >&2; exit 1; }
echo "ingested $(python3 -c 'import json,sys; print(json.load(sys.stdin)["ingested"])' < "$WORK/ingest.json") of $NLINES lines"
stop
[ -f "$WORK/data/trie.json" ] || { echo "the clean stop wrote no trie.json" >&2; cat "$WORK/run1.log" >&2; exit 1; }

# Round 1 is asked after the restart too, so both answer sets come from files a fresh
# process reopened. What differs between the rounds is only whether trie.json exists.
start "$WORK/run2.log" || exit 1
ask "$PORT" "$WORK/before.txt"
stop

rm "$WORK/data/trie.json"
echo "deleted trie.json"

start "$WORK/run3.log" || exit 1
ask "$PORT" "$WORK/after.txt"
stop

echo
echo "before (trie.json present)"; sed 's/^/  /' "$WORK/before.txt"
echo "after  (trie.json deleted)"; sed 's/^/  /' "$WORK/after.txt"
echo
grep -E "WARN|restored|catalog" "$WORK/run3.log" | sed 's/^/  run3: /'

if cmp -s "$WORK/before.txt" "$WORK/after.txt"; then
  echo "identical: deleting trie.json changed no answer"
  exit 0
fi
echo "DIFFERENT: deleting trie.json changed the answers"
exit 1
