#!/usr/bin/env bash
# Shared by every Part A script: paths, the results CSV, server start/stop, machine info.
# Shell plus the Python standard library only.
set -uo pipefail

LABRS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(dirname "$LABRS_DIR")"
BIN="$REPO_DIR/target/release/kibbr"
OUT="$LABRS_DIR/out"
CSV="$LABRS_DIR/results/results.csv"
DATA_1M="$OUT/data/1m.ndjson"
DATA_300K="$OUT/data/300k.ndjson"
PY=python3
mkdir -p "$OUT" "$LABRS_DIR/results"

GIT_SHA="$(git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

build_binary() {
  (cd "$REPO_DIR" && cargo build --release 2>&1 | tail -1)
}

# The corpora. gen-logs is deterministic, so these are rebuilt rather than kept.
ensure_data() {
  [[ -s "$DATA_1M" ]] || { mkdir -p "$OUT/data"; "$BIN" gen-logs --lines 1000000 --seed 42 > "$DATA_1M"; }
  [[ -s "$DATA_300K" ]] || head -300000 "$DATA_1M" > "$DATA_300K"
}

csv_quote() {
  local s="$1"
  if [[ "$s" == *,* || "$s" == *\"* ]]; then
    s="${s//\"/\"\"}"; printf '"%s"' "$s"
  else
    printf '%s' "$s"
  fi
}

# csv_row bench variant metric value unit note
csv_row() {
  [[ -s "$CSV" ]] || echo "when,git_sha,bench,variant,metric,value,unit,note" > "$CSV"
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$NOW_ISO" "$GIT_SHA" "$1" "$(csv_quote "$2")" "$(csv_quote "$3")" "$4" "$5" "$(csv_quote "$6")" >> "$CSV"
}

# Median and spread of three values, printed as "median spread_pct".
median3() {
  $PY - "$@" <<'PYEOF'
import sys
v = sorted(float(x) for x in sys.argv[1:])
n = len(v)
med = v[n//2] if n % 2 else (v[n//2-1] + v[n//2]) / 2
spread = 0 if med == 0 else (v[-1] - v[0]) / med * 100
print(f"{med:.4f} {spread:.1f}")
PYEOF
}

# record3 bench variant metric unit note v1 v2 v3 -> writes median and spread rows.
record3() {
  local bench="$1" variant="$2" metric="$3" unit="$4" note="$5"; shift 5
  read -r med spread < <(median3 "$@")
  csv_row "$bench" "$variant" "$metric" "$med" "$unit" "$note"
  csv_row "$bench" "$variant" "${metric}_spread" "$spread" "pct_of_median" "max-min over 3 runs: $*"
  echo "  $variant $metric = $med $unit (spread ${spread}% over runs: $*)"
}

# Recorded once per results.csv: the report dedups on bench=machine.
machine_info() {
  grep -q ',machine,' "$CSV" 2>/dev/null && return
  csv_row machine cpu model 0 text "$(sysctl -n machdep.cpu.brand_string)"
  csv_row machine cores count "$(sysctl -n hw.ncpu)" cores "logical; $(sysctl -n hw.physicalcpu) physical"
  csv_row machine ram bytes "$(sysctl -n hw.memsize)" bytes ""
  csv_row machine filesystem name 0 text "$(diskutil info / | awk -F': *' '/File System Personality/{print $2}')"
  csv_row machine disk type 0 text "$(diskutil info / | awk -F': *' '/Solid State/{print ($2=="Yes")?"SSD":"HDD"} /Device Node/{n=$2} END{}') $(diskutil info / | awk -F': *' '/Device \/ Media Name/{print $2}')"
  csv_row machine kernel version 0 text "$(uname -sr)"
  csv_row machine rustc version 0 text "$(rustc --version)"
  csv_row machine python version 0 text "$($PY --version)"
}

SERVER_PID=""
SERVER_PORT=""
# start_server port data_dir [extra serve args...]; waits for /health. Sets SERVER_PID.
start_server() {
  local port="$1" dir="$2"; shift 2
  mkdir -p "$dir"
  # The first 1M-row run died with EMFILE at 256 descriptors: a flush tick opens one
  # segment per leaf per day. Raised for every server this lab starts.
  ulimit -n 65536 2>/dev/null
  # SERVER_LOG_DIR moves the log off the data volume (A5 fills that volume, and a server
  # whose stderr is on a full disk cannot even report the shutdown).
  SERVER_LOG="${SERVER_LOG_DIR:+$SERVER_LOG_DIR/$(basename "$dir").server.log}"; SERVER_LOG="${SERVER_LOG:-$dir.server.log}"
  "$BIN" serve --port "$port" --data-dir "$dir" "$@" > "$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  SERVER_PORT=$port
  for _ in $(seq 1 600); do
    curl -sf "http://127.0.0.1:$port/health" >/dev/null 2>&1 && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died: $(tail -3 "$SERVER_LOG")"; return 1; }
    sleep 0.05
  done
  echo "server did not answer /health"; return 1
}

# Same as start_server but timed: seconds from spawn until /health answers, left in
# COLD_START. Not printed, because a command substitution would run it in a subshell
# and lose SERVER_PID - which is how one A2 run left a server holding the port.
COLD_START=""
start_server_timed() {
  local t0 t1
  t0=$($PY -c 'import time; print(time.time())')
  start_server "$@" || return 1
  t1=$($PY -c 'import time; print(time.time())')
  COLD_START=$($PY -c "print(f'{$t1 - $t0:.3f}')")
}

# kill_server skips the shutdown flush (minutes for a big buffer) when the data dir is
# about to be deleted anyway.
kill_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -KILL "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  kill -TERM "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

# Waits until /stats reports buffered_rows == 0 (everything reached a segment file).
wait_flushed() {
  local port="$1" secs="${2:-1200}" start=$SECONDS
  while (( SECONDS - start < secs )); do
    local b
    # /stats waits on the store lock, which a flush tick holds; -m bounds that wait.
    b=$(curl -s -m 10 "http://127.0.0.1:$port/stats" | $PY -c 'import json,sys; print(json.load(sys.stdin)["storage"]["buffered_rows"])' 2>/dev/null || echo 1)
    [[ "$b" == "0" ]] && return 0
    sleep 0.2
  done
  echo "  (still buffered after ${secs}s)"
}

rss_kb() { ps -o rss= -p "$1" | tr -d ' '; }

trap 'stop_server' EXIT
