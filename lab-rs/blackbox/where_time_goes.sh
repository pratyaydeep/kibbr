#!/usr/bin/env bash
# A1 - lines per second for `triedb parse` alone, the CLI load, and HTTP /ingest with
# 1, 2, 4, 8 senders; the drain rate from buffer to disk; and the crash-replay estimate.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
BENCH=A1_where_time_goes
machine_info; ensure_data
LINES_1M=1000000; LINES_300K=300000
wall() { $PY -c 'import subprocess,sys,time; t=time.perf_counter(); subprocess.run(sys.argv[1:], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); print(f"{time.perf_counter()-t:.3f}")' "$@"; }

echo "== A1.1 triedb parse alone (1M lines, stdout to /dev/null)"
for mode in table json; do
  v=()
  for run in 1 2 3; do
    if [[ $mode == json ]]; then s=$(wall "$BIN" parse "$DATA_1M" --json); else s=$(wall "$BIN" parse "$DATA_1M"); fi
    v+=("$($PY -c "print(f'{$LINES_1M/$s:.1f}')")")
  done
  record3 $BENCH "parse_$mode" lines_per_sec lines_per_sec "triedb parse $mode output > /dev/null, 1M lines" "${v[@]}"
done

echo "== A1.2 CLI ingest: there is no 'triedb ingest' subcommand (ABSENT). The nearest thing is"
echo "   'triedb query FILE --count', which parses every line into the in-memory trie and stores."
csv_row $BENCH cli_ingest status 0 absent "no 'triedb ingest' subcommand; measured 'query FILE --count' (parse + trie + in-memory store) instead"
v=(); secs=()
for run in 1 2 3; do
  s=$(wall "$BIN" query "$DATA_1M" --count --format json); secs+=("$s")
  v+=("$($PY -c "print(f'{$LINES_1M/$s:.1f}')")")
done
record3 $BENCH cli_query_count_load lines_per_sec lines_per_sec "query 1m.ndjson --count: every line parsed into trie + memory store" "${v[@]}"
record3 $BENCH cli_query_count_load seconds_per_1M_lines s "estimate of crash replay time per 1,000,000 lines of WAL backlog" "${secs[@]}"

echo "== A1.3 HTTP /ingest, 300k lines, fresh server per run, accept rate (buffer, not disk)"
for storage in disk memory; do
  for senders in 1 2 4 8; do
    [[ $storage == memory && $senders != 4 ]] && continue
    v=()
    for run in 1 2 3; do
      dir="$OUT/a1/${storage}_${senders}_$run"; rm -rf "$dir"
      start_server 7720 "$dir" --storage $storage || exit 1
      j=$($PY "$LABRS_DIR/blackbox/http_ingest.py" "$DATA_300K" http://127.0.0.1:7720/ingest $senders 2000)
      echo "  $storage senders=$senders run=$run $j"
      v+=("$(echo "$j" | $PY -c 'import json,sys; print(json.load(sys.stdin)["lines_per_sec"])')")
      kill_server; rm -rf "$dir" "$dir.server.log"
    done
    record3 $BENCH "http_${storage}_${senders}_senders" lines_per_sec lines_per_sec "300k lines, batches of 2000 lines, $senders concurrent senders, --storage $storage; rate at which /ingest answered 200" "${v[@]}"
  done
done

echo "== A1.4 drain: 20k lines with 4 senders, then seconds until buffered_rows==0 (disk)"
echo "   (100k lines span more template-days than kern.maxfilesperproc=10240 and never drain: ABSENT)"
csv_row $BENCH drain_100k status 0 absent "100k seed-42 lines touch >10,240 template-day segments; each open segment holds a descriptor; the tick flush logs EMFILE forever. 20k lines (6.9k segments) measured instead"
v=(); fps=(); files=()
head -20000 "$DATA_1M" > "$OUT/data/20k.ndjson"
for run in 1 2 3; do
  dir="$OUT/a1/drain_$run"; rm -rf "$dir"
  start_server 7720 "$dir" || exit 1
  $PY "$LABRS_DIR/blackbox/http_ingest.py" "$OUT/data/20k.ndjson" http://127.0.0.1:7720/ingest 4 2000 >/dev/null
  t0=$($PY -c 'import time; print(time.time())')
  wait_flushed 7720 600
  t1=$($PY -c 'import time; print(time.time())')
  d=$($PY -c "print(f'{$t1-$t0:.2f}')"); n=$(find "$dir" -type f -name '*.col' | wc -l | tr -d ' ')
  v+=("$d"); files+=("$n"); fps+=("$($PY -c "print(f'{$n/$d:.1f}')")")
  echo "  run=$run drain=${d}s files=$n"
  kill_server; rm -rf "$dir" "$dir.server.log"
done
record3 $BENCH drain_20k seconds s "20k lines accepted, then time until /stats buffered_rows==0" "${v[@]}"
record3 $BENCH drain_20k segment_files files "one segment file per template per day" "${files[@]}"
record3 $BENCH drain_20k files_per_sec files_per_sec "files written per second of drain; the drain includes the 60 s row-group timeout a quiet leaf waits before it is written" "${fps[@]}"
csv_row $BENCH perf_profile status 0 absent "macOS: no perf(1); CARGO_PROFILE_RELEASE_DEBUG profile not taken"
