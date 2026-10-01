#!/usr/bin/env bash
# A2 - for --templates 100 / 1,000 / 10,000: peak memory during ingest, idle memory
# after the flush, files on disk, cold-start time until /health answers.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
BENCH=A2_memory_and_startup
machine_info
# 20,000 lines: `--templates 100` alone splits into 6,355 fingerprints (optional
# fields), and the flush holds a descriptor per template-day, so more lines do not
# drain on this machine (kern.maxfilesperproc = 10,240; see A1). Enough to see how
# memory, files and start-up scale with the template count.
LINES=${A2_LINES:-20000}
for T in 100 1000 10000; do
  f="$OUT/data/t${T}_${LINES}.ndjson"
  [[ -s "$f" ]] || "$BIN" gen-logs --lines $LINES --templates $T --seed 42 > "$f"
  peak=(); idle=(); files=(); cold=(); maxrss=(); drain=(); buffered=(); wfails=(); shut=(); errs=(); rowsback=()
  for run in 1 2 3; do
    dir="$OUT/a2/t${T}_$run"; rm -rf "$dir" "$dir.server.log" "$dir.time"; mkdir -p "$dir"
    # /usr/bin/time -l is macOS's -v: "maximum resident set size" in bytes at exit.
    ulimit -n 65536 2>/dev/null
    /usr/bin/time -l "$BIN" serve --port 7730 --data-dir "$dir" > "$dir.server.log" 2> "$dir.time" &
    TIME_PID=$!
    for _ in $(seq 1 600); do curl -sf http://127.0.0.1:7730/health >/dev/null 2>&1 && break; sleep 0.05; done
    SERVER_PID=$(pgrep -P $TIME_PID -f 'kibbr serve' | head -1); SERVER_PORT=7730
    [[ -n "$SERVER_PID" ]] || { echo "no server under time(1); $(tail -2 "$dir.time")"; exit 1; }
    # ps-sampled peak while ingesting, in case the process's own peak is later in the flush.
    ( while kill -0 $SERVER_PID 2>/dev/null; do rss_kb $SERVER_PID; sleep 0.2; done ) > "$dir.rss" &
    SAMPLER=$!
    $PY "$LABRS_DIR/blackbox/http_ingest.py" "$f" http://127.0.0.1:7730/ingest 4 2000 >/dev/null
    t0=$($PY -c 'import time; print(time.time())')
    # Capped at 180 s: above ~10,240 template-days the tick flush hits EMFILE and never
    # finishes (see A1). The buffered count and write failures at the cap are recorded.
    wait_flushed 7730 120
    t1=$($PY -c 'import time; print(time.time())')
    drain+=("$($PY -c "print(f'{$t1-$t0:.1f}')")")
    read -r bufd wfail < <(curl -s -m 60 http://127.0.0.1:7730/stats | $PY -c 'import json,sys; d=json.load(sys.stdin)["storage"]; print(d["buffered_rows"], d["write_failures"])' 2>/dev/null || echo "-1 -1")
    buffered+=("$bufd"); wfails+=("$wfail")
    sleep 1
    idle_kb=$(rss_kb $SERVER_PID); idle+=("$idle_kb")
    n=$(find "$dir" -type f | wc -l | tr -d ' '); files+=("$n")
    t2=$SECONDS; kill -TERM $SERVER_PID; wait $TIME_PID 2>/dev/null; kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null
    shut+=("$(( SECONDS - t2 ))")
    # The server's stderr shares the .time file with time's own summary.
    err=$(grep -v 'maximum resident\|real \|page \|swaps\|block \|messages \|signals\|context sw\|shared memory\|unshared\|instructions\|cycles\|peak memory\|kibbr: \(SIGTERM\|stop\|in-flight\|WAL\|trie.json\|close\|exit\)' "$dir.time" | grep -v '^ *$' | sed 's/leaf_[0-9]*/leaf_N/; s#/[^ ]*seg_[0-9]*.col#SEG#' | sort | uniq -c | sort -rn | head -1)
    errs+=("$err")
    peak+=("$(sort -n "$dir.rss" | tail -1)")
    maxrss+=("$(awk '/maximum resident set size/{print $1}' "$dir.time")")
    SERVER_PID=""
    start_server_timed 7730 "$dir" || exit 1
    c=$COLD_START; cold+=("$c")
    rows=$(curl -s -XPOST -H 'content-type: application/json' -d '{"q":"COUNT LOG {}"}' http://127.0.0.1:7730/query | $PY -c 'import json,sys; print(json.load(sys.stdin)["count"])')
    rowsback+=("$rows")
    echo "  T=$T run=$run shutdown=${shut[${#shut[@]}-1]}s top_stderr=[${errs[${#errs[@]}-1]}] buffered_at_cap=${buffered[${#buffered[@]}-1]} write_failures=${wfails[${#wfails[@]}-1]} peak_rss=${peak[${#peak[@]}-1]}KB time_maxrss=${maxrss[${#maxrss[@]}-1]}B idle=${idle_kb}KB files=$n drain=${drain[${#drain[@]}-1]}s cold_start=${c}s rows_after_restart=$rows"
    stop_server
    rm -rf "$dir" "$dir.server.log" "$dir.time" "$dir.rss"
  done
  note="gen-logs --lines $LINES --templates $T --seed 42; 4 senders"
  record3 $BENCH "templates_$T" peak_rss_kb kB "ps-sampled every 0.2s during ingest+flush; $note" "${peak[@]}"
  record3 $BENCH "templates_$T" time_l_max_rss_bytes bytes "/usr/bin/time -l maximum resident set size (whole life incl. shutdown flush)" "${maxrss[@]}"
  record3 $BENCH "templates_$T" idle_rss_kb kB "after buffered_rows==0" "${idle[@]}"
  record3 $BENCH "templates_$T" files_on_disk files "find -type f in the data dir" "${files[@]}"
  record3 $BENCH "templates_$T" drain_seconds s "seconds from last /ingest answer until buffered_rows==0, capped at 180" "${drain[@]}"
  record3 $BENCH "templates_$T" rows_still_buffered_at_cap rows "buffered_rows when the drain wait ended (0 = drained)" "${buffered[@]}"
  record3 $BENCH "templates_$T" write_failures count "/stats write_failures (EMFILE flush ticks)" "${wfails[@]}"
  record3 $BENCH "templates_$T" shutdown_seconds s "SIGTERM until exit: the final flush writes every leaf" "${shut[@]}"
  csv_row $BENCH "templates_$T" top_stderr_line 0 text "most frequent server stderr line, run 3: ${errs[${#errs[@]}-1]}"
  record3 $BENCH "templates_$T" rows_after_restart rows "COUNT LOG {} after SIGTERM + restart; $LINES were accepted with a 200" "${rowsback[@]}"
  record3 $BENCH "templates_$T" cold_start_seconds s "spawn until /health answers, on the data dir just written" "${cold[@]}"
done
