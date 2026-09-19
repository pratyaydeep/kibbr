#!/usr/bin/env bash
# A3 - S1..S4 typical and worst-1% latency with the server idle, then while 4 senders
# ingest at full speed; the ingest rate with and without the searches running.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$LABRS_DIR/blackbox/searches.sh"
BENCH=A3_search_under_load
machine_info; ensure_data
"$LABRS_DIR/blackbox/prepare_corpus.sh" || exit 1
ITER=${A3_ITER:-20}; LOAD_SECS=${A3_LOAD_SECS:-32}
run_searches() { # $1 = url, $2 = label, $3 = per-search seconds (0 = ITER iterations)
  for s in "${SEARCH_NAMES[@]}"; do
    q=$(search_q "$s")
    if [[ "$3" == 0 ]]; then j=$($PY "$LABRS_DIR/blackbox/search.py" "$1" "$q" $ITER); else j=$($PY "$LABRS_DIR/blackbox/search.py" "$1" "$q" 0 --duration "$3"); fi
    echo "$s $2 $j"
    echo "$j" > "$OUT/a3/${2}_${s}_$run.json"
  done
}
mkdir -p "$OUT/a3"
for run in 1 2 3; do
  dir="$OUT/a3/data"; rm -rf "$dir"; cp -R "$OUT/corpus/data" "$dir"
  start_server 7740 "$dir" || exit 1
  echo "== run $run: idle"
  run_searches http://127.0.0.1:7740/query idle 0
  echo "== run $run: ingest alone, 4 senders, ${LOAD_SECS}s"
  $PY "$LABRS_DIR/blackbox/http_ingest.py" "$DATA_300K" http://127.0.0.1:7740/ingest 4 2000 --duration $LOAD_SECS > "$OUT/a3/ingest_alone_$run.json"
  cat "$OUT/a3/ingest_alone_$run.json"
  echo "== run $run: ingest + searches"
  $PY "$LABRS_DIR/blackbox/http_ingest.py" "$DATA_300K" http://127.0.0.1:7740/ingest 4 2000 --duration $LOAD_SECS > "$OUT/a3/ingest_with_$run.json" &
  LOADER=$!
  sleep 1
  run_searches http://127.0.0.1:7740/query loaded $(( (LOAD_SECS - 4) / 4 ))
  wait $LOADER; cat "$OUT/a3/ingest_with_$run.json"
  curl -s -m 60 http://127.0.0.1:7740/stats | $PY -c 'import json,sys; d=json.load(sys.stdin)["storage"]; print("  after load: buffered_rows", d["buffered_rows"], "write_failures", d["write_failures"])'
  kill_server; rm -rf "$dir" "$dir.server.log"
done
for s in "${SEARCH_NAMES[@]}"; do
  for state in idle loaded; do
    for m in client_p50_ms client_p99_ms server_p50_ms server_p99_ms; do
      v=(); for run in 1 2 3; do v+=("$($PY -c "import json; print(json.load(open('$OUT/a3/${state}_${s}_$run.json'))['$m'])")"); done
      record3 $BENCH "${s}_$state" "$m" ms "$(search_q "$s"); $state = $([[ $state == idle ]] && echo "no ingest" || echo "4 senders ingesting at full speed")" "${v[@]}"
    done
  done
done
for state in alone with_searches; do
  f=$([[ $state == alone ]] && echo ingest_alone || echo ingest_with)
  v=(); for run in 1 2 3; do v+=("$($PY -c "import json; print(json.load(open('$OUT/a3/${f}_$run.json'))['lines_per_sec'])")"); done
  record3 $BENCH "ingest_$state" lines_per_sec lines_per_sec "4 senders, ${LOAD_SECS}s, on top of the 300k-row corpus" "${v[@]}"
done
