#!/usr/bin/env bash
# A4 - the four standard searches on an idle server over the 300k-line corpus, three runs
# each: the server's reported duration, rows scanned, files opened.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
source "$LABRS_DIR/blackbox/searches.sh"
BENCH=A4_baseline_searches
machine_info
"$LABRS_DIR/blackbox/prepare_corpus.sh" || exit 1
CORPUS="$OUT/corpus/data"
start_server 7711 "$CORPUS" || exit 1
echo "== A4 baseline searches (idle server, 300k rows on disk, cold start, 3 runs each)"
printf '%-4s %-12s %-12s %-12s %-12s %-10s %s\n' S server_ms client_ms rows_scanned files_opened count spread
for s in "${SEARCH_NAMES[@]}"; do
  q=$(search_q "$s")
  sv=(); cl=(); scanned=0; opened=0; cnt=0; rg=0
  for run in 1 2 3; do
    j=$($PY "$LABRS_DIR/blackbox/search.py" "http://127.0.0.1:7711/query" "$q" 1)
    read -r a b c d e f < <(echo "$j" | $PY -c 'import json,sys; d=json.load(sys.stdin); print(d["server_p50_ms"], d["client_p50_ms"], d["rows_scanned"], d["files_opened"], d["count"], d["row_groups_read"])')
    sv+=("$a"); cl+=("$b"); scanned=$c; opened=$d; cnt=$e; rg=$f
  done
  read -r med spread < <(median3 "${sv[@]}")
  read -r cmed cspread < <(median3 "${cl[@]}")
  printf '%-4s %-12s %-12s %-12s %-12s %-10s %s\n' "$s" "$med" "$cmed" "$scanned" "$opened" "$cnt" "${spread}%"
  csv_row $BENCH "$s" server_duration_ms "$med" ms "$q"
  csv_row $BENCH "$s" server_duration_ms_spread "$spread" pct_of_median "runs: ${sv[*]}"
  csv_row $BENCH "$s" client_ms "$cmed" ms "wall time seen by the HTTP client"
  csv_row $BENCH "$s" rows_scanned "$scanned" rows ""
  csv_row $BENCH "$s" files_opened "$opened" files ""
  csv_row $BENCH "$s" row_groups_read "$rg" row_groups ""
  csv_row $BENCH "$s" count "$cnt" rows "matching rows (S4: total rows the LIMIT paged over)"
done
csv_row $BENCH S4_via_9200 status 0 absent "no :9200 _search surface; S4 uses TrieQL LIMIT 500 on :7700"
stop_server
