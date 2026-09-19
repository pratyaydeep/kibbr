#!/usr/bin/env bash
# Builds the shared searchable corpus once: the first 300,000 lines of the seed-42 file
# through HTTP /ingest into out/corpus/data, flushed and shut down cleanly. A4 and A3
# start on it.
#
# Why 300k and not 1M: the server keeps one open file per template-day segment while
# it is open, the seed-42 corpus spans 31 days over ~870 templates (~27,000 segments),
# and macOS caps a process at kern.maxfilesperproc = 10,240 descriptors. The 1M-line
# attempt logged "Too many open files" and left 433 leaves buffered after 25 minutes.
# The cap is kern.maxfilesperproc and no ulimit raises it (tested: any rlimit fails at
# 10,237 files). So the periodic flush can never drain a corpus with more template-days
# than that; the graceful shutdown seals one partition at a time and writes everything.
# The corpus is therefore built by SIGTERM, and its rows are on disk after the shutdown.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
CORPUS="$OUT/corpus/data"
if [[ -s "$CORPUS/trie.json" ]]; then echo "corpus present at $CORPUS"; exit 0; fi
ensure_data
rm -rf "$OUT/corpus"; mkdir -p "$OUT/corpus"
start_server 7710 "$CORPUS" || exit 1
csv_row A4_baseline_searches corpus_1M status 0 absent "1M lines do not drain: ~27k template-day segments each hold an open fd; kern.maxfilesperproc=10240 on this Mac; server logged EMFILE, 433 leaves stayed buffered after 25 min. Corpus is 300k lines instead"
echo "ingesting 300k lines with 4 senders"
$PY "$LABRS_DIR/blackbox/http_ingest.py" "$DATA_300K" http://127.0.0.1:7710/ingest 4 2000
sleep 2
curl -s -m 30 http://127.0.0.1:7710/stats | $PY -c 'import json,sys; d=json.load(sys.stdin); print("rows", d["storage"]["rows"], "buffered", d["storage"]["buffered_rows"], "templates", d["trie"]["templates"])'
t0=$(date +%s); stop_server; echo "graceful shutdown (final flush of every leaf): $(( $(date +%s) - t0 ))s"
csv_row A4_baseline_searches corpus_300k shutdown_flush_seconds "$(( $(date +%s) - t0 ))" s "SIGTERM with ~290k rows still buffered: the final flush writes every leaf, one partition at a time"
csv_row A4_baseline_searches corpus_300k emfile_lines_in_log "$(grep -c 'Too many open files' "$CORPUS.server.log")" count "periodic flush ticks that hit kern.maxfilesperproc before the shutdown"
echo "files: $(find "$CORPUS" -type f | wc -l | tr -d ' ')  size: $(du -sh "$CORPUS" | cut -f1)"
