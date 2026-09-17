#!/usr/bin/env bash
# What `[query] max_rows` is worth, in bytes - and the proof that what it hands back is a
# page rather than a truncation.
#
# The pair is measured with one binary: `--max-rows 0` is the uncapped shape, which is what
# an unlimited `MATCH` did before the backstop existed, and the default is the same query
# under it. So this stays reproducible after the old binary is gone.
#
# Three numbers, and the middle one is the point:
#
#   COUNT          the floor - the file is in memory, and no answer is materialised
#   MATCH  capped  the floor plus one page
#   MATCH  uncapped  the floor plus a second full copy of every matching row
#
# `--format ndjson` is measured beside `json` because it was documented as the way to ask
# for everything without paying for it. It is not: the rows are ranked into a `Vec<Row>`
# before a byte is written, and its RSS tracks the row count exactly as `json`'s does.
#
#   LINES=200000 CAP=10000 scripts/backstop_rss.sh
#
# Peak RSS is the whole reason to run this against a big corpus, so mind what else is
# resident. 5,000,000 lines needs ~6 GB for the store alone, and the uncapped run on top of
# that is what an 8 GB machine cannot do - which is the finding, not a reason to skip it.

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
LINES=${LINES:-200000}
CAP=${CAP:-10000}
SEED=${SEED:-5}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
INPUT="$WORK/input.ndjson"

# Written down rather than piped: every page below re-reads it, because nothing persists a
# database between two CLI runs yet.
"$BIN" gen-logs --lines "$LINES" --seed "$SEED" > "$INPUT" || exit 1
echo "corpus: $(wc -l < "$INPUT" | tr -d ' ') lines, $(du -h "$INPUT" | cut -f1)"
echo

# Peak resident size of one query, in MB. macOS reports it in bytes; Linux in KB.
peak() { # label args...
  local label=$1; shift
  local stats
  stats=$(/usr/bin/time -l "$BIN" query "$INPUT" "$@" 2>&1 >/dev/null \
          | awk '/maximum resident/ {rss = $1} /real/ {secs = $1}
                 END { printf "%.0f MB   %ss", (rss > 1048576 ? rss / 1048576 : rss / 1024), secs }')
  printf '  %-44s %s\n' "$label" "$stats"
}

echo "── peak RSS ──"
peak "COUNT LOG {}                    (the floor)" 'COUNT LOG {}'
peak "MATCH LOG {}    json    capped  ($CAP)" --max-rows "$CAP" 'MATCH LOG {}'
peak "MATCH LOG {}    json    uncapped" --max-rows 0 'MATCH LOG {}'
peak "MATCH LOG {}    ndjson  capped  ($CAP)" --max-rows "$CAP" 'MATCH LOG {}' --format ndjson
peak "MATCH LOG {}    ndjson  uncapped" --max-rows 0 'MATCH LOG {}' --format ndjson

# ── The page, walked to the end ──────────────────────────────────────────────────────
#
# A backstop that cut the answer and offered no way past it would be a truncation. This is
# the claim that it is not: pages requested exactly as a client that never learned to page
# would request them - no `LIMIT` on any of them - reproduce the uncapped answer row for
# row, in order.
echo
echo "── paging the backstop to completion ──"

rows_json() { python3 -c '
import json, sys
for r in json.load(sys.stdin)["rows"]:
    print(json.dumps(r, sort_keys=True))
'; }

cursor_of() { python3 -c '
import json, sys
print(json.load(sys.stdin)["meta"]["next_cursor"] or "")
'; }

pages=0
cursor=""
: > "$WORK/paged.ndjson"
while :; do
  if [ -n "$cursor" ]; then q="MATCH LOG {} AFTER \"$cursor\""; else q="MATCH LOG {}"; fi
  out=$("$BIN" query "$INPUT" --max-rows "$CAP" "$q" --format json) || exit 1
  printf '%s' "$out" | rows_json >> "$WORK/paged.ndjson"
  cursor=$(printf '%s' "$out" | cursor_of)
  pages=$((pages + 1))
  [ -z "$cursor" ] && break
done

"$BIN" query "$INPUT" --max-rows 0 'MATCH LOG {}' --format json | rows_json > "$WORK/whole.ndjson"

paged=$(wc -l < "$WORK/paged.ndjson" | tr -d ' ')
whole=$(wc -l < "$WORK/whole.ndjson" | tr -d ' ')
echo "  $pages pages of $CAP  →  $paged rows;  the uncapped answer is $whole rows"

if cmp -s "$WORK/paged.ndjson" "$WORK/whole.ndjson"; then
  echo "  ok    paging the backstop tiles the uncapped answer exactly, in order"
else
  echo "  FAIL  the paged answer differs from the uncapped one"
  diff "$WORK/whole.ndjson" "$WORK/paged.ndjson" | head -20
  exit 1
fi
