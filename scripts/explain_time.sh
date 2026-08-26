#!/usr/bin/env bash
# The claim: `--explain` executes nothing and runs in constant time regardless of how much
# data there is. Timed against a 1k-line file and a 1M-line one - a thousandfold.
#
# Two numbers per input, and the difference between them is the point:
#
#   walk      what `--explain` reports. Its cost is the trie's size, and the trie stops
#             growing once the templates have all been seen. This is the constant.
#   wall      the whole process, which still has to build that trie by reading the file.
#             Nothing persists the trie yet, so this is linear and says so.
#
# The comparison that matters is the walk against the scan of a real query on the same
# input: same walk, and then the scan the walk avoided.
#
#   REPEATS=5 scripts/explain_time.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
REPEATS=${REPEATS:-5}
PATTERN=${PATTERN:---match event=login_ok}

cargo build --release --quiet || exit 1
for size in 1k 1m; do
  f="testdata/m2/$size.ndjson"
  [ -f "$f" ] && continue
  lines=1000; [ "$size" = 1m ] && lines=1000000
  "$BIN" gen-logs --lines "$lines" --seed 5 > "$f"
done

# Best of N. The minimum, not the mean: a slower run measured the machine, not the walk.
best_walk() {
  local best=""
  for _ in $(seq 1 "$REPEATS"); do
    us=$("$BIN" query "$1" $PATTERN --explain 2>/dev/null | awk '
      /the walk took/ {
        for (i = 1; i <= NF; i++) if ($i == "took") { v = $(i + 1); break }
        # Strip the unit by class, not by length: `µ` is one character to some awks and
        # two bytes to others, and getting that wrong silently drops a digit.
        scale = (v ~ /ms$/) ? 1000 : 1
        gsub(/[^0-9.]/, "", v)
        printf "%.1f", v * scale
      }')
    if [ -z "$best" ] || awk "BEGIN{exit !($us < $best)}"; then best=$us; fi
  done
  echo "$best"
}

wall() { { time -p "$@" > /dev/null 2>&1 ; } 2>&1 | awk '/^real/{printf "%.0fms", $2*1000}'; }

printf '%-6s %10s %10s %8s %13s %13s %12s\n' \
  input lines templates nodes "walk (µs)" "explain wall" "count wall"
for size in 1k 1m; do
  f="testdata/m2/$size.ndjson"
  n=$(wc -l < "$f" | tr -d ' ')
  # `demo`'s summary line is where the trie reports its own size. Read by the word
  # before each label, not by splitting on punctuation - the counts are printed with
  # thousands separators, and a comma is not a field boundary here.
  read -r tmpl nodes <<< "$("$BIN" demo "$f" 2>/dev/null | awk '/templates;/ {
      gsub(/,/, "")
      for (i = 1; i <= NF; i++) {
        if ($i == "templates;") t = $(i - 1)
        if ($i == "nodes")      n = $(i - 1)
      }
      print t, n }')"
  p1=$(best_walk "$f")
  ew=$(wall "$BIN" query "$f" $PATTERN --explain)
  cw=$(wall "$BIN" query "$f" $PATTERN --count)
  printf '%-6s %10s %10s %8s %13s %13s %12s\n' "$size" "$n" "$tmpl" "$nodes" "$p1" "$ew" "$cw"
done

cat <<'NOTE'

The walk tracks the node count, not the line count: a thousandfold more data is a
couple of times more trie, because a file stops producing new templates long before
it stops producing lines. That is the constant being claimed, and it is what the walk
costs on a server that already holds the trie.

`explain wall` is not that number and does not claim to be. Nothing persists the trie
yet, so the process has to read the file to build one before it can walk it
- linear, and the same work `count wall` does before its scan. What `--explain` skips is
the scan entirely: no row is materialised and no store is opened.
NOTE

echo
echo "the same query, walk then scan:"
for size in 1k 1m; do
  echo "  $size:"
  "$BIN" query "testdata/m2/$size.ndjson" $PATTERN --explain 2>/dev/null | sed 's/^/    /'
  "$BIN" query "testdata/m2/$size.ndjson" $PATTERN --count   2>/dev/null | sed 's/^/    /'
done
