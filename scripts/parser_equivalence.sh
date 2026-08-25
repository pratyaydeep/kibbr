#!/usr/bin/env bash
# The second differential pair: the TrieQL parser against the flags.
#
# Both sides are the same binary, the same file and the same engine - the only difference
# is whether the `Query` was built from flags or from a query string. So a disagreement is
# a parser bug with no hand-written assertion anywhere, and the pair costs nothing to keep:
# every flag combination the CLI already supports is a free test of the grammar.
#
# Compared: the whole `--format json` body, meta included, with `duration_ms` removed -
# two runs of the same engine differ only in how long they took.
#
# Not compared: `--exists`. The grammar has two verbs, MATCH and COUNT, and neither asks
# `QueryMode::Exists` - the flag is the only way to reach it. That gap is reported below
# rather than closed here.
#
# The `SINCE` rows are weaker than they look: each side resolves `now` in its own process,
# so a window that both sides agree is empty is all they prove. Every generated timestamp
# is from 2024 and every `SINCE` here is hours, so that is exactly what happens - the
# absolute `BETWEEN` rows are the ones carrying the time clause.
#
# python3 is used once, to harvest key/value pairs out of `parse --json`, for the reason
# given in scripts/agree.sh: splitting a JSON object in shell is how a harness starts lying.
#
#   COMBOS=200 SEED=3 NLINES=4000 scripts/parser_equivalence.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
COMBOS=${COMBOS:-100}
SEED=${SEED:-7}
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-1}

# The generator's clock: 2024-01-15T10:30:00Z, plus up to 30 days (src/gen.rs).
BASE=1705314600
SPAN=2592000

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --templates 12 --seed "$GEN_SEED" > "$INPUT"

# One `L` row per sampled line holding that line's shell-safe `key=value` terms, plus a `K`
# row per distinct key for `GROUP BY`. Grouped by line, not pooled, so the terms of one
# query co-occur on some template - see agree.sh.
#
# Non-ASCII values are kept: `échec` and `用户未找到` are in the corpus, and the string
# lexer should be walking characters rather than bytes. Whitespace and the characters that
# would need quoting on either surface are dropped - a quoting bug here would be
# indistinguishable from a disagreement.
"$BIN" parse "$INPUT" --json 2>/dev/null | python3 -c '
import json, sys
keys, rows = set(), []
bad = set(" \t\x27\"*$`\\=,{}:")
safe = lambda s: not any(c in bad or ord(c) < 33 for c in s)
for line in sys.stdin:
    terms = []
    for k, v in json.loads(line)["values"].items():
        # The key is filtered for `GROUP BY` too, not only as half of a term: a key with a
        # space in it is a bare word on one surface and a quoted one on the other, and the
        # difference would be reported as a disagreement about the answer.
        if safe(k):
            keys.add(k)
        if v and len(v) <= 40 and safe(v) and safe(k):
            terms.append("%s=%s" % (k, v))
    if terms:
        rows.append(terms)
for terms in rows[:600]: print("L\t" + "\t".join(terms))
for k in sorted(keys):   print("K\t%s" % k)
' > "$WORK/harvest.tsv"

ROWS=(); KEYS=()
while IFS= read -r rec; do
  case "$rec" in
    L*) ROWS[${#ROWS[@]}]="${rec#L$'\t'}" ;;
    K*) KEYS[${#KEYS[@]}]="${rec#K$'\t'}" ;;
  esac
done < "$WORK/harvest.tsv"

if [ ${#ROWS[@]} -eq 0 ]; then echo "no candidate values harvested" >&2; exit 1; fi
if [ ${#KEYS[@]} -eq 0 ]; then echo "no candidate keys harvested" >&2; exit 1; fi
echo "input: $NLINES lines, ${#KEYS[@]} distinct keys, ${#ROWS[@]} sampled lines"

term_from() { IFS=$'\t' read -r -a f <<< "${ROWS[$1]}"; echo "${f[$((RANDOM % ${#f[@]}))]}"; }

RANDOM=$SEED
rfc3339() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

# The answer, with the one field two runs of the same engine are not expected to share.
#
# stderr is folded in rather than dropped: if one side fails and the other answers, that
# has to read as a disagreement. A silenced error would compare equal to nothing at all.
answer() { "$BIN" query "$INPUT" "$@" --format json 2>&1 | sed 's/"duration_ms":[-+0-9.e]*//'; }

# ── The grammar's own examples, which have to parse before the rest is worth running ──
examples=0
for q in \
  'COUNT LOG {"event": "login_failed"} SINCE 1h' \
  'MATCH LOG {"event": "processed"} SINCE 24h GROUP BY level'
do
  if out=$("$BIN" explain "$q" 2>&1); then
    examples=$((examples + 1))
  else
    echo "GRAMMAR EXAMPLE FAILED TO PARSE  $q"
    echo "$out"
  fi
done
echo "grammar examples: $examples of 2 parse"

# ── The differential ─────────────────────────────────────────────────────────────────
disagreements=0
known=0
unknown=0
nonempty=0
for i in $(seq 1 "$COMBOS"); do
  FLAGS=(); PAIRS=""

  row=$((RANDOM % ${#ROWS[@]}))
  for _ in $(seq 1 $((1 + RANDOM % 3))); do
    from=$row
    if [ $((RANDOM % 4)) -eq 0 ]; then from=$((RANDOM % ${#ROWS[@]})); fi
    term=$(term_from "$from")
    case $((RANDOM % 6)) in
      0) term="${term%%=*}=*" ;;
      1) term="${term%%=*}=no_such_value_$i" ;;
    esac
    FLAGS[${#FLAGS[@]}]="--match"; FLAGS[${#FLAGS[@]}]="$term"

    # The same term as a pattern pair. A flag `k=*` is `MatchValue::Any`, which the grammar
    # spells as a bare `*` - the quoted `"*"` next to it is the literal value.
    k="${term%%=*}"; v="${term#*=}"
    if [ "$v" = "*" ]; then pair="\"$k\": *"; else pair="\"$k\": \"$v\""; fi
    if [ -n "$PAIRS" ]; then PAIRS="$PAIRS, $pair"; else PAIRS="$pair"; fi
  done

  case $((RANDOM % 3)) in
    0) FLAGS[${#FLAGS[@]}]="--count"; VERB="COUNT"; GROUP="" ;;
    1) FLAGS[${#FLAGS[@]}]="--fetch"; VERB="MATCH"; GROUP="" ;;
    2) g="${KEYS[$((RANDOM % ${#KEYS[@]}))]}"
       if [ $((RANDOM % 3)) -eq 0 ]; then g="$g,${KEYS[$((RANDOM % ${#KEYS[@]}))]}"; fi
       FLAGS[${#FLAGS[@]}]="--group-by"; FLAGS[${#FLAGS[@]}]="$g"
       # Either verb reaches `GroupBy`, as `--group-by` does over `--fetch`. Alternating
       # is what checks that.
       if [ $((RANDOM % 2)) -eq 0 ]; then VERB="COUNT"; else VERB="MATCH"; fi
       GROUP=" GROUP BY ${g//,/, }" ;;
  esac

  WINDOW=""
  case $((RANDOM % 3)) in
    0) a=$((BASE + RANDOM % SPAN)); b=$((BASE + RANDOM % SPAN))
       if [ "$a" -gt "$b" ]; then t=$a; a=$b; b=$t; fi
       a=$(rfc3339 $a); b=$(rfc3339 $b)
       FLAGS[${#FLAGS[@]}]="--between"; FLAGS[${#FLAGS[@]}]="$a"; FLAGS[${#FLAGS[@]}]="$b"
       WINDOW=" BETWEEN $a AND $b" ;;
    1) d="$((1 + RANDOM % 48))h"
       FLAGS[${#FLAGS[@]}]="--since"; FLAGS[${#FLAGS[@]}]="$d"
       WINDOW=" SINCE $d" ;;
  esac

  TRIEQL="$VERB LOG {$PAIRS}$WINDOW$GROUP"

  f=$(answer "${FLAGS[@]}")
  t=$(answer "$TRIEQL")

  case "$f" in
    '{"exists":true'*) known=$((known + 1)) ;;
    *)                 unknown=$((unknown + 1)) ;;
  esac
  case "$f" in
    *'"count":0,'*) ;;
    *)              nonempty=$((nonempty + 1)) ;;
  esac

  if [ "$f" = "$t" ]; then
    continue
  fi
  disagreements=$((disagreements + 1))
  echo "DISAGREE  flags: ${FLAGS[*]}"
  echo "          trieql: $TRIEQL"
  echo "  flags   $f"
  echo "  trieql  $t"
done

echo
echo "$COMBOS combinations: $known known template, $unknown unknown template, $nonempty returned rows"
echo "$COMBOS combinations, $disagreements disagreements"
echo "not covered: --exists (the grammar has no verb for QueryMode::Exists)"
[ "$disagreements" -eq 0 ] && [ "$examples" -eq 2 ]
