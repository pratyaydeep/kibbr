#!/usr/bin/env bash
# The first differential pair: the query engine against the brute-force oracle.
#
# Both sides are the same binary given the same flags on the same file - `--reference`
# is the only difference - so a disagreement is a bug with no hand-written assertion
# anywhere. The flag combinations are drawn from values that are actually in the file,
# which is what makes most of them hit something rather than all missing.
#
# Compared: exists, count, rows, groups, and templates_matched. Not compared:
# duration, files_opened and rows_scanned - the oracle opens nothing and reads every
# line, so those differ by design.
#
# python3 is used once, to harvest key/value pairs out of `parse --json`. Splitting a
# JSON object in shell is how a test harness starts lying about what it tested.
#
#   COMBOS=100 SEED=7 NLINES=2000 scripts/agree.sh

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

# One `L` row per sampled line, holding that line's shell-safe `key=value` terms, plus a
# `K` row per distinct key for `--group-by`.
#
# Grouped by line, not pooled, because terms drawn from a pool almost never co-occur on
# one template - a hundred such queries would agree on "unknown template" a hundred times
# and check nothing. Terms from one line are guaranteed to have a template that holds
# them all; the cross-line draw below is what still reaches the unknown case.
#
# Shell-safe only: a quoting bug here would be indistinguishable from a disagreement.
"$BIN" parse "$INPUT" --json 2>/dev/null | python3 -c '
import json, sys
keys, rows = set(), []
for line in sys.stdin:
    terms = []
    for k, v in json.loads(line)["values"].items():
        keys.add(k)
        if v and len(v) <= 40 and all(32 < ord(c) < 127 for c in v) \
           and not any(c in v for c in " \x27\"*$`\\="):
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
echo "input: $NLINES lines, ${#KEYS[@]} distinct keys, ${#ROWS[@]} sampled lines"

# One `key=value` term off a sampled line, chosen by index so the caller can take several
# from the same line and be sure a template holds them together.
term_from() { IFS=$'\t' read -r -a f <<< "${ROWS[$1]}"; echo "${f[$((RANDOM % ${#f[@]}))]}"; }

# That line's own `ts`, or empty if the template keeps its time under another name.
ts_of() { IFS=$'\t' read -r -a f <<< "${ROWS[$1]}"; for x in "${f[@]}"; do
            case "$x" in ts=*) echo "${x#ts=}"; return ;; esac; done; }

RANDOM=$SEED
rfc3339() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

# The answer, with the fields the two sides are not expected to share removed.
answer() { "$BIN" query "$INPUT" "$@" --format json 2>/dev/null | sed 's/,"meta":{.*}}$/}/'; }
templates() { "$BIN" query "$INPUT" "$@" --format json 2>/dev/null | grep -o '"templates_matched":[0-9]*'; }

# Tallied because "zero disagreements" over a hundred queries that all matched nothing
# would be worth nothing. The split is the evidence that the run had something to check.
disagreements=0
known=0
unknown=0
nonempty=0
for i in $(seq 1 "$COMBOS"); do
  ARGS=()

  row=$((RANDOM % ${#ROWS[@]}))

  # Decided before the terms are drawn: a window whose edge is this row's own timestamp
  # only settles `[since, until)` if the row is one the pattern selects, so that draw
  # takes every term off this line unaltered and keeps the answer down to the boundary.
  window=$((RANDOM % 4))
  hit=$(ts_of "$row")
  boundary=0
  if [ "$window" -eq 3 ] && [ -n "$hit" ]; then boundary=1; fi

  # 1-3 terms, normally off one line so a template exists that holds them all. One draw
  # in four crosses to a second line and one value in six is replaced, which is what
  # reaches the "unknown template" answer and the `*` wildcard.
  for _ in $(seq 1 $((1 + RANDOM % 3))); do
    from=$row
    term=""
    if [ "$boundary" -eq 0 ]; then
      if [ $((RANDOM % 4)) -eq 0 ]; then from=$((RANDOM % ${#ROWS[@]})); fi
      term=$(term_from "$from")
      case $((RANDOM % 6)) in
        0) term="${term%%=*}=*" ;;
        1) term="${term%%=*}=no_such_value_$i" ;;
      esac
    else
      term=$(term_from "$from")
    fi
    ARGS[${#ARGS[@]}]="--match"; ARGS[${#ARGS[@]}]="$term"
  done

  case $((RANDOM % 4)) in
    0) ARGS[${#ARGS[@]}]="--count" ;;
    1) ARGS[${#ARGS[@]}]="--fetch" ;;
    2) ARGS[${#ARGS[@]}]="--exists" ;;
    3) g="${KEYS[$((RANDOM % ${#KEYS[@]}))]}"
       if [ $((RANDOM % 3)) -eq 0 ]; then g="$g,${KEYS[$((RANDOM % ${#KEYS[@]}))]}"; fi
       # One draw in three buckets by time instead. Where the boundaries fall is the whole
       # of what the two sides could disagree about here - both floor against the epoch,
       # and each does its own arithmetic to get there - so the widths span three orders
       # of magnitude and the zero-filled buckets are compared along with the rest.
       if [ $((RANDOM % 3)) -eq 0 ]; then
         w=("5m" "1h" "6h" "1d"); g="time(${w[$((RANDOM % 4))]}),$g"
       fi
       ARGS[${#ARGS[@]}]="--group-by"; ARGS[${#ARGS[@]}]="$g" ;;
  esac

  case "$window" in
    0) a=$((BASE + RANDOM % SPAN)); b=$((BASE + RANDOM % SPAN))
       if [ "$a" -gt "$b" ]; then t=$a; a=$b; b=$t; fi
       ARGS[${#ARGS[@]}]="--between"
       ARGS[${#ARGS[@]}]="$(rfc3339 $a)"; ARGS[${#ARGS[@]}]="$(rfc3339 $b)" ;;
    # Every generated timestamp is from 2024, so this window is always empty - which is
    # the `exists: true, count: 0` answer, checked on both sides.
    1) ARGS[${#ARGS[@]}]="--since"; ARGS[${#ARGS[@]}]="$((1 + RANDOM % 48))h" ;;
    # The half-open edge, both ways round: as `until` the row is out, as `since` it is in.
    # Random endpoints never land on a row, so they cannot tell `[since, until)` from
    # `[since, until]` - a one-character bug either side could have.
    3) if [ "$boundary" -eq 1 ]; then
         ARGS[${#ARGS[@]}]="--between"
         if [ $((RANDOM % 2)) -eq 0 ]; then
           ARGS[${#ARGS[@]}]="$(rfc3339 $BASE)"; ARGS[${#ARGS[@]}]="$hit"
         else
           ARGS[${#ARGS[@]}]="$hit"; ARGS[${#ARGS[@]}]="$(rfc3339 $((BASE + SPAN)))"
         fi
       fi ;;
  esac

  e=$(answer "${ARGS[@]}");            r=$(answer "${ARGS[@]}" --reference)
  et=$(templates "${ARGS[@]}");        rt=$(templates "${ARGS[@]}" --reference)

  case "$e" in
    '{"exists":true'*) known=$((known + 1)) ;;
    *)                 unknown=$((unknown + 1)) ;;
  esac
  case "$e" in
    *'"count":0,'*) ;;
    *)              nonempty=$((nonempty + 1)) ;;
  esac

  if [ "$e" = "$r" ] && [ "$et" = "$rt" ]; then
    continue
  fi
  disagreements=$((disagreements + 1))
  echo "DISAGREE  ${ARGS[*]}"
  echo "  engine    $et  $e"
  echo "  reference $rt  $r"
done

echo
echo "$COMBOS combinations: $known known template, $unknown unknown template, $nonempty returned rows"
echo "$COMBOS combinations, $disagreements disagreements"
[ "$disagreements" -eq 0 ]
