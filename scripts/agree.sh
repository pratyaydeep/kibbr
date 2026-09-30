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
# Two knobs let another script ask this exact query set of something else, rather than keep
# a copy of the draws that would drift from this one:
#
#   INPUT=<file>     harvest from and ask about that file instead of a generated one, so a
#                    caller holding the same lines draws from them.
#   PRINT_QUERIES=1  print each combination's flags on one line, TAB between fields, and
#                    exit 0 without answering or gating. No draw reads an answer, so the
#                    RANDOM sequence is the one a normal run takes.
#
# Which term a line gives is drawn inside `$(term_from ...)`, and bash reseeds RANDOM from
# the clock in a subshell, so two runs with one SEED still differ there. A caller prints
# the list once and asks every side that one list.
#
# BIN names another build, as in the other scripts; the default is built first.
#
#   COMBOS=100 SEED=7 NLINES=2000 scripts/agree.sh
#   INPUT=testdata/two-doors/200000-seed1.ndjson PRINT_QUERIES=1 scripts/agree.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/kibbr}
COMBOS=${COMBOS:-100}
SEED=${SEED:-7}
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-1}
PRINT_QUERIES=${PRINT_QUERIES:-}

# The generator's clock: 2024-01-15T10:30:00Z, plus up to 30 days (src/gen.rs).
BASE=1705314600
SPAN=2592000

[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# A named file that is not there is refused, not replaced: a generated one in its place
# would draw a query set the caller never asked for, and nothing downstream could tell.
if [ -n "${INPUT:-}" ]; then
  [ -f "$INPUT" ] || { echo "INPUT=$INPUT is not a file" >&2; exit 1; }
else
  INPUT="$WORK/input.ndjson"
  "$BIN" gen-logs --lines "$NLINES" --templates 12 --seed "$GEN_SEED" > "$INPUT"
fi

# One `L` row per sampled line, holding the line's time key and its shell-safe
# `key=value` terms, plus a `K` row per distinct key for `--group-by`.
#
# Grouped by line, not pooled, because terms drawn from a pool almost never co-occur on
# one template - a hundred such queries would agree on "unknown template" a hundred times
# and check nothing. Terms from one line are guaranteed to have a template that holds
# them all; the cross-line draw below is what still reaches the unknown case.
#
# The time key is the first name on the default `time_columns` list that the line
# carries, or `-`. It is used to *choose* lines, never to answer anything: at least a
# quarter of the combinations below touch a template whose time key is not `ts`, because
# those were the 41.6% of rows no time-range query could see before per-template time
# sources, and a run that never drew one would agree on them a hundred times without
# checking.
#
# Shell-safe only: a quoting bug here would be indistinguishable from a disagreement.
"$BIN" parse "$INPUT" --json 2>/dev/null | python3 -c '
import json, sys
TIME = ["@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix", "datetime",
        "event_time", "created_at"]
keys, rows = set(), []
for line in sys.stdin:
    parsed = json.loads(line)
    terms = []
    for k, v in parsed["values"].items():
        keys.add(k)
        if v and len(v) <= 40 and all(32 < ord(c) < 127 for c in v) \
           and not any(c in v for c in " \x27\"*$`\\="):
            terms.append("%s=%s" % (k, v))
    if terms:
        tkey = next((t for t in TIME if t in parsed["keys"]), "-")
        rows.append((tkey, terms))
for tkey, terms in rows[:600]: print("L\t%s\t%s" % (tkey, "\t".join(terms)))
for k in sorted(keys):   print("K\t%s" % k)
' > "$WORK/harvest.tsv"

ROWS=(); TKEY=(); KEYS=(); NONTS=()
while IFS= read -r rec; do
  case "$rec" in
    L*) rest="${rec#L$'\t'}"
        TKEY[${#TKEY[@]}]="${rest%%$'\t'*}"
        ROWS[${#ROWS[@]}]="${rest#*$'\t'}" ;;
    K*) KEYS[${#KEYS[@]}]="${rec#K$'\t'}" ;;
  esac
done < "$WORK/harvest.tsv"
for at in "${!TKEY[@]}"; do
  [ "${TKEY[$at]}" != "ts" ] && NONTS[${#NONTS[@]}]=$at
done

if [ ${#ROWS[@]} -eq 0 ]; then echo "no candidate values harvested" >&2; exit 1; fi
if [ ${#NONTS[@]} -eq 0 ]; then echo "no sampled line keeps its time under a name other than ts" >&2; exit 1; fi
summary="input: $NLINES lines, ${#KEYS[@]} distinct keys, ${#ROWS[@]} sampled lines, ${#NONTS[@]} with a time key other than ts"
# Kept off stdout when printing: there, every stdout line is a combination.
if [ "$PRINT_QUERIES" = 1 ]; then echo "$summary" >&2; else echo "$summary"; fi

# One `key=value` term off a sampled line, chosen by index so the caller can take several
# from the same line and be sure a template holds them together.
term_from() { IFS=$'\t' read -r -a f <<< "${ROWS[$1]}"; echo "${f[$((RANDOM % ${#f[@]}))]}"; }

# That line's own time, as RFC 3339, or empty if it has no time key. An epoch is
# converted here so the boundary draw below works on every template, not only the ones
# that spell their time `ts`; milliseconds are cut to the second, which keeps the edge
# on the row's side of the boundary either way round.
time_of() { IFS=$'\t' read -r -a f <<< "${ROWS[$1]}"; local k="${TKEY[$1]}"; for x in "${f[@]}"; do
              case "$x" in
                "$k="*) v="${x#*=}"
                        case "$k" in
                          ts_unix) rfc3339 "$v" ;;
                          ts_ms)   rfc3339 "${v%???}" ;;
                          *)       echo "$v" ;;
                        esac; return ;;
              esac; done; }

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
windowed=0
nonts=0
for i in $(seq 1 "$COMBOS"); do
  ARGS=()

  # Every fourth draw is forced onto a line whose time key is not `ts`, and every fourth
  # is forced to carry a window, so each is at least a quarter of the run whatever the
  # random draws do. The two are staggered so half of the forced windows land on the
  # forced lines.
  row=$((RANDOM % ${#ROWS[@]}))
  if [ $((i % 4)) -eq 0 ]; then row=${NONTS[$((RANDOM % ${#NONTS[@]}))]}; fi
  [ "${TKEY[$row]}" != "ts" ] && nonts=$((nonts + 1))

  # Decided before the terms are drawn: a window whose edge is this row's own timestamp
  # only settles `[since, until)` if the row is one the pattern selects, so that draw
  # takes every term off this line unaltered and keeps the answer down to the boundary.
  window=$((RANDOM % 4))
  if [ $((i % 4)) -eq 1 ] && [ "$window" -eq 2 ]; then window=0; fi
  hit=$(time_of "$row")
  boundary=0
  if [ "$window" -eq 3 ] && [ -n "$hit" ]; then boundary=1; fi
  case "$window" in 0|1) windowed=$((windowed + 1)) ;; 3) [ "$boundary" -eq 1 ] && windowed=$((windowed + 1)) ;; esac

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
    # Every generated timestamp is from 2024, so a row with its own time is outside this
    # window; a row on arrival time is inside it, at the `now` both sides share.
    # Before rows with no time of their own took their arrival time, this was always the
    # `exists: true, count: 0` answer.
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

  if [ "$PRINT_QUERIES" = 1 ]; then
    (IFS=$'\t'; printf '%s\n' "${ARGS[*]}")
    continue
  fi

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
[ "$PRINT_QUERIES" = 1 ] && exit 0

echo
echo "$COMBOS combinations: $known known template, $unknown unknown template, $nonempty returned rows"
echo "$COMBOS combinations: $windowed with a window, $nonts on a template whose time key is not ts"
echo "$COMBOS combinations, $disagreements disagreements"
status=0
[ "$windowed" -ge $((COMBOS / 4)) ] || { echo "FAIL: fewer than a quarter carried a window"; status=1; }
[ "$nonts" -ge $((COMBOS / 4)) ] || { echo "FAIL: fewer than a quarter touched a template whose time key is not ts"; status=1; }
[ "$disagreements" -eq 0 ] || status=1
exit $status
