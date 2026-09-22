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
# Also asserted, and not by a differential: the permanent set at the bottom pins the things
# the grammar cannot state about itself - that `LIMIT`/`--limit` and `AFTER`/`--after`
# resolve to the same rows, that the rows a limit returns are the *newest* ones, that paging
# with `AFTER` tiles the unpaged answer exactly, and that a `MATCH` naming no `LIMIT` is cut
# at `max_rows` as a page rather than a truncation - it pages to the same row set the
# uncapped query returns. The "newest" check runs against the reference scanner, sorted
# descending and truncated, because that is a claim about the data and not about the two
# surfaces agreeing with each other.
#
# The random combinations run with no `--max-rows`, so the 10,000-row default is out of
# reach of a 2,000-line corpus and every answer above the backstop section is uncapped.
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
answer() { raw "$@" | sed 's/"duration_ms":[-+0-9.e]*//'; }

# The same answer with nothing removed, so it is still JSON. `answer` blanks `duration_ms`
# in place, which leaves a value-less key that a JSON parser is right to reject - fine for
# the string comparison the differential does, useless to the checks that read a field.
#
# stderr is kept off the answer and printed only when the query failed, which is the case
# it was ever wanted for. Merging the two streams corrupts the answer on this corpus: the
# ladder reads a ten-digit id outside the epoch window as an integer and one inside it as
# a date, so those columns widen to text and log a line each, and a log line written into
# the middle of a one-line JSON answer is not JSON. The status returned is the query's.
raw() {
  local out status
  out=$("$BIN" query "$INPUT" "$@" --format json 2>"$WORK/query.err")
  status=$?
  printf '%s\n' "$out"
  [ $status -eq 0 ] || cat "$WORK/query.err"
  return $status
}

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
limited=0
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
       # `time(...)` is in the grammar and on the flag, so it is in the comparison. A
       # clause one surface can say and the other cannot is the drift this script exists
       # to catch, and it would go uncaught the moment the clause is added to only one.
       if [ $((RANDOM % 3)) -eq 0 ]; then
         w=("5m" "1h" "6h" "1d"); g="time(${w[$((RANDOM % 4))]}),$g"
       fi
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

  # `LIMIT` on a third of them. Only `MATCH` returns rows, so a limit is only asked for
  # where it can change the answer - on the others the grammar accepts it and `Query::new`
  # drops it, which the permanent set below checks directly.
  LIMIT=""
  if [ "$VERB" = "MATCH" ] && [ -z "$GROUP" ] && [ $((RANDOM % 3)) -eq 0 ]; then
    n=$((1 + RANDOM % 20))
    FLAGS[${#FLAGS[@]}]="--limit"; FLAGS[${#FLAGS[@]}]="$n"
    LIMIT=" LIMIT $n"
    limited=$((limited + 1))
  fi

  TRIEQL="$VERB LOG {$PAIRS}$WINDOW$GROUP$LIMIT"

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
echo "$COMBOS combinations: $known known template, $unknown unknown template, $nonempty returned rows, $limited with a LIMIT"
echo "$COMBOS combinations, $disagreements disagreements"
echo "not covered: --exists (the grammar has no verb for QueryMode::Exists)"

# ── The permanent set ────────────────────────────────────────────────────────────────
#
# The random combinations above are a differential and prove only that two surfaces agree.
# These four are the queries whose *answers* are pinned, because each one names a decision
# the grammar does not: the empty pattern, the absence of a cap, the order a limit
# truncates, and that paging tiles the answer.
#
# Each row is: the TrieQL query, the flag spelling of the same query, and what the answer
# has to be. `-` for the flag spelling means the query has no flag equivalent.
echo
echo "── permanent set ──"

pinned=0
failures=0

# The named fields of `--format json`, so a check reads as a sentence rather than as a
# `grep` over a JSON blob.
field() { python3 -c '
import json, sys
d = json.load(sys.stdin)
d["meta"]["duration_ms"] = 0
for k in sys.argv[1:]:
    d = d["meta"] if k == "meta" else d[k]
print(json.dumps(d))
' "$@"; }

rows_json() { python3 -c '
import json, sys
for r in json.load(sys.stdin)["rows"]:
    print(json.dumps(r, sort_keys=True))
'; }

# The effective time of every returned row, in the order they came back: the
# row's time column - the first of the default list it carries - or `_received_at`
# where it is on arrival time. Rendered as one sortable spelling, because a template may
# spell its time as RFC 3339, ten digits or thirteen and the order is over instants, not
# text. The ordering guarantee is about which rows and in what order, and the time is the
# whole of what it promises - comparing it rather than the rows themselves is what makes
# the check robust to two rows sharing an instant, which the tiebreak settles but the
# guarantee does not speak to.
row_ts() { python3 -c '
import json, sys
from datetime import datetime, timezone
TIME = ["@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix", "datetime",
        "event_time", "created_at", "_received_at"]
def instant(row):
    for k in TIME:
        v = row.get(k)
        if v is None:
            continue
        if isinstance(v, str) and v.isdigit() and len(v) == 10:
            return datetime.fromtimestamp(int(v), timezone.utc).isoformat()
        if isinstance(v, str) and v.isdigit() and len(v) == 13:
            return datetime.fromtimestamp(int(v) / 1000, timezone.utc).isoformat()
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            continue
    return "None"
print("\n".join(instant(r) for r in json.load(sys.stdin)["rows"]))
'; }

check() { # name expected actual
  if [ "$2" = "$3" ]; then
    pinned=$((pinned + 1))
    echo "  ok    $1"
  else
    failures=$((failures + 1))
    echo "  FAIL  $1"
    echo "        expected  $2"
    echo "        actual    $3"
  fi
}

# The two surfaces on the same query, for the ones that have a flag spelling.
same() { # name trieql flags...
  local name=$1 q=$2; shift 2
  check "$name" "$(answer "$q")" "$(answer "$@")"
}

# ── MATCH LOG {} SINCE 1h LIMIT 10 ───────────────────────────────────────────────────
#
# `SINCE 1h` is empty against a 2024 corpus by design (see the note at the top), so the
# ordering rows below use `SINCE 100000d` to reach the data. This one pins the pair.
same "MATCH LOG {} SINCE 1h LIMIT 10  ≡  --fetch --since 1h --limit 10" \
     'MATCH LOG {} SINCE 1h LIMIT 10' --fetch --since 1h --limit 10

# ── MATCH LOG {} SINCE 1h  (no limit on either side) ─────────────────────────────────
same "MATCH LOG {} SINCE 1h  ≡  --fetch --since 1h" \
     'MATCH LOG {} SINCE 1h' --fetch --since 1h

# No default cap. `MATCH` with no `LIMIT` returns every matching row, is not truncated, and
# offers no next page - there is nothing after "everything".
uncapped=$(raw 'MATCH LOG {} SINCE 100000d' | field meta)
dated=$(raw 'COUNT LOG {} SINCE 100000d' | field count)
check "MATCH with no LIMIT returns every row, untruncated, with no cursor" \
      "$dated false None" \
      "$(printf '%s' "$uncapped" | python3 -c 'import json,sys; m=json.load(sys.stdin); print(m["rows_returned"], str(m["truncated"]).lower(), m["next_cursor"])')"

# ── COUNT LOG {}  (all templates, all rows) ──────────────────────────────────────────
#
# The empty pattern is every template, and `COUNT` is uncapped: a limit bounds rows
# returned and `COUNT` returns none, so a cap here would silently answer a different
# question. `wc -l` is the independent number.
total=$(wc -l < "$INPUT" | tr -d ' ')
check "COUNT LOG {} counts every line in the file" \
      "$total" \
      "$(raw 'COUNT LOG {}' | field count)"
check "COUNT LOG {} is not truncated" \
      "false" \
      "$(raw 'COUNT LOG {}' | field meta truncated | tr -d '"')"
same "COUNT LOG {}  ≡  --count" 'COUNT LOG {}' --count

# `MATCH LOG {}` reaches every template too, and the walk is the same one.
check "MATCH LOG {} and COUNT LOG {} match the same templates" \
      "$(raw 'COUNT LOG {}' | field meta templates_matched)" \
      "$(raw 'MATCH LOG {}' | field meta templates_matched)"

# ── MATCH LOG {"event":"login_failed"} SINCE 24h LIMIT 5 ─────────────────────────────
same 'MATCH LOG {"event":"login_failed"} SINCE 24h LIMIT 5  ≡  the flags' \
     'MATCH LOG {"event":"login_failed"} SINCE 24h LIMIT 5' \
     --fetch --match event=login_failed --since 24h --limit 5

# ── The ordering guarantee, against the oracle ───────────────────────────────────────
#
# Not a differential between the two surfaces - they share an engine and would agree on a
# wrong order. The reference scanner reaches the answer by sorting every matching row and
# taking the front, which is the definition the engine's bounded heap has to reproduce.
#
# `--reference` is only reachable through the flags, so the flag spelling is the one run
# against it; the `same` rows above are what carry that back to TrieQL.
for spec in "5 --match event=login_failed" "10 " "1 " "3 --match level=ERROR"; do
  n=${spec%% *}; extra=${spec#* }
  # shellcheck disable=SC2086
  engine_ts=$(raw --fetch $extra --since 100000d --limit "$n" | row_ts)
  # shellcheck disable=SC2086
  oracle_ts=$(raw --fetch $extra --since 100000d --limit "$n" --reference | row_ts)
  check "LIMIT $n ${extra:-(no term)} returns the oracle's newest $n, in order" \
        "$oracle_ts" "$engine_ts"

  # Descending, checked on its own: the oracle could agree with the engine on a shared
  # mistake about direction, and this cannot.
  sorted=$(printf '%s\n' "$engine_ts" | sort -r)
  check "LIMIT $n ${extra:-(no term)} is newest-first" "$sorted" "$engine_ts"
done

# Every row has a time: a template that spells its time `ts_unix` or `ts_ms`
# is ordered by it, and a template with no time column is ordered by the instant its
# rows arrived. So an unwindowed answer is newest-first from end to end, with no row left
# over to sort last. The limit is above the row count on purpose, so the page cannot fill
# from one template alone and leave this checking nothing.
all_ts=$(raw 'MATCH LOG {} LIMIT 100000' | row_ts)
check "every row of an unwindowed answer has a time" \
      "0" "$(printf '%s\n' "$all_ts" | grep -c '^None$')"
check "an unwindowed answer is newest-first from end to end" \
      "$(printf '%s\n' "$all_ts" | sort -r)" \
      "$all_ts"

# The whole answer against the oracle, in full and in order. Rows sharing an instant are
# settled by the tiebreak, and the tiebreak is what the two sides have to compute
# separately and land on together.
check "an unwindowed answer matches the oracle row for row, in order" \
      "$(raw --fetch --limit 100000 --reference | rows_json)" \
      "$(raw --fetch --limit 100000 | rows_json)"

# The newest row is the same row whether or not a limit was asked for - a limit truncates
# the tail, never the head.
head_capped=$(raw 'MATCH LOG {} SINCE 100000d LIMIT 1' | row_ts)
head_full=$(raw 'MATCH LOG {} SINCE 100000d LIMIT 2000' | row_ts | head -1)
check "LIMIT 1 returns the same row as the head of the unlimited answer" \
      "$head_full" "$head_capped"

# ── Paging ───────────────────────────────────────────────────────────────────────────
#
# The claim `AFTER` exists to make: walking the answer a page at a time produces the same
# rows, in the same order, as asking for it whole. A gap or a repeat at a page boundary is
# the failure mode keyset paging is chosen to avoid, so it is checked directly rather than
# inferred from the two surfaces agreeing.
#
# `MATCH LOG {}` with no window is the hard case on purpose: it reaches every template,
# whichever name it keeps its time under, and rows sharing an instant have their page
# boundaries carried entirely by the tiebreak.
page_all() { # trieql-without-limit  page-size
  local base=$1 size=$2 cursor="" q out
  while :; do
    if [ -n "$cursor" ]; then q="$base LIMIT $size AFTER \"$cursor\""; else q="$base LIMIT $size"; fi
    out=$(raw "$q") || return 1
    printf '%s' "$out" | rows_json
    cursor=$(printf '%s' "$out" | field meta next_cursor | tr -d '"')
    [ "$cursor" = "null" ] && return 0
  done
}

for spec in 'MATCH LOG {}|7' 'MATCH LOG {"event":"login_failed"} SINCE 100000d|5' 'MATCH LOG {} SINCE 100000d|500'; do
  base=${spec%|*}; size=${spec#*|}
  check "paging $base at LIMIT $size tiles the whole answer" \
        "$(raw "$base" | rows_json)" \
        "$(page_all "$base" "$size")"
done

# The flag surface pages identically, which is what keeps `--after` and `AFTER` one feature.
first=$(raw --fetch --match event=login_failed --since 100000d --limit 4)
cur=$(printf '%s' "$first" | field meta next_cursor | tr -d '"')
check "--after and AFTER resolve to the same page" \
      "$(raw --fetch --match event=login_failed --since 100000d --limit 4 --after "$cur" | rows_json)" \
      "$(raw "MATCH LOG {\"event\":\"login_failed\"} SINCE 100000d LIMIT 4 AFTER \"$cur\"" | rows_json)"

# A cursor names a leaf by its ordinal among the leaves *this* pattern matched, so the same
# text under a different pattern names a different leaf. Refused rather than answered.
if raw "MATCH LOG {} LIMIT 4 AFTER \"$cur\"" | grep -q '"exists"'; then
  failures=$((failures + 1)); echo "  FAIL  a cursor from another query was accepted"
else
  pinned=$((pinned + 1)); echo "  ok    a cursor from another query is refused"
fi

# ── The max_rows backstop ────────────────────────────────────────────────────────────
#
# A query naming no `LIMIT` is cut at the backstop. The claim under test is that the cut is
# a *page size*: the answer says who cut it and hands back a cursor, and walking that cursor
# reproduces the uncapped answer exactly. A backstop that could not do this would be a
# truncation - the same rows missing, with nothing to say so.
#
# 50 against a 2,000-line corpus, so the cap is reached many times over; the default is
# 10,000 and out of reach here, which is what keeps every check above uncapped.
CAP=50

# `raw`, under a backstop. Separate rather than a global, so nothing above changes meaning.
capped() {
  local cap=$1 out status
  shift
  out=$("$BIN" query "$INPUT" --max-rows "$cap" "$@" --format json 2>"$WORK/query.err")
  status=$?
  printf '%s\n' "$out"
  [ $status -eq 0 ] || cat "$WORK/query.err"
  return $status
}

# Paging with no `LIMIT` on any request: the backstop supplies the page size every time,
# which is the shape a client that never learned to page produces.
page_backstop() { # cap trieql-without-limit
  local cap=$1 base=$2 cursor="" q out
  while :; do
    if [ -n "$cursor" ]; then q="$base AFTER \"$cursor\""; else q="$base"; fi
    out=$(capped "$cap" "$q") || return 1
    printf '%s' "$out" | rows_json
    cursor=$(printf '%s' "$out" | field meta next_cursor | tr -d '"')
    [ "$cursor" = "null" ] && return 0
  done
}

echo
echo "── the max_rows backstop ──"

cut=$(capped "$CAP" 'MATCH LOG {}' | field meta)
check "a MATCH with no LIMIT is cut at max_rows, says so, and offers a cursor" \
      "$CAP true backstop cursor" \
      "$(printf '%s' "$cut" | python3 -c 'import json,sys; m=json.load(sys.stdin); print(m["rows_returned"], str(m["truncated"]).lower(), m["limit_source"], "cursor" if m["next_cursor"] else "none")')"

check "the backstopped page still reports the whole answer's count" \
      "$(raw 'COUNT LOG {}' | field count)" \
      "$(capped "$CAP" 'MATCH LOG {}' | field count)"

check "paging the backstop tiles the same answer as the uncapped query" \
      "$(raw 'MATCH LOG {}' | rows_json)" \
      "$(page_backstop "$CAP" 'MATCH LOG {}')"

check "a caller's LIMIT above max_rows is honoured, not clamped to it" \
      "120 caller" \
      "$(capped "$CAP" 'MATCH LOG {} LIMIT 120' | field meta | python3 -c 'import json,sys; m=json.load(sys.stdin); print(m["rows_returned"], m["limit_source"])')"

check "COUNT is untouched by a backstop below its total" \
      "$(raw 'COUNT LOG {}' | field count) false unlimited" \
      "$(capped "$CAP" 'COUNT LOG {}' | python3 -c 'import json,sys; d=json.load(sys.stdin); m=d["meta"]; print(d["count"], str(m["truncated"]).lower(), m["limit_source"])')"

# ── Cursors that must be refused ─────────────────────────────────────────────────────
#
# Every one of these has to be an error. A cursor is the one input that can be *wrong*
# without looking wrong - resolved against the layout it does not describe, it answers
# confidently from the wrong row, which is the failure paging exists to prevent.
echo
echo "── refused cursors ──"

refused() { # name cursor
  local name=$1 forged=$2 out status
  out=$("$BIN" query "$INPUT" \
        "MATCH LOG {\"event\":\"login_failed\"} SINCE 100000d LIMIT 4 AFTER \"$forged\"" \
        --format json 2>&1)
  status=$?
  if [ $status -eq 0 ] || printf '%s' "$out" | grep -q '"rows"'; then
    failures=$((failures + 1))
    echo "  FAIL  $name was accepted"
    echo "        $out"
  else
    pinned=$((pinned + 1))
    echo "  ok    $name refused: $(printf '%s' "$out" | head -1 | cut -c1-96)"
  fi
}

body=${cur#*.}
# A cursor from a superseded page order. Nothing reads one and nothing translates one.
refused "a v1 cursor"        "v1.$body"
refused "an unknown version" "v99.$body"
refused "no version at all"  "$body"
refused "a garbled body"     "v2.$(printf '%s' "$body" | tr '0-9a-f' 'zzzzzzzzzzzzzzzz')"
refused "a short body"       "v2.00ff"
refused "an empty cursor"    ""
# One nibble of the guard flipped: well-formed, the right version, the right length, and no
# longer this query's. The only check here that reaches past `decode` into `Cursor::check`.
flip_last() { python3 -c 'import sys; c = sys.argv[1]; print(c[:-1] + ("1" if c[-1] == "a" else "a"))' "$1"; }
refused "a tampered guard"   "$(flip_last "$cur")"

echo
echo "permanent set: $pinned pinned, $failures failures"
[ "$disagreements" -eq 0 ] && [ "$examples" -eq 2 ] && [ "$failures" -eq 0 ]
