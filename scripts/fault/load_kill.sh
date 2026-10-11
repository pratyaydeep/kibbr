#!/usr/bin/env bash
# A load killed with kill -9 at any moment leaves the whole load or none of it.
#
# `kibbr load` writes its files under staging/, commits them with one manifest record,
# then moves them into data/. A kill can land before the record, between the record and
# the move, or in the middle of the move, and the next start has to finish a committed load
# and undo an uncommitted one. A partial answer after the restart is rows no one committed
# to, or rows a committed load lost; either way COUNT stops meaning what was loaded.
#
# One uninterrupted load of the same input is the reference: its wall time T sets the kill
# window, its answers are "whole", and a fresh empty directory's answers are "absent".
# Then ROUNDS times, on a fresh directory: load alone, kill -9 after a delay drawn uniform
# in [0, 1.1 T], restart the server, and ask again.
#
# What this asserts, per round:
#
#   1. After the restart every answer of the battery equals the whole reference or every
#      one equals the absent reference - nothing in between, and never a mix.
#   2. The restart leaves no staging/ behind: an interrupted load is finished or undone
#      at startup, not left for the next load to trip over.
#   3. The same load again, through the running server: after "absent" it commits and the
#      answers become whole; after "whole" it is refused as a repeat, naming the load id,
#      and the answers do not move. A load undone at startup that still counted as loaded
#      would block its own retry forever.
#   4. The server stops on SIGTERM with exit 0.
#
# A kill that lands after the loader has already exited is "finished" and must be whole.
# The run fails if every round was finished: then no kill landed and nothing was tested.
#
# The delays come from a seeded generator as fractions of T, and each is printed, so a
# failing round replays with the same SEED and T_MS pinned to the printed T.
#
#   NLINES=200000 ROUNDS=20 SEED=1 scripts/fault/load_kill.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
NLINES=${NLINES:-200000}
GEN_SEED=${GEN_SEED:-42}
ROUNDS=${ROUNDS:-20}
SEED=${SEED:-1}
T_MS=${T_MS:-}

# Another build can be tested by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1

# Written under a temporary name and renamed: a generation cut short must not leave a
# truncated file that later runs trust. The load id hashes the file's name and bytes, so
# the name stays fixed for a given corpus.
mkdir -p testdata/load-kill
INPUT=testdata/load-kill/farm-${NLINES}-seed${GEN_SEED}.ndjson
if [ ! -s "$INPUT" ]; then
  "$BIN" gen-logs --profile farm --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT.tmp.$$" \
    && mv "$INPUT.tmp.$$" "$INPUT" || { rm -f "$INPUT.tmp.$$"; exit 1; }
fi

WORK=$(mktemp -d)
PID=""
LPID=""
cleanup() {
  [ -n "$LPID" ] && kill -KILL "$LPID" 2>/dev/null
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/help.py" <<'PY'
import json, random, sys, time, urllib.error, urllib.request

def ask(port, q):
    body = json.dumps({"q": q}).encode()
    req = urllib.request.Request("http://127.0.0.1:%s/query" % port, body,
                                 {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"http_error": e.code}
    except Exception as e:
        return {"http_error": str(e)}

def comparable(a):
    """The rows, and only the rows.

    `exists` and `templates_matched` are left out: they answer from the template catalog,
    and a template an undone load introduced may outlive it - it names a shape and holds no
    row. Zero-count groups are dropped for the same reason: a zero-filled time bucket says
    where the store thinks its range is, not that a row is there."""
    groups = [g for g in (a.get("groups") or []) if g.get("count")]
    return {"count": a.get("count"), "groups": groups, "http_error": a.get("http_error")}

def queries(path):
    return [q for q in open(path).read().splitlines() if q.strip()]

cmd, args = sys.argv[1], sys.argv[2:]

if cmd == "now_ms":
    print(int(time.time() * 1000))

elif cmd == "key":  # key <input> <head lines>
    # `level` when the input has it anywhere in its head; a group per value partitions every
    # row, the ones without it included, so it need not be on most lines. Otherwise the
    # most widespread key with few enough values that each group is a real slice.
    path, head = args[0], int(args[1])
    seen, values = {}, {}
    with open(path) as f:
        for i, line in enumerate(f):
            if i >= head:
                break
            try:
                o = json.loads(line)
            except ValueError:
                continue
            if not isinstance(o, dict):
                continue
            for k, v in o.items():
                seen[k] = seen.get(k, 0) + 1
                s = values.setdefault(k, set())
                if len(s) <= 32:
                    s.add(json.dumps(v, sort_keys=True))
    if "level" in seen:
        print("level")
    else:
        few = [k for k in seen if len(values[k]) <= 32]
        if not few:
            sys.exit("no key in the first %d lines has 32 or fewer values" % head)
        print(sorted(few, key=lambda k: (-seen[k], k))[0])

elif cmd == "delays":  # delays <seed> <t_ms> <rounds>
    rng = random.Random(int(args[0]))
    t = float(args[1])
    for _ in range(int(args[2])):
        print(int(round(rng.uniform(0, 1.1) * t)))

elif cmd == "field":  # field <name>, the report on stdin; empty when there is none
    report = None
    for line in sys.stdin.read().splitlines():
        try:
            report = json.loads(line)
        except ValueError:
            pass
    if isinstance(report, dict) and args[0] in report:
        print(report[args[0]])

elif cmd == "battery":  # battery <port> <queries file>
    print(json.dumps([comparable(ask(args[0], q)) for q in queries(args[1])], sort_keys=True))

elif cmd == "count":  # count <battery file>: the first query's count
    print(json.load(open(args[0]))[0]["count"])

elif cmd == "clean":  # clean <battery file>: exit 1 if any answer was an error
    sys.exit(1 if any(a["http_error"] for a in json.load(open(args[0]))) else 0)

elif cmd == "classify":  # classify <got> <whole> <absent> <queries>: outcome, then why
    got, whole, absent = (json.load(open(p)) for p in args[:3])
    qs = queries(args[3])
    if got == whole:
        print("whole")
    elif got == absent:
        print("absent")
    else:
        print("partial")
        why = []
        for q, g, w, a in zip(qs, got, whole, absent):
            if g in (w, a):
                continue
            if g["http_error"]:
                why.append("%s answered %s" % (q, g["http_error"]))
            elif g["count"] not in (w["count"], a["count"]):
                why.append("%s: count %s, want %s or %s" % (q, g["count"], w["count"], a["count"]))
            else:
                why.append("%s: groups match neither reference" % q)
        if not why:
            # Each answer matches one reference, but not all the same one.
            why.append("answers mix the references: " + ", ".join(
                "%s %s" % (q, "whole" if g == w else "absent")
                for q, g, w in zip(qs, got, whole)))
        print("; ".join(why))
PY
H="$WORK/help.py"

# Sets PID and PORT rather than echoing them, so it must be called in this shell: in a
# subshell the server is not this shell's child, and `wait` could not collect its exit.
PORT=""
start() {  # start <logfile> <serve flags...>
  local log=$1; shift
  "$BIN" serve --port 0 "$@" 2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then
    kill -KILL "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""
    return 1
  fi
}

STOP_CODE=""
stop() {
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  STOP_CODE=$?
  PID=""
}

# ── The battery ──────────────────────────────────────────────────────────────────────
# COUNT is the headline; by the hour and by one key's values catch a load that is short in
# one place and doubled in another, which leaves the total right.
KEY=$(python3 "$H" key "$INPUT" 10000) || exit 1
Q="$WORK/queries.txt"
{
  echo 'COUNT LOG {}'
  echo 'COUNT LOG {} GROUP BY time(1h)'
  echo "COUNT LOG {} GROUP BY $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$KEY")"
} > "$Q"

FAIL=0

# ── The references ───────────────────────────────────────────────────────────────────
T0=$(python3 "$H" now_ms)
"$BIN" load --data-dir "$WORK/ref" --json "$INPUT" > "$WORK/ref.out" 2> "$WORK/ref.err"
REF_CODE=$?
T1=$(python3 "$H" now_ms)
MEASURED=$((T1 - T0))
if [ "$REF_CODE" != 0 ]; then
  echo "FAIL the reference load exited $REF_CODE, want 0"
  tail -5 "$WORK/ref.err" | sed 's/^/    /'
  exit 1
fi
N=$(python3 "$H" field rows < "$WORK/ref.out")
LOAD_ID=$(python3 "$H" field load_id < "$WORK/ref.out")
if [ -z "$N" ] || [ -z "$LOAD_ID" ]; then
  echo "FAIL the reference load printed no report with rows and load_id:"
  head -c 600 "$WORK/ref.out" | sed 's/^/    /'
  exit 1
fi
T=${T_MS:-$MEASURED}

start "$WORK/ref.log" --data-dir "$WORK/ref" ||
  { echo "FAIL no server started on the reference directory"; tail -20 "$WORK/ref.log"; exit 1; }
python3 "$H" battery "$PORT" "$Q" > "$WORK/whole.json"
stop
[ "$STOP_CODE" = 0 ] || { echo "FAIL the reference server stopped with exit $STOP_CODE, want 0"; FAIL=1; }

start "$WORK/empty.log" --data-dir "$WORK/empty" ||
  { echo "FAIL no server started on an empty directory"; tail -20 "$WORK/empty.log"; exit 1; }
python3 "$H" battery "$PORT" "$Q" > "$WORK/absent.json"
stop
[ "$STOP_CODE" = 0 ] || { echo "FAIL the empty server stopped with exit $STOP_CODE, want 0"; FAIL=1; }

WHOLE_COUNT=$(python3 "$H" count "$WORK/whole.json")
ABSENT_COUNT=$(python3 "$H" count "$WORK/absent.json")

echo "input      $INPUT, $NLINES lines"
echo "reference  load $LOAD_ID, $N rows in ${MEASURED}ms alone; COUNT LOG {} whole $WHOLE_COUNT, absent $ABSENT_COUNT"
echo "battery    $(tr '\n' '|' < "$Q" | sed 's/|$//; s/|/, /g')"
echo "delays     uniform in [0, 1.1 x ${T}ms], SEED=$SEED (replay: SEED=$SEED T_MS=$T ROUNDS=$ROUNDS scripts/fault/load_kill.sh)"

# The references have to be worth comparing against. Every farm line is valid and carries
# its own time, so a report short of NLINES means "whole" is not the whole input; and an
# error in the whole reference would let an erroring restart pass as whole.
[ "$N" = "$NLINES" ] || { echo "FAIL the reference load reported $N rows, want $NLINES"; FAIL=1; }
[ "$WHOLE_COUNT" = "$N" ] || { echo "FAIL COUNT LOG {} on the reference is $WHOLE_COUNT, want $N"; FAIL=1; }
[ "$ABSENT_COUNT" = "0" ] || { echo "FAIL COUNT LOG {} on an empty directory is $ABSENT_COUNT, want 0"; FAIL=1; }
python3 "$H" clean "$WORK/whole.json" || { echo "FAIL the reference battery answered an error"; FAIL=1; }
[ "$FAIL" = 0 ] || exit 1

DELAYS=()
while IFS= read -r d; do DELAYS[${#DELAYS[@]}]=$d; done < <(python3 "$H" delays "$SEED" "$T" "$ROUNDS")

# ── The rounds ───────────────────────────────────────────────────────────────────────
echo
printf '%5s  %8s  %-9s  %s\n' round "delay ms" outcome verdict
WHOLE=0; ABSENT=0; FINISHED=0; FAILED=0
for r in $(seq 1 "$ROUNDS"); do
  D="$WORK/r$r"
  delay=${DELAYS[$((r - 1))]}
  why=()

  "$BIN" load --data-dir "$D" "$INPUT" > "$WORK/load.$r.out" 2> "$WORK/load.$r.err" &
  LPID=$!
  sleep "$(awk -v ms="$delay" 'BEGIN { printf "%.3f", ms / 1000 }')"
  # The exit status says whether the kill landed: 137 is SIGKILL, 0 is a loader that had
  # already exited. Asking kill -0 first would race the loader's own exit; an unreaped
  # child keeps its pid, so the kill cannot reach another process and the status is this
  # loader's.
  kill -KILL "$LPID" 2>/dev/null
  wait "$LPID" 2>/dev/null
  LCODE=$?
  LPID=""

  # The restart: startup finishes or undoes whatever the kill interrupted.
  state=""
  if ! start "$WORK/serve.$r.log" --data-dir "$D"; then
    outcome="no start"
    why[${#why[@]}]="the server did not start after the kill"
  else
    python3 "$H" battery "$PORT" "$Q" > "$WORK/got.$r.json"
    python3 "$H" classify "$WORK/got.$r.json" "$WORK/whole.json" "$WORK/absent.json" "$Q" \
      > "$WORK/class.$r"
    state=$(sed -n 1p "$WORK/class.$r")
    detail=$(sed -n 2p "$WORK/class.$r")
    case "$LCODE" in
      137) outcome=$state ;;
      0)   outcome=finished ;;
      *)   outcome="exit $LCODE"
           why[${#why[@]}]="the loader exited $LCODE on its own before the kill, want 0 or killed" ;;
    esac
    if [ "$state" = partial ]; then
      why[${#why[@]}]="after the restart $detail"
    elif [ "$LCODE" = 0 ] && [ "$state" != whole ]; then
      why[${#why[@]}]="the loader exited 0 but after the restart the load is $state, want whole"
    fi
    if [ -e "$D/staging" ]; then
      left=$(find "$D/staging" -mindepth 1 -maxdepth 2 2>/dev/null | sed "s|^$D/||" | head -3 | tr '\n' ' ')
      why[${#why[@]}]="staging/ survived startup${left:+, holding $left}${left:-, empty}"
    fi

    # The same load again, as a client of the running server.
    if [ "$state" = whole ] || [ "$state" = absent ]; then
      "$BIN" load --data-dir "$D" --json "$INPUT" > "$WORK/client.$r.out" 2> "$WORK/client.$r.err"
      CCODE=$?
      python3 "$H" battery "$PORT" "$Q" > "$WORK/again.$r.json"
      again=$(python3 "$H" classify "$WORK/again.$r.json" "$WORK/whole.json" "$WORK/absent.json" "$Q" | head -1)
      if [ "$state" = absent ]; then
        crows=$(python3 "$H" field rows < "$WORK/client.$r.out")
        cid=$(python3 "$H" field load_id < "$WORK/client.$r.out")
        [ "$CCODE" = 0 ] || why[${#why[@]}]="the load after an undone one exited $CCODE, want 0: $(tail -1 "$WORK/client.$r.err")"
        [ "$crows" = "$N" ] || why[${#why[@]}]="the load after an undone one reported ${crows:-no} rows, want $N"
        [ "$cid" = "$LOAD_ID" ] || why[${#why[@]}]="the load after an undone one has id ${cid:-none}, want $LOAD_ID"
        [ "$again" = whole ] || why[${#why[@]}]="after the load through the server the answers are $again, want whole"
      else
        [ "$CCODE" = 1 ] || why[${#why[@]}]="the repeated load exited $CCODE, want 1"
        grep -q 'was loaded before' "$WORK/client.$r.err" ||
          why[${#why[@]}]="the repeated load's stderr does not say 'was loaded before': $(tail -1 "$WORK/client.$r.err")"
        grep -q "$LOAD_ID" "$WORK/client.$r.err" ||
          why[${#why[@]}]="the repeated load's stderr does not name load $LOAD_ID"
        [ "$again" = whole ] || why[${#why[@]}]="after the refused repeat the answers are $again, want whole unchanged"
      fi
    fi

    stop
    [ "$STOP_CODE" = 0 ] || why[${#why[@]}]="the server stopped with exit $STOP_CODE on SIGTERM, want 0"
  fi

  verdict=ok
  if [ ${#why[@]} -gt 0 ]; then
    verdict=FAIL
    FAILED=$((FAILED + 1))
  else
    case "$outcome" in
      whole) WHOLE=$((WHOLE + 1)) ;;
      absent) ABSENT=$((ABSENT + 1)) ;;
      finished) FINISHED=$((FINISHED + 1)) ;;
    esac
  fi
  printf '%5s  %8s  %-9s  %s\n' "$r" "$delay" "$outcome" "$verdict"
  if [ "$verdict" = FAIL ]; then
    for w in "${why[@]}"; do echo "       FAIL round $r: $w"; done
    # Not every WARN: farm values widen columns on every start, and those lines would bury
    # what recovery said.
    { tail -2 "$WORK/load.$r.err"; grep -hE 'ERROR|panic|staging|recover' "$WORK/serve.$r.log"; } \
      2>/dev/null | head -6 | sed 's/^/         /'
  fi
  rm -rf "$D"
done

echo
echo "$ROUNDS rounds: $WHOLE whole, $ABSENT absent, $FINISHED finished; $FAILED failed"
[ "$FAILED" = 0 ] || FAIL=1
if [ "$FINISHED" = "$ROUNDS" ]; then
  echo "FAIL every kill landed after the loader had exited, so none tested a load in flight: raise NLINES"
  FAIL=1
fi
if [ "$FAIL" = 0 ]; then
  echo "PASS $((WHOLE + ABSENT)) of $ROUNDS kills landed mid-load ($N rows, ${MEASURED}ms alone), none partial: $((WHOLE + FINISHED)) repeats refused, $ABSENT undone loads re-loaded whole"
  exit 0
fi
exit 1
