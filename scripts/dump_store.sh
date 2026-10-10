#!/usr/bin/env bash
# Materialise on disk what `kibbr query` builds in memory: the templates, and one file
# per template holding that template's rows.
#
# Nothing persists yet (crates/kibbr-storage/src/db.rs) - every query rebuilds the trie and the leaf stores
# from the file and throws them away. This writes that state down so it can be read:
#
#   <out>/templates.json          one entry per template: id, fingerprint, keys, types, rows
#   <out>/rows/<template_id>.jsonl  that template's `Vec<Row>`, one Row per line, file order
#
# The rows are the *stored* shape, not the input lines: nested objects flattened to dotted
# keys, arrays kept whole as a raw JSON string, values carrying the type the ladder chose,
# and a `Datetime` as the epoch nanoseconds a `ColValue` actually holds - `kibbr query
# --format ndjson` is the surface that renders those as RFC 3339. Keys are sorted.
#
#   scripts/dump_store.sh --lines 5000 --templates 20 --seed 3
#   scripts/dump_store.sh --input sample.ndjson --out /tmp/dump
#   scripts/dump_store.sh --verify          # every row against the engine's own output
#
# Reads `kibbr parse --json`, which reports the template every line landed in. The row is
# rebuilt by the rule in `db::row_of` - typed value if the key has one, the raw string
# otherwise, which is how a promoted dimension carries its value. Keep the two in step:
# if `row_of` changes, this is wrong until it changes too.

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/kibbr
SEED=42
LINES=1000
TEMPLATES=12
OUT=testdata/store
INPUT=""
VERIFY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --seed)      SEED="$2";      shift 2 ;;
    --lines)     LINES="$2";     shift 2 ;;
    --templates) TEMPLATES="$2"; shift 2 ;;
    --out)       OUT="$2";       shift 2 ;;
    --input)     INPUT="$2";     shift 2 ;;
    --verify)    VERIFY=1;        shift ;;
    -h|--help)   sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

cargo build --release --quiet || exit 1
rm -rf "$OUT"
mkdir -p "$OUT/rows"

# `--input` dumps a file that already exists; otherwise the corpus is generated, and the
# seed is what makes the dump reproducible.
if [ -z "$INPUT" ]; then
  INPUT="$OUT/input.ndjson"
  "$BIN" gen-logs --lines "$LINES" --templates "$TEMPLATES" --seed "$SEED" > "$INPUT" || exit 1
  echo "generated $LINES lines from seed $SEED into $INPUT"
else
  echo "reading $INPUT"
fi

"$BIN" parse "$INPUT" --json 2>/dev/null | OUT="$OUT" python3 -c '
import json, os, sys
from collections import OrderedDict

out = os.environ["OUT"]
templates = OrderedDict()   # template_id -> the entry written to templates.json
handles = {}                # template_id -> open .jsonl, so a template is written once

def row_of(rec):
    """One `Row`, by the rule in db.rs `row_of`.

    `key in typed_values` and not `.get(key)`: an explicit JSON null is present there with
    a null value, and `.get` cannot tell that from a key the map does not hold. The two
    are different rows - a null the ladder saw, versus a promoted key whose value lives in
    the path and comes back off `values` as a string.
    """
    row = {}
    for key in rec["keys"]:
        if key in rec["typed_values"]:
            row[key] = rec["typed_values"][key]
        else:
            row[key] = rec["values"].get(key, "")
    return row

for line in sys.stdin:
    rec = json.loads(line)
    tid = rec["template_id"]

    if tid not in templates:
        templates[tid] = {
            "template_id": tid,
            "fingerprint": rec["fingerprint"],
            "keys": rec["keys"],
            # Collected as a set per key, not taken off the first line: the fingerprint is
            # built from key names, so two lines can agree on the shape and disagree on a
            # column type. Reporting that is the point rather than hiding it.
            "types": {k: [v] for k, v in rec["types"].items()},
            "rows": 0,
            "file": "rows/%d.jsonl" % tid,
        }
        handles[tid] = open(os.path.join(out, "rows", "%d.jsonl" % tid), "w")

    entry = templates[tid]
    entry["rows"] += 1
    for k, v in rec["types"].items():
        seen = entry["types"].setdefault(k, [])
        if v not in seen:
            seen.append(v)

    handles[tid].write(json.dumps(row_of(rec), ensure_ascii=False, sort_keys=True) + "\n")

for h in handles.values():
    h.close()

conflicts = 0
for entry in templates.values():
    # One type collapses to the type; more than one stays a list, and is counted.
    resolved = {}
    for k, seen in entry["types"].items():
        resolved[k] = seen[0] if len(seen) == 1 else sorted(seen)
        if len(seen) > 1:
            conflicts += 1
    entry["types"] = resolved

body = {
    "templates": len(templates),
    "rows": sum(e["rows"] for e in templates.values()),
    "type_conflicts": conflicts,
    # Largest first: the leaf a query is most likely to be paying for.
    "entries": sorted(templates.values(), key=lambda e: (-e["rows"], e["template_id"])),
}
with open(os.path.join(out, "templates.json"), "w") as f:
    json.dump(body, f, indent=2, ensure_ascii=False)
    f.write("\n")

print("%d templates, %d rows" % (body["templates"], body["rows"]))
if conflicts:
    print("%d key(s) carried more than one type within a template" % conflicts)
' || exit 1

# The dump is worth nothing if it disagrees with the thing it claims to be a picture of.
# Row counts come from a different code path here - the engine, over its own stores.
DUMPED=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rows"])' "$OUT/templates.json")
ENGINE=$("$BIN" query "$INPUT" 'COUNT LOG {}' --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')
FILES=$(cat "$OUT"/rows/*.jsonl | wc -l | tr -d ' ')

if [ "$DUMPED" = "$ENGINE" ] && [ "$FILES" = "$ENGINE" ]; then
  echo "verified: $ENGINE rows in the engine, $DUMPED in templates.json, $FILES across the row files"
else
  echo "MISMATCH: engine $ENGINE, templates.json $DUMPED, row files $FILES" >&2
  exit 1
fi

echo "wrote $OUT/templates.json and $(ls "$OUT/rows" | wc -l | tr -d ' ') row files under $OUT/rows/"

# The counts above prove the dump has the right number of rows, not the right rows. This
# compares every one of them against what the engine returns for the same file, which is
# the only thing that can catch this script drifting from `db::row_of`.
if [ "$VERIFY" -eq 1 ]; then
  # `--max-rows 0`: this is the caller that really does want every row, and the default
  # backstop would hand back a page of 10,000 and a cursor - a dump silently short of the
  # store it claims to be.
  "$BIN" query "$INPUT" --max-rows 0 'MATCH LOG {}' --format ndjson > "$OUT/.engine.jsonl" || exit 1
  OUT="$OUT" python3 -c '
import collections, datetime, json, os

out = os.environ["OUT"]

def rfc3339(ns):
    """What `fmt::rfc3339` prints for the same instant.

    Duplicated from Rust on purpose and only here: the dump holds the nanoseconds a
    `ColValue::Datetime` actually is, the engine prints RFC 3339, and something has to
    bridge the two to compare them. Integer arithmetic throughout - 1.7e18 nanoseconds
    does not fit a float64 mantissa. AutoSi means 0, 3, 6 or 9 fractional digits, the
    smallest that keeps the value, which is not the same as stripping trailing zeros.
    """
    secs, frac = divmod(ns, 1_000_000_000)
    text = datetime.datetime.fromtimestamp(secs, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    if frac:
        digits = 3 if frac % 1_000_000 == 0 else 6 if frac % 1_000 == 0 else 9
        text += ("." + "%09d" % frac)[: 1 + digits]
    return text + "Z"

meta = json.load(open(os.path.join(out, "templates.json")))
dumped = collections.Counter()
for entry in meta["entries"]:
    # Per template: the same column name is a Datetime under one fingerprint and an
    # Int64 under another, and a global type map silently mangles the difference.
    stamps = {k for k, t in entry["types"].items() if t == "Datetime"}
    for line in open(os.path.join(out, entry["file"])):
        row = json.loads(line)
        row = {k: rfc3339(v) if k in stamps and isinstance(v, int) else v for k, v in row.items()}
        dumped[json.dumps(row, sort_keys=True, ensure_ascii=False)] += 1

engine = collections.Counter()
for line in open(os.path.join(out, ".engine.jsonl")):
    engine[json.dumps(json.loads(line), sort_keys=True, ensure_ascii=False)] += 1

if dumped == engine:
    print("verified: all %d rows identical to the engine, row for row" % sum(engine.values()))
    raise SystemExit(0)

only_dump, only_engine = dumped - engine, engine - dumped
print("MISMATCH: %d rows only in the dump, %d only in the engine"
      % (sum(only_dump.values()), sum(only_engine.values())))
for text in list(only_dump)[:2]:   print("  dump  ", text[:200])
for text in list(only_engine)[:2]: print("  engine", text[:200])
raise SystemExit(1)
' || exit 1
  rm -f "$OUT/.engine.jsonl"
fi
