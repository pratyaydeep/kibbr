#!/usr/bin/env bash
# How much a log file looks like real logs, side by side for any number of files: its
# size and what `gzip -9` and `zstd -19` make of it, how many templates `kibbr parse`
# finds and how unevenly the rows fall across them, and which columns hold the bytes and
# the entropy. Run it on generated data beside a sample of real logs, and the gap between
# the columns is how far the generator is from the real thing.
#
# The last block is the four ways the hostile generator was measured unlike real logs:
# rows per (template, day), days per template, timestamps out of order, and columns whose
# rows carry more than one type. A template's time is its first key, in the order of the
# default `[schema] time_columns`, that is a Datetime on the template's first line - the
# rule the store uses - and a template with none has no time and is left out of that block.
#
# Order is counted three ways. At face value, a line is earlier when its time is less than
# the line before's - but nginx writes whole seconds, so in a file that is in order to the
# millisecond its lines still read as earlier than the millisecond-stamped lines from the
# same second. Beyond precision, a line is earlier only when the whole interval its text
# can mean ends before the last line's time. Within a template compares a template's rows
# with each other, which is the order a column store sees.
#
# "gz share" is a column's own `gzip -9` bytes as a share of all columns' - which column the
# entropy lives in. Distinct values are counted over the raw text, so `200` and `"200"` are
# one value, as the column store sees them.
#
# python3 is used for the tally: a million `parse --json` records is not a job for awk.
#
#   scripts/log_report.sh old.ndjson farm.ndjson
#   TOP=15 scripts/log_report.sh real.ndjson

set -uo pipefail

BIN=./target/release/kibbr
TOP=${TOP:-10}

[ $# -ge 1 ] || { echo "usage: $0 FILE [FILE...]" >&2; exit 2; }
# Absolute before the `cd` below, so a path is read from where it was typed.
files=()
for f in "$@"; do
  case $f in /*) files+=("$f") ;; *) files+=("$PWD/$f") ;; esac
done
set -- "${files[@]}"
cd "$(dirname "$0")/.."
for f in "$@"; do
  [ -f "$f" ] || { echo "$f: not a file" >&2; exit 2; }
done
command -v zstd >/dev/null || { echo "zstd not found" >&2; exit 2; }

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

TALLY=$(cat <<'PY'
import json, re, sys, zlib
from collections import Counter, defaultdict

TIME_COLUMNS = ["@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix",
                "datetime", "event_time", "created_at"]
DAY_NS = 86_400 * 10**9

rows = 0
tmpl_rows = Counter()
tmpl_time = {}
tmpl_days = defaultdict(Counter)
col_bytes = Counter()
col_rows = Counter()
col_seen = defaultdict(set)
col_types = defaultdict(Counter)
col_z = {}
col_zbytes = Counter()
# (template, key) -> the row types it has carried, nulls aside.
cell_types = defaultdict(set)
ordered = backwards = far_back = beyond = 0
last_t = None
last_by_template = {}
in_template = in_template_back = 0
FRACTION = re.compile(r"[T ]\d\d:\d\d:\d\d(?:\.(\d+))?")


def precision_ns(raw):
    """How wide an instant the text can mean: a second with no fraction, less with one."""
    if raw.isdigit():
        return 10**9 if len(raw) == 10 else 10**6
    m = FRACTION.search(raw)
    digits = len(m.group(1)) if m and m.group(1) else 0
    return 10 ** max(0, 9 - digits)

rejected = 0

for line in sys.stdin:
    try:
        rec = json.loads(line)
    except ValueError:
        rejected += 1
        continue
    rows += 1
    tid = rec["template_id"]
    tmpl_rows[tid] += 1
    values, types, typed = rec["values"], rec["types"], rec["typed_values"]
    if tid not in tmpl_time:
        tmpl_time[tid] = next((k for k in TIME_COLUMNS if types.get(k) == "Datetime"), None)
    tk = tmpl_time[tid]
    if tk is not None and types.get(tk) == "Datetime":
        t = typed[tk]
        tmpl_days[tid][t // DAY_NS] += 1
        if last_t is not None:
            ordered += 1
            if t < last_t:
                backwards += 1
            if t + precision_ns(values[tk]) <= last_t:
                beyond += 1
            if t < last_t - 10**9:
                far_back += 1
        last_t = t
        prev = last_by_template.get(tid)
        if prev is not None:
            in_template += 1
            if t < prev:
                in_template_back += 1
        last_by_template[tid] = t
    for k, raw in values.items():
        ty = types.get(k)
        b = raw.encode()
        col_bytes[k] += len(b)
        col_rows[k] += 1
        col_seen[k].add(hash(raw))
        col_types[k][ty] += 1
        if ty != "Null":
            cell_types[(tid, k)].add(ty)
        z = col_z.get(k)
        if z is None:
            z = col_z[k] = zlib.compressobj(9)
        col_zbytes[k] += len(z.compress(b + b"\n"))

for k, z in col_z.items():
    col_zbytes[k] += len(z.flush())

counts = sorted(tmpl_rows.values())
def pct(xs, p):
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else 0

per_day = sorted(n for d in tmpl_days.values() for n in d.values())
days_per = sorted(len(d) for d in tmpl_days.values())
widened = [(tid, k) for (tid, k), ts in cell_types.items() if len(ts) > 1]
top = [k for k, _ in col_bytes.most_common(int(sys.argv[1]))]
ztotal = sum(col_zbytes.values()) or 1
vtotal = sum(col_bytes.values()) or 1

json.dump({
    "rows": rows,
    "rejected": rejected,
    "templates": len(tmpl_rows),
    "median": pct(counts, 0.5),
    "p90": pct(counts, 0.9),
    "max": counts[-1] if counts else 0,
    "top10_share": sum(counts[-10:]) / rows if rows else 0,
    "single_row_templates": sum(1 for c in counts if c == 1),
    "undated_templates": sum(1 for v in tmpl_time.values() if v is None),
    "undated_rows": sum(n for t, n in tmpl_rows.items() if tmpl_time[t] is None),
    "tday_median": pct(per_day, 0.5),
    "days_median": pct(days_per, 0.5),
    "days_max": days_per[-1] if days_per else 0,
    "backwards": backwards / ordered if ordered else 0,
    "far_back": far_back / ordered if ordered else 0,
    "beyond": beyond / ordered if ordered else 0,
    "in_template_back": in_template_back / in_template if in_template else 0,
    "widened_columns": len(widened),
    "widened_keys": sorted({k for _, k in widened}),
    "keys": len(col_bytes),
    "top": [{
        "key": k,
        "bytes": col_bytes[k],
        "share": col_bytes[k] / vtotal,
        "rows": col_rows[k],
        "distinct": len(col_seen[k]),
        "types": "/".join(t for t, _ in col_types[k].most_common() if t),
        "gz": col_zbytes[k],
        "gz_share": col_zbytes[k] / ztotal,
    } for k in top],
}, sys.stdout)
PY
)

# Each file's three measurements run at once; zstd -19 is the slow one. Every job is
# waited for by its own pid, so one that failed stops the report rather than printing a
# ratio of nothing.
i=0
pids=()
for f in "$@"; do
  # Lines as `parse` counts them: blank ones are skipped, a last one without a newline
  # is not.
  { LC_ALL=C grep -cv '^[[:space:]]*$' "$f" || true; } > "$WORK/$i.lines"
  wc -c < "$f" > "$WORK/$i.size"
  (gzip -9 -c "$f" | wc -c > "$WORK/$i.gz") &
  pids+=($!)
  (zstd -19 -q -c "$f" | wc -c > "$WORK/$i.zst") &
  pids+=($!)
  (
    "$BIN" parse "$f" --json 2>"$WORK/$i.parse_err" | python3 -c "$TALLY" "$TOP" > "$WORK/$i.json"
    echo "${PIPESTATUS[0]} ${PIPESTATUS[1]}" > "$WORK/$i.status"
  ) &
  pids+=($!)
  i=$((i + 1))
done
for pid in "${pids[@]}"; do
  wait "$pid" || { echo "a measurement failed; no report" >&2; exit 1; }
done

python3 - "$WORK" "$TOP" "$@" <<'PY'
import json, os, re, sys

work, top = sys.argv[1], int(sys.argv[2])
files = sys.argv[3:]
cols = []
for i, f in enumerate(files):
    d = json.load(open(os.path.join(work, f"{i}.json")))
    d["name"] = os.path.basename(f)
    d["raw"] = os.path.getsize(f)
    d["gz"] = int(open(os.path.join(work, f"{i}.gz")).read())
    d["zst"] = int(open(os.path.join(work, f"{i}.zst")).read())
    # One line per rejected input line; the closing summary and any `caused by` are not.
    d["rejected"] += sum(1 for l in open(os.path.join(work, f"{i}.parse_err"))
                         if re.match(r"kibbr: line \d+:", l))
    parse_status, tally_status = open(os.path.join(work, f"{i}.status")).read().split()
    # `parse` exits 1 when it rejected a line and carried on; anything else is a failure.
    if tally_status != "0" or parse_status not in ("0", "1"):
        sys.exit(f"{f}: parse exited {parse_status}, the tally {tally_status}")
    lines = int(open(os.path.join(work, f"{i}.lines")).read())
    if d["rows"] + d["rejected"] != lines:
        sys.exit(f"{f}: parsed {d['rows']:,} and rejected {d['rejected']:,} of {lines:,} lines")
    if d["raw"] != int(open(os.path.join(work, f"{i}.size")).read()):
        sys.exit(f"{f}: changed while it was being measured")
    cols.append(d)

def row(label, fmt):
    print(f"  {label:<34}" + "".join(f"{fmt(c):>22}" for c in cols))

print()
row("", lambda c: c["name"])
row("lines", lambda c: f"{c['rows']:,}")
row("raw bytes", lambda c: f"{c['raw']:,}")
row("bytes per line", lambda c: f"{c['raw'] / max(c['rows'], 1):,.0f}")
row("gzip -9", lambda c: f"{c['gz'] / c['raw']:.3f}x  ({c['gz']:,})")
row("zstd -19", lambda c: f"{c['zst'] / c['raw']:.3f}x  ({c['zst']:,})")
row("templates", lambda c: f"{c['templates']:,}")
row("rows/template median", lambda c: f"{c['median']:,}")
row("rows/template p90", lambda c: f"{c['p90']:,}")
row("rows/template max", lambda c: f"{c['max']:,}")
row("top 10 templates' share", lambda c: f"{100 * c['top10_share']:.1f}%")
row("templates with one row", lambda c: f"{c['single_row_templates']:,}")
row("distinct keys", lambda c: f"{c['keys']:,}")
print()
row("rows per (template, day) median", lambda c: f"{c['tday_median']:,}")
row("days per template median / max", lambda c: f"{c['days_median']} / {c['days_max']}")
row("lines earlier than the one before", lambda c: f"{100 * c['backwards']:.2f}%")
row("  ... beyond either's precision", lambda c: f"{100 * c['beyond']:.2f}%")
row("  ... by more than a second", lambda c: f"{100 * c['far_back']:.2f}%")
row("  ... within one template", lambda c: f"{100 * c['in_template_back']:.2f}%")
row("columns carrying two types", lambda c: f"{c['widened_columns']:,}")
row("templates with no time", lambda c: f"{c['undated_templates']:,} ({c['undated_rows']:,} rows)")
for c in cols:
    if c["widened_keys"]:
        print(f"    {c['name']}: two types in {', '.join(c['widened_keys'])}")
    if c["rejected"]:
        print(f"    {c['name']}: {c['rejected']} lines rejected by parse")

for c in cols:
    print(f"\n  {c['name']}: the {top} largest columns, by raw value bytes")
    print(f"    {'key':<30}{'bytes':>14}{'share':>8}{'rows':>11}{'distinct':>11}{'gz share':>10}  types")
    for t in c["top"]:
        print(f"    {t['key'][:29]:<30}{t['bytes']:>14,}{100 * t['share']:>7.1f}%{t['rows']:>11,}"
              f"{t['distinct']:>11,}{100 * t['gz_share']:>9.1f}%  {t['types']}")
PY
