# Generate logs with kibbr, and parse them

`gen-logs` writes NDJSON to stdout; `parse` prints the fingerprint and the inferred type of
every value. Both assume `kibbr` is on your `PATH`; `cargo run --release -- gen-logs --lines 5`
is the same without it (the `--` separates cargo's flags from kibbr's).

## Generate

```
kibbr gen-logs --lines 2 --seed 7                      # two lines of the hostile profile
kibbr gen-logs --lines 1000 --seed 42 | shasum -a 256  # 491d250b...936abe, every time
kibbr gen-logs --profile farm --lines 10000 --seed 42  # one day of a ten-service estate
```

| flag | default | what it does |
|---|---|---|
| `--lines N` | 1000 | how many lines to write |
| `--templates T` | 12 | `hostile` only: how many distinct log shapes to draw from |
| `--seed S` | 42 | same seed, byte-identical output: nothing reads the wall clock |
| `--profile P` | `hostile` | `hostile`, or `farm`: one day in time order, shaped like real logs |
| `--day`, `--services`, `--dirty-rate` | 2024-01-15, 10, 0.5 | `farm` only: services 1 to 10 (below 6, no access log); dirty rate in percent |

- `hostile` is hostile on purpose: hand-written fixtures do not surface type-inference bugs.
- Every run holds nested objects, optional fields, unicode (CJK, emoji, RTL, combining
  accents), empty strings, explicit `null`s, arrays, and ten-digit integers that are Unix
  timestamps beside ten-digit integers that are not.
- Field order is shuffled per line, so the fingerprint has to earn its stability.

## Parse

```
kibbr gen-logs --lines 1000 --seed 42 > sample.ndjson
kibbr parse sample.ndjson --limit 1             # a file
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - --limit 1   # or stdin
```

```
line 1
  fingerprint       err.cause.code | err.cause.detail | err.code | err.trace_id | level | msg | service | ts_unix
  err.cause.code    Int64      9621
  err.cause.detail  String     échec d'authentification
  err.code          Int64      3495
  err.trace_id      Uuid       aeb53b34-0c10-4971-8a69-db9873af8965
  level             String     error
  msg               String     retry
  service           String     auth
  ts_unix           Datetime   1707680634  (= 2024-02-11T19:43:54Z)

1 lines, 1 templates
```

- **fingerprint**: the flattened, sorted key set; nested objects become dotted paths.
- Arrays are never flattened: the fingerprint would depend on array length. An array is
  one value at its key, held as its JSON text in a column of text and marked as an array,
  so `["en"]` and the string `"[\"en\"]"` are two values. `--json` shows the text under
  `values` and the array under `typed_values`.
- An object with no members is a value too: `{"a":{"b":{}}}` has the key `a.b`, whose
  value is `{}`. So a line with an empty object is another template than the same line
  without that key.
- `{}`, `[]`, `null`, `""`, `"{}"`, `"[]"` and a missing key are seven different things,
  and a query gives each back as what it was.
- A number inside an array is read as a line's own numbers are: `[1.50]` is `[1.5]`.
- An array or an empty object under a promoted key is not promoted.
- A low-cardinality key can be promoted into the fingerprint as `event=login_failed`, which
  is what makes the trie branch usefully.
- **The middle column** is the type the ladder chose; the first rule that matches wins.
- **`(= ...)`** appears when the ladder read the text as something else, such as a ten-digit
  integer read as a timestamp.
- `""` is an empty string that is present, as opposed to a key that is absent.
- `--limit N` stops after N lines; without it 1,000 lines print about 10,000.

## Things to try

The type ladder, by hand. Each command is followed by what this build prints.

```
echo '{"event":"login_failed","user":{"id":"u42","email":"u42@corp.example.com"},"order_id":1700000000,"tags":["a","b"],"note":"","deleted_at":null}' | kibbr parse -
```

Parses a line you wrote: `order_id` comes out `Datetime 1700000000 (= 2023-11-14T22:13:20Z)`.

- "Ten or thirteen digits, 2000 to 2100" sits above "parses as i64": a timestamp misread as an integer turns
  every time-range query into a full scan, and an order ID called a timestamp is the price.
- Change the value to `170000000` and it is `Int64`. A column that holds both is `Int64`
  too: epoch digits beside an integer are integers, not text.

```
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | grep -c "(= "
```

How often the ladder re-read a value across a run: `905`.

```
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | awk '/^  / && $1 != "fingerprint" {print $2}' | sort | uniq -c | sort -rn
```

The types that show up: 3295 String, 1573 Int64, 1519 Datetime, 488 Float64, 403 Ip, 397 Uuid, 271 Bool, 245 Null.

```
kibbr gen-logs --lines 1000 --templates 12 --seed 42 | kibbr parse - | tail -1
```

The template count: `1000 lines, 356 templates`. Twelve shapes are not twelve fingerprints -
an optional field present on one line and absent on the next is a different key set. That
number is what the schema-merging bucket bound has to survive.

```
kibbr gen-logs --lines 3 --templates 1 --seed 1 | kibbr parse -
```

Template 0 is a fixed kitchen sink: nested objects, nulls, empty strings, unicode, arrays, a real timestamp and a fake one on every line.

```
diff <(kibbr gen-logs --lines 5000 --templates 40 --seed 99) \
     <(kibbr gen-logs --lines 5000 --templates 40 --seed 99)
```

Reproducibility: prints nothing and exits 0.

```
echo '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}' | kibbr parse -
echo 'not json' | kibbr parse -
echo '[1,2,3]' | kibbr parse -
```

Breaks it on purpose; each line is refused by number and the run goes on:

```
kibbr: line 1: nesting deeper than 8 levels at key `a.b.c.d.e.f.g.h`
kibbr: line 1: invalid JSON at offset 1: expected ident
kibbr: line 1: top-level JSON value is an array, expected an object
```

each followed by `0 lines, 0 templates, 1 lines rejected` and `kibbr: 1 of 1 lines rejected`.

```
kibbr gen-logs --lines 5 --seed 3 | kibbr parse - --json | jq .
```

One JSON record a line instead of the table: `template_id`, `fingerprint`, `keys`, `values`
(raw text), `typed_values` (decoded - timestamps as Unix nanoseconds), `types`,
`is_new_template`, `raw_line`. Key order is stable run to run, so it diffs cleanly.

```
kibbr parse tests/data/type_ladder.ndjson
```

The fixture: hand-written values, each labelled with the type it must get - the ten-digit
cases, `"NaN"`, `"TRUE"`, `"9223372036854775808"`, `[1700000000]` and the rest.
[`tests/data/README.md`](../tests/data/README.md) says why each is there. Add a line in the
same shape and the test suite checks your case too.
