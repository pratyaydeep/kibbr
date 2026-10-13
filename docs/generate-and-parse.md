# Generate logs, and parse them

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
| `--day`, `--services`, `--dirty-rate` | 2024-01-15, 10, 0.5 | `farm` only |

- `hostile` is hostile on purpose: hand-written fixtures do not surface type-inference bugs.
- Every run holds nested objects, optional fields, unicode (CJK, emoji, RTL, combining
  accents), empty strings, explicit `null`s, arrays, and ten-digit integers that are Unix
  timestamps beside ten-digit integers that are not.
- Field order is shuffled per line, so the fingerprint has to earn its stability.

## Parse

```
kibbr gen-logs --lines 1000 --seed 42 > testdata/sample.ndjson
kibbr parse testdata/sample.ndjson --limit 1             # a file
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
- Arrays are never flattened: the fingerprint would depend on array length.
- A low-cardinality key can be promoted into the fingerprint as `event=login_failed`, which
  is what makes the trie branch usefully.
- **The middle column** is the type the ladder chose; the first rule that matches wins.
- **`(= ...)`** appears when the ladder read the text as something else, such as a ten-digit
  integer read as a timestamp.
- `""` is an empty string that is present, as opposed to a key that is absent.
- `--limit N` stops after N lines; without it 1,000 lines print about 10,000.
