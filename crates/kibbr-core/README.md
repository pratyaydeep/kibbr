# kibbr-core

What a log line is before anything stores it: its keys flattened, each value typed by the
ladder, and the fingerprint its template is found by - with the errors a line is refused
with, and the few things every crate above prints and draws with.

The bottom of the workspace: this crate names no other kibbr crate.

## Entry points

| name | where | what it does |
|---|---|---|
| `parse_line` | `flatten.rs` | one raw line into a `ParsedLog`: keys, values, types, fingerprint |
| `flatten` | `flatten.rs` | a JSON object into dotted key paths and their values |
| `infer` | `infer.rs` | one value's text into a `Typed`: the ladder |
| `ColumnType`, `ColValue` | `types.rs` | the types a column can hold, and a typed value |
| `read_line` | `lines.rs` | the next line of an input, under a byte cap |

## What it is responsible for

- **Flattening and the fingerprint.** Nested objects become dotted paths; nesting
  past `MAX_DEPTH` (8) is refused; arrays are never flattened. The fingerprint is the
  sorted key set, with a low-cardinality key promoted into it as `key=value`.
- **The type ladder.** First match wins, and the order matters more than the rules:
  "exactly ten digits" sits above "parses as i64". The epoch window bounds which
  integers are instants.
- **What goes in comes back out.** A typed value keeps the form it arrived in
  (`DtForm`), so storage can print the original back.
- **One line's limits.** `MAX_LINE_BYTES` is 64 MiB; a longer line is skipped to its end
  and reported, and the input carries on.
- **`\_received_at`**, the system column's name (`RECEIVED_AT_COLUMN`): it is not a key and
  never part of a fingerprint.

## Files

```
src/flatten.rs    flattening, fingerprints, the parsed row
src/infer.rs      the type ladder - one rung per rule, in order
src/types.rs      ColumnType, ColValue
src/key.rs        KeyPath: a dotted key, split and joined
src/lines.rs      reading lines under a byte cap
src/fmt.rs        how a value, a count and an instant are printed
src/rng.rs        xoshiro256**, written out so the seed is ours, not a dependency's
src/error.rs      what is wrong with one line, and how an error chain is printed
```

## Tests

```
cargo test -p kibbr-core
```

17 unit tests. The ladder's labelled cases are `testdata/fixture.ndjson`, run through the
binary by `tests/fixture.rs` at the repository root.
