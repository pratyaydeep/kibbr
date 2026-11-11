# kibbr-engine

No HTTP, no flags, no async runtime: whoever holds the `Database` does those.

The whole database behind one handle, `Database`: a line parsed and its template found, its
row logged and placed, the day files, loads and compactions, and TrieQL answered over them.

The bottom of the workspace: this crate names no other kibbr crate. `kibbr-server` and the
binary each hold one `Database`. The machinery is private to the crate; `internals` is a
doc-hidden module with no stability promise, holding what the command line's diagnostics
(`demo`, `bench-storage`) and the `cursor` fuzz harness read past the handle. `kibbr-server`
never names it (`scripts/check/no_internals.sh`).

## Entry points

| name | where | what it does |
|---|---|---|
| `Database::claim`, `Claim::open` | `database.rs` | serve a data directory: refuse another format version and take its `LOCK`, then restore it and replay its log |
| `Database::open_offline` | `database.rs` | open a data directory with its log left as it is: `kibbr load`, `kibbr compact` |
| `Database::in_memory` | `database.rs` | a database with no directory and no log |
| `Options` | `database.rs` | what a database is opened with: its `Storage` and its `Schema`; no directory is a database in memory |
| `Database::ingest` | `database.rs` | the live door: an NDJSON body parsed, routed, logged and applied |
| `Database::load` | `database.rs` | the bulk door: lines staged and committed in one manifest record |
| `Database::compact` | `database.rs` | a day's small row groups rebuilt into full ones |
| `Database::plan` | `database.rs` | a `Query` into a `Plan`: the leaves that can match. Reads no row |
| `Database::execute` | `database.rs` | a `Query` over the stores into a `QueryResult`; `execute_until` and `answer_until` can be told to stop |
| `Database::close` | `database.rs` | the fixed close order; `unlock` lets the `LOCK` go after it |
| `trieql::parse` | `query/trieql.rs` | a TrieQL string into a `Query`, or a `ParseError` that points at the text |
| `Query`, `QueryResult` | `query/mod.rs` | what is asked, and the one shape every surface answers with |
| `Limits` | `query/limits.rs` | what a query may not exceed: `max_rows`, `max_buckets`, `max_groups` |
| `parse_line` | `parse/flatten.rs` | one raw line into a `ParsedLog`: keys, values, types, fingerprint |
| `flatten` | `parse/flatten.rs` | a JSON object into dotted key paths and their values |
| `read_line` | `parse/lines.rs` | the next line of an input, under a byte cap |
| `infer` | `model/infer.rs` | one value's text into a `Typed`: the ladder |
| `ColumnType`, `ColValue` | `model/types.rs` | the types a column can hold, and a typed value |

`Database` also answers `templates`, `stats`, `degraded` (what `/health` reports),
`flush_due`, `compact_due` and `checkpoint`.

## What it is responsible for

- **One handle**: three ways to open - served, offline, in memory - and one way to close.
  Back-pressure comes out of it as values; whoever holds the handle decides what a wait or
  a refusal looks like.
- **Flattening and the fingerprint.** Nested objects become dotted paths; nesting
  past `MAX_DEPTH` (8) is refused; arrays are never flattened. The fingerprint is the
  sorted key set, with a low-cardinality key promoted into it as `key=value`.
- **The type ladder.** First match wins, and the order matters more than the rules:
  "exactly ten digits" sits above "parses as i64". The epoch window bounds which
  integers are instants.
- **One type a column.** A column's type is the join of its values' types, and never
  narrows; two types that do not join are `String`. The one pair that is not: epoch digits
  the ladder read as a `Datetime` and an integer are `Int64` (`widen`), each value shown
  as the digits it arrived as.
- **What a time column reads** beyond the ladder (`time_form_nanos`): epoch seconds with
  up to nine fraction digits, sixteen and nineteen digits, and a date and time with one of
  eleven zone names. Read only where a template's time source is chosen and a row's time
  is read; the column's type is the ladder's.
- **What goes in comes back out.** A typed value keeps the form it arrived in
  (`DtForm`), so storage can print the original back.
- **One line's limits.** `MAX_LINE_BYTES` is 64 MiB; a longer line is skipped to its end
  and reported, and the input carries on.
- **`\_received_at`**, the system column's name (`RECEIVED_AT_COLUMN`): it is not a key and
  never part of a fingerprint.
- **The trie** and its on-disk forms: `trie.json`, a readable snapshot, and the
  template catalog, written and flushed before the line that made a template counts.
- **A template's time source**, chosen once from the time columns - the first whose
  value on the template's first line reads as a time - and kept.
- **Format v3: one shared data file per UTC day** - frames and the schema record,
  row groups, chunks, forms, the footer and trailer, recovery of an unsealed file - and its
  encodings. The test vectors are `testdata/format/`.
- **Row order and the paging cursor**: a row's position among its template's rows
  of the day never moves, compaction included.
- **The manifest**: `manifest.log` names every live data file; a load or a
  compaction is published in one record, and a start deletes what it does not name.
- **Compaction**: a day's small row groups rebuilt into full ones at zstd 9.
- **The write-ahead log**: record format, recovery, group commit. A line acknowledged is
  stored exactly once, whatever kills the process.
- **The write path**; a store write failing is not an error the sender hears.
- **Framing**: the catalog, the manifest, the day files and the log share `format/frame.rs`,
  every frame under a CRC-32C.
- **One owner**: `LOCK` - one process owns a data directory.
- **The close order**: final flush, compactions joined, the checkpoint, `trie.json`, the
  stores, the log.
- **TrieQL**: `COUNT`, `MATCH`, `LOG {...}`, `SINCE`, `BETWEEN`, `GROUP BY`,
  `ORDER BY time [ASC|DESC]`, `LIMIT`, `AFTER`. The flags of `kibbr query` build the same `Query`;
  `scripts/check/parser_equivalence.sh` holds the two to each other.
- **Two phases, one of them free**: the trie walk costs no I/O; only the leaves it
  finds are scanned.
- **Unknown is not empty**: a template that does not exist is `exists: false`, not a
  count of 0.
- **The result shape**: `exists`, `count`, `rows` and the `times` each was ranked by,
  `groups`, and `meta` with what the query cost.
- **The backstop**: a `MATCH` with no `LIMIT` is cut at `max_rows` and says so, with a
  `next_cursor`; the `Cursor` resumes at a row's position in its template's day.
- **Buckets**: `GROUP BY time(w)` boundaries are multiples of `w` from the Unix epoch,
  empty buckets are zeros, and a range over `max_buckets` or `max_groups` is refused, not
  truncated (`check_buckets`).
- **Threads**: a scan fans out on the store's own small pool, never the global one.

## Files

```
src/model/        ColumnType, ColValue, the type ladder, a template's tokens, the row
src/parse/        reading lines under a byte cap, flattening and fingerprints, KeyPath
src/fmt.rs        how a value, a count and an instant are printed
src/rng.rs        xoshiro256**, written out so the seed is ours, not a dependency's
src/error/        Error and IngestError, what is wrong with one line, how an error chain is printed
src/lock.rs       the LOCK file: one process owns a data directory
src/format/       format v3: the framing, the builder, the file writer and reader, the columns
src/templates/    the trie and its key index, the catalog, trie.json
src/rows/         the leaf stores - memory, and the day files - the scan, the manifest, compaction
src/ingest.rs     one parsed line into the trie and its store - the write path
src/wal.rs        the write-ahead log: group commit, the one applier, replay, the checkpoint
src/load.rs       the bulk door: staging, the load id, the commit
src/compact.rs    which days a run takes, its report, the one-at-a-time job table
src/query/        TrieQL, the plan, the scan, buckets, the cursor, the limits, the result JSON
src/lifecycle.rs  open - restore and replay - the checkpoint, the close order
src/database.rs   Database and Options: the three ways to open, and everything after
src/fuzzing/      the wal_record, segment and record_log harnesses, with the fuzzing feature
```

Each module's unit tests are in a `tests.rs` beside it, as `src/wal/tests.rs`;
`format/encodings.rs` and `format/vectors.rs` are modules of tests of their own.

## Tests

```
cargo test -p kibbr-engine --all-features
```

340 unit tests, 9 of them the fuzz harnesses', built only with the `fuzzing` feature. The
`test-support` feature holds the fixtures the tests of the crates above build with. The
ladder's labelled cases are `testdata/fixture.ndjson`, run through the binary by
`tests/fixture.rs` at the repository root. The engine is also held to a brute-force oracle
(`kibbr query --reference`) by `scripts/check/agree.sh`, and its parser is fuzzed by
`fuzz/fuzz_targets/trieql.rs`. The crash and fault cases that need a real process are the
scripts under `scripts/fault/`.
