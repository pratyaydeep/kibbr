# trieDB

A log store that fingerprints each line's shape and infers a type for every value.

**Status: M0** - the generator and the parser. There is no database yet. Two
subcommands: `gen-logs` makes reproducible, deliberately nasty NDJSON, and `parse`
prints what the type ladder made of it.

---

## Build it

```
cargo build --release
```

The binary lands at `target/release/triedb`. Everything below assumes it is on your
`PATH`:

```
export PATH="$PWD/target/release:$PATH"
```

If you would rather not touch `PATH`, use `cargo run --release --` in place of
`triedb` - note the `--`, which separates cargo's flags from the program's:

```
cargo run --release -- gen-logs --lines 5
```

To install it properly: `cargo install --path .`

Check it works:

```
$ triedb --help
TrieDB - log store

Usage: triedb <COMMAND>

Commands:
  gen-logs  Generate reproducible NDJSON logs on stdout
  parse     Print the fingerprint and inferred types of each line
  help      Print this message or the help of the given subcommand(s)
```

## Generate logs

`gen-logs` writes NDJSON to **stdout** - one JSON object per line.

```
$ triedb gen-logs --lines 2 --seed 7
{"err":{"trace_id":"26dd6ba0-018e-4163-8a97-1122d61f6197","code":7751,"cause":{"detail":"🔥 rate limit hit 🚨"}},"msg":"login","ts_unix":1706539241,"service":"auth","level":"fatal"}
{"event":"rate_limited","device":{"owner":{"email":"u18897@auth.example.com"},"serial":2743511012,"fw":218.51,"online":false},"ts":"2024-02-02T06:59:28Z"}
```

| flag | default | what it does |
|---|---|---|
| `--lines N` | 1000 | how many lines to write |
| `--templates T` | 12 | how many distinct log shapes to draw from |
| `--seed S` | 42 | same seed → byte-identical output |

The data is hostile on purpose, because hand-written fixtures do not surface the
type-inference bugs. Every run contains nested objects, optional fields that appear
on some lines of a template and not others, unicode (CJK, emoji, RTL, combining
accents), empty strings, explicit `null`s, arrays, and - the important one -
ten-digit integers that **are** Unix timestamps alongside ten-digit integers that
are **not**. Field order is shuffled per line, so the fingerprint has to earn its
stability.

Nothing reads the wall clock, so the seed fixes the output completely:

```
$ triedb gen-logs --lines 1000 --seed 42 | shasum -a 256
491d250b905b730c237a9a0642b55cf97610fe7945cc6fdb53a474175a936abe  -
$ triedb gen-logs --lines 1000 --seed 42 | shasum -a 256
491d250b905b730c237a9a0642b55cf97610fe7945cc6fdb53a474175a936abe  -
```

## Feed them to the parser

`parse` takes a file path, or `-` for stdin. Both forms work:

```
triedb gen-logs --lines 1000 --seed 42 > testdata/sample.ndjson
triedb parse testdata/sample.ndjson --limit 1
```

```
triedb gen-logs --lines 1000 --seed 42 | triedb parse - --limit 1
```

Either way:

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

Reading that:

- **fingerprint** - the flattened, sorted key set. Nested objects become dotted
  paths. Arrays are never flattened, since that would make the fingerprint depend on
  array length. A low-cardinality key can be promoted into the fingerprint itself as
  `event=login_failed`, which is what will make the trie branch usefully at M1.
- **the middle column** is the type the ladder chose, first match wins.
- **`(= …)`** shows up when the ladder turned the text into something else - a
  ten-digit integer read as a timestamp. That is the rule doing its job, out loud.
- `""` is an empty string that is *present*, as opposed to a key that is absent.

`--limit N` stops after N lines. Without it, 1000 lines is about 10,000 lines of
output, so pipe it somewhere.

---

## Things to try

**Parse a line you wrote yourself.** Fastest way to get a feel for the ladder:

```
$ echo '{"event":"login_failed","user":{"id":"u42","email":"u42@corp.example.com"},"order_id":1700000000,"tags":["a","b"],"note":"","deleted_at":null}' | triedb parse -
line 1
  fingerprint  deleted_at | event=login_failed | note | order_id | tags | user.email | user.id
  deleted_at   Null       null
  note         String     ""
  order_id     Datetime   1700000000  (= 2023-11-14T22:13:20Z)
  tags         String     ["a","b"]
  user.email   String     u42@corp.example.com
  user.id      String     u42
```

`order_id` is an order ID. It was called a timestamp. That is the most consequential
decision in the whole design - "exactly ten digits" sits *above* "parses as i64",
because a timestamp misread as an integer turns every time-range query into a full
scan, and this misclassification is the accepted price. Change the value to
`170000000` and watch it become `Int64`.

**See how often that fires** across a real run:

```
$ triedb gen-logs --lines 1000 --seed 42 | triedb parse - | grep -c "(= "
1293
```

**Count what types actually show up:**

```
$ triedb gen-logs --lines 1000 --seed 42 | triedb parse - | awk '/^  / && $1 != "fingerprint" {print $2}' | sort | uniq -c | sort -rn
3295 String
1907 Datetime
1185 Int64
 488 Float64
 403 Ip
 397 Uuid
 271 Bool
 245 Null
```

**Watch the template count explode.** Twelve shapes do not give you twelve
fingerprints: an optional field present on one line and absent on the next is a
different key set, and therefore a different template.

```
$ triedb gen-logs --lines 1000 --templates 12 --seed 42 | triedb parse - | tail -1
1000 lines, 356 templates
```

That number is the thing the schema-merging bucket bound will have to survive.

**Use a single template** to see every hostile case at once - template 0 is a fixed
kitchen sink, so `--templates 1` gives you nested objects, nulls, empty strings,
unicode, arrays, a real timestamp and a fake one on every line:

```
triedb gen-logs --lines 3 --templates 1 --seed 1 | triedb parse -
```

**Confirm reproducibility yourself:**

```
diff <(triedb gen-logs --lines 5000 --templates 40 --seed 99) \
     <(triedb gen-logs --lines 5000 --templates 40 --seed 99)
```

**Break it on purpose.** Nesting past depth 8 is refused, and so is anything that is
not a JSON object:

```
$ echo '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}' | triedb parse -
triedb: nesting deeper than 8 levels at key `a.b.c.d.e.f.g.h`

$ echo 'not json' | triedb parse -
triedb: line 1: invalid JSON
  caused by: expected ident at line 1 column 2

$ echo '[1,2,3]' | triedb parse -
triedb: top-level JSON value is an array, expected an object
```

**Get it as JSON** instead of the human table. `--json` emits one record per line
with `values` (raw text), `typed_values` (decoded - timestamps as unix nanoseconds)
and `types`. Key ordering is stable run to run, so it diffs cleanly:

```
triedb gen-logs --lines 5 --seed 3 | triedb parse - --json | jq .
```

**Read the fixture.** `testdata/fixture.ndjson` is 44 hand-written values, each
labelled with the type it must get. [`testdata/FIXTURE.md`](testdata/FIXTURE.md)
explains why each one is there - the ten-digit cases, `"NaN"`, `"TRUE"`,
`"9223372036854775808"`, `[1700000000]`, and the rest:

```
triedb parse testdata/fixture.ndjson
```

Add a line in the same shape and the test suite will check your case too.

## Serve it over HTTP

```
triedb serve
```

**Storage is memory and only memory.** Nothing is written to disk - there is no
write-ahead log and no snapshot of the trie - so everything ingested dies with the
process. The banner says so on startup, and `/stats` says so as a field:

```

triedb serve

  listening   http://127.0.0.1:7700

  STORAGE IS IN MEMORY. Everything ingested is lost when this process exits.
  Nothing is written to disk: there is no write-ahead log and no snapshot of
  the trie. A restart starts an empty database. Do not point anything at this
  that you would be unhappy to lose.

  endpoints   POST /ingest      NDJSON body, one log line per line
              POST /query       {"q": "<trieql>"}
              GET  /templates   every template, its columns and row count
              GET  /stats       throughput, template count, storage size
              GET  /health      {"ok":true}

  max rows    10000 - a MATCH with no LIMIT is answered a page at a time,
              truncated with a next_cursor to continue from

  shutdown    SIGINT (Ctrl-C) or SIGTERM. In-flight requests finish first.
```

The banner is on stderr, so redirecting stdout does not silence the part about
losing data. `--host` and `--port` move the listener; the default is loopback,
because `--host 0.0.0.0` publishes an unauthenticated write endpoint and that
should be something you typed.

**Feed it and ask it something:**

```
triedb gen-logs --lines 500 --seed 42 > logs.ndjson
curl -s -X POST --data-binary @logs.ndjson  http://127.0.0.1:7700/ingest
curl -s -X POST -d '{"q": "COUNT LOG {\"level\": \"error\"}"}' http://127.0.0.1:7700/query
curl -s http://127.0.0.1:7700/templates
curl -s http://127.0.0.1:7700/stats
```

`/query` takes TrieQL, and answers with the JSON `query --format json` prints -
one shape, built in one place, so the two surfaces cannot drift.

**A `MATCH` with no `LIMIT` gets a page, not everything.** Every matching row
would otherwise be materialised in the server before a byte of the response is
written, which is a denial-of-service path reachable by accident from a
terminal. So the answer is cut at `max_rows` - 10,000 by default, `--max-rows`
or `TRIEDB__QUERY__MAX_ROWS` to change it, `0` to turn it off - and says so:
`truncated: true`, `limit_source: "backstop"`, and a `next_cursor` to hand back
as `AFTER` for the rest. A `LIMIT` you asked for is honoured above it rather
than clamped to it, and `COUNT` and `GROUP BY` return no rows for it to bound.

**A bad line costs you that line and nothing else.** Corrupt three lines of the
five hundred and the other 497 still land:

```
{"ok":false,"ingested":497,"errors":[
  {"line":137,"msg":"invalid JSON at offset 29: EOF while parsing a value"},
  {"line":400,"msg":"top-level JSON value is an array, expected an object"},
  {"line":461,"msg":"invalid UTF-8: byte 0xe9 at offset 30"}]}
```

The status is 200, not a 4xx: the body was accepted and most of it was stored, and
telling a log shipper to retry the batch would duplicate the 497 that landed. Line
numbers count every physical line, blanks included, so the number indexes into the
body you sent.

**Ctrl-C finishes what it started.** SIGINT and SIGTERM both cancel one
`CancellationToken`; the listener stops accepting, requests already running finish,
and the shutdown walks a fixed order naming the steps this milestone does not have
yet:

```
triedb: SIGINT received - shutting down
triedb: stop accepting - 1 request(s) in flight
triedb: in-flight requests complete
triedb: final flush - skipped, storage is memory (M5 puts the stores on disk)
triedb: WAL checkpoint - skipped, there is no WAL yet (M6)
triedb: trie.json - skipped, nothing persists the trie yet (M5)
triedb: close stores - 328 store(s), 60,997 row(s) discarded, they were only in memory
triedb: close WAL - skipped, there is no WAL yet (M6)
triedb: exit 0
```

Naming the skipped steps is the point. Four of the six are the difference between
this and a database that survives being restarted.

## Run the tests

```
cargo test
```

```
running 9 tests
test fingerprint_is_order_independent ... ok
test low_cardinality_key_is_promoted_into_the_fingerprint ... ok
test every_fixture_value_gets_its_labelled_type ... ok
test flattening_matches_the_worked_example ... ok
test nesting_beyond_the_limit_is_rejected ... ok
test limit_stops_early ... ok
test generation_is_reproducible ... ok
test json_output_is_stable_across_runs ... ok
test generated_data_is_hostile_enough ... ok

test result: ok. 9 passed; 0 failed
```

Every test runs the real binary and reads its stdout. Nothing reaches inside the
crate and there are no mocks. The one that matters is
`every_fixture_value_gets_its_labelled_type` - that is the milestone's done-when.

Lints have to be clean:

```
cargo clippy --all-targets -- -D warnings
```

## Layout

```
src/main.rs       CLI, error reporting
src/gen.rs        the generator - templates, value kinds
src/rng.rs        xoshiro256**, written out so the seed is ours, not a dependency's
src/flatten.rs    flattening, fingerprints, the parsed row
src/infer.rs      the type ladder - one rung per rule, in order
src/parse.rs      the `parse` output
src/types.rs      ColumnType, ColValue
src/error.rs      thiserror enums
src/trie.rs       the trie - nodes, edges, leaf metadata
src/store.rs      in-memory leaf stores and the scan interface
src/db.rs         lines to a trie and its stores - the write path
src/engine.rs     the walk, the scan, the result
src/trieql.rs     the query grammar
src/query.rs      the `query` command, and the result JSON both surfaces answer with
src/serve.rs      the HTTP surface, the banner, the shutdown sequence
tests/fixture.rs  outer-surface tests
tests/serve.rs    the HTTP contract, over a bare TcpStream
```

One crate, no workspace. No traits - there is one implementation of everything so
far, and an interface with a single caller is a guess. That includes the ingest
path: a future `Ingestor` would sit between the API and the trie, and it is a plain
function until the WAL and the flush timer give it a second caller to answer to.
