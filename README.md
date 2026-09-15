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

  endpoints   GET  /            the log viewer, in a browser
              POST /ingest      NDJSON body, one log line per line
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

**`/ingest` takes gzip.** Log text compresses about tenfold and a shipper will
squash a batch if the server can take one, so `Content-Encoding: gzip` (or
`x-gzip`, or `identity`, or nothing) is accepted and anything else is a `415`
that names what is:

```
gzip -c logs.ndjson | curl -s -X POST --data-binary @- \
  -H 'Content-Encoding: gzip' http://127.0.0.1:7700/ingest
```

A body that says gzip and is not is a `400` naming gzip, not a crash - `/ingest`
is reachable by anything that can open a socket, and a decoder that unwinds on
malformed input turns one bad shipper config into an outage for everybody else.

**Two size limits, because one cannot see a bomb.** The first is on what
arrives: 10 MiB, `--max-body-bytes` or `TRIEDB__API__MAX_BODY_BYTES`. That is
the only place a compressed body *can* be measured, and it is not enough on its
own - a carefully built 1 MiB body inflates to gigabytes, passes the first check
by three orders of magnitude, and kills the process while being decompressed. So
the size is checked a second time as it inflates, at `--max-decompressed-bytes`
(64 MiB, the line limit, which it may not exceed), and the reader stops *at* the
cap rather than after it. Either one is a `413`.

**Past capacity, `/ingest` says 503 - it does not go quiet.** `--ingest-concurrency`
bodies are parsed at once, one per core by default. A body that waits longer than
100ms for a slot is answered `503` with a `Retry-After: 1` rather than left to
run long, and `/stats` counts it as `ingest.requests_shed`.

The status is the point. A shipper that gets no answer keeps the batch, times
out, and opens *another* connection - so a server that is merely slow gathers
connections until it is unreachable, and every shipper in the fleet is doing it
at once. An immediate refusal is the kinder answer precisely because a shipper
knows what to do with one: Vector's adaptive concurrency reads a 503 as "send
fewer" and a timeout as "try harder". That is also why there is no server-side
request timeout here - it would manufacture the failure it is meant to prevent.

**`/ingest` is single tenant, deliberately.** It takes no stream or tenant
identifier, in the path or in a header, and everything ingested lands in the one
trie this process holds. Said out loud because the omission gets expensive with
time: once a fleet of shippers is configured against this URL, *adding* an
identifier is a breaking change to every one of their configs. It is also not a
header away - the trie path is built from each line's own keys, so separating
tenants means either splicing a token into the fingerprint (which changes every
template id) or one trie per tenant, with a tenant selector on `/query`,
`/templates`, `/stats` and the viewer. Revisit at M9, with the deployment work.

**A `MATCH` with no `LIMIT` gets a page, not everything.** Every matching row
would otherwise be materialised in the server before a byte of the response is
written, which is a denial-of-service path reachable by accident from a
terminal. So the answer is cut at `max_rows` - 10,000 by default, `--max-rows`
or `TRIEDB__QUERY__MAX_ROWS` to change it, `0` to turn it off - and says so:
`truncated: true`, `limit_source: "backstop"`, and a `next_cursor` to hand back
as `AFTER` for the rest. A `LIMIT` you asked for is honoured above it rather
than clamped to it, and `COUNT` and `GROUP BY` return no rows for it to bound.

**`GROUP BY time(5m)` emits empty buckets as zeros.** Six hours at five minutes
is 72 observations, and a bucket nothing landed in is one of them - the thing was
quiet, which is not the same statement as a bucket nobody asked about. So the
result is a list rather than a map of the buckets that happened to have rows:

```
$ triedb query logs.ndjson \
    'COUNT LOG {"level":"ERROR"} BETWEEN 2024-01-15T09:00:00Z AND 2024-01-15T12:00:00Z GROUP BY time(5m)'
bucket                count
2024-01-15T09:55:00Z  3
2024-01-15T10:00:00Z  0
2024-01-15T10:05:00Z  0
…
36 groups, 64 rows   (templates matched: 1, rows scanned: 132, 175µs)
```

Boundaries are multiples of the width counted **from the Unix epoch**, never from
where the query starts, so two overlapping ranges put their bars in the same
places - a chart that shifts when you nudge the range is a bug nobody reports
clearly. A second key is zero-filled too: three levels means three series, each
with a point in all 72 buckets.

A consequence worth knowing before you count bars: the range emits every bucket it
*touches*, so an edge that does not land on the grid straddles one. `SINCE 6h` at
`time(5m)` gives 72 buckets if `now` happens to be on a five-minute boundary and 73
otherwise - the first and last are partial, and both are real observations. Six
hours from 10:03:17 covers 10:00 through 16:00, which is 73 five-minute slots. The
comment above `bucket_of` in `src/engine.rs` draws it.

A bucketed query returns aggregates rather than rows, so `max_rows` does not reach
it. `max_buckets` (10,000) and `max_groups` (100,000) do, and going over is a
refusal naming both counts rather than a chart with its tail quietly missing:

```
$ triedb explain 'COUNT LOG {} SINCE 30d GROUP BY time(1s)'
triedb: `GROUP BY time(1s)` over this range needs 2,592,001 buckets; the limit is
10,000 - widen the bucket or narrow the range
```

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

## Look at it in a browser

`GET /` is a log viewer. Open <http://127.0.0.1:7700/> after starting the server.

It is one file - `src/viewer.html`, inline CSS and vanilla JS, compiled into the
binary with `include_str!`. There is nothing to deploy beside the binary, no build
step, and no request to anywhere but this server; `tests/serve.rs` asserts the last
of those rather than trusting it. It is served `no-store`, because a viewer cached
against a newer server is a debugging tool lying about what the database answered.

It exists to be the first real client of TrieQL and of the cursor, and it is
deliberately small: no saved queries, no dashboards, no auto-refresh, no auth, no
persistence of any kind.

- **Rows are the JSON lines they were ingested as**, not a table. `MATCH LOG {}`
  spans 870 templates whose columns barely overlap, and the table that used to
  union them was 56 columns of mostly "this template has no such column".
- **The keys the query named are highlighted**, and their values too when the
  query asked for a value and the row carries it - so a highlight always means
  "this is why the row is here".
- **Only the lines on screen are in the DOM.** A page is up to `max_rows`; spacers
  stand in for the rest and scrolling re-cuts the window, so a 10,000-row answer
  paints in about 30ms instead of blocking the tab for ten seconds. The cost is
  that the browser's own Ctrl+F searches the rendered lines only - the database is
  the thing to ask instead.
- **The chart is the same query, counted.** `GROUP BY time(...)` at a width chosen
  to land near 60 bars; clicking a bar narrows the range to that bucket and re-runs.
  Empty buckets are bars of zero, because the engine zero-fills them.
- **The viewer never injects a `LIMIT`.** A query naming none meets `max_rows` and
  the banner says so, which is the point: this is the client most likely to trip
  the backstop, and hiding it here would hide it everywhere.
- **The range control is authoritative.** It opens on the newest ten minutes of
  whatever is stored, found by asking the engine rather than guessing. Clearing
  both boxes runs the query with no time clause at all.

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
src/viewer.html   the log viewer served at `/` - one file, compiled into the binary
tests/fixture.rs  outer-surface tests
tests/serve.rs    the HTTP contract, over a bare TcpStream
```

One crate, no workspace. No traits - there is one implementation of everything so
far, and an interface with a single caller is a guess. That includes the ingest
path: a future `Ingestor` would sit between the API and the trie, and it is a plain
function until the WAL and the flush timer give it a second caller to answer to.
