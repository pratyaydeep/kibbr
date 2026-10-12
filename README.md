# kibbr

TrieDB is now kibbr.

A log store that fingerprints each line's shape and infers a type for every value.

**Status:** a work in progress. It began as two subcommands - `gen-logs` makes
reproducible, deliberately nasty NDJSON, and `parse` prints what the type ladder made
of it - and the later sections add the HTTP server and the viewer.

---

## Build it

```
cargo build --release
```

The binary uses [mimalloc](https://github.com/microsoft/mimalloc) as its allocator, because
on macOS arm64 the system one aborted the server with "pointer being freed was not
allocated" at unrelated free sites that neither the code (no `unsafe`) nor AddressSanitizer
could account for.

The binary lands at `target/release/kibbr`. Everything below assumes it is on your
`PATH`:

```
export PATH="$PWD/target/release:$PATH"
```

If you would rather not touch `PATH`, use `cargo run --release --` in place of
`kibbr` - note the `--`, which separates cargo's flags from the program's:

```
cargo run --release -- gen-logs --lines 5
```

To install it properly: `cargo install --path .`

Check it works:

```
$ kibbr --help
kibbr - log store

Usage: kibbr <COMMAND>

Commands:
  gen-logs  Generate reproducible NDJSON logs on stdout
  parse     Print the fingerprint and inferred types of each line
  help      Print this message or the help of the given subcommand(s)
```

## Generate logs

`gen-logs` writes NDJSON to **stdout** - one JSON object per line.

```
$ kibbr gen-logs --lines 2 --seed 7
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
$ kibbr gen-logs --lines 1000 --seed 42 | shasum -a 256
491d250b905b730c237a9a0642b55cf97610fe7945cc6fdb53a474175a936abe  -
$ kibbr gen-logs --lines 1000 --seed 42 | shasum -a 256
491d250b905b730c237a9a0642b55cf97610fe7945cc6fdb53a474175a936abe  -
```

## Feed them to the parser

`parse` takes a file path, or `-` for stdin. Both forms work:

```
kibbr gen-logs --lines 1000 --seed 42 > testdata/sample.ndjson
kibbr parse testdata/sample.ndjson --limit 1
```

```
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - --limit 1
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
  `event=login_failed`, which is what makes the trie branch usefully.
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
$ echo '{"event":"login_failed","user":{"id":"u42","email":"u42@corp.example.com"},"order_id":1700000000,"tags":["a","b"],"note":"","deleted_at":null}' | kibbr parse -
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
$ kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | grep -c "(= "
1293
```

**Count what types actually show up:**

```
$ kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | awk '/^  / && $1 != "fingerprint" {print $2}' | sort | uniq -c | sort -rn
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
$ kibbr gen-logs --lines 1000 --templates 12 --seed 42 | kibbr parse - | tail -1
1000 lines, 356 templates
```

That number is the thing the schema-merging bucket bound will have to survive.

**Use a single template** to see every hostile case at once - template 0 is a fixed
kitchen sink, so `--templates 1` gives you nested objects, nulls, empty strings,
unicode, arrays, a real timestamp and a fake one on every line:

```
kibbr gen-logs --lines 3 --templates 1 --seed 1 | kibbr parse -
```

**Confirm reproducibility yourself:**

```
diff <(kibbr gen-logs --lines 5000 --templates 40 --seed 99) \
     <(kibbr gen-logs --lines 5000 --templates 40 --seed 99)
```

**Break it on purpose.** Nesting past depth 8 is refused, and so is anything that is
not a JSON object:

```
$ echo '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}' | kibbr parse -
kibbr: nesting deeper than 8 levels at key `a.b.c.d.e.f.g.h`

$ echo 'not json' | kibbr parse -
kibbr: line 1: invalid JSON
  caused by: expected ident at line 1 column 2

$ echo '[1,2,3]' | kibbr parse -
kibbr: top-level JSON value is an array, expected an object
```

**Get it as JSON** instead of the human table. `--json` emits one record per line
with `values` (raw text), `typed_values` (decoded - timestamps as unix nanoseconds)
and `types`. Key ordering is stable run to run, so it diffs cleanly:

```
kibbr gen-logs --lines 5 --seed 3 | kibbr parse - --json | jq .
```

**Read the fixture.** `testdata/fixture.ndjson` is 44 hand-written values, each
labelled with the type it must get. [`testdata/FIXTURE.md`](testdata/FIXTURE.md)
explains why each one is there - the ten-digit cases, `"NaN"`, `"TRUE"`,
`"9223372036854775808"`, `[1700000000]`, and the rest:

```
kibbr parse testdata/fixture.ndjson
```

Add a line in the same shape and the test suite will check your case too.

## Serve it over HTTP

```
kibbr serve
```

**Storage is memory and only memory.** Nothing is written to disk - there is no
write-ahead log and no snapshot of the trie - so everything ingested dies with the
process. The banner says so on startup, and `/stats` says so as a field:

```

kibbr serve

  listening   http://127.0.0.1:7700

  STORAGE IS IN MEMORY. Everything ingested is lost when this process exits.
  Nothing is written to disk: there is no write-ahead log and no snapshot of
  the trie. A restart starts an empty database. Do not point anything at this
  that you would be unhappy to lose.

  endpoints   GET  /            the log viewer, in a browser
              POST /ingest      NDJSON body, one log line per line
              POST /query       {"q": "<trieql>"}
              POST /load        a bulk load job: what `kibbr load` sends
              POST /compact     a compaction job: what `kibbr compact` sends
              GET  /templates   every template, its columns and row count
              GET  /stats       throughput, template count, storage size
              GET  /health      {"ok":true}

  max rows    10000 - a MATCH with no LIMIT is answered a page at a time,
              truncated with a next_cursor to continue from

  requests    answered within 30s or timed out: 408 for a body that has not
              arrived, 504 for a query, whose scan is stopped. Not an
              /ingest body read whole (--request-timeout).

  shutdown    SIGINT (Ctrl-C) or SIGTERM. In-flight requests finish first; one
              still in flight 25s later is answered 503 and let go.
```

The banner is on stderr, so redirecting stdout does not silence the part about
losing data. `--host` and `--port` move the listener; the default is loopback,
because `--host 0.0.0.0` publishes an unauthenticated write endpoint and that
should be something you typed.

**Feed it and ask it something:**

```
kibbr gen-logs --lines 500 --seed 42 > logs.ndjson
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
arrives: 10 MiB, `--max-body-bytes` or `KIBBR__API__MAX_BODY_BYTES`. That is
the only place a compressed body *can* be measured, and it is not enough on its
own - a carefully built 1 MiB body inflates to gigabytes, passes the first check
by three orders of magnitude, and kills the process while being decompressed. So
the size is checked a second time as it inflates, at `--max-decompressed-bytes`
(64 MiB, the line limit, which it may not exceed), and the reader stops *at* the
cap rather than after it. Either one is a `413`.

**Past capacity, `/ingest` waits a moment and then says 503 - it does not go
quiet.** `--ingest-concurrency` bodies are parsed at once, one per core by
default. A body that finds no slot waits for one at the door, with its body
unread, and one that has waited `--ingest-wait-ms` (100 by default,
`KIBBR__API__INGEST_WAIT_MS`, 30000 at most, `0` for no wait at all) is answered
`503` with a `Retry-After: 1` rather than left to run long. A body that arrives
while the rows waiting in memory hold more than twice `--max-buffer-bytes`
(512 MiB by default) waits there the same way, for the flush that is already on
its way: past the limit the largest buffers are written early, and past twice it
the flush is behind what is arriving, so a body taken then would be memory the
process may not have. A store that cannot write at all empties no buffer, and
that body is refused at once, with the store's error.

With `--storage disk` the `200` waits for the write-ahead log's flush and for the
store to take the rows, so the log is the third place a body can be held. At most
256 bodies are between the door and their answer at once (twice
`--ingest-concurrency`, where that is more), and at most `--wal-queue-bytes` of
lines (256 MiB by default, `KIBBR__STORAGE__WAL_QUEUE_BYTES`) are in the log and
not yet in the store. A body that has been parsed and finds the log full waits
for room, for up to the same `--ingest-wait-ms` again, and keeps its slot while
it does - so once every slot is held that way the bodies behind wait at the door,
unread, and a slow disk slows its senders instead of filling memory. A body that
gets no room in that time is shed with the same `503`; one whose sender hangs up
while it waits is let go, with nothing of it stored.

`/stats` says which of these is happening. `ingest.requests_shed` counts every
body told to come back, and `ingest.shed` counts them by what there was none of:
`permit`, `place`, `line`, `buffers`, `log`, `stopping`. `ingest.door` is the
bodies waiting at the door now and how long they have waited in all, and
`storage.wal` has `queue_bytes`, `unapplied_bytes` and `queue_waiting` - what is
waiting for the log's writer, what it has written that the store does not have
yet, and the bodies waiting for room. With `--ingest-wait-ms` above 100 no more
bodies wait at the door than may be inside (`ingest.door.line_max`), and one more
is shed at once (`line`). A server that is stopping keeps nobody waiting. A body
waiting at the door is refused at once, unread (`stopping`). A body that has been
parsed and has no room in the log is refused at once too, whether it was already
waiting for room or finds none when its parse ends (`log`). A body the log has
taken is finished and answered `200`.

And while the log cannot write at all - a full volume - every body is
refused `503` with the write error, before it is read, until a write succeeds.
A store that cannot write refuses nothing: the line is in the log, and the answer
is `200`. A body of which no line was taken, because the catalog could not write
the record each one needed - a template never seen before, on that full volume -
is a `503` with a `Retry-After` as well, answered after it was read and with its
errors by line still in it.
A body is written to the log whole, so one whose lines come to more than
1 GiB as log records - tens of millions of very short lines - is a `413`, decided
before it is parsed: send it as smaller bodies.

The status is the point. A shipper that gets no answer keeps the batch, times
out, and opens *another* connection - so a server that is merely slow gathers
connections until it is unreachable, and every shipper in the fleet is doing it
at once. An immediate refusal is the kinder answer precisely because a shipper
knows what to do with one: Vector's adaptive concurrency reads a 503 as "send
fewer" and a timeout as "try harder". That is also why there is no server-side
request timeout here - it would manufacture the failure it is meant to prevent.

A refusal that comes before the body is read leaves the connection unable to
carry another request, so the answer says `Connection: close` and the server
closes it: a shipper that keeps its connection sends the retry on a new one. An
answer without the header leaves the connection open.

**`/ingest` is single tenant, deliberately.** It takes no stream or tenant
identifier, in the path or in a header, and everything ingested lands in the one
trie this process holds. Said out loud because the omission gets expensive with
time: once a fleet of shippers is configured against this URL, *adding* an
identifier is a breaking change to every one of their configs. It is also not a
header away - the trie path is built from each line's own keys, so separating
tenants means either splicing a token into the fingerprint (which changes every
template id) or one trie per tenant, with a tenant selector on `/query`,
`/templates`, `/stats` and the viewer. Revisit with the deployment work.

**A `MATCH` with no `LIMIT` gets a page, not everything.** Every matching row
would otherwise be materialised in the server before a byte of the response is
written, which is a denial-of-service path reachable by accident from a
terminal. So the answer is cut at `max_rows` - 10,000 by default, `--max-rows`
or `KIBBR__QUERY__MAX_ROWS` to change it, `0` to turn it off - and says so:
`truncated: true`, `limit_source: "backstop"`, and a `next_cursor` to hand back
as `AFTER` for the rest. A `LIMIT` you asked for is honoured above it rather
than clamped to it, and `COUNT` and `GROUP BY` return no rows for it to bound.

**A query may choose a day's rows on several threads.** A day of one template is
stored as row groups, and choosing which rows of a row group match needs nothing
but that row group, so a run of them can be shared out: the thread answering the
query reads every row group and hands every row over, in order, and between the
two it and a few others each take the next row group nobody has taken. The answer
and every counter in `meta` are the same either way. `--scan-threads`
(`KIBBR__QUERY__SCAN_THREADS`) is how many threads one query may use, its own
among them: half the cores and at most 4 by default, `1` for never, and more than
the machine has cores is refused at startup. A run holding fewer than
`--scan-fan-rows` rows (`KIBBR__QUERY__SCAN_FAN_ROWS`, 32,768) is not worth
waking a thread for and is never shared, which is most queries; nor is any run
while an `/ingest` body is between the door and its answer - its parse and its
wait for the log's flush both - so a server taking steady ingest, however light,
answers each query on one thread, and `queries.scan_fan_outs` stays where it was.
It buys time and costs CPU - on eight cores, a count over 433,000 rows of a day
in about half the time for 14 to 34% more CPU in replays of the bench's scan
(`kibbr bench` has not yet taken that row). `/stats` counts the runs shared out
as `queries.scan_fan_outs`, beside the two settings as this server resolved
them, `queries.scan_threads` and `queries.scan_fan_rows`, and the startup banner
says them too.

**`GROUP BY time(5m)` emits empty buckets as zeros.** Six hours at five minutes
is 72 observations, and a bucket nothing landed in is one of them - the thing was
quiet, which is not the same statement as a bucket nobody asked about. So the
result is a list rather than a map of the buckets that happened to have rows:

```
$ kibbr query logs.ndjson \
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
comment above `bucket_of` in `crates/kibbr-engine/src/engine.rs` draws it.

A bucketed query returns aggregates rather than rows, so `max_rows` does not reach
it. `max_buckets` (10,000) and `max_groups` (100,000) do, and going over is a
refusal naming both counts rather than a chart with its tail quietly missing:

```
$ kibbr explain 'COUNT LOG {} SINCE 30d GROUP BY time(1s)'
kibbr: `GROUP BY time(1s)` over this range needs 2,592,001 buckets; the limit is
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
telling a log shipper to retry the batch would duplicate the 497 that landed. The
one report that is not a 200 has `ingested: 0` and an `errors` of nothing but writes
the server could not make (`not ingested: catalog: ...`), at least one: that body is
a `503` with a `Retry-After`, because none of it landed and sending it again
duplicates nothing. Line numbers count every physical line, blanks included, so the number
indexes into the body you sent.

**Ctrl-C finishes what it started.** SIGINT and SIGTERM both cancel one
`CancellationToken`; the listener stops accepting, requests already running finish -
or are answered `503` once the shutdown deadline passes, 25 seconds by default -
and the shutdown walks a fixed order naming the steps a memory server skips.
With `--storage memory`:

```
kibbr: SIGINT received - shutting down
kibbr: stop accepting - 1 request(s) in flight
kibbr: in-flight requests complete
kibbr: final flush - skipped, storage is memory (--data-dir puts the stores on disk)
kibbr: WAL checkpoint - skipped, storage is memory
kibbr: trie.json - skipped, storage is memory
kibbr: close stores - 328 store(s), 60,997 row(s) discarded, they were only in memory
kibbr: close WAL - skipped, storage is memory
kibbr: exit 0
```

Naming the skipped steps is the point. Four of the six are the difference between
this and a database that survives being restarted - which is what the default,
`--storage disk`, is: a line is in the write-ahead log under `<data-dir>/wal/`, flushed,
before `/ingest` answers `200`, and a start after `kill -9` puts back every line no data
file holds yet, once. `scripts/fault/crash_test.sh` kills a server twenty times mid-ingest and
holds what is stored to what was acknowledged, line by line.

## Load closed days

`/ingest` is the live door: lines as they happen. `kibbr load` is the bulk door: a
closed day of lines - or six months of them - loaded whole.

```
kibbr load --data-dir kibbr-data logs/2024-01-15/          # every file under it
kibbr load --data-dir kibbr-data yesterday.ndjson.gz       # gzip, zstd or NDJSON
zcat archive.gz | kibbr load --data-dir kibbr-data -       # standard input
```

- **All or nothing.** The load writes finished files into `staging/`, then publishes
  every one of them in one `manifest.log` record. A load killed at any moment is either
  wholly there after the next start or wholly absent - `scripts/fault/load_kill.sh` kills it at
  random twenty times to check.
- **Never twice, for the same inputs.** A load's id is a hash, over the whole set of
  inputs, of each one's file name and decompressed bytes. The same set loaded again under
  the same names is refused, even if a file was recompressed in place. A renamed file
  (`day.ndjson` loaded again as `day.ndjson.gz`), the same bytes piped through `-`, or a
  file loaded again beside new ones is a new load, and its rows are stored twice.
- **Where it runs.** One process owns a data directory, by an advisory lock on its
  `LOCK` file. If a server holds it, `load` asks that server to run the load
  (`POST /load`), and the rows are queryable the moment it commits; if nothing does,
  `load` takes the directory and runs alone.
- **One source per day.** Kibbr keeps no row identity, so the two doors cannot remove
  duplicates between them. `--replace-day 2024-01-15` publishes the load as the whole of
  that day, retiring what the day held in the same record - how a day that arrived live is
  swapped for a clean copy from the archive.
- **Rows with no time of their own** take their arrival time, which for an old file is
  the wrong day. The load counts every such row before it commits, and stops, loading
  nothing, if more than 1% (`--max-arrival-share`) would - naming the templates and their
  Datetime-typed keys, the names to add to the time columns. It looks at the first 10,000
  rows before it starts as well, so a file with no time from its first line stops at once.
  `--proceed` loads anyway. The report always says how many rows took their arrival time,
  and which day that is. A template keeps the time source it was made with, and a load
  stopped by the count over every row has made its new templates already, so set the
  time columns before loading a new source, not after.
- `--max-mb-per-sec` and `--threads` keep a load from crowding the live door out of CPU -
  the rate holds the pass that parses and encodes - and `--load-buffer-bytes` (64 MiB of
  rows unless told otherwise) keeps it from crowding it out of memory.

`scripts/check/two_doors.sh` sends the same 200,000 lines through both doors into two data
directories and asks both `scripts/check/agree.sh`'s query set: they must answer identically.

It, `scripts/check/store_agree.sh` (the memory store against the disk store) and
`scripts/check/roundtrip.sh` (every line back out as it went in) each print how many runs of
row groups their disk servers shared between threads, and with `NEED_FAN_OUTS=1` fail
at none. That is for a run made to hold the shared scan to the plain one, with
`KIBBR__QUERY__SCAN_THREADS=4 KIBBR__QUERY__SCAN_FAN_ROWS=0` in the environment: on
the scripts' own data almost nothing is shared out, and such a run would pass having
checked nothing. `two_doors.sh` with `BIN` an earlier build and `RESTART_BIN` this one
holds every shared-out answer to that build's; `store_agree.sh` wants `BODIES=4` and a
short row-group timeout, and `roundtrip.sh` wants `FARM=60000`, to have a day in two
row groups at all - each script's header has the line.

## Compact a finished day

The live door closes a row group whenever its timeout fires, so a day arrives as thousands
of small row groups over several files. `kibbr compact` rebuilds them: each template's
rows of the day into 8,192-row groups at zstd 9, in finished files, swapped in with one
`manifest.log` record.

```
kibbr compact --data-dir kibbr-data                     # every day that is due
kibbr compact --data-dir kibbr-data --day 2024-01-15    # that day, now
```

- **Nothing a query can see moves.** Not a row, not a count, and not a row's position
  among its template's rows of the day - so a cursor handed out before a compaction
  resumes after it, and a query already running finishes on the old files.
  `scripts/check/compact_agree.sh` asks `scripts/check/agree.sh`'s query set of a live day, compacts
  it three ways, and asks again.
- **When.** A day is due the day after its last live row. A server looks every
  `--compact-interval-ms` - a minute; `0` never looks - and compacts what is due;
  `--day` compacts a day whatever its age. A day with nothing to compact, as a loaded day
  is, is left untouched: no record, and `2024-01-15: nothing to compact`.
- **Where it runs** is decided as for `load`. A server holding the directory is asked
  (`POST /compact`), one compaction at a time; with none, the command takes the directory
  and runs alone - under its own `KIBBR__SCHEMA__EPOCH_WINDOW`, so give it the server's.
  A path that is not a kibbr data directory - no `manifest.log` in it - is refused with
  exit 1, and nothing is created or deleted there.
- **Killed at any moment, every row is there once.** The rebuilt files are files nobody
  has named until the one record names them, and a start deletes what the manifest does
  not name. `scripts/fault/compact_kill.sh` kills it twenty times, alone and under a server.
- `/stats` has `storage.compaction`: whether one is running, how many days were rebuilt,
  the last one's report - the one `--json` prints - and the last error. A day that fails
  is left for twice as long each time, and never makes `/ingest` refuse.

**What a file costs to store**, by one command:

```
kibbr bench-storage day.ndjson            # add --es http://localhost:9200 to compare
```

prints the file's size raw, under `gzip -9` and `zstd -19`, and as kibbr holds it - data
files, catalog and manifest, in bytes and in disk blocks, with the data files broken down
into column data, row group headers, schema records and footers - and kibbr's total as a
share of each compressor's. A second table is the same lines as a live day: through the
live door into a directory of its own, flushed 1,440 times over the file, or once a line
if it has fewer - a day that arrives evenly, flushed once a minute - measured as that
leaves it and again once compacted, with the second as a share of the first in bytes,
files and row groups.

## Look at it in a browser

`GET /` is a log viewer. Open <http://127.0.0.1:7700/> after starting the server.

It is one file - `crates/kibbr-server/src/viewer.html`, inline CSS and vanilla JS, compiled into the
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

## Settings: a flag, a variable, a file

Every setting is read the same way: **the flag, or else the variable, or else the
file, or else the default.** The file is TOML and is whatever `--config` names - any
command takes it, before its name or after - and with no `--config` no file is read. A
key's variable is `KIBBR__`, its section and its name in capitals, so the file's keys
are the names the variables already had:

```toml
[storage]
base_path = "/var/lib/kibbr/data"   # --data-dir             KIBBR__STORAGE__BASE_PATH
trie_file = "/var/lib/kibbr/data/trie.json"                # --trie-file
max_open_segments = 512             # --max-open-segments
segment_max_bytes = 536870912       # --segment-max-bytes
max_buffer_bytes = 536870912        # --max-buffer-bytes
compact_interval_ms = 60000         # --compact-interval-ms  0 never looks
wal_segment_bytes = 67108864        # --wal-segment-bytes
wal_queue_bytes = 268435456         # --wal-queue-bytes

[ingestor]
flush_interval_ms = 500             # --flush-interval-ms
row_group_timeout_ms = 300000       # --row-group-timeout-ms
batch_size = 500                    # --batch-size

[api]
native_addr = "127.0.0.1:7700"      # --host and --port, each over its half
max_body_bytes = 10485760           # --max-body-bytes
max_decompressed_bytes = 67108864   # --max-decompressed-bytes
ingest_concurrency = 8              # --ingest-concurrency   default: one per core
ingest_wait_ms = 100                # --ingest-wait-ms
shutdown_deadline = "25s"           # --shutdown-deadline    ms, s, m or h; "0s" is none
request_timeout = "30s"             # --request-timeout      ms, s, m or h; "0s" is none

[query]
max_rows = 10000                    # --max-rows
max_buckets = 10000                 # --max-buckets
max_groups = 100000                 # --max-groups
scan_threads = 4                    # --scan-threads         default: half the cores, at most 4
scan_fan_rows = 32768               # --scan-fan-rows

[schema]
time_columns = ["@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix",
                "datetime", "event_time", "created_at"]    # --time-columns
epoch_window = ["2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"]

[log]
level = "info"                      # --log-level            trace, debug, info, warn, error
format = "text"                     # --log-format           text or json
```

Those are the defaults, and every key is optional. A key that is not in that list is
refused, with its line and the keys its section does have - a setting that was read and
dropped would be one its writer believes is in force:

```
$ kibbr gen-logs --lines 1 --config bad.toml
kibbr: --config bad.toml: TOML parse error at line 2, column 1
  |
2 | wal_dir = "/x"
  | ^^^^^^^
unknown field `wal_dir`, expected one of `base_path`, `trie_file`, `max_open_segments`, `segment_max_bytes`, `max_buffer_bytes`, `compact_interval_ms`, `wal_segment_bytes`, `wal_queue_bytes`
```

What stays a flag only: `--storage memory`, and what one run of `load` or `compact`
is asked to do (`--threads`, `--replace-day`, `--day` and the rest). On `serve`,
`--threads` is `--scan-threads` under a shorter name.

**`shutdown_deadline`** is how long a stopping server gives the requests in flight.
One still in flight then - a sender that stopped halfway through a body - is answered
`503` where it stands, and the final flush, the checkpoint and the trie snapshot
follow, so the process exits 0 inside whatever grace it was given (Kubernetes: 30
seconds) and the next start has nothing to replay. `"0s"` waits for every request,
however long it takes. A query cut off there stops scanning; it used to run on, and the
process waited for it. `scripts/fault/shutdown_deadline.sh` holds a stop to the thirty
seconds, five times: 2,500,000 rows stored and 100,000 more in buffers, one client that
sent half an `/ingest` body, one idle socket, and `MATCH LOG {}` with `--max-rows 0`
running when the signal arrives. The process was gone 27.1 to 27.6 seconds after it,
exit 0, and a restart read nothing back from the log and counted 2,600,000. The build
before was gone at 49.5.

**`request_timeout`** is how long one request may take, from its headers to its
answer. A body that has not arrived by then is answered `408` and its connection
closed. A query still running is answered `504` and its scan is told to stop - at its
next row group it does, and the server says `query stopped` in its log and counts it
in `/stats` as `queries.stopped`; the same happens to a query whose client hangs up.
An `/ingest` body that was read whole is not timed out: its waits are
`ingest_wait_ms`'s, and a `504` for lines the write-ahead log has taken would have
their shipper send them twice. `"0s"` is no timeout.

**The log** is the server's lifecycle lines, warnings and errors, on stderr. As
`text` they are the lines this README shows - `kibbr: ` and a sentence, `WARN` or
`ERROR` before a warning's or an error's. As `json` each is one object a line, for
whatever collects a container's stderr, and at `debug` every request is a line too:

```
$ kibbr --log-format json serve --storage memory
{"ts":"2026-10-10T14:50:07.252Z","level":"info","target":"kibbr_server::lifecycle","msg":"listening on http://127.0.0.1:61046","addr":"127.0.0.1:61046","storage":"memory","data_dir":"","max_rows":10000,"ingest_concurrency":8,"ingest_wait_ms":100,"shutdown_deadline_ms":25000,"request_timeout_ms":30000,"scan_threads":4}
{"ts":"2026-10-10T14:50:07.252Z","level":"warn","target":"kibbr_server::lifecycle","msg":"storage is in memory: everything ingested is lost when this process exits"}
{"ts":"2026-10-10T14:50:08.745Z","level":"info","target":"kibbr_server::lifecycle","msg":"SIGTERM received - shutting down"}
```

What a command prints on stdout - `query`, `parse`, `explain`, `gen-logs` - is the
same bytes whatever the log is set to.

## Ask a running server

`kibbr templates` and `kibbr stats` are `GET /templates` and `GET /stats`, asked for
you. The server is the one holding `--data-dir` (its `LOCK` file says where it
listens), or `--addr host:port`; `--json` prints the answer as the server sent it.

```
$ kibbr templates --config kibbr.toml
 ID  ROWS  TIME            PATH
204    63  column:ts_unix  err.cause.code, err.cause.detail, err.code, err.trace_id, level, msg, service, ts_unix
163    35  column:ts_unix  err.cause.code, err.code, err.trace_id, level, msg, service, ts_unix
...
473 templates

$ kibbr stats --config kibbr.toml
uptime_seconds                        4.048999209
started_at                            2026-10-10T14:56:15.555577Z
storage.kind                          disk
storage.durable                       true
...
trie.templates                        473
ingest.lines_ingested                 2000
ingest.ingest_wait_ms                 100
ingest.shutdown_deadline_ms           25000
ingest.request_timeout_ms             30000
```

A name on the left is the path to the same value in the JSON.

`kibbr health` is `GET /health` the same way, for a machine: it prints `ok` - or
`degraded` and why - and exits 0 while the server answers `"ok":true`, and exits 1 when
nothing answers. A degraded server cannot write and still answers queries, so it is not
one to restart, and exits 0.

## Run it in a container

```
$ docker build -t kibbr .
$ docker run -d --name kibbr --stop-timeout 30 -p 127.0.0.1:7700:7700 -v kibbr-data:/var/lib/kibbr kibbr serve
$ curl -s 127.0.0.1:7700/health
{"ok":true,"status":"ok"}
```

The image is the one binary, linked statically against musl, on
`gcr.io/distroless/static-debian12:nonroot`: 13,634,682 bytes, no shell, user 65532.
Built on this laptop in 2 minutes 30 seconds, the two base images already pulled. Its
`HEALTHCHECK` is `kibbr health`, since there is no curl in it to ask with. What it sets,
each a variable a flag still wins over: the data under the volume
(`/var/lib/kibbr/data`, the trie snapshot beside it), every interface on port 7700 -
loopback inside a container is reachable from nowhere - and the log as JSON.

**`--stop-timeout 30`.** Docker kills a container ten seconds after it asks it to stop;
a kibbr with a request in flight gives it 25 (`shutdown_deadline`) before the final
flush. Killed early, nothing acknowledged is lost and the next start reads the log back.

Ingest, stop, start again on the same volume, same count - 50,000 lines of `gen-logs`,
every row still in a buffer when the stop came:

```
$ curl -s -X POST 127.0.0.1:7700/query -H 'Content-Type: application/json' --data '{"q":"COUNT LOG {}"}'
{"exists":true,"count":50000,...
$ docker stop kibbr          # 1.07 s, exit code 0
$ docker logs kibbr 2>&1 | tail -6
{..."msg":"final flush - 50,000 row(s) into 13,691 row group(s) in 31 file(s), 31 sealed, 8,020,407 byte(s), 0.77s"}
{..."msg":"WAL checkpoint - position 50,000 of 50,000; 1 segment(s), 4,197,272 byte(s)"}
{..."msg":"trie.json - /var/lib/kibbr/trie.json 2,316,614 byte(s), 864 template(s)"}
{..."msg":"close stores - 864 store(s), 50,000 row(s) written"}
{..."msg":"close WAL - closed"}
{..."msg":"exit 0"}
$ docker rm kibbr && docker run -d --name kibbr --stop-timeout 30 -p 127.0.0.1:7700:7700 -v kibbr-data:/var/lib/kibbr kibbr serve
$ curl -s -X POST 127.0.0.1:7700/query -H 'Content-Type: application/json' --data '{"q":"COUNT LOG {}"}'
{"exists":true,"count":50000,...
```

`bench/run.sh` runs `kibbr bench` in a container on the farm day of seed 42 and writes
`bench/results/<date>.md`: the bench's table with a `shed` column - what was turned away
while each row was measured. It measures in the Dockerfile's `bench` target, which is
the same binary on a base that has the `ps` and `df` the bench runs; the script checks
the two images hold the same bytes. `bench/run.sh --lines 200000` is the laptop's run.
`--threads T` is `kibbr bench --threads T`: every server the bench starts is given
`--scan-threads T`, and with no `--threads` each takes the default. A `KIBBR__` variable
does not reach them either way - the bench takes those out of its children.

`deploy/` holds the Kubernetes manifests - a StatefulSet of one pod and its volume - and
the two Vector sinks; `deploy/README.md` says what of them was checked and what was not.

## Run the tests

```
cargo test --workspace
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
`every_fixture_value_gets_its_labelled_type`: every hand-labelled value in
`testdata/fixture.ndjson` must come out with the type its label says.

Lints have to be clean:

```
cargo clippy --workspace --all-targets -- -D warnings
```

## Layout

A workspace of five crates, each naming only the ones under it - `kibbr`, then
`kibbr-server`, `kibbr-engine`, `kibbr-storage`, `kibbr-core` - and `fuzz/` beside them.

```
Cargo.toml                the workspace, and the `kibbr` crate: the binary - the command line
src/main.rs               the clap definitions and the dispatch
src/config.rs             where a setting comes from: a flag, a `KIBBR__` variable, the `--config` file, a default
src/logs.rs               the process's own log: `tracing` events as lines of text or of JSON
src/error.rs              thiserror enums of the commands, and what ends one
src/client.rs             the small HTTP client `load`, `compact` and `bench-storage --es` speak
src/cmd/parse.rs          the `parse` output
src/cmd/demo.rs           the `demo` tree
src/cmd/query.rs          the `query` command: the flags, the table, the plan
src/cmd/load.rs           `kibbr load` - the command: alone, or asked of the server holding the directory
src/cmd/compact.rs        `kibbr compact` - the command, the same two ways
src/cmd/ask.rs            `kibbr templates`, `kibbr stats` and `kibbr health`
src/cmd/bench_storage.rs  `kibbr bench-storage`
src/cmd/bench/            `kibbr bench`
src/gen/                  the generators - `hostile` (mod.rs) and the log farm (farm.rs)
src/reference/            the brute-force oracle behind `query --reference`
src/fuzzing/              what each fuzz target holds its subject to
tests/fixture.rs          outer-surface tests
tests/serve.rs            the HTTP contract, over a bare TcpStream - in this process, or of the binary
Dockerfile                the image: a static binary on distroless, and the `bench` target
bench/run.sh              `kibbr bench` in a container; bench/results/ holds what it measured
deploy/                   the Kubernetes manifests and the Vector sinks

crates/kibbr-server/      the server, as a library: needs kibbr-engine, kibbr-storage and kibbr-core
  src/lib.rs              what the handlers share, and a data directory restored
  src/settings.rs         `Settings`: every value a server runs on, plain data with a default
  src/lifecycle.rs        `run` - the runtime, the flush clock, signals, shutdown, the banner
  src/routes.rs           the router, `/templates`, `/stats`, `/health`, the request timeout
  src/handlers.rs         `/ingest`'s door, `/query`, `/load`
  src/ingest.rs           a body's lines into the write-ahead log and the store
  src/compaction.rs       `/compact`, and the look for a day that is due
  src/query.rs            a TrieQL string into a query, and the result JSON both surfaces answer with
  src/load.rs             the bulk door: staging, the load id, the commit
  src/compact.rs          a compaction's run, and what a server keeps about the ones it runs
  src/error.rs            what stops a server, and what stops the live door taking a body
  src/viewer.html         the log viewer served at `/` - one file, compiled in

crates/kibbr-engine/      a query: needs kibbr-storage and kibbr-core
  src/engine.rs           the plan, the scan, the result
  src/trieql.rs           the query grammar
  src/limits.rs           what a query may not exceed

crates/kibbr-storage/     where templates and rows are kept: needs kibbr-core
  src/trie.rs             the trie - nodes, edges, leaf metadata, the key index
  src/catalog.rs          the template catalog that makes the trie durable
  src/triefile.rs         trie.json
  src/store/              the leaf stores - memory, and format v3's day files - and the scan
  src/wal.rs              the write-ahead log: group commit, the one applier, replay, the checkpoint
  src/frame.rs            the framing the catalog, the manifest, the day files and the log share
  src/db.rs               lines to a trie and its stores - the write path
  src/lock.rs             the LOCK file: one process owns a data directory

crates/kibbr-core/        a line, before anything stores it: needs no other crate
  src/flatten.rs          flattening, fingerprints, the parsed row
  src/infer.rs            the type ladder - one rung per rule, in order
  src/types.rs            ColumnType, ColValue
  src/lines.rs            reading lines under a byte cap
  src/fmt.rs              how a value, a count and an instant are printed
  src/rng.rs              xoshiro256**, written out so the seed is ours, not a dependency's
  src/error.rs            what is wrong with one line, and how an error chain is printed
```

No traits - there is one implementation of everything so
far, and an interface with a single caller is a guess. That includes the ingest
path: a future `Ingestor` would sit between the API and the trie, and it is a plain
function until something gives it a second caller to answer to.
