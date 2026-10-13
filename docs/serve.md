# Serve it over HTTP

Commands first, one line each; then what each endpoint does when pushed, a fact a line.

```
kibbr serve                                   # rows under ./kibbr-data, 127.0.0.1:7700
kibbr serve --data-dir /var/lib/kibbr/data    # the data directory, created if absent
kibbr serve --storage memory                  # nothing written; all of it lost at exit
kibbr gen-logs --lines 500 --seed 42 > logs.ndjson
curl -s -X POST --data-binary @logs.ndjson http://127.0.0.1:7700/ingest     # NDJSON in
curl -s -X POST -d '{"q": "COUNT LOG {\"level\": \"error\"}"}' http://127.0.0.1:7700/query
curl -s http://127.0.0.1:7700/templates       # every template, its columns and row count
curl -s http://127.0.0.1:7700/stats           # throughput, template count, storage size
curl -s http://127.0.0.1:7700/health          # {"ok":true,"status":"ok"}
gzip -c logs.ndjson | curl -s -X POST --data-binary @- \
  -H 'Content-Encoding: gzip' http://127.0.0.1:7700/ingest                  # gzip in
```

Also served: `GET /` (the [viewer](viewer.md)), `POST /load` ([load](load.md)) and
`POST /compact` ([compact](compact.md)).

## The server

- **Storage is disk by default**: a line is in the write-ahead log under `<data-dir>/wal/`,
  flushed, before `/ingest` answers `200`; a start after `kill -9` puts back every line no
  data file holds yet, once. `scripts/fault/crash_test.sh` kills a server twenty times
  mid-ingest and holds what is stored to what was acknowledged, line by line.
- `--storage memory` writes nothing - no log, no trie snapshot. The banner says so at
  startup and `/stats` says so as a field.
- The banner is on stderr, so redirecting stdout does not silence it.
- `--host` and `--port` move the listener. The default is loopback: `--host 0.0.0.0`
  publishes an unauthenticated write endpoint, and that should be something you typed.
- `/query` takes TrieQL and answers with the JSON `kibbr query --format json` prints: one
  shape, built in one place, so the two surfaces cannot drift.

## `/ingest`

- **Encodings.** `Content-Encoding: gzip`, `x-gzip`, `identity` or none; anything else is a
  `415` naming what is accepted. A body that says gzip and is not is a `400`, not a crash.
- **Two size limits, because one cannot see a bomb.** On the wire: 10 MiB,
  `--max-body-bytes`. As it inflates: 64 MiB, `--max-decompressed-bytes` (at most the line
  limit), and the reader stops at the cap, not after it. Either is a `413`.
- **A bad line costs that line only.** The answer is `200` with
  `{"ok":false,"ingested":2,"errors":[{"line":2,"msg":"invalid JSON at offset 4: EOF while parsing a value"},...]}`;
  a retry would duplicate what landed. Line numbers count every physical line, blanks too.
- **The one report that is not a `200`** has `ingested: 0` and only errors the server could
  not write (`not ingested: catalog: ...`): a `503` with `Retry-After`, since nothing landed.
- **Past capacity it waits a moment, then says `503`; it does not go quiet.**
  `--ingest-concurrency` bodies are parsed at once, one per core by default. A body with no
  slot waits at the door, unread, for `--ingest-wait-ms` (100; at most 30000; `0` is no
  wait), then gets `503` with `Retry-After: 1`.
- **Buffers.** A body arriving while buffered rows hold more than twice `--max-buffer-bytes`
  (512 MiB) waits the same way for the flush; past the limit itself the largest buffers are
  written early. A store that cannot write at all empties no buffer, and that body is
  refused at once with the store's error.
- **The log.** At most 256 bodies are between the door and their answer (twice
  `--ingest-concurrency`, where that is more), and at most `--wal-queue-bytes` (256 MiB) of
  lines are in the log and not yet in the store. A parsed body that finds the log full waits
  up to `--ingest-wait-ms` again, keeping its slot - so a slow disk slows its senders
  instead of filling memory - then is shed with the same `503`. One whose sender hangs up
  while it waits is let go, with nothing of it stored.
- **A log that cannot write at all** (a full volume) refuses every body `503` with the write
  error, before reading it, until a write succeeds. A store that cannot write refuses
  nothing: the line is in the log and the answer is `200`.
- A body is written to the log whole: one over 1 GiB as log records is a `413`, decided
  before it is parsed. Send smaller bodies.
- **Why a status and not silence.** A shipper that gets no answer keeps the batch, times
  out and opens another connection, so a slow server gathers connections until it is
  unreachable. Vector's adaptive concurrency reads a `503` as "send fewer" and a timeout as
  "try harder".
- A refusal sent before the body is read says `Connection: close`, and the server closes
  it; an answer without the header leaves the connection open.
- **Single tenant, deliberately.** No stream or tenant identifier, in the path or a header.
  Adding one later changes every shipper's config, and it is not a header away: the trie
  path is built from each line's keys, so tenants mean a token in the fingerprint (every
  template id changes) or a trie per tenant with a selector on `/query`, `/templates`,
  `/stats` and the viewer. Revisit with the deployment work.
- **`/stats` says which of these is happening.** `ingest.requests_shed` counts bodies told
  to come back; `ingest.shed` counts them by what there was none of: `permit`, `place`,
  `line`, `buffers`, `log`, `stopping`. `ingest.door` is the bodies waiting now and how long
  they have waited in all; with `--ingest-wait-ms` above 100 no more wait than may be inside
  (`ingest.door.line_max`), and one more is shed at once (`line`). `storage.wal` has
  `queue_bytes`, `unapplied_bytes` and `queue_waiting`.
- **A stopping server keeps nobody waiting.** A body at the door is refused unread
  (`stopping`); a parsed body with no room in the log is refused (`log`); a body the log
  has taken is finished and answered `200`.

## `/query`

- **A `MATCH` with no `LIMIT` gets a page, not everything.** It is cut at `max_rows`
  (10,000; `--max-rows`, `0` turns it off) and says so: `truncated: true`,
  `limit_source: "backstop"`, and a `next_cursor` to hand back as `AFTER`. A `LIMIT` you
  asked for is honoured above it; `COUNT` and `GROUP BY` return no rows for it to bound.
- **Scan threads.** A day of one template is row groups, and a run of them can be shared
  between threads; the answer and every counter in `meta` are the same either way.
  `--scan-threads` is how many one query may use, its own among them: half the cores and at
  most 4 by default, `1` for never, more than the machine has cores refused at startup.
- A run under `--scan-fan-rows` rows (32,768) is never shared, nor any run while an
  `/ingest` body is between the door and its answer - so a server under steady ingest
  answers each query on one thread.
- It buys time and costs CPU: on eight cores, a count over 433,000 rows of a day in about
  half the time for 14 to 34% more CPU, in replays of the bench's scan (`kibbr bench` has
  not yet taken that row). `/stats` has
  `queries.scan_fan_outs`, `queries.scan_threads` and `queries.scan_fan_rows`.

```
kibbr query day.ndjson 'COUNT LOG {"event":"login_ok"} BETWEEN 2024-01-15T09:00:00Z AND 2024-01-15T12:00:00Z GROUP BY time(5m)'
kibbr explain 'COUNT LOG {} SINCE 30d GROUP BY time(1s)'
```

The first, on the quickstart's `day.ndjson`, prints 36 buckets, `2024-01-15T09:00:00Z  0`,
`2024-01-15T09:05:00Z  2` and on, then `36 groups, 16 rows`. The second is refused:
``kibbr: `GROUP BY time(1s)` over this range needs 2,592,001 buckets; the limit is 10,000 - widen the bucket or narrow the range``.

- **`GROUP BY time(5m)` emits empty buckets as zeros.** A bucket nothing landed in is an
  observation - the thing was quiet - so the result is a list, not a map of busy buckets.
- Boundaries are multiples of the width from the Unix epoch, never from where the query
  starts, so two overlapping ranges put their bars in the same places.
- A second key is zero-filled too: three levels are three series, each with every bucket.
- The range emits every bucket it touches. `SINCE 6h` at `time(5m)` is 72 buckets if `now`
  is on a five-minute boundary and 73 otherwise; the first and last are partial. The comment
  above `bucket_of` in `crates/kibbr-engine/src/engine.rs` draws it.
- A bucketed query returns aggregates, so `max_rows` does not reach it; `max_buckets`
  (10,000) and `max_groups` (100,000) do, and going over is a refusal naming both counts.

## Stopping

SIGINT (Ctrl-C) and SIGTERM cancel one `CancellationToken`: the listener stops accepting,
requests in flight finish - or are answered `503` once the shutdown deadline passes, 25
seconds by default - and the shutdown walks a fixed order. With `--storage memory` it names
each step it skips, four of the six: they are what a database that survives a restart does.

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
