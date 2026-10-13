# kibbr

kibbr is a log store for structured JSON application logs, in one static binary. It splits
each line in two: the set of keys - the line's shape - goes into a trie held in memory, and
the values go to disk as typed columns at the leaf that shape ends in. A query walks the
trie first to find which leaves can match, and scans only their columns.

kibbr was called TrieDB.

## What it is not

Check these against your own logs first.

- **Not for free text.** Structured JSON only: a line with no stable key set has nothing
  to fingerprint and gets no benefit.
- **Not a full-text search engine.** A query filters columns; it cannot find a substring
  across every field the way an inverted index does.
- **Not for arbitrary key sets.** Logs that attach user-supplied keys make templates
  without end: 60 kinds of generated log became 4,931 templates once optional fields counted.
- **Not sold on speed.** A query on a promoted value is one trie edge and a column scan; one
  on any other key walks every template first. The pitch is space.
- **Not merging schemas.** Merging templates is parked, so an optional field present
  on one line and absent on the next is two templates.

## Install

```
cargo build --release
export PATH="$PWD/target/release:$PATH"     # or: cargo install --path .
```

```
$ kibbr --version
kibbr 0.1.0
```

## Quickstart

Five commands: make a day of logs, serve, ingest, query, compact. Run `serve` in a second
terminal; Ctrl-C stops it cleanly. Outputs are this build's, cut where `...` says so.

```
$ kibbr gen-logs --profile farm --lines 10000 --seed 42 > day.ndjson
```

10,000 lines of one simulated day, 2024-01-15: 4,877,696 bytes, the same for the same seed.

```
$ kibbr serve --data-dir kibbr-data
kibbr: catalog.log - .../kibbr-data/catalog.log not found, starting empty

kibbr serve

  listening   http://127.0.0.1:7700
  ...
```

```
$ curl -s -X POST --data-binary @day.ndjson http://127.0.0.1:7700/ingest
{"ok":true,"ingested":10000,"errors":[]}
```

A `200` means every line is in the write-ahead log, flushed. A body may be 10 MiB.

```
$ curl -s -X POST -d '{"q": "COUNT LOG {\"event\": \"login_ok\"}"}' http://127.0.0.1:7700/query
{"exists":true,"count":113,"rows":[],"groups":[],"meta":{"templates_matched":1,"template_ids":[111],"files_opened":0,"leaves_opened":1,"row_groups_read":0,"row_groups_pruned":0,"bytes_read":0,"rows_scanned":113,"rows_returned":0,"truncated":false,"limit_source":"unlimited","next_cursor":null,"duration_ms":3.586125}}
```

```
$ kibbr compact --data-dir kibbr-data --day 2024-01-15
compacted 2024-01-15: 10,000 rows, 105 row groups in 1 file(s) (483,545 bytes) -> 105 row groups in 1 file(s) (479,121 bytes), in 0.2s
```

The same query answers `"count":113` after it: compaction moves nothing a query can see.

## The numbers

**Space: 0.076x of raw**, on 200,000 farm lines loaded through the bulk door.

```
$ kibbr gen-logs --profile farm --lines 200000 --seed 42 > farm.ndjson
$ kibbr bench-storage farm.ndjson
  ...
                                   bytes   disk blocks    x raw
  raw                         97,713,008        23,856    1.000
  gzip -9                     11,046,950         2,739    0.113
  zstd -19                     5,995,347         1,548    0.061
  kibbr total                  7,404,656         1,844    0.076
  ...
  kibbr total is 0.670 of gzip -9 and 1.235 of zstd -19
```

**Query time: four p50s**, on an idle server holding 1,000,000 farm lines. Measured by
`kibbr bench --lines 1000000` natively on macOS - an Apple M2, 8 cores, each query on the
default 4 scan threads - at commit `624c714`, and saved as `testdata/bench/after-3.json`.
This is not the Docker series under `bench/results/`. The saved result, printed (the
right-hand column; `--table` measures nothing):

```
$ kibbr bench --table testdata/bench/after-2.json testdata/bench/after-3.json
                                               after 2 (parallel)    after 3 (map)
S1 promoted value   p50 / p99                  0.18 / 0.21 ms        0.16 / 0.19 ms (1.2x / 1.1x)
S2 unpromoted key   p50 / p99                  1.15 / 1.52 ms        1.00 / 1.38 ms (1.2x / 1.1x)
S3 one id           p50 / p99                  2.53 / 2.62 ms        2.39 / 2.45 ms (1.06x / 1.07x)
S4 newest 500       p50 / p99                  40.8 / 41.7 ms        41.1 / 42.5 ms (0.99x / 0.98x)
  ...
```

| | query | p50 |
|---|---|---:|
| S1 | `COUNT LOG {"event":"login_ok"}` | 0.16 ms |
| S2 | `COUNT LOG {"level":"info"}` | 1.00 ms |
| S3 | `MATCH LOG {"job.id":"20834abd-1e3d-462e-b89b-fb1d4643e6bb"}` | 2.39 ms |
| S4 | `MATCH LOG {} LIMIT 500` | 41.1 ms |

A laptop number compares only with one from the same session: [bench/README.md](bench/README.md).

## Crates

A workspace of five crates, each naming only the ones under it.

| crate | what it is |
|---|---|
| [`kibbr-core`](crates/kibbr-core/README.md) | a line before anything stores it: flattening, the type ladder, the fingerprint |
| [`kibbr-storage`](crates/kibbr-storage/README.md) | where templates and rows are kept: the trie, the catalog, the day files, the write-ahead log |
| [`kibbr-engine`](crates/kibbr-engine/README.md) | a query: TrieQL, the plan over the trie, the scan, the result |
| [`kibbr-server`](crates/kibbr-server/README.md) | the HTTP server as a library: `Settings` in, `run()` until stopped |
| `kibbr` | the binary, at the repository root: the command line over the four |

## More

- [docs/generate-and-parse.md](docs/generate-and-parse.md) - `gen-logs` and `parse`
- [docs/things-to-try.md](docs/things-to-try.md) - the type ladder, by hand
- [docs/serve.md](docs/serve.md) - the HTTP API: `/ingest`, `/query`, limits, shutdown
- [docs/settings.md](docs/settings.md) - a flag, a variable, a file
- [docs/ask.md](docs/ask.md) - `templates`, `stats` and `health`
- [docs/load.md](docs/load.md) - the bulk door: closed days
- [docs/compact.md](docs/compact.md) - compaction, and `bench-storage`
- [docs/viewer.md](docs/viewer.md) - the log viewer at `/`
- [docs/container.md](docs/container.md) - the image
- [docs/layout.md](docs/layout.md) - the binary's files, the tests, the lints
- [deploy/README.md](deploy/README.md) - Kubernetes manifests and the Vector sinks
- [bench/README.md](bench/README.md) - `bench/run.sh`, here and on a cloud box
- [scripts/README.md](scripts/README.md) - the check, fault and measure scripts
- [testdata/FIXTURE.md](testdata/FIXTURE.md) - the hand-labelled type fixture

## Licence

[Elastic License 2.0](LICENSE).
