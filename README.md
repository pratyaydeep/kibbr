# kibbr

[![crates.io](https://img.shields.io/crates/v/kibbr.svg)](https://crates.io/crates/kibbr)
[![license: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

**A log store for structured JSON logs that keeps ~25% of the raw bytes on real-world data and still answers queries in milliseconds. One static binary, written in Rust.**

Keeping logs often costs more than producing them. Most teams pay for a hot search cluster to hold weeks of data nobody searches, or move it to an archive nobody can query. kibbr is for the months in between.

```
newest ~3 days        Elasticsearch / Loki   fast, expensive
3 days to ~6 months   kibbr                  still queryable, a fraction of the space
older                 cold archive           cheap, not queryable
```

## At a glance

| | size vs raw | vs gzip -9 | vs zstd -19 |
|---|---:|---:|---:|
| Application logs (200k simulated lines) | **0.076×** | 0.67× of it | 1.24× of it |
| GitHub's public event stream (926k real events, 1.5 GB) | **0.246×** live + compacted, 0.311× bulk-loaded | larger | larger |

Query time on 1,000,000 application log lines (Apple M2, p50):

| query | p50 |
|---|---:|
| count by a promoted value `COUNT LOG {"event":"login_ok"}` | 0.16 ms |
| count by any key `COUNT LOG {"level":"info"}` | 1.00 ms |
| find one id `MATCH LOG {"job.id":"…"}` | 2.39 ms |
| newest 500 lines `MATCH LOG {} LIMIT 500` | 41.1 ms |

On the GitHub data kibbr is **bigger than gzip**. That's a real result, and [the reasons are below](#on-real-data). A gzip file is smaller, but you can't query it.

## Try it in a minute

```sh
cargo install kibbr
```

or build the container (one static binary on distroless):

```sh
docker build -t kibbr .
docker run -d --name kibbr --stop-timeout 30 -p 127.0.0.1:7700:7700 -v kibbr-data:/var/lib/kibbr kibbr serve
```

`--stop-timeout 30` gives a stop time to finish requests and flush; Docker's default ten seconds is too short.

Then make a day of logs, send it, ask it something:

```sh
$ kibbr gen-logs --profile farm --lines 10000 --seed 42 > day.ndjson
#   with Docker: docker run --rm kibbr gen-logs --profile farm --lines 10000 --seed 42 > day.ndjson
$ kibbr serve --data-dir kibbr-data          # skip if you used Docker

$ curl -s -X POST --data-binary @day.ndjson http://127.0.0.1:7700/ingest
{"ok":true,"ingested":10000,"errors":[]}

$ curl -s -X POST -d '{"q": "COUNT LOG {\"event\": \"login_ok\"}"}' http://127.0.0.1:7700/query
{"exists":true,"count":113, ... "duration_ms":3.58}
```

Open **http://127.0.0.1:7700/** for the built-in log viewer: query, a histogram you can click into, and the matched keys highlighted in every row.

A `200` from `/ingest` means every line is in the write-ahead log and flushed. To shrink a finished day:

```sh
$ kibbr compact --data-dir kibbr-data --day 2024-01-15
```

Compaction changes no query answer, only the bytes on disk.

## How it works

Every structured log line is really two things: a **shape**, meaning which keys it has, and **values**.

```
{"ts":"2024-01-15T09:12:03Z","level":"info","event":"login_ok","user":"u_812","ms":41}

shape   {ts, level, event, user, ms}            → stored once, in a trie in memory
values  2024-01-15T09:12:03Z  info  login_ok  u_812  41
                                                → appended to typed columns on disk
```

An application emits thousands of lines but only dozens of shapes. kibbr keeps each shape once, in an in-memory trie. Each shape's values go to disk as typed columns at the trie leaf where that shape ends.

A query walks the trie first. Shapes that can't match are skipped without touching disk, and kibbr scans only the columns of the leaves that can match.

Each column picks its own encoding for each row group:

- same-value-once for columns that never change
- delta encoding for timestamps
- varints for integers
- zstd only where it still beats all of the above

## Built not to lose your logs

- **Write-ahead log, replayed without loss or duplicates.** This was tested by killing the process mid-write, repeatedly. Nothing that was acknowledged is lost, and replay never stores a line twice. (A client that retries a request it already sent can still send it twice; see [docs/serve.md](docs/serve.md).) Fault scripts: [`scripts/`](scripts/README.md).
- **The trie is a cache, not the source of truth.** If you delete it, the catalog log rebuilds it on the next start and every query gives the same answers.
- **Clean shutdown.** On SIGTERM, in-flight requests get a deadline, then kibbr flushes, checkpoints and exits 0.

## On real data

To get a number from data kibbr didn't generate, it was benchmarked on 41 hours of [GH Archive](https://www.gharchive.org/) (2026-10-14 08:00 to 2026-10-16 00:00 UTC):

```
                                   bytes    x raw
  raw                      1,508,356,119    1.000
  gzip -9                    323,942,064    0.215
  zstd -19                   211,208,542    0.140
  kibbr, bulk load           469,603,650    0.311
  kibbr, live + compacted    371,705,546    0.246
```

Here kibbr loses to gzip, for three reasons:

1. **Free text.** Issue bodies, comments and diff hunks are 26% of kibbr's total, and a column store has no advantage on prose.
2. **Many shapes.** GitHub events produce 1,516 distinct key sets. Each one pays for its own catalog entry, schema and small row groups, which together are 19% of the total.
3. **Nesting.** Columns like `repo.url` and `repo.name` repeat each other. kibbr encodes each column on its own and can't use that redundancy; zstd's long window can.

Application logs, with few shapes and little prose, are where kibbr wins. The query timings above are from application logs; the GitHub run measured storage only. Full write-up: [bench/results/gharchive-2026-10-14.md](bench/results/gharchive-2026-10-14.md).

## What it is not

Check these against your own logs before you try it:

- **Not for free text.** Structured JSON only. A line with no stable key set gets no benefit.
- **Not a full-text search engine.** Queries filter columns; there is no inverted index for substring search across every field.
- **Not for unbounded key sets.** Logs with user-supplied keys create shapes without end: 60 kinds of generated log became 4,931 shapes once optional fields counted.
- **Not sold on speed.** A query on a promoted value is one trie edge and a column scan. A query on any other key walks every shape first. The main benefit is space.
- **Not merging schemas, yet.** An optional field present on one line and absent on the next makes two shapes.
- **No Elasticsearch-compatible API, and no Kibana or Grafana plugin.** It has its own query language (TrieQL), HTTP API and viewer.

## Getting logs in

There are two ways in:

- **Live**, a line at a time, through `POST /ingest`.
- **Bulk**, a closed day at a time, with `kibbr load`.

[`deploy/vector/kibbr.toml`](deploy/vector/kibbr.toml) is a Vector config that parses each container's JSON line, keeps the namespace, pod and container names, and sends to kibbr and to S3. [`deploy/k8s/`](deploy/k8s) has a single-pod StatefulSet. The Vector sink has been run end to end on one machine. The Kubernetes manifests pass schema validation but have **not** been applied to a real cluster; [deploy/README.md](deploy/README.md) lists exactly what was and wasn't checked.

## Docs

| | |
|---|---|
| [docs/serve.md](docs/serve.md) | HTTP API: `/ingest`, `/query`, limits, shutdown |
| [docs/load.md](docs/load.md) | bulk loading closed days |
| [docs/compact.md](docs/compact.md) | compaction and `bench-storage` |
| [docs/viewer.md](docs/viewer.md) | the built-in log viewer |
| [docs/container.md](docs/container.md) | the Docker image |
| [docs/settings.md](docs/settings.md) | flags, env vars, [`kibbr.toml.example`](kibbr.toml.example) |
| [docs/ask.md](docs/ask.md) | `templates`, `stats`, `health` |
| [docs/things-to-try.md](docs/things-to-try.md) | the type ladder, by hand |
| [docs/generate-and-parse.md](docs/generate-and-parse.md) | `gen-logs` and `parse` |
| [docs/layout.md](docs/layout.md) | the code, the tests, the lints |
| [bench/README.md](bench/README.md) | running the benchmarks yourself |

The code is a workspace of five crates: [`kibbr-core`](crates/kibbr-core/README.md) (flattening, types, fingerprints), [`kibbr-storage`](crates/kibbr-storage/README.md) (trie, catalog, day files, WAL), [`kibbr-engine`](crates/kibbr-engine/README.md) (TrieQL, planning, scanning), [`kibbr-server`](crates/kibbr-server/README.md) (HTTP), and the `kibbr` binary over them.

## Status

kibbr is currently under development. It runs, it measures, and it isn't finished. Schema merging and a cold S3 tier are designed but not built.

If you keep logs in Elasticsearch or Loki mostly so you have them later, the most useful thing you can do is run `kibbr bench-storage your-logs.ndjson` and [open an issue](https://github.com/pratyaydeep/kibbr/issues) with what it prints, good or bad.

## Licence

AGPL-3.0-only. You can run it for your own logs freely. If you modify it and offer it as a service, you must publish your changes. Contributions are accepted under MIT ([LICENSE-CONTRIBUTOR](LICENSE-CONTRIBUTOR)). Other terms: [COMMERCIAL.md](COMMERCIAL.md). kibbr was previously called TrieDB.