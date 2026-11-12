# kibbr: a log store for structured JSON logs

[![crates.io](https://img.shields.io/crates/v/kibbr.svg)](https://crates.io/crates/kibbr)
[![license: AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)

**kibbr stores structured JSON logs in a fraction of their size and keeps them queryable. 69.6 GB of real MongoDB server logs take 366 MB, about half of what zstd -19 needs. One static binary, written in Rust.**

Keeping logs often costs more than producing them. Most teams pay for a hot search cluster to hold weeks of data nobody searches, or move it to an archive nobody can query. kibbr is for the months in between.

```
newest ~3 days        Elasticsearch / Loki   fast, expensive
3 days to ~6 months   kibbr                  still queryable, a fraction of the space
older                 cold archive           cheap, not queryable
```

## Results

Five public log datasets, measured with kibbr 0.4.0 on one AWS machine in November 2026. Ratios are raw size : stored size.

| Dataset | Lines | Raw | kibbr | zstd -19 | gzip -9 | kibbr against zstd -19 |
|---|---:|---:|---:|---:|---:|---|
| [Elasticsearch server logs](bench/results/compression/elasticsearch.md) | 14,001,234 | 8.57 GB | 206.7 : 1 (41.4 MB) | 109.6 : 1 (78.1 MB) | 53.9 : 1 (158.9 MB) | 47% smaller |
| [MongoDB server logs](bench/results/compression/mongodb.md) | 186,287,600 | 69.58 GB | 190.0 : 1 (366.3 MB) | 90.0 : 1 (772.8 MB) | 50.8 : 1 (1.37 GB) | 53% smaller |
| [PostgreSQL jsonlog](bench/results/compression/postgresql.md) | 1,000,000 | 0.41 GB | 43.2 : 1 (9.5 MB) | 36.7 : 1 (11.2 MB) | 29.5 : 1 (14.0 MB) | 15% smaller |
| [HDFS logs](bench/results/compression/hdfs-logs.md) | 20,000,000 | 7.19 GB | 33.2 : 1 (217.0 MB) | 33.8 : 1 (213.1 MB) | 17.2 : 1 (419.4 MB) | 2% larger |
| [CockroachDB logs](bench/results/compression/cockroachdb.md) | 16,520,377 | 10.51 GB | 30.6 : 1 (343.5 MB) | 36.1 : 1 (291.4 MB) | 20.4 : 1 (515.6 MB) | 18% larger |

GB and MB are decimal. kibbr is smaller than gzip -9 on all five datasets and smaller than zstd -19 on three. Each dataset name links to its full table.

The other difference is what you are left with. A gzip or zstd file is an archive, and to ask it anything you decompress it first. kibbr's bytes are a database: counts, filters, time ranges and `GROUP BY` run against them as they are.

### Where kibbr loses

- **CockroachDB and HDFS logs.** Long free-text messages dominate both. In the HDFS data the message body is about 90% of kibbr's bytes. zstd matches repeats across the whole file, while kibbr compresses each column chunk separately.
- **Spark event logs.** YScope's fifth dataset is not in the table, because kibbr currently stores it larger than raw. Its environment events use data as JSON keys (jar paths, setting names), which creates thousands of templates with one row and about a thousand columns each. A fix is being researched.
- **kibbr's own generated application logs.** On 200,000 generated lines an earlier build came to 0.076× of raw. That is 0.67× of gzip -9 but 1.24× of zstd -19.
- **GitHub's public event stream.** An earlier build, on a laptop, came out larger than gzip. The numbers and the reasons are [below](#githubs-event-stream-an-earlier-run).

### How it was measured

- **Machine.** One fresh AWS EC2 c7i.2xlarge in ap-southeast-2: Intel Xeon Platinum 8488C, 8 vCPUs, 16 GB RAM, a 200 GB gp3 volume, Ubuntu 24.04. kibbr was built from source at commit `4a77f39`, and the machine was terminated afterwards.
- **kibbr's number.** Every line went in through the live ingest path as a day arriving over 24 hours (1,440 flushes), and the day was then compacted. That is the state a real deployment ends in. A bulk load lands within 3% of the same size, except on MongoDB, where it is 172 : 1.
- **gzip and zstd.** gzip -9 ran on one thread and zstd -19 on 8 threads, which makes its output about 0.035% larger than one thread would. Sizes are file bytes, not disk blocks.
- **Datasets.** MongoDB, PostgreSQL, CockroachDB and Elasticsearch are from [YScope's public CLP datasets](https://docs.yscope.com/clp/main/user-docs/resources-datasets.html) on Zenodo, with checksums verified. The HDFS logs are Quickwit's tutorial dataset; Quickwit does not say where they come from.
- **Reproduce it.** [`bench/compression.py`](bench/compression.py) downloads each source listed in [`bench/compression.json`](bench/compression.json) and runs `kibbr bench-storage` on it. [`bench/cloud/aws.sh`](bench/cloud/aws.sh) does the same on a fresh EC2 machine. See [bench/README.md](bench/README.md).

### GitHub's event stream, an earlier run

An earlier build was run on 41 hours of [GH Archive](https://www.gharchive.org/) (2026-10-14 08:00 to 2026-10-16 00:00 UTC), on an Apple M2 laptop:

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

This run measured storage only. Full write-up: [bench/results/gharchive-2026-10-14.md](bench/results/gharchive-2026-10-14.md).

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

Open **http://127.0.0.1:7700/** for the built-in web console: search, matches over time, the matched keys highlighted in every row, the template tree, and the server's status.

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

## The same JSON back

Every line comes back as the same JSON values: numbers, strings, arrays and empty objects. This was checked on 0.4.0 with 200,000-line samples of each of the five datasets. It is not byte-identical: keys come back sorted, and rows come back in time order, not file order.

## Built not to lose your logs

- **Write-ahead log, replayed without loss or duplicates.** This was tested by killing the process mid-write, repeatedly. Nothing that was acknowledged is lost, and replay never stores a line twice. (A client that retries a request it already sent can still send it twice; see [docs/serve.md](docs/serve.md).) Fault scripts: [`scripts/`](scripts/README.md).
- **The trie is a cache, not the source of truth.** If you delete it, the catalog log rebuilds it on the next start and every query gives the same answers.
- **Clean shutdown.** On SIGTERM, in-flight requests get a deadline, then kibbr flushes, checkpoints and exits 0.

## Query times

Measured on 1,000,000 generated application log lines on an Apple M2 laptop, with an earlier build (p50). Speed benchmarks on a server have not been run yet, so read these as a rough guide.

| query | p50 |
|---|---:|
| count by a promoted value `COUNT LOG {"event":"login_ok"}` | 0.16 ms |
| count by any key `COUNT LOG {"level":"info"}` | 1.00 ms |
| find one id `MATCH LOG {"job.id":"…"}` | 2.39 ms |
| newest 500 lines `MATCH LOG {} LIMIT 500` | 41.1 ms |

## What it is not

Check these against your own logs before you try it:

- **Not for free text.** Structured JSON only. A line with no stable key set gets no benefit. Logs that are mostly long messages come out about the size of a zstd -19 file, or larger.
- **Not a full-text search engine.** Queries filter columns; there is no inverted index for substring search across every field.
- **Not for unbounded key sets.** Logs with data in their keys create shapes without end: Spark event logs come out larger than raw, and 60 kinds of generated log became 4,931 shapes once optional fields counted.
- **Not sold on speed.** A query on a promoted value is one trie edge and a column scan. A query on any other key walks every shape first. The main benefit is space.
- **Not merging schemas, yet.** An optional field present on one line and absent on the next makes two shapes.
- **No Elasticsearch-compatible API, and no Kibana or Grafana plugin.** It has its own query language (TrieQL), HTTP API and viewer.

## Getting logs in

There are two ways in:

- **Live**, a line at a time, through `POST /ingest`.
- **Bulk**, a closed day at a time, with `kibbr load`.

[`deploy/vector/kibbr.toml`](deploy/vector/kibbr.toml) is a Vector config that parses each container's JSON line, keeps the namespace, pod and container names, and sends to kibbr and to S3. [`deploy/k8s/`](deploy/k8s) has a single-pod StatefulSet. The Vector sink has been run end to end on one machine. The Kubernetes manifests pass schema validation but have **not** been applied to a real cluster; [deploy/README.md](deploy/README.md) lists exactly what was and wasn't checked.

## Docs

Website: **https://pratyaydeep.github.io/kibbr/**. The API of the crates is on [docs.rs](https://docs.rs/kibbr).

| | |
|---|---|
| [docs/serve.md](docs/serve.md) | HTTP API: `/ingest`, `/query`, limits, shutdown |
| [docs/load.md](docs/load.md) | bulk loading closed days |
| [docs/compact.md](docs/compact.md) | compaction and `bench-storage` |
| [docs/viewer.md](docs/viewer.md) | the built-in web console, and building it from `web/` |
| [docs/container.md](docs/container.md) | the Docker image |
| [docs/settings.md](docs/settings.md) | flags, env vars, [`kibbr.toml.example`](kibbr.toml.example) |
| [docs/ask.md](docs/ask.md) | `templates`, `stats`, `health` |
| [docs/things-to-try.md](docs/things-to-try.md) | the type ladder, by hand |
| [docs/generate-and-parse.md](docs/generate-and-parse.md) | `gen-logs` and `parse` |
| [docs/layout.md](docs/layout.md) | the code, the tests, the lints |
| [bench/README.md](bench/README.md) | running the benchmarks yourself |

The code is a workspace of three crates: [`kibbr-engine`](crates/kibbr-engine/README.md) (the whole database behind one handle, `Database`: flattening, types, trie, catalog, day files, WAL, TrieQL, planning, scanning), [`kibbr-server`](crates/kibbr-server/README.md) (HTTP over one `Database`), and the `kibbr` binary over them. What were `kibbr-core` and `kibbr-storage` are now modules of `kibbr-engine` (`model` and `parse`; `format`, `templates`, `rows`, `ingest`, `wal` and `lock`); on crates.io their names stop at 0.2.3.

## Status

kibbr is currently under development. It runs, it measures, and it isn't finished. Schema merging and a cold S3 tier are designed but not built. Next are speed benchmarks on a server, a same-machine comparison with clp-s, and a fix for logs like Spark's.

If you keep logs in Elasticsearch or Loki mostly so you have them later, the most useful thing you can do is run `kibbr bench-storage your-logs.ndjson` and [open an issue](https://github.com/pratyaydeep/kibbr/issues) with what it prints, good or bad.

## Licence

AGPL-3.0-only. You can run it for your own logs freely. If you modify it and offer it as a service, you must publish your changes. Contributions are accepted under MIT ([LICENSE-CONTRIBUTOR](LICENSE-CONTRIBUTOR)). Other terms: [COMMERCIAL.md](COMMERCIAL.md).
