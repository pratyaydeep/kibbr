# kibbr benchmarks

These are kibbr's storage results in full, and how they were measured.

## Results

Five public log datasets, measured with kibbr 0.4.0 on one AWS machine in November 2026. Ratios are raw size : stored size.

| Dataset | Lines | Raw | kibbr | zstd -19 | gzip -9 | kibbr against zstd -19 |
|---|---:|---:|---:|---:|---:|---|
| [Elasticsearch server logs](../bench/results/compression/elasticsearch.md) | 14,001,234 | 8.57 GB | 206.7 : 1 (41.4 MB) | 109.6 : 1 (78.1 MB) | 53.9 : 1 (158.9 MB) | 47% smaller |
| [MongoDB server logs](../bench/results/compression/mongodb.md) | 186,287,600 | 69.58 GB | 190.0 : 1 (366.3 MB) | 90.0 : 1 (772.8 MB) | 50.8 : 1 (1.37 GB) | 53% smaller |
| [PostgreSQL jsonlog](../bench/results/compression/postgresql.md) | 1,000,000 | 0.41 GB | 43.2 : 1 (9.5 MB) | 36.7 : 1 (11.2 MB) | 29.5 : 1 (14.0 MB) | 15% smaller |
| [HDFS logs](../bench/results/compression/hdfs-logs.md) | 20,000,000 | 7.19 GB | 33.2 : 1 (217.0 MB) | 33.8 : 1 (213.1 MB) | 17.2 : 1 (419.4 MB) | 2% larger |
| [CockroachDB logs](../bench/results/compression/cockroachdb.md) | 16,520,377 | 10.51 GB | 30.6 : 1 (343.5 MB) | 36.1 : 1 (291.4 MB) | 20.4 : 1 (515.6 MB) | 18% larger |

GB and MB are decimal. kibbr is smaller than gzip -9 on all five datasets and smaller than zstd -19 on three. Each dataset name links to its full table.

The other difference is what you are left with. A gzip or zstd file is an archive, and to ask it anything you decompress it first. kibbr's bytes are a database: counts, filters, time ranges and `GROUP BY` run against them as they are.

## Where kibbr loses

- **CockroachDB and HDFS logs.** Long free-text messages dominate both. In the HDFS data the message body is about 90% of kibbr's bytes. zstd matches repeats across the whole file, while kibbr compresses each column chunk separately.
- **Spark event logs.** YScope's fifth dataset is not in the table, because kibbr currently stores it larger than raw. Its environment events use data as JSON keys (jar paths, setting names), which creates thousands of templates with one row and about a thousand columns each. A fix is being researched.
- **kibbr's own generated application logs.** On 200,000 generated lines an earlier build came to 0.076× of raw. That is under gzip -9 but 1.25× of zstd -19.
- **GitHub's public event stream.** An earlier build, on a laptop, came out larger than gzip. The numbers and the reasons are [below](#githubs-event-stream-an-earlier-run).

## How it was measured

- **Machine.** One fresh AWS EC2 c7i.2xlarge in ap-southeast-2: Intel Xeon Platinum 8488C, 8 vCPUs, 16 GB RAM, a 200 GB gp3 volume, Ubuntu 24.04. kibbr was built from source at commit `4a77f39`, and the machine was terminated afterwards.
- **kibbr's number.** Every line went in through the live ingest path as a day arriving over 24 hours (1,440 flushes), and the day was then compacted. That is the state a real deployment ends in. A bulk load lands within 3% of the same size, except on MongoDB, where it is 172 : 1.
- **gzip and zstd.** gzip -9 ran on one thread and zstd -19 on 8 threads, which makes its output about 0.035% larger than one thread would. Sizes are file bytes, not disk blocks.
- **Datasets.** MongoDB, PostgreSQL, CockroachDB and Elasticsearch are from [YScope's public CLP datasets](https://docs.yscope.com/clp/main/user-docs/resources-datasets.html) on Zenodo; MongoDB, CockroachDB and Elasticsearch were checked against their published checksums. The HDFS logs are Quickwit's tutorial dataset; Quickwit does not say where they come from.
- **Reproduce it.** [`bench/compression.py`](../bench/compression.py) downloads each source listed in [`bench/compression.json`](../bench/compression.json) and runs `kibbr bench-storage` on it. [`bench/cloud/aws.sh`](../bench/cloud/aws.sh) does the same on a fresh EC2 machine. See [bench/README.md](../bench/README.md).

## GitHub's event stream, an earlier run

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

This run measured storage only. Full write-up: [bench/results/gharchive-2026-10-14.md](../bench/results/gharchive-2026-10-14.md).

## Query times

Measured on 1,000,000 generated application log lines on an Apple M2 laptop, with an earlier build (p50). Speed benchmarks on a server have not been run yet, so read these as a rough guide.

| query | p50 |
|---|---:|
| count by a promoted value `COUNT LOG {"event":"login_ok"}` | 0.16 ms |
| count by any key `COUNT LOG {"level":"info"}` | 1.00 ms |
| find one id `MATCH LOG {"job.id":"…"}` | 2.39 ms |
| newest 500 lines `MATCH LOG {} LIMIT 500` | 41.1 ms |
