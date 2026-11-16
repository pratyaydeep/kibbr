# bench

`kibbr-bench` measures how fast a build of kibbr is, on the farm day of seed 42. It is a
binary of its own, in `crates/kibbr-devtools/`, and is not part of what is published. It
measures the `kibbr` binary that is in its own directory: every server, load and
`gen-logs` of a run is that file, started as a child. It runs natively; there is no bench
image.

## Three rules

**Build both binaries together.** Building `kibbr-bench` does not build `kibbr`, and the
other way round.

```
cargo build --release --workspace
```

The bench stops when there is no `kibbr` beside it. When the `kibbr` there is older than
the bench, it measures it and prints a `WARNING` under the table: if `kibbr`'s source
changed in between, the number is an old build's. A result records the digest of both.

**A laptop bench number is comparable only with one taken in the same session.** Day to
day the p99s drift by about 20%, so a number from another day is not a baseline: take the
"before" again, then the "after".

**A result is comparable only with one taken by the same bench.** A result records a
digest of the bench's own source, and the table says `NOT THE SAME BENCH` of a baseline
that has another. Every result saved while the bench was `kibbr bench`, a subcommand of
the binary it measured, has another: the files `baseline` and `after-*` under `scripts/bench/results/` among them.

## Run it

```
target/release/kibbr-bench --lines 1000000 --profile farm
target/release/kibbr-bench --lines 1000000 --profile farm --baseline scripts/bench/results/after-3.json
target/release/kibbr-bench --table scripts/bench/results/after-2.json scripts/bench/results/after-3.json
```

The first measures and saves a result. The second also prints each number's speed-up on a
saved result. The third measures nothing: it prints saved results side by side.

On mains, Low Power Mode off, other applications closed. It needs `git`, `ps` and `df`.
`--threads` is at most the machine's cores; say what the machine is when quoting a number.
A `KIBBR__` variable does not reach the servers: the bench takes those out of its
children.

## What it writes

Under `--dir`, which is `testdata/bench/` unless given:

```
farm-<lines>-seed42/              the loaded day, kept, so a second run does not load it again
results/<UTC time>-<commit>.json  the result, every sample in it
results/<UTC time>-<commit>.log   the raw log
```

`--out FILE` saves the result under a name of your own. A result is never written over.

## Flags

`kibbr-bench --help` has every flag. The ones a comparison depends on:

| flag | default | what it does |
|---|---|---|
| `--lines N` | none | lines in the loaded day and in the live stream; the measurement is 1000000 |
| `--profile farm` | none | the only profile measured |
| `--threads T` | none | every server measured is started with `--scan-threads T` |
| `--runs R` | 3 | how many times the whole sequence is run; odd |
| `--samples K` | 100 | timed asks of each query, and timed plans of each shape |
| `--body-lines L` | 2000 | lines in one `/ingest` body |
| `--unchecked` | | skips the checks and allows a debug build: not a measurement |

## The earlier Docker results

`scripts/bench/results/2026-10-11.{md,json,log}` were measured in a container by `scripts/bench/run.sh`,
on the Dockerfile's `bench` target. Both are gone: the bench is not in the image's binary
any more. Those results are a series of their own, and are not comparable with a native
one.

## Compression on real corpora

`scripts/bench/compression.py` runs `kibbr bench-storage` on log corpora it downloads, and keeps
one result a source in `scripts/bench/results/compression.json`, keyed by the source's `tag`, with
each also written as `scripts/bench/results/compression/<tag>.md`. The sources are in
`scripts/bench/compression.json`: a `tag`, a `url` (or a local `path`, or `urls` for a corpus
published as many NDJSON files), and optionally `files` (a glob over an archive),
`time_columns`, `sha256` and `note`. `urls` takes a list, or a pattern and a range -
`{"pattern": ".../file_{:04d}.json.gz", "from": 1, "to": 1000}` - and streams each file
into one, keeping no download; a run that stops picks up after the last file it finished.

```
scripts/bench/compression.py                      # every source not measured yet
scripts/bench/compression.py --tag postgresql     # that one only
scripts/bench/compression.py --input mine.json --output mine-results.json
```

A tag already in the output is not run again; the script says so. Delete its entry to
measure it again. Downloads, the logs read out of them and each run's raw output are kept under
`testdata/compression/<tag>/`, so a second run does not fetch again. Needs `python3`, and
`zstd` only for a zstd file inside an archive that unpacks to several files.

The bytes are the same run to run; the load and compaction times are not a measurement
unless the machine was quiet.
