# bench

`scripts/bench/run.sh` runs `kibbr-bench` inside the image, on the farm day of seed 42. The
bench is a binary of its own (`src/bench/`, `cargo build --release --features bench`) that
measures the `kibbr` beside it; the script builds the image, runs the bench in a container,
brings the result out and writes it as a table with a `shed` column - what was turned away
while each row was measured.

## Two rules

**A laptop bench number is comparable only with one taken in the same session.** Day to
day the p99s drift by about 20%, so a number from another day is not a baseline: take the
"before" again, then the "after".

**A Docker run passes `--threads 4` explicitly.** With no `--threads` each server takes the
build's default - half the cores, at most 4 - and in Docker Desktop's 4-CPU VM that is 2.

Docker results are their own series. They are this image's on this machine's Docker - on a
laptop, a Linux VM with some of the cores and memory - and are not comparable with the
native `scripts/bench/results/native/after-*.json` files, which were measured on macOS
without one, and the bench says so if asked to compare them.

## Here, on the laptop

```
scripts/bench/run.sh --threads 4                 # 200,000 lines, the default
scripts/bench/run.sh --threads 4 --unchecked     # not a measurement: for trying the script
```

On mains, Low Power Mode off, other applications closed. It needs a Docker daemon, `git`
and `python3`.

## On a cloud box

```
git clone https://github.com/pratyaydeep/kibbr && cd kibbr
scripts/bench/run.sh --lines 1000000 --threads 8
```

`--threads` is at most the box's cores; say what the box is when quoting the number.

To start a box that runs a job, uploads its results and terminates itself, with nothing
to log in to, see [cloud/README.md](cloud/README.md): `scripts/bench/cloud/aws.sh launch`.

## What it writes

Under `scripts/bench/results/`, named for the UTC date (`-2`, `-3` for a second and third run of
one day - a result is never written over):

```
<date>.md     the table, and what it was measured on
<date>.json   the bench's own result, every sample in it
<date>.log    the bench's raw log
```

## Flags

| flag | default | what it does |
|---|---|---|
| `--lines N` | 200000 | lines in the loaded day and in the live stream |
| `--threads T` | none | `kibbr-bench --threads T`: every server measured is started with `--scan-threads T` |
| `--runs R` | 3 | as `kibbr-bench --runs` |
| `--samples K` | 100 | as `kibbr-bench --samples` |
| `--unchecked` | | as `kibbr-bench --unchecked`: not a measurement |
| `--image NAME` | `kibbr` | the deployed image's tag; the bench's is `NAME:bench` |
| `--no-build` | | use the two images as they are |
| `--volume NAME` | `kibbr-bench` | the Docker volume |

- **Two images, one binary.** `kibbr` is the image that is deployed: distroless, with no
  `ps` and no `df`, which the bench runs. `kibbr:bench` is the Dockerfile's `bench` target,
  the same `kibbr` binary on a base that has them, with `kibbr-bench` beside it as the
  entrypoint; the script checks the two hold the same `kibbr` bytes.
- **A volume, not a bind mount.** On Docker Desktop a bind mount is a file server between
  two kernels, and the number would be its speed. The volume keeps the loaded day, so a
  second run does not load it again; `docker volume rm kibbr-bench` gives the space back.
- `--threads` is refused if the bench in the image has no such flag (an older image under
  `--no-build`): a result headed "8 threads" that used 4 is worse than none.
- A `KIBBR__` variable does not reach the servers either way: the bench takes those out of
  its children.

The native series is `kibbr-bench` itself, run beside a release `kibbr`:

```
cargo build --release --features bench    # target/release/kibbr and kibbr-bench
target/release/kibbr-bench --help
target/release/kibbr-bench --table scripts/bench/results/native/after-2.json \
    scripts/bench/results/native/after-3.json
```

`--table` prints saved results side by side without measuring anything.

## Compression on real corpora

`scripts/bench/compression.py` runs `kibbr bench-storage` on log corpora it downloads, and
keeps one result a source in `scripts/bench/results/compression.json`, keyed by the source's
`tag`, with each also written as `scripts/bench/results/compression/<tag>.md`. The sources
are in `scripts/bench/compression.json`: a `tag`, a `url` (or a local `path`, or `urls` for
a corpus published as many NDJSON files), and optionally `files` (a glob over an archive),
`time_columns`, `sha256` and `note`. `urls` takes a list, or a pattern and a range -
`{"pattern": ".../file_{:04d}.json.gz", "from": 1, "to": 1000}` - and streams each file into
one, keeping no download; a run that stops picks up after the last file it finished.

```
scripts/bench/compression.py                      # every source not measured yet
scripts/bench/compression.py --tag postgresql     # that one only
scripts/bench/compression.py --input mine.json --output mine-results.json
```

A tag already in the output is not run again; the script says so. Delete its entry to
measure it again. Downloads, the logs read out of them and each run's raw output are kept under
`target/scratch/compression/<tag>/`, so a second run does not fetch again. Needs `python3`, and
`zstd` only for a zstd file inside an archive that unpacks to several files.

The bytes are the same run to run; the load and compaction times are not a measurement
unless the machine was quiet.
