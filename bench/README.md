# bench

`bench/run.sh` runs `kibbr bench` inside the image, on the farm day of seed 42. The bench
itself is in the binary (`src/cmd/bench/`); the script builds the image, runs the bench in
a container, brings the result out and writes it as a table with a `shed` column - what was
turned away while each row was measured.

## Two rules

**A laptop bench number is comparable only with one taken in the same session.** Day to
day the p99s drift by about 20%, so a number from another day is not a baseline: take the
"before" again, then the "after".

**A Docker run passes `--threads 4` explicitly.** With no `--threads` each server takes the
build's default - half the cores, at most 4 - and in Docker Desktop's 4-CPU VM that is 2.

Docker results are their own series. They are this image's on this machine's Docker - on a
laptop, a Linux VM with some of the cores and memory - and are not comparable with the
native `testdata/bench/after-*.json` files, which were measured on macOS without one, and
the bench says so if asked to compare them.

## Here, on the laptop

```
bench/run.sh --threads 4                 # 200,000 lines, the default
bench/run.sh --threads 4 --unchecked     # not a measurement: for trying the script
```

On mains, Low Power Mode off, other applications closed. It needs a Docker daemon, `git`
and `python3`.

## On a cloud box

```
git clone https://github.com/pratyaydeep/kibbr && cd kibbr
bench/run.sh --lines 1000000 --threads 8
```

`--threads` is at most the box's cores; say what the box is when quoting the number.

## What it writes

Under `bench/results/`, named for the UTC date (`-2`, `-3` for a second and third run of
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
| `--threads T` | none | `kibbr bench --threads T`: every server measured is started with `--scan-threads T` |
| `--runs R` | 3 | as `kibbr bench --runs` |
| `--samples K` | 100 | as `kibbr bench --samples` |
| `--unchecked` | | as `kibbr bench --unchecked`: not a measurement |
| `--image NAME` | `kibbr` | the deployed image's tag; the bench's is `NAME:bench` |
| `--no-build` | | use the two images as they are |
| `--volume NAME` | `kibbr-bench` | the Docker volume |

- **Two images, one binary.** `kibbr` is the image that is deployed: distroless, with no
  `ps` and no `df`, which the bench runs. `kibbr:bench` is the Dockerfile's `bench` target,
  the same binary on a base that has them; the script checks the two hold the same bytes.
- **A volume, not a bind mount.** On Docker Desktop a bind mount is a file server between
  two kernels, and the number would be its speed. The volume keeps the loaded day, so a
  second run does not load it again; `docker volume rm kibbr-bench` gives the space back.
- `--threads` is refused if the bench in the image has no such flag (an older image under
  `--no-build`): a result headed "8 threads" that used 4 is worse than none.
- A `KIBBR__` variable does not reach the servers either way: the bench takes those out of
  its children.

The native series is `kibbr bench` itself: `kibbr bench --help`, and
`kibbr bench --table testdata/bench/after-2.json testdata/bench/after-3.json` to print saved
results side by side without measuring anything.
