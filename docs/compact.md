# Compact a finished day

The live door closes a row group whenever its timeout fires, so a day arrives as thousands
of small row groups over several files. `kibbr compact` rebuilds them: each template's rows
of the day into 8,192-row groups at zstd 9, in finished files, swapped in with one
`manifest.log` record.

```
kibbr compact --data-dir kibbr-data                     # every day that is due
kibbr compact --data-dir kibbr-data --day 2024-01-15    # that day, now, whatever its age
kibbr compact --data-dir kibbr-data --day 2024-01-15 --json   # the report as one JSON object
kibbr bench-storage day.ndjson            # what a file costs to store; --es URL to compare
```

A compaction prints `compacted 2024-01-15: 10,000 rows, 105 row groups in 1 file(s) (483,545 bytes) -> 105 row groups in 1 file(s) (479,121 bytes), in 0.2s`; a second run of it, `2024-01-15: nothing to compact`.

- **Nothing a query can see moves.** Not a row, not a count, not a row's position among its
  template's rows of the day - so a cursor handed out before a compaction resumes after it,
  and a query already running finishes on the old files.
  `scripts/check/compact_agree.sh` asks `scripts/check/agree.sh`'s query set of a live day,
  compacts it three ways, and asks again.
- **When.** A day is due the day after its last live row. A server looks every
  `--compact-interval-ms` - a minute; `0` never looks - and compacts what is due.
- A day with nothing to compact, as a loaded day is, is left untouched: no record.
- **Where it runs** is decided as for [`load`](load.md). A server holding the directory is
  asked (`POST /compact`), one compaction at a time; with none, the command takes the
  directory and runs alone - under its own `KIBBR__SCHEMA__EPOCH_WINDOW`, so give it the
  server's.
- A path that is not a kibbr data directory - no `manifest.log` in it - is refused with
  exit 1, and nothing is created or deleted there.
- **Killed at any moment, every row is there once.** The rebuilt files are files nobody has
  named until the one record names them, and a start deletes what the manifest does not
  name. `scripts/fault/compact_kill.sh` kills it twenty times, alone and under a server.
- `/stats` has `storage.compaction`: whether one is running, how many days were rebuilt,
  the last one's report - the one `--json` prints - and the last error. A day that fails is
  left for twice as long each time, and never makes `/ingest` refuse.

## `bench-storage`

- The first table is the file's size raw, under `gzip -9` and `zstd -19`, and as kibbr
  holds it - data files, catalog and manifest, in bytes and in disk blocks, the data files
  broken down into column data, row group headers, schema records and footers - with
  kibbr's total as a share of each compressor's.
- The second table is the same lines as a live day: through the live door into a directory
  of its own, flushed 1,440 times over the file, or once a line if it has fewer - a day
  that arrives evenly, flushed once a minute - measured as that leaves it and again once
  compacted, the second as a share of the first in bytes, files and row groups.
- On 200,000 farm lines: 0.076x of raw loaded; 0.348x live before compaction, 0.071x after.
