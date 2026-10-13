# Load closed days

`/ingest` is the live door: lines as they happen. `kibbr load` is the bulk door: a closed
day of lines - or six months of them - loaded whole.

```
kibbr load --data-dir kibbr-data logs/2024-01-15/               # every file under it
kibbr load --data-dir kibbr-data yesterday.ndjson.gz            # gzip, zstd or NDJSON
gzip -dc archive.gz | kibbr load --data-dir kibbr-data -        # standard input
kibbr load --data-dir kibbr-data --replace-day 2024-01-15 day.ndjson.gz   # swap a day's source
```

A load prints one line: `loaded 2,000 rows from 2,000 lines (0 rejected) as load f6a406cde2df80e9: 80 row groups in 1 file(s) over 1 day(s), 0.1s; ...`

- **All or nothing.** Finished files go into `staging/`, then one `manifest.log` record
  publishes all of them. A load killed at any moment is wholly there after the next start or
  wholly absent; `scripts/fault/load_kill.sh` kills it at random twenty times to check.
- **Never twice, for the same inputs.** A load's id is a hash, over the whole set of inputs,
  of each one's file name and decompressed bytes. The same set under the same names is
  refused (`load ... was loaded before`), even if a file was recompressed in place.
- A renamed file (`day.ndjson` again as `day.ndjson.gz`), the same bytes through `-`, or a
  file loaded again beside new ones is a new load, and its rows are stored twice.
- **Where it runs.** One process owns a data directory, by an advisory lock on its `LOCK`
  file. If a server holds it, `load` asks that server (`POST /load`) and the rows are
  queryable the moment it commits; if nothing does, `load` takes the directory and runs alone.
- **One source per day.** kibbr keeps no row identity, so the two doors cannot remove
  duplicates between them. `--replace-day` publishes the load as the whole of that day,
  retiring what the day held in the same record - how a day that arrived live is swapped
  for a clean copy from the archive.
- **Rows with no time of their own** take their arrival time, which for an old file is the
  wrong day. The load counts them before it commits and stops, loading nothing, if more
  than 1% (`--max-arrival-share`) would - naming the templates and their Datetime-typed
  keys, the names to add to the time columns. It checks the first 10,000 rows before it
  starts as well. `--proceed` loads anyway; the report always says how many rows took
  their arrival time, and which day that is.
- A template keeps the time source it was made with, and a load stopped by the count has
  made its new templates already: set the time columns before loading a new source.
- `--max-mb-per-sec` and `--threads` keep a load from crowding the live door out of CPU;
  `--load-buffer-bytes` (64 MiB of rows) keeps it from crowding it out of memory.

## The scripts that hold the doors to each other

```
scripts/check/two_doors.sh      # 200,000 lines through each door; both must answer agree.sh's queries identically
scripts/check/store_agree.sh    # the memory store against the disk store
scripts/check/roundtrip.sh      # every line back out as it went in
```

- Each prints how many runs of row groups its disk servers shared between threads, and
  with `NEED_FAN_OUTS=1` fails at none.
- That is for a run holding the shared scan to the plain one, with
  `KIBBR__QUERY__SCAN_THREADS=4 KIBBR__QUERY__SCAN_FAN_ROWS=0` set: on the scripts' own data
  almost nothing is shared out, and such a run would pass having checked nothing.
- `two_doors.sh` with `BIN` an earlier build and `RESTART_BIN` this one holds every
  shared-out answer to that build's. `store_agree.sh` wants `BODIES=4` and a short
  row-group timeout, and `roundtrip.sh` wants `FARM=60000`, to have a day in two row groups
  at all. Each script's header has the line. See [scripts/README.md](../scripts/README.md).
