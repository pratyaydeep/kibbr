# kibbr storage on GH Archive, 2026-10-14

GitHub's public event stream, as GH Archive keeps it: one gzip'd NDJSON file an hour from
`https://data.gharchive.org/YYYY-MM-DD-H.json.gz`. Compression only; the query timings
were not run.

## The data

- **Hours:** 41 consecutive hours, 2026-10-14 08:00 UTC through 2026-10-16 00:00 UTC. The
  8 business hours asked for (2026-10-14-08 to -15) came to only 588,936,715 bytes raw.
  Hours were added one at a time until the total passed 1.5 GB. An hour of this stream
  is 12-25 MB gzip'd and 26-117 MB raw, much smaller than GH Archive hours used to be.
- **File:** `testdata/gharchive/day8h.ndjson`, the hours run through `gzip -dc` in order.
  The order is in `testdata/gharchive/hours.txt`.
- **Size:** 926,688 lines, 1,508,356,119 bytes.
- **Rejected lines:** 12. Each of these lines holds two events run together (`…"}{"id":"…`),
  a quirk of GH Archive itself. kibbr refuses them as `invalid JSON … trailing characters`,
  which is correct. 926,676 rows load.
- **`kibbr parse` on the first 20 lines:** none rejected. `created_at` is `Datetime` in
  all 20.
- **Ids typed as Datetime:** `repo.id`, `payload.repository_id` and
  `payload.pull_request.{base,head}.repo.id` are typed `Datetime` whenever the id falls
  in the epoch-seconds range (for example `1229656655 (= 2008-12-19T03:17:35Z)`). Smaller
  ids are typed `Int64`.
  - Over the whole load, this caused 1,196 `widened … to String` warnings.
  - `repo.id` ends up `String` in 922,873 rows, `Datetime` in 3,082 and `Int64` in 718.
  - `payload.repository_id` is `String` in all 608,643 rows that have it.
- **What the stream no longer carries:** `PushEvent` has no `payload.commits`, only
  `before`, `head`, `ref`, `push_id` and `repository_id`. `PullRequestEvent` carries a
  trimmed `pull_request`: refs, shas and urls, with no body. So there are no commit
  messages in this data. The only PR text is review and review-comment bodies.

## `kibbr bench-storage testdata/gharchive/day8h.ndjson`

```
bench-storage testdata/gharchive/day8h.ndjson
  926,688 lines, 926,676 rows as load cbfe254d030bacb8: 11,176 row groups in 3 file(s) over 3 day(s), loaded in 67.7s

                                   bytes   disk blocks    x raw
  raw                      1,508,356,119       368,252    1.000
  gzip -9                    323,942,064        79,088    0.215
  zstd -19                   211,208,542        51,565    0.140
  kibbr total                469,603,650       114,652    0.311
    data files               431,893,174       105,444    0.286
      column data            380,689,794                  0.252
      row group headers       21,564,801                  0.014
      schema records          14,595,278                  0.010
      footers                 15,043,229                  0.010
      file headers                    72                  0.000
    catalog.log               37,709,855         9,207    0.025
    manifest.log                     621             1    0.000

  kibbr total is 1.450 of gzip -9 and 2.223 of zstd -19
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 926,688 lines, 926,676 rows, 49,405 row groups in 3 file(s) over 3 day(s), ingested in 79.6s
  after compaction: 926,676 rows, 2,535 row groups in 3 file(s) over 3 day(s), compacted in 29.3s

                                   bytes   disk blocks    x raw
  kibbr total, before        707,060,864       172,624    0.469
    data files               669,349,973       163,416    0.444
      column data            566,786,881                  0.376
      row group headers       71,548,269                  0.047
      schema records          14,595,278                  0.010
      footers                 16,419,473                  0.011
      file headers                    72                  0.000
    catalog.log               37,709,855         9,207    0.025
    manifest.log                   1,036             1    0.000
  kibbr total, after         371,705,546        90,752    0.246
    data files               333,993,817        81,544    0.221
      column data            298,843,030                  0.198
      row group headers        5,823,149                  0.004
      schema records          14,595,278                  0.010
      footers                 14,732,288                  0.010
      file headers                    72                  0.000
    catalog.log               37,709,855         9,207    0.025
    manifest.log                   1,874             1    0.000

  after compaction is 0.526 of before in bytes, 1.000 in files and 0.051 in row groups
```

### Stage times

These times are indicative only. The run was not on a quiet machine: an
ANECompilerService process was using about 98% CPU, a VM was running, swap was 4.4 of
5.1 GB, and a scratch `cargo build` ran alongside it for about one minute. The machine
switched from AC power to battery during the run.

| stage | time |
|---|---:|
| kibbr load | 67.7 s |
| live ingest, 1,440 flushes | 79.6 s |
| compaction | 29.3 s |
| raw + gzip -9 + zstd -19, together (the command does not split them) | about 596 s |
| whole command (`/usr/bin/time`) | 773.4 s wall, 875.2 s user; peak RSS 890 MB |

## Templates

**1,516 templates.** The ten largest hold 810,660 rows, 87.5% of all rows.

`kibbr templates` failed on this data dir: the client stopped reading at byte 24,820,591
of a 29,860,486-byte `/templates` answer, then reported "not JSON". The template list
below was read from `GET /templates` with curl.

| template | event | rows | keys |
|---:|---|---:|---:|
| 104 | PushEvent, no org | 530,032 | 18 |
| 106 | PushEvent, org | 78,611 | 23 |
| 109 | CreateEvent, no org | 65,140 | 19 |
| 108 | PullRequestEvent, no org | 35,175 | 28 |
| 111 | DeleteEvent, no org | 24,313 | 17 |
| 107 | CreateEvent, org | 22,133 | 24 |
| 101 | PullRequestEvent, org | 17,320 | 33 |
| 105 | DeleteEvent, org | 14,869 | 22 |
| 117 | PullRequestReviewEvent | 14,229 | 62 |
| 125 | WatchEvent | 8,838 | 14 |

Rows by event type, from the raw file:

| event type | rows |
|---|---:|
| PushEvent | 608,644 |
| CreateEvent | 87,273 |
| PullRequestEvent | 68,513 |
| DeleteEvent | 39,182 |
| IssuesEvent | 32,923 |
| IssueCommentEvent | 28,320 |
| PullRequestReviewEvent | 21,909 |
| PullRequestReviewCommentEvent | 14,426 |
| WatchEvent | 12,943 |
| ReleaseEvent | 7,877 |
| ForkEvent | 2,654 |
| MemberEvent | 1,014 |

The issue, comment and release events spread over the long tail of templates.
`payload.issue.*`, for example, nests assignee, milestone, labels, reactions and
`performed_via_github_app`, and each combination of those makes its own key set.

## The ten largest columns, by bytes on disk

How this was measured: the stored chunk length of every column chunk, summed over the
loaded data dir's three sealed `.col` files. This is the `kibbr total` row above, whose
column data is 380,689,794 bytes. The reader was a scratch tool over the public
`kibbr_storage::store::v3::file` reader, kept outside the repo.

- **Raw** is the column's chunk body before compression.
- **Type** is what the catalog holds for the column. A string chunk may also carry a
  second typed piece for rows that look like numbers or dates.

| column | type | rows | stored bytes | raw bytes | share of column data |
|---|---|---:|---:|---:|---:|
| payload.issue.body | String | 61,230 | 73,313,621 | 203,698,968 | 19.3% |
| payload.comment.body | String | 43,233 | 21,430,583 | 59,528,501 | 5.6% |
| payload.comment.diff_hunk | String | 14,425 | 18,328,107 | 61,025,362 | 4.8% |
| repo.url | String | 926,673 | 16,443,129 | 49,180,499 | 4.3% |
| repo.name | String | 926,673 | 15,807,272 | 25,682,988 | 4.2% |
| payload.head | String (sha) | 608,643 | 15,718,193 | 27,913,219 | 4.1% |
| payload.before | String (sha) | 608,643 | 15,714,180 | 27,905,827 | 4.1% |
| actor.url | String | 926,676 | 8,405,157 | 30,422,414 | 2.2% |
| actor.login | String | 926,676 | 7,772,922 | 11,678,116 | 2.0% |
| repo.id | String, widened from Datetime/Int64 | 926,673 | 6,910,917 | 7,582,598 | 1.8% |

## Free text

The free-text columns are 123,652,382 bytes stored, out of 362,480,018 bytes raw. That is
**26.3% of kibbr's total** (469,603,650) and 32.5% of its column data. The columns
counted are:

- every `*.body`: issue, comment, review, release and discussion;
- `payload.comment.diff_hunk`.

Adding titles and descriptions gives 133,077,483 bytes, 28.3% of the total. Commit
messages contribute nothing, because this stream no longer has them. After a live day
is compacted, the same columns are 102,834,532 bytes: 27.7% of that total (371,705,546)
and 34.4% of its column data.

## Read

On this data kibbr stores 0.311× of raw: worse than gzip -9 (0.215×), and 2.2× zstd -19.
On the farm day it stores 0.076×, under gzip and 1.25× zstd. There are three reasons:

- **Free text.** Prose, markdown and diff hunks are 26% of the total. A column store has
  no edge over a byte compressor on them, and these compress only about 2.9×.
- **Template count.** There are 1,516 templates, where the farm day has few.
  - Each template carries its own catalog entry and schema records, and its own row groups
    (11,176 groups for 926,676 rows, about 83 rows a group). The catalog, schemas, footers
    and row-group headers together come to 88.9 MB, 19% of the total.
  - Most templates hold too few rows for their dictionaries and deltas to pay for
    themselves.
- **Nesting.** GitHub's objects nest deeply and repeat across columns: `repo.url` is
  `repo.name` with a prefix, and `actor.url` and `actor.avatar_url` are `actor.login`
  and `actor.id` with a prefix. kibbr encodes each column on its own, so it cannot see
  that redundancy. zstd's long window can.

The 40-hex shas (`before`, `head`, `base/head.sha`) are random and cost the same in
every scheme.

Compacting a day taken live brings it to 0.246×, below the 0.311× of the bulk load,
because row groups drop from 49,405 to 2,535. It is still above gzip.

Two defects also cost space: ids widened to `String`, and `kibbr templates` failing on a
30 MB answer.

## What was measured

- commit `d9a3681f6167a6718060af4fbd88ba8234308069`, `target/release/kibbr` rebuilt
  before the run; nothing in `src/` changed.
- `kibbr bench-storage testdata/gharchive/day8h.ndjson --scratch testdata/gharchive/bench-scratch --keep`.
- macOS 14.3 (Darwin 23.3.0), Apple silicon laptop; not quiet (see the stage times).
