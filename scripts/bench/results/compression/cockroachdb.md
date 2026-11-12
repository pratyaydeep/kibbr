# kibbr storage on cockroachdb, 2026-11-12

One node's JSON logs from a three-node CockroachDB cluster under YCSB for three days (Zenodo 10516387), from YScope's CLP datasets: 9.79 GiB, 16,520,377 lines

- **Source:** https://zenodo.org/records/10516387/files/cockroachdb.tar.gz?download=1
- **Download:** `cockroachdb.tar.gz`, 554,664,494 bytes, sha256 `9b6913ae481e387090dc3fbc37e3b699dc634496af982962ca4bd946b482470d`
- **Measured:** `cockroachdb/cockroach.node1.log`: 16,520,377 lines, 16,520,377 rows

**10,506,623,721 bytes ingested.** As a plain file that takes 10,506,629,120 bytes on this disk; kibbr keeps it in 342,786,048, 96.7% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 10,506,623,721 | 10,506,629,120 | 1.000 | - | - |
| gzip -9 | 515,625,649 | 515,629,056 | 0.049 | - | - |
| zstd -19, 8 thread(s) | 291,375,481 | 291,381,248 | 0.028 | - | - |
| kibbr, bulk load | 342,774,200 | 342,786,048 | 0.033 | 0.665 | 1.176 |
| kibbr, live day compacted | 343,540,791 | 343,552,000 | 0.033 | 0.666 | 1.179 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /opt/kibbr/testdata/compression/cockroachdb/logs-b529a53a6eb6.ndjson
  16,520,377 lines, 16,520,377 rows as load fcecef8005fa1f7a: 2,629 row groups in 4 file(s) over 4 day(s), loaded in 472.8s

                                   bytes   disk blocks    x raw
  raw                     10,506,623,721     2,565,095    1.000
  gzip -9                    515,625,649       125,886    0.049
  zstd -19                   291,375,481        71,138    0.028
  kibbr total                342,774,200        83,688    0.033
    data files               342,637,190        83,653    0.033
      column data            341,531,169                  0.033
      row group headers          932,840                  0.000
      schema records              38,080                  0.000
      footers                    135,005                  0.000
      file headers                    96                  0.000
    catalog.log                  136,198            34    0.000
    manifest.log                     812             1    0.000

  kibbr total is 0.665 of gzip -9 and 1.176 of zstd -19
  gzip -9 ran on 1 thread in 200.7s; zstd -19 on 8 thread(s) in 1805.3s
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 16,520,377 lines, 16,520,377 rows, 10,226 row groups in 4 file(s) over 4 day(s), ingested in 469.4s
  after compaction: 16,520,377 rows, 2,278 row groups in 4 file(s) over 4 day(s), compacted in 94.0s

                                   bytes   disk blocks    x raw
  kibbr total, before        427,090,977       104,274    0.041
    data files               426,953,403       104,239    0.041
      column data            422,620,501                  0.040
      row group headers        3,886,229                  0.000
      schema records              38,080                  0.000
      footers                    408,497                  0.000
      file headers                    96                  0.000
    catalog.log                  136,198            34    0.000
    manifest.log                   1,376             1    0.000
  kibbr total, after         343,540,791        83,875    0.033
    data files               343,402,094        83,840    0.033
      column data            342,433,878                  0.033
      row group headers          807,491                  0.000
      schema records              38,080                  0.000
      footers                    122,549                  0.000
      file headers                    96                  0.000
    catalog.log                  136,198            34    0.000
    manifest.log                   2,499             1    0.000

  after compaction is 0.804 of before in bytes, 1.000 in files and 0.223 in row groups
```

## What it was measured with

- kibbr 0.4.0, commit `4a77f39f5165adeae9f91f62a48c560908205e56`
- `kibbr bench-storage logs-b529a53a6eb6.ndjson --scratch <scratch> --time-columns timestamp`
- Linux 7.0.0-1014-aws, x86_64, Intel(R) Xeon(R) Platinum 8488C, 8 cores (4 physical), 16,462,835,712 bytes of memory, on aws c7i.2xlarge, ap-southeast-2
- 4 warning(s) in bench-storage's log; 3218.5 s start to end, download included
