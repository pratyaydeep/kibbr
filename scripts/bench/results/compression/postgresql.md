# kibbr storage on postgresql, 2026-11-11

PostgreSQL 15 jsonlog under YCSB workloads A-E (Zenodo 10516402)

- **Source:** https://zenodo.org/records/10516402/files/postgresql.tar.gz?download=1, files matching `*.log`
- **Download:** `postgresql.tar.gz`, 15,296,662 bytes, sha256 `2a8c2d87da92401a1c0d404d7e953e6395e4d7c80346988358e020744de6635b`
- **Measured:** `postgresql/postgresql.log`: 1,000,000 lines, 1,000,000 rows

**411,917,670 bytes ingested.** As a plain file that takes 411,918,336 bytes on this disk; kibbr keeps it in 9,547,776, 97.7% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 411,917,670 | 411,918,336 | 1.000 | - | - |
| gzip -9 | 13,977,981 | 13,979,648 | 0.034 | - | - |
| zstd -19, 8 thread(s) | 11,228,776 | 11,231,232 | 0.027 | - | - |
| kibbr, bulk load | 9,538,517 | 9,547,776 | 0.023 | 0.682 | 0.849 |
| kibbr, live day compacted | 9,538,960 | 9,547,776 | 0.023 | 0.682 | 0.850 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /opt/kibbr/testdata/compression/postgresql/logs-858bd4fccc0f.ndjson
  1,000,000 lines, 1,000,000 rows as load d9a55d98acee7117: 139 row groups in 1 file(s) over 1 day(s), loaded in 16.2s

                                   bytes   disk blocks    x raw
  raw                        411,917,670       100,566    1.000
  gzip -9                     13,977,981         3,413    0.034
  zstd -19                    11,228,776         2,742    0.027
  kibbr total                  9,538,517         2,331    0.023
    data files                 9,511,765         2,323    0.023
      column data              9,455,673                  0.023
      row group headers           44,010                  0.000
      schema records               3,325                  0.000
      footers                      8,733                  0.000
      file headers                    24                  0.000
    catalog.log                   26,500             7    0.000
    manifest.log                     252             1    0.000

  kibbr total is 0.682 of gzip -9 and 0.849 of zstd -19
  gzip -9 ran on 1 thread in 5.5s; zstd -19 on 8 thread(s) in 79.6s
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 1,000,000 lines, 1,000,000 rows, 2,967 row groups in 1 file(s) over 1 day(s), ingested in 23.4s
  after compaction: 1,000,000 rows, 139 row groups in 1 file(s) over 1 day(s), compacted in 4.5s

                                   bytes   disk blocks    x raw
  kibbr total, before         14,425,051         3,525    0.035
    data files                14,398,182         3,517    0.035
      column data             13,317,072                  0.032
      row group headers          967,220                  0.002
      schema records               3,325                  0.000
      footers                    110,541                  0.000
      file headers                    24                  0.000
    catalog.log                   26,500             7    0.000
    manifest.log                     369             1    0.000
  kibbr total, after           9,538,960         2,331    0.023
    data files                 9,511,810         2,323    0.023
      column data              9,455,673                  0.023
      row group headers           44,010                  0.000
      schema records               3,325                  0.000
      footers                      8,778                  0.000
      file headers                    24                  0.000
    catalog.log                   26,500             7    0.000
    manifest.log                     650             1    0.000

  after compaction is 0.661 of before in bytes, 1.000 in files and 0.047 in row groups
```

## What it was measured with

- kibbr 0.4.0, commit `4a77f39f5165adeae9f91f62a48c560908205e56`
- `kibbr bench-storage logs-858bd4fccc0f.ndjson --scratch <scratch>`
- Linux 7.0.0-1014-aws, x86_64, Intel(R) Xeon(R) Platinum 8488C, 8 cores (4 physical), 16,462,835,712 bytes of memory, on aws c7i.2xlarge, ap-southeast-2
- 0 warning(s) in bench-storage's log; 128.4 s start to end, download included
