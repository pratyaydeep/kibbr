# kibbr storage on postgresql, 2026-11-06

PostgreSQL 15 jsonlog under YCSB workloads A-E (Zenodo 10516402)

- **Source:** https://zenodo.org/records/10516402/files/postgresql.tar.gz?download=1, files matching `*.log`
- **Download:** `postgresql.tar.gz`, 15,296,662 bytes, sha256 `2a8c2d87da92401a1c0d404d7e953e6395e4d7c80346988358e020744de6635b`
- **Measured:** `postgresql/postgresql.log`: 1,000,000 lines, 1,000,000 rows

**411,917,670 bytes ingested.** As a plain file that takes 411,918,336 bytes on this disk; kibbr keeps it in 10,534,912, 97.4% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 411,917,670 | 411,918,336 | 1.000 | - | - |
| gzip -9 | 13,977,981 | 14,426,112 | 0.034 | - | - |
| zstd -19 | 11,214,796 | 11,538,432 | 0.027 | - | - |
| kibbr, bulk load | 9,542,666 | 10,534,912 | 0.023 | 0.683 | 0.851 |
| kibbr, live day compacted | 9,543,109 | 10,596,352 | 0.023 | 0.683 | 0.851 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /Users/pratyaydeep/repo/kibbr/testdata/compression/postgresql/unpacked/postgresql/postgresql.log
  1,000,000 lines, 1,000,000 rows as load cba99710dbf1efc3: 139 row groups in 1 file(s) over 1 day(s), loaded in 11.5s

                                   bytes   disk blocks    x raw
  raw                        411,917,670       100,566    1.000
  gzip -9                     13,977,981         3,522    0.034
  zstd -19                    11,214,796         2,817    0.027
  kibbr total                  9,542,666         2,572    0.023
    data files                 9,516,130         2,564    0.023
      column data              9,456,785                  0.023
      row group headers           46,651                  0.000
      schema records               3,631                  0.000
      footers                      9,039                  0.000
      file headers                    24                  0.000
    catalog.log                   26,284             7    0.000
    manifest.log                     252             1    0.000

  kibbr total is 0.683 of gzip -9 and 0.851 of zstd -19
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 1,000,000 lines, 1,000,000 rows, 2,967 row groups in 1 file(s) over 1 day(s), ingested in 18.0s
  after compaction: 1,000,000 rows, 139 row groups in 1 file(s) over 1 day(s), compacted in 2.9s

                                   bytes   disk blocks    x raw
  kibbr total, before         14,505,556         3,593    0.035
    data files                14,478,903         3,585    0.035
      column data             13,340,808                  0.032
      row group headers        1,023,593                  0.002
      schema records               3,631                  0.000
      footers                    110,847                  0.000
      file headers                    24                  0.000
    catalog.log                   26,284             7    0.000
    manifest.log                     369             1    0.000
  kibbr total, after           9,543,109         2,587    0.023
    data files                 9,516,175         2,579    0.023
      column data              9,456,785                  0.023
      row group headers           46,651                  0.000
      schema records               3,631                  0.000
      footers                      9,084                  0.000
      file headers                    24                  0.000
    catalog.log                   26,284             7    0.000
    manifest.log                     650             1    0.000

  after compaction is 0.658 of before in bytes, 1.000 in files and 0.047 in row groups
```

## What it was measured with

- kibbr 0.2.1, commit `cb990fbce3094a8b69fd98abf651c8343056077c`
- `kibbr bench-storage postgresql.log --scratch <scratch>`
- Darwin 23.3.0, arm64, Apple M2, 8 cores, 8,589,934,592 bytes of memory
- 36 warning(s) in bench-storage's log; 260.0 s start to end, download included
