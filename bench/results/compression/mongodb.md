# kibbr storage on mongodb, 2026-11-06

MongoDB JSON logs under YCSB workloads A-E (Zenodo 11075361), from YScope's CLP datasets: 260 rotated files, 69.6 GB

- **Source:** https://zenodo.org/records/11075361/files/mongodb.tar.gz?download=1, files matching `mongod.log.*`
- **Download:** `mongodb.tar.gz`, 1,590,845,765 bytes, sha256 `ffc0ac59d70c23b4c9f355386da1033d75d0af2ea2a7897f9f1575f8f6eafccf`
- **Measured:** `mongodb/mongod.log.2023-03-22T03-45-46`, `mongodb/mongod.log.2023-03-22T03-47-16`, `mongodb/mongod.log.2023-03-22T03-48-25`, `mongodb/mongod.log.2023-03-22T03-49-34`, `mongodb/mongod.log.2023-03-22T03-50-43`, `mongodb/mongod.log.2023-03-22T03-51-53`, `mongodb/mongod.log.2023-03-22T03-53-02`, `mongodb/mongod.log.2023-03-22T03-54-11`, `mongodb/mongod.log.2023-03-22T03-55-20`, `mongodb/mongod.log.2023-03-22T03-56-29` and 250 more (260 files, joined in this order): 186,287,600 lines, 186,287,600 rows

**69,582,861,765 bytes ingested.** As a plain file that takes 69,599,236,096 bytes on this disk; kibbr keeps it in 403,042,304, 99.4% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 69,582,861,765 | 69,599,236,096 | 1.000 | - | - |
| gzip -9 | 1,370,504,369 | 1,376,763,904 | 0.020 | - | - |
| kibbr, bulk load | 399,554,041 | 403,042,304 | 0.006 | 0.292 | - |
| kibbr, live day compacted | 366,218,726 | 369,430,528 | 0.005 | 0.267 | - |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /Users/pratyaydeep/repo/kibbr/testdata/compression/mongodb/logs-d25e7831eeff.ndjson
  186,287,600 lines, 186,287,600 rows as load 9f9be7ac1bd74c20: 63,786 row groups in 1 file(s) over 1 day(s), loaded in 1138.8s

                                   bytes   disk blocks    x raw
  raw                     69,582,861,765    16,992,001    1.000
  gzip -9                  1,370,504,369       336,124    0.020
  kibbr total                399,554,041        98,399    0.006
    data files               399,274,842        98,317    0.006
      column data            380,941,548                  0.005
      row group headers       15,939,496                  0.000
      schema records              47,283                  0.000
      footers                  2,346,491                  0.000
      file headers                    24                  0.000
    catalog.log                  278,945            81    0.000
    manifest.log                     254             1    0.000

  kibbr total is 0.292 of gzip -9; zstd -19 was not run (--no-zstd)
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 186,287,600 lines, 186,287,600 rows, 61,790 row groups in 1 file(s) over 1 day(s), ingested in 1614.8s
  after compaction: 186,287,600 rows, 22,871 row groups in 1 file(s) over 1 day(s), compacted in 323.3s

                                   bytes   disk blocks    x raw
  kibbr total, before        411,836,907       102,484    0.006
    data files               411,557,591       102,413    0.006
      column data            392,794,688                  0.006
      row group headers       16,440,961                  0.000
      schema records              47,283                  0.000
      footers                  2,274,635                  0.000
      file headers                    24                  0.000
    catalog.log                  278,945            70    0.000
    manifest.log                     371             1    0.000
  kibbr total, after         366,218,726        90,193    0.005
    data files               365,939,127        90,122    0.005
      column data            360,234,913                  0.005
      row group headers        4,783,311                  0.000
      schema records              47,283                  0.000
      footers                    873,596                  0.000
      file headers                    24                  0.000
    catalog.log                  278,945            70    0.000
    manifest.log                     654             1    0.000

  after compaction is 0.889 of before in bytes, 1.000 in files and 0.370 in row groups
```

## What it was measured with

- kibbr 0.2.1, commit `5feb7d9a77e85a6994444dc6ab29dcde8d5aebc1`, **with uncommitted changes**
- `kibbr bench-storage logs-d25e7831eeff.ndjson --scratch <scratch> --time-columns t.$date --no-zstd`
- Darwin 23.3.0, arm64, Apple M2, 8 cores, 8,589,934,592 bytes of memory
- 10 warning(s) in bench-storage's log; 3389.1 s start to end, download included
