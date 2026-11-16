# kibbr storage on mongodb, 2026-11-12

MongoDB JSON logs under YCSB workloads A-E (Zenodo 11075361), from YScope's CLP datasets: 260 rotated files, 69.6 GB

- **Source:** https://zenodo.org/records/11075361/files/mongodb.tar.gz?download=1, files matching `mongod.log.*`
- **Download:** `mongodb.tar.gz`, 1,590,845,765 bytes, sha256 `ffc0ac59d70c23b4c9f355386da1033d75d0af2ea2a7897f9f1575f8f6eafccf`
- **Measured:** `mongodb/mongod.log.2023-03-22T03-45-46`, `mongodb/mongod.log.2023-03-22T03-47-16`, `mongodb/mongod.log.2023-03-22T03-48-25`, `mongodb/mongod.log.2023-03-22T03-49-34`, `mongodb/mongod.log.2023-03-22T03-50-43`, `mongodb/mongod.log.2023-03-22T03-51-53`, `mongodb/mongod.log.2023-03-22T03-53-02`, `mongodb/mongod.log.2023-03-22T03-54-11`, `mongodb/mongod.log.2023-03-22T03-55-20`, `mongodb/mongod.log.2023-03-22T03-56-29` and 250 more (260 files, joined in this order): 186,287,600 lines, 186,287,600 rows

**69,582,861,765 bytes ingested.** As a plain file that takes 69,582,872,576 bytes on this disk; kibbr keeps it in 403,660,800, 99.4% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 69,582,861,765 | 69,582,872,576 | 1.000 | - | - |
| gzip -9 | 1,370,504,369 | 1,370,509,312 | 0.020 | - | - |
| zstd -19, 8 thread(s) | 772,760,114 | 772,767,744 | 0.011 | - | - |
| kibbr, bulk load | 403,645,884 | 403,660,800 | 0.006 | 0.295 | 0.522 |
| kibbr, live day compacted | 366,284,541 | 366,297,088 | 0.005 | 0.267 | 0.474 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /opt/kibbr/testdata/compression/mongodb/logs-d25e7831eeff.ndjson
  186,287,600 lines, 186,287,600 rows as load 9f9be7ac1bd74c20: 70,594 row groups in 1 file(s) over 1 day(s), loaded in 2281.9s

                                   bytes   disk blocks    x raw
  raw                     69,582,861,765    16,988,006    1.000
  gzip -9                  1,370,504,369       334,597    0.020
  zstd -19                   772,760,114       188,664    0.011
  kibbr total                403,645,884        98,550    0.006
    data files               403,358,767        98,478    0.006
      column data            382,649,645                  0.005
      row group headers       18,068,891                  0.000
      schema records              47,946                  0.000
      footers                  2,592,261                  0.000
      file headers                    24                  0.000
    catalog.log                  286,863            71    0.000
    manifest.log                     254             1    0.000

  kibbr total is 0.295 of gzip -9 and 0.522 of zstd -19
  gzip -9 ran on 1 thread in 1088.4s; zstd -19 on 8 thread(s) in 7749.0s
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 186,287,600 lines, 186,287,600 rows, 61,791 row groups in 1 file(s) over 1 day(s), ingested in 2556.7s
  after compaction: 186,287,600 rows, 22,872 row groups in 1 file(s) over 1 day(s), compacted in 473.6s

                                   bytes   disk blocks    x raw
  kibbr total, before        412,275,182       100,656    0.006
    data files               411,987,948       100,584    0.006
      column data            392,871,534                  0.006
      row group headers       16,793,091                  0.000
      schema records              47,946                  0.000
      footers                  2,275,353                  0.000
      file headers                    24                  0.000
    catalog.log                  286,863            71    0.000
    manifest.log                     371             1    0.000
  kibbr total, after         366,284,541        89,428    0.005
    data files               365,997,024        89,356    0.005
      column data            360,246,541                  0.005
      row group headers        4,828,199                  0.000
      schema records              47,946                  0.000
      footers                    874,314                  0.000
      file headers                    24                  0.000
    catalog.log                  286,863            71    0.000
    manifest.log                     654             1    0.000

  after compaction is 0.888 of before in bytes, 1.000 in files and 0.370 in row groups
```

## What it was measured with

- kibbr 0.4.0, commit `4a77f39f5165adeae9f91f62a48c560908205e56`
- `kibbr bench-storage logs-d25e7831eeff.ndjson --scratch <scratch> --time-columns t.$date`
- Linux 7.0.0-1014-aws, x86_64, Intel(R) Xeon(R) Platinum 8488C, 8 cores (4 physical), 16,462,835,712 bytes of memory, on aws c7i.2xlarge, ap-southeast-2
- 6 warning(s) in bench-storage's log; 14586.7 s start to end, download included
