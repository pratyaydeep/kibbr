# scripts/bench/cloud

A bench on a fresh EC2 box, started from the laptop. The box clones kibbr from GitHub,
builds it, runs one job, uploads what it measured to S3 and terminates itself: no SSH, no
open port, and nothing left running to pay for. `box.sh` is what the box runs; `aws.sh` is
everything around it.

## Once

```
brew install awscli
aws configure                 # an IAM user's access key, not the root account's; region ap-south-2
scripts/bench/cloud/aws.sh setup
```

`setup` makes a bucket, `kibbr-bench-<account id>`, and a role the box runs as that can
write to that bucket and do nothing else. It is safe to run again.

Two things a new account may need first, and `setup` checks both:

- **Hyderabad (`ap-south-2`) is an opt-in region.** Turn it on in the console, Account ->
  AWS Regions, and give it a few minutes.
- **The vCPU quota.** A new account's on-demand quota is often below 8, the size of one
  `m7i.2xlarge`. Service Quotas -> Amazon EC2 -> Running On-Demand Standard instances,
  ask for 16.

## Each run

```
git push                                  # the box measures what is on GitHub
scripts/bench/cloud/aws.sh launch                 # every compression source not measured yet
scripts/bench/cloud/aws.sh status                 # where it is: the log is uploaded every 5 minutes
scripts/bench/cloud/aws.sh fetch                  # when status says done
git diff scripts/bench/results
```

`fetch` puts the results in `scripts/bench/results/compression.json` and
`scripts/bench/results/compression/<tag>.md`, and everything the box wrote, its whole log and
`box.txt` (what the box was), under `target/scratch/cloud/<run>/`.

## Jobs

| job | what it runs | box | about |
|---|---|---|---|
| `compression` | `scripts/bench/compression.py`: every source in `scripts/bench/compression.json` not in `scripts/bench/results/compression.json` yet | `m7i.2xlarge`, 100 GB gp3 | 1 h and about ₹60 for cockroachdb, elasticsearch and spark, zstd -19 on all 8 vCPUs; about 3 h with `--zstd-threads 1` |

```
scripts/bench/cloud/aws.sh launch --tags "spark"            # one source
scripts/bench/cloud/aws.sh launch --no-zstd                 # without zstd -19, most of the time
scripts/bench/cloud/aws.sh launch --zstd-threads 1          # the plain, smallest zstd -19: slower
scripts/bench/cloud/aws.sh launch --spot                    # usually much cheaper (aws ec2 describe-spot-price-history says how much)
```

The compressed bytes are the same on any machine; the load and compaction times are this
box's, and say so. A run on spot is fine for bytes, but AWS can take the box back, and a
box taken back uploads nothing past its last 5-minute log. A box that hangs terminates
after `--hours` (8).

zstd -19's bytes depend on its thread count: 1 is the plain encoder, the smallest; more
split the input into jobs, a little larger and close to that many times faster. Each
result records the count it ran with, as `zstd_19_threads`. An input smaller than one
zstd job, about 32 MB at level 19, gets no speedup from more threads.

MongoDB's result has no `zstd -19` column: on one thread it is about 7 hours of the
69.6 GB, and under 2 on eight. To fill it in, take its entry out of
`scripts/bench/results/compression.json`, push, and `launch --tags mongodb --disk 250 --hours 6`.

## When something goes wrong

`aws.sh status` shows the status file (`running`, `done`, or `failed at <step>`) and the
end of the log. `aws.sh runs` lists every run and any box still alive; `aws.sh kill`
terminates every bench box in the region.
