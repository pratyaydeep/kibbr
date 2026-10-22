# Run it in a container

```
docker build -t kibbr .                    # the image: one static binary on distroless
docker run -d --name kibbr --stop-timeout 30 -p 127.0.0.1:7700:7700 -v kibbr-data:/var/lib/kibbr kibbr serve
curl -s 127.0.0.1:7700/health              # {"ok":true,"status":"ok","version":"0.1.1","source":"https://github.com/pratyaydeep/kibbr"}
curl -s -X POST 127.0.0.1:7700/query -H 'Content-Type: application/json' --data '{"q":"COUNT LOG {}"}'
docker stop kibbr                          # a clean stop: exit code 0
docker logs kibbr 2>&1 | tail -6           # the final flush, the checkpoint, ... "exit 0"
docker rm kibbr                            # the volume keeps the data; run again, same count
```

- **The image** is the one binary, linked statically against musl, on
  `gcr.io/distroless/static-debian12:nonroot`: 13,999,018 bytes, no shell, user 65532. It
  was built on this laptop in 2 minutes 30 seconds, the two base images already pulled.
- Its `HEALTHCHECK` is `kibbr health`, since there is no curl in it to ask with.
- **What it sets**, each a variable a flag still wins over: the data under the volume
  (`/var/lib/kibbr/data`, the trie snapshot beside it), every interface on port 7700 -
  loopback inside a container is reachable from nowhere - and the log as JSON.
- **`--stop-timeout 30`.** Docker kills a container ten seconds after it asks it to stop; a
  kibbr with a request in flight gives it 25 (`shutdown_deadline`) before the final flush.
  Killed early, nothing acknowledged is lost and the next start reads the log back.
- **Ingest, stop, start again on the same volume, same count.** Measured with 50,000 lines
  of `gen-logs`, every row still in a buffer when the stop came; `docker stop` took 1.07 s:

```
{..."msg":"final flush - 50,000 row(s) into 13,691 row group(s) in 31 file(s), 31 sealed, 8,020,407 byte(s), 0.77s"}
{..."msg":"WAL checkpoint - position 50,000 of 50,000; 1 segment(s), 4,197,272 byte(s)"}
{..."msg":"trie.json - /var/lib/kibbr/trie.json 2,316,614 byte(s), 864 template(s)"}
{..."msg":"close stores - 864 store(s), 50,000 row(s) written"}
{..."msg":"close WAL - closed"}
{..."msg":"exit 0"}
```

- `bench/run.sh` runs `kibbr bench` in a container: [bench/README.md](../bench/README.md).
- `deploy/` holds the Kubernetes manifests - a StatefulSet of one pod and its volume - and
  the two Vector sinks; [deploy/README.md](../deploy/README.md) says what of them was
  checked and what was not.
