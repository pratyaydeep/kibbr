# Deploying kibbr on Kubernetes

**These manifests are untested.** There was no cluster to apply them to. They were
written from the design and the Vector 0.37.1 reference and checked as far as a laptop
can check them; the list of what that covers, and what it does not, is the second half
of this file. Read them as a first draft by someone who has not seen your cluster.

## What is here

```
k8s/namespace.yaml        the `kibbr` namespace, held to the `restricted` pod security profile
k8s/serviceaccount.yaml   the pod's identity, annotated with its IAM role (IRSA)
k8s/service.yaml          kibbr-svc, which Vector sends to; and the StatefulSet's headless Service
k8s/statefulset.yaml      ONE pod, and a 500 GiB gp3 volume at /var/lib/kibbr
k8s/storageclass.yaml     a `gp3` class - only for a cluster that has none; not in the kustomization
k8s/kustomization.yaml    kubectl apply -k deploy/k8s
iam/kibbr-policy.json         what the role may do: Put, Get, Delete and List on one bucket
iam/kibbr-trust-policy.json   who may assume it: the `kibbr` service account, through the cluster's OIDC provider
vector/kibbr.toml         what is added to Vector's configuration: one transform, two sinks
vector/kibbr.test.toml    what that file is checked with; not deployed
```

## One replica

`replicas: 1` is the design. A data directory has one writer, and nothing copies rows
between pods, so a second replica is a second, separate database: it would take half of
what Vector sends through the Service and answer every query with half of the logs.
While the one pod is down - a restart, a node drain - Vector keeps what it could not
send in a buffer on the node's disk and sends it when the pod is back.

## Before applying

Each of these is marked `REPLACE` where it is written:

- `k8s/statefulset.yaml`: the image. Build it from the Dockerfile at the repository's
  root and push it to a registry the cluster pulls from; name one build, not `latest`.
- `k8s/statefulset.yaml`: the CPU and memory. They are a guess, not a measurement.
- `k8s/serviceaccount.yaml` and `iam/kibbr-trust-policy.json`: the AWS account id, and in
  the trust policy the cluster's OIDC provider id (`aws eks describe-cluster --query
  cluster.identity.oidc.issuer`).
- `iam/kibbr-policy.json` and `vector/kibbr.toml`: the bucket, `my-kibbr-logs`.
- `vector/kibbr.toml`: `inputs = ["kubernetes_logs"]` is the id of the `kubernetes_logs`
  source in the configuration this is added to, if it has another name there.

Then, in this order:

```
aws iam create-role --role-name kibbr --assume-role-policy-document file://deploy/iam/kibbr-trust-policy.json
aws iam put-role-policy --role-name kibbr --policy-name kibbr-s3 --policy-document file://deploy/iam/kibbr-policy.json
kubectl apply -f deploy/k8s/storageclass.yaml     # only if the cluster has no `gp3` class
kubectl apply -k deploy/k8s
kubectl -n kibbr rollout status statefulset/kibbr
```

and add `vector/kibbr.toml` to the Vector DaemonSet's ConfigMap. Vector's own service
account needs `s3:PutObject` on the bucket's `logs/` prefix for the archive sink; that
role belongs to the DaemonSet and is not written here.

## What the pieces decide

- **Thirty seconds to stop.** `terminationGracePeriodSeconds: 30`, and kibbr gives the
  requests in flight 25 of them before it flushes, checkpoints and exits 0
  (`scripts/shutdown_deadline.sh`: gone 27.1 to 27.6 seconds after SIGTERM with a query
  and a half-sent body in flight). Raise the grace and `KIBBR__API__SHUTDOWN_DEADLINE`
  together.
- **All three probes are `/health`.** It answers `"ok":true` once the server listens and
  goes on answering it when the disk is full or the log cannot write
  (`"status":"degraded"`), because a restart does not empty a disk. The startup probe
  allows ten minutes: a start after a kill reads the write-ahead log back before it
  listens.
- **IRSA is for the cold tier, which is not built.** Nothing in this build of kibbr
  calls S3. The role is the one the design names.
- **Vector sends the application's own JSON line, not its envelope.** A
  `kubernetes_logs` event holds the container's line as a string in `.message`. The
  transform puts the parsed object in the event's place and keeps a line that is not a
  JSON object as `{"message": "..."}`. No pod, namespace or container name is kept;
  adding one adds a key to every template, so decide it before the first line is stored.
- **Both sinks read the transform.** What is archived to S3 is what was sent live.
- **The archive is an object for each Vector pod and hour**, sooner at 100 MB, under
  `logs/date=YYYY-MM-DD/hour=HH/`. A closed day is loaded with
  `aws s3 sync s3://<bucket>/logs/date=2026-10-09/ day/ && kibbr load day/` on a machine
  that has the files; the manifests do not put them on the pod's volume.

## What was checked

- **The Kubernetes manifests against the API's schemas**, with kubeconform in strict
  mode - an unknown or misspelled field is an error - for Kubernetes 1.25, 1.29 and
  1.31: 6 resources, 6 valid, each time.

  ```
  docker run --rm -v "$PWD/deploy/k8s:/m:ro" ghcr.io/yannh/kubeconform:latest \
    -strict -summary -kubernetes-version 1.29.0 \
    /m/namespace.yaml /m/serviceaccount.yaml /m/service.yaml /m/statefulset.yaml /m/storageclass.yaml
  ```

- **The kustomization builds**: `kubectl kustomize deploy/k8s` prints its four files'
  five resources.
- **`kubectl apply --dry-run=client` could not be run.** Without a cluster it stops at
  `The connection to the server localhost:8080 was refused`: it asks the API server
  which kinds exist before it reads a file.
- **`vector/kibbr.toml` loads in Vector 0.37.1 itself** - the pinned version, as a
  container - beside a `kubernetes_logs` source:

  ```
  docker run --rm -v "$PWD/deploy/vector:/cfg:ro" timberio/vector:0.37.1-distroless-libc \
    validate --no-environment /cfg/kibbr.toml /cfg/kibbr.test.toml
  ```

  `--no-environment` is what makes that possible with no cluster and no bucket, and it
  is also what it leaves out: no healthcheck is run and no credential is looked for.
  The design's `encoding.codec = "ndjson"` does not load on 0.37.1; the codec is `json`
  and the newlines come from `framing.method = "newline_delimited"`.
- **The transform**, by three `vector test` cases in `vector/kibbr.test.toml`: a JSON
  object line, a line of text, and a line that is JSON and not an object.

  ```
  docker run --rm -v "$PWD/deploy/vector:/cfg:ro" timberio/vector:0.37.1-distroless-libc \
    test /cfg/kibbr.toml /cfg/kibbr.test.toml
  ```

- **The live sink, end to end, once, on one machine**: Vector 0.37.1 in a container with
  a `demo_logs` source in the `kubernetes_logs` source's place, the transform and the
  `kibbr` sink as written but for the host name, and the kibbr image as the server. 2,000
  lines sent; Vector's healthcheck of `/health` passed; kibbr counted 2,000 rows in one
  template of the lines' own keys, none rejected, none shed.
- **The image the StatefulSet runs**: not root, a volume at `/var/lib/kibbr`, ingest,
  stop, start on the same volume, same count (the repository's README has the run).

## What was not

- **Nothing was applied to a cluster.** Not the scheduling, the volume's provisioning
  and its `fsGroup` ownership, the probes' timing, the pod security profile's verdict,
  the Service's name resolving, or a rolling restart.
- **IRSA**: the annotation and the two IAM documents were written from the EKS
  documentation and parse as JSON. No role was created and no token was exchanged.
- **The S3 sink has never written an object.** Its settings load; its key layout, its
  hourly batches, its memory on a busy node and whether a restarted Vector sends an
  unfinished hour again are as the reference describes them and were not observed.
  `kibbr load` has not read a file Vector wrote.
- **A line with no time of its own** is stored by the live door under the time it
  arrived. The archive does not record that time, so `kibbr load` gives such a line the
  time of the load, and refuses a day in which more than 1% of the lines are like that
  (`--max-arrival-share`). On such data the two doors do not agree.
- **Vector 0.37.1 on the cluster's `kubernetes_logs` events.** The transform was tested
  on events made by hand and by `demo_logs`, not on a container runtime's.
- **Resource requests, the disk buffers' sizes and the startup probe's ten minutes** are
  reasoned, not measured.
