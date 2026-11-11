#!/usr/bin/env bash
# What a cloud bench box runs, start to end, with no one logged in: set up, build kibbr,
# run one job, upload what it measured, and switch itself off. `bench/cloud/aws.sh launch`
# sends this file as the instance's user data, with the settings below written above it,
# and starts the instance so that switching off terminates it: a box that ran is a box
# that is gone, and the only thing it leaves is its upload.
#
# Settings, as environment variables (aws.sh writes them; a run by hand exports them):
#
#   KIBBR_JOB        compression                                      [compression]
#   KIBBR_RUN        the run's name: its folder in the bucket          [UTC time and job]
#   KIBBR_BUCKET     s3://bucket/prefix the run's folder goes under, or file:///dir for
#                    a run that only copies (a test on a machine of one's own)
#   KIBBR_REPO       the repository cloned            [https://github.com/pratyaydeep/kibbr]
#   KIBBR_REF        the commit, branch or tag measured                     [main]
#   KIBBR_TAGS       compression: the sources measured, space-separated  [every source not
#                    in bench/results/compression.json yet]
#   KIBBR_NO_ZSTD    compression: 1 is `--no-zstd`
#   KIBBR_ZSTD_THREADS  compression: N is `--zstd-threads N`; zstd -19's threads, 1 the
#                    plain and smallest              [unset: every CPU the box has]
#   KIBBR_MAX_HOURS  the box switches itself off after this, finished or not      [8]
#   KIBBR_KEEP       1: no watchdog and no switching off at the end (a run by hand)
#   KIBBR_SETUP      0: install nothing; the machine has git, python3, zstd, cargo
#   KIBBR_HOME       where the repository is cloned                    [/opt/kibbr]
#
# What the run's folder holds, uploaded as it goes:
#
#   log.txt          this script's output, every 5 minutes and at the end
#   status           running, done, or failed and the step that failed
#   results.tgz      at the end, done or failed: bench/results/ and each source's
#                    bench-storage output, a `urls` source's sha256 of each file it
#                    fetched, and box.txt - what the box was
#
# The box is the one place the numbers come from, so it is described once, in box.txt,
# and named in each result: `KIBBR_MACHINE` is the instance type and region, and
# bench/compression.py writes it beside the CPU and memory it reads itself.

set -uo pipefail
export HOME=${HOME:-/root}
export DEBIAN_FRONTEND=noninteractive

JOB=${KIBBR_JOB:-compression}
RUN=${KIBBR_RUN:-$(date -u +%Y%m%dT%H%M%SZ)-$JOB}
BUCKET=${KIBBR_BUCKET:?KIBBR_BUCKET: an s3:// or file:// place the results go}
REPO=${KIBBR_REPO:-https://github.com/pratyaydeep/kibbr}
REF=${KIBBR_REF:-main}
MAX_HOURS=${KIBBR_MAX_HOURS:-8}
KEEP=${KIBBR_KEEP:-0}
SETUP=${KIBBR_SETUP:-1}
SRC=${KIBBR_HOME:-/opt/kibbr}
OUT=$(mktemp -d)
LOG=$OUT/log.txt
STEP=start

exec > >(tee -a "$LOG") 2>&1
say() { echo "[$(date -u +%H:%M:%S)] $*"; }

# ── Where the results go ──
put() { # put FILE NAME: FILE as <run>/NAME
  case "$BUCKET" in
    file://*) mkdir -p "${BUCKET#file://}/$RUN" && cp "$1" "${BUCKET#file://}/$RUN/$2" ;;
    s3://*) aws s3 cp --only-show-errors "$1" "${BUCKET%/}/$RUN/$2" ;;
    *) echo "KIBBR_BUCKET is neither s3:// nor file://: $BUCKET" >&2; return 1 ;;
  esac
}
status() { echo "$*" > "$OUT/status"; put "$OUT/status" status; }

# ── The end, however it comes ──
UPLOADER=""
finish() {
  local code=$?
  [ -z "$UPLOADER" ] || kill "$UPLOADER" 2>/dev/null
  if [ "$code" = 0 ]; then status "done"; say "done"; else status "failed at $STEP (exit $code)"; say "failed at $STEP (exit $code)"; fi
  if [ -d "$SRC" ]; then
    cp "$OUT/box.txt" "$SRC/bench/results/box.txt" 2>/dev/null
    # The results and each run's own output, with a `urls` source's sha256 of each file it
    # fetched; not the downloads, nor the logs read out of them.
    (cd "$SRC" && shopt -s nullglob && tar czf "$OUT/results.tgz" --ignore-failed-read bench/results \
      testdata/compression/*/bench-storage-* testdata/compression/*/*.sha256) && put "$OUT/results.tgz" results.tgz
  fi
  sleep 1 # the last lines through tee
  put "$LOG" log.txt
  if [ "$KEEP" != 1 ]; then
    say "switching off"
    shutdown -h now
  fi
}
trap finish EXIT

# A box that hangs bills until someone notices; this one stops at the hour it was given.
[ "$KEEP" = 1 ] || shutdown -h "+$((MAX_HOURS * 60))" "kibbr bench: past KIBBR_MAX_HOURS"

say "kibbr bench box: job $JOB, run $RUN, $REPO at $REF, results to $BUCKET"

# ── Set up ──
STEP=setup
if [ "$SETUP" = 1 ]; then
  apt-get update -q && apt-get install -y -q build-essential pkg-config git python3 zstd curl unzip || exit 1
  if ! command -v aws >/dev/null; then
    ARCH=$(uname -m) # x86_64 or aarch64, as AWS names its CLI's builds
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH.zip" -o /tmp/awscli.zip &&
      unzip -q /tmp/awscli.zip -d /tmp && /tmp/aws/install || exit 1
  fi
  if ! command -v cargo >/dev/null; then
    # No toolchain here: the repository's rust-toolchain.toml says which, and rustup
    # installs that one on the first cargo inside it.
    curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none || exit 1
  fi
fi
# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
status running

# The log goes up as the run goes, so `aws.sh status` can show where it is.
( while sleep 300; do put "$LOG" log.txt; done ) &
UPLOADER=$!

# ── What the box is ──
TYPE=-; REGION=-
if TOKEN=$(curl -fsS -m 2 -X PUT http://169.254.169.254/latest/api/token \
     -H "X-aws-ec2-metadata-token-ttl-seconds: 300" 2>/dev/null); then
  md() { curl -fsS -m 2 -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/meta-data/$1"; }
  TYPE=$(md instance-type); REGION=$(md placement/region)
fi
KIBBR_MACHINE="aws $TYPE, $REGION"
[ "$TYPE" != - ] || KIBBR_MACHINE=$(hostname)
export KIBBR_MACHINE
{
  echo "run: $RUN"
  echo "machine: $KIBBR_MACHINE"
  echo "kernel: $(uname -srm)"
  echo "os: $(. /etc/os-release && echo "$PRETTY_NAME")"
  lscpu | grep -E '^(Model name|CPU\(s\)|Thread|Core|Socket)'
  free -b | sed -n 2p | awk '{print "memory bytes: " $2}'
  df -B1 --output=source,fstype,size "${SRC%/*}" | tail -1 | awk '{print "disk: " $1 " " $2 " " $3 " bytes"}'
} > "$OUT/box.txt"
cat "$OUT/box.txt"

# ── The code ──
STEP=clone
if [ ! -d "$SRC/.git" ]; then
  git clone -q "$REPO" "$SRC" || exit 1
fi
cd "$SRC" || exit 1
git fetch -q origin "$REF" && git checkout -q FETCH_HEAD || exit 1
say "kibbr at $(git rev-parse HEAD)"

STEP=build
cargo build --release --locked --bin kibbr || exit 1
target/release/kibbr --version

# ── The job ──
STEP=$JOB
case "$JOB" in
  compression)
    ARGS=(--no-build)
    for t in ${KIBBR_TAGS:-}; do ARGS+=(--tag "$t"); done
    [ "${KIBBR_NO_ZSTD:-0}" != 1 ] || ARGS+=(--no-zstd)
    [ -z "${KIBBR_ZSTD_THREADS:-}" ] || ARGS+=(--zstd-threads "$KIBBR_ZSTD_THREADS")
    say "bench/compression.py ${ARGS[*]}"
    python3 bench/compression.py "${ARGS[@]}" || exit 1
    ;;
  *)
    say "no such job: $JOB"; exit 2 ;;
esac
STEP=upload
