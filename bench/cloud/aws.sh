#!/usr/bin/env bash
# A kibbr bench on a fresh EC2 box, from the laptop: the box builds kibbr, runs one job,
# uploads what it measured to S3 and terminates itself. No SSH, no open port, nothing to
# remember to switch off. What the box runs is bench/cloud/box.sh; this is everything
# around it.
#
#   bench/cloud/aws.sh setup                 # once: the bucket, and a role that can only
#                                            # write to it; checks the region and quota
#   bench/cloud/aws.sh launch                # the compression job on the default box
#   bench/cloud/aws.sh status                # the latest run: its status and last lines
#   bench/cloud/aws.sh fetch                 # its results, into the repository
#   bench/cloud/aws.sh runs                  # every run in the bucket, and live boxes
#   bench/cloud/aws.sh kill                  # terminate every live bench box
#
# launch's flags:
#
#   --job JOB        compression                                     [compression]
#   --type TYPE      the instance type                               [m7i.2xlarge]
#   --disk GB        the root volume, gp3                            [100]
#   --spot           a spot instance: usually much cheaper (aws ec2
#                    describe-spot-price-history says how much), but AWS can take it
#                    back, and a run taken back uploads nothing
#   --ref REF        what is measured: a commit, branch or tag that is on GitHub   [HEAD,
#                    which must be pushed: the box clones from GitHub, not from here]
#   --tags "A B"     compression: these sources only            [every one not measured]
#   --no-zstd        compression: as bench/compression.py --no-zstd
#   --zstd-threads N compression: as bench/compression.py --zstd-threads; 1 is the plain,
#                    smallest zstd -19          [every vCPU the box has]
#   --hours H        the box terminates itself after H hours, finished or not   [8]
#
# The region is $AWS_REGION, else the CLI's own, else ap-south-2 (Hyderabad): the same
# price as Mumbai and the nearer of the two to Bengaluru. Hyderabad is an opt-in region:
# a new account turns it on in the console, Account -> AWS Regions, once. The bucket is
# kibbr-bench-<account id>, in that region; $KIBBR_BENCH_BUCKET names another.
#
# The compression job on m7i.2xlarge, cockroachdb, elasticsearch and spark: about 1 hour
# (about ₹60 with the volume, before GST, at about $0.42 an hour on demand in ap-south-2)
# with zstd -19 on all 8 vCPUs, and about 3 hours with --zstd-threads 1, where zstd -19 is
# most of it. MongoDB's zstd -19 is about 7 hours on one thread and under 2 on eight:
# give it --hours 6.

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

die() { echo "aws.sh: $*" >&2; exit 1; }
command -v aws >/dev/null || die "no aws CLI: brew install awscli, then aws configure"

REGION=${AWS_REGION:-$(aws configure get region 2>/dev/null)}
REGION=${REGION:-ap-south-2}
export AWS_REGION=$REGION AWS_PAGER=""
ROLE=kibbr-bench

account() { aws sts get-caller-identity --query Account --output text; }
bucket() {
  if [ -n "${KIBBR_BENCH_BUCKET:-}" ]; then echo "$KIBBR_BENCH_BUCKET"; return; fi
  local id; id=$(account) || die "aws sts get-caller-identity failed: is the CLI configured?"
  echo "kibbr-bench-$id"
}

# The newest run's name, or the one given.
run_name() {
  if [ -n "${1:-}" ]; then echo "$1"; return; fi
  aws s3 ls "s3://$(bucket)/" | awk '$1 == "PRE" {print $2}' | sed 's,/$,,' | sort | tail -1
}

setup() {
  local b; b=$(bucket)
  echo "region $REGION, bucket $b"
  aws ec2 describe-availability-zones --query 'AvailabilityZones[0].ZoneName' --output text >/dev/null 2>&1 ||
    die "$REGION does not answer: if it is ap-south-2, turn it on in the console (Account -> AWS Regions) and wait a few minutes"

  if aws s3api head-bucket --bucket "$b" 2>/dev/null; then
    echo "bucket: there"
  else
    if [ "$REGION" = us-east-1 ]; then
      aws s3api create-bucket --bucket "$b" >/dev/null || die "could not make the bucket"
    else
      aws s3api create-bucket --bucket "$b" --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null ||
        die "could not make the bucket"
    fi
    echo "bucket: made"
  fi

  if aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    echo "role: there"
  else
    aws iam create-role --role-name "$ROLE" --description "kibbr bench box: write its results, nothing else" \
      --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
      >/dev/null || die "could not make the role"
    echo "role: made"
  fi
  # Written every time, so a bucket renamed later is the one the role can write to.
  aws iam put-role-policy --role-name "$ROLE" --policy-name write-results --policy-document \
    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::$b/*\"}]}" ||
    die "could not set the role's policy"

  if aws iam get-instance-profile --instance-profile-name "$ROLE" >/dev/null 2>&1; then
    echo "instance profile: there"
  else
    aws iam create-instance-profile --instance-profile-name "$ROLE" >/dev/null ||
      die "could not make the instance profile"
    aws iam add-role-to-instance-profile --instance-profile-name "$ROLE" --role-name "$ROLE" ||
      die "could not put the role in the instance profile"
    echo "instance profile: made (IAM takes a few seconds to see it; launch waits if it must)"
  fi

  # A new account's vCPU quota is often below one 8-vCPU box.
  local od spot
  od=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A \
    --query 'Quota.Value' --output text 2>/dev/null)
  spot=$(aws service-quotas get-service-quota --service-code ec2 --quota-code L-34B43A08 \
    --query 'Quota.Value' --output text 2>/dev/null)
  echo "vCPU quota in $REGION: on-demand standard ${od:-unknown}, spot standard ${spot:-unknown}"
  case "${od%.*}" in
    ''|*[!0-9]*) ;;
    *) [ "${od%.*}" -ge 8 ] || echo "  below 8: m7i.2xlarge will be refused. Ask for 16 in the console:" \
         "Service Quotas -> Amazon EC2 -> Running On-Demand Standard instances (usually granted within a day)" ;;
  esac
}

launch() {
  local job=compression type=m7i.2xlarge disk=100 spot=0 ref="" tags="" nozstd=0 zthreads="" hours=8
  while [ $# -gt 0 ]; do
    case "$1" in
      --job) job=$2; shift 2 ;;
      --type) type=$2; shift 2 ;;
      --disk) disk=$2; shift 2 ;;
      --spot) spot=1; shift ;;
      --ref) ref=$2; shift 2 ;;
      --tags) tags=$2; shift 2 ;;
      --no-zstd) nozstd=1; shift ;;
      --zstd-threads) zthreads=$2; shift 2 ;;
      --hours) hours=$2; shift 2 ;;
      *) die "launch: unknown argument $1" ;;
    esac
  done
  case "$zthreads" in
    '') ;;
    *[!0-9]*|0*) die "launch: --zstd-threads wants a whole number, 1 or more: $zthreads" ;;
  esac
  # Whole numbers: box.sh's watchdog is `shutdown +$((hours * 60))`, and bash's arithmetic
  # refuses 9.5 with an error the box carries on past, with no watchdog at all.
  case "$hours" in
    ''|*[!0-9]*|0*) die "launch: --hours wants a whole number, 1 or more: $hours" ;;
  esac
  case "$disk" in
    ''|*[!0-9]*|0*) die "launch: --disk wants a whole number of GB, 1 or more: $disk" ;;
  esac

  # The box clones GitHub: what is measured has to be there.
  if [ -z "$ref" ]; then
    ref=$(git rev-parse HEAD) || die "not in a git checkout"
    git diff --quiet HEAD -- src crates Cargo.toml Cargo.lock bench ||
      echo "aws.sh: uncommitted changes to the source or bench/ are not on the box: it measures $ref" >&2
  fi
  local named
  if named=$(git ls-remote --exit-code origin "$ref" 2>/dev/null); then
    # A branch or tag, as it is on GitHub now: a push during the run does not change it.
    ref=$(echo "$named" | head -1 | cut -f1)
  else
    git fetch -q origin >/dev/null 2>&1
    git branch -r --contains "$ref" 2>/dev/null | grep -q . ||
      die "$ref is not on GitHub: push it first, since the box clones from there"
  fi

  local b arch ami root run ud market=()
  b=$(bucket)
  arch=$(aws ec2 describe-instance-types --instance-types "$type" \
    --query 'InstanceTypes[0].ProcessorInfo.SupportedArchitectures[0]' --output text 2>/dev/null) ||
    die "$type is not offered in $REGION"
  case "$arch" in x86_64) arch=amd64 ;; arm64) ;; *) die "$type: architecture $arch" ;; esac
  ami=$(aws ssm get-parameter --name "/aws/service/canonical/ubuntu/server/24.04/stable/current/$arch/hvm/ebs-gp3/ami-id" \
    --query Parameter.Value --output text) || die "no Ubuntu 24.04 image found in $REGION"
  root=$(aws ec2 describe-images --image-ids "$ami" --query 'Images[0].RootDeviceName' --output text)
  run=$(date -u +%Y%m%dT%H%M%SZ)-$job-$type

  ud=$(mktemp)
  {
    echo '#!/bin/bash'
    echo "export KIBBR_JOB=$job KIBBR_RUN=$run KIBBR_BUCKET=s3://$b KIBBR_REF=$ref KIBBR_MAX_HOURS=$hours"
    echo "export KIBBR_TAGS='$tags' KIBBR_NO_ZSTD=$nozstd KIBBR_ZSTD_THREADS=$zthreads"
    sed 1d bench/cloud/box.sh
  } > "$ud"
  [ "$spot" = 0 ] || market=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')

  local id _
  for _ in 1 2 3 4 5 6; do
    id=$(aws ec2 run-instances --image-id "$ami" --instance-type "$type" \
      --iam-instance-profile "Name=$ROLE" \
      --instance-initiated-shutdown-behavior terminate \
      --metadata-options HttpTokens=required,HttpEndpoint=enabled \
      --block-device-mappings "[{\"DeviceName\":\"$root\",\"Ebs\":{\"VolumeSize\":$disk,\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
      --user-data "file://$ud" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=kibbr-bench},{Key=kibbr-run,Value=$run}]" \
      ${market[@]+"${market[@]}"} \
      --query 'Instances[0].InstanceId' --output text 2>"$ud.err") && break
    # A profile made a moment ago is not yet one EC2 can see.
    grep -q "Invalid IAM Instance Profile" "$ud.err" || { cat "$ud.err" >&2; rm -f "$ud" "$ud.err"; die "run-instances failed"; }
    sleep 10
  done
  rm -f "$ud" "$ud.err"
  [ -n "${id:-}" ] || die "the instance profile is still not seen: run launch again in a minute"

  echo "launched $id: $type in $REGION$([ "$spot" = 1 ] && echo ", spot"), $job at $ref"
  echo "run $run, results to s3://$b/$run/"
  echo "it terminates itself when done, or after $hours h. Watch: bench/cloud/aws.sh status"
}

status() {
  local b r; b=$(bucket); r=$(run_name "${1:-}")
  [ -n "$r" ] || die "no runs in s3://$b yet"
  echo "run $r: $(aws s3 cp --only-show-errors "s3://$b/$r/status" - 2>/dev/null || echo 'no status yet (still setting up?)')"
  aws ec2 describe-instances --filters "Name=tag:kibbr-run,Values=$r" \
    --query 'Reservations[].Instances[].[InstanceId,State.Name,InstanceType,LaunchTime]' --output text
  echo "--- last of log.txt (uploaded every 5 minutes)"
  aws s3 cp --only-show-errors "s3://$b/$r/log.txt" - 2>/dev/null | tail -n "${LINES_SHOWN:-25}"
}

fetch() {
  local b r dir; b=$(bucket); r=$(run_name "${1:-}")
  [ -n "$r" ] || die "no runs in s3://$b yet"
  dir=testdata/cloud/$r
  mkdir -p "$dir"
  aws s3 cp --only-show-errors --recursive "s3://$b/$r/" "$dir/" || die "could not download s3://$b/$r/"
  echo "run $r: $(cat "$dir/status" 2>/dev/null || echo 'no status')"
  [ -f "$dir/results.tgz" ] || die "no results.tgz yet: the box is still running, or it was lost (see $dir/log.txt)"
  tar xzf "$dir/results.tgz" -C "$dir" || die "could not unpack $dir/results.tgz"
  # The box started from this repository's results and only added to them, so its copy
  # replaces ours; git diff shows what it added.
  if [ -f "$dir/bench/results/compression.json" ]; then
    cp "$dir/bench/results/compression.json" bench/results/compression.json
    mkdir -p bench/results/compression
    cp "$dir"/bench/results/compression/*.md bench/results/compression/ 2>/dev/null
    echo "bench/results/compression.json and compression/*.md updated: git diff bench/results"
  fi
  echo "everything the box wrote, its log and box.txt: $dir/"
}

runs() {
  local b; b=$(bucket)
  echo "runs in s3://$b:"
  aws s3 ls "s3://$b/" | awk '$1 == "PRE" {print "  " $2}' | sed 's,/$,,'
  echo "live bench boxes in $REGION:"
  # shellcheck disable=SC2016
  aws ec2 describe-instances --filters Name=tag:Name,Values=kibbr-bench \
    Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].[InstanceId,State.Name,InstanceType,Tags[?Key==`kibbr-run`]|[0].Value]' --output text
}

kill_boxes() {
  local ids
  ids=$(aws ec2 describe-instances --filters Name=tag:Name,Values=kibbr-bench \
    Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  [ -n "$ids" ] || { echo "no live bench boxes in $REGION"; return; }
  # shellcheck disable=SC2086
  aws ec2 terminate-instances --instance-ids $ids --query 'TerminatingInstances[].[InstanceId,CurrentState.Name]' --output text
}

case "${1:-}" in
  setup) setup ;;
  launch) shift; launch "$@" ;;
  status) status "${2:-}" ;;
  fetch) fetch "${2:-}" ;;
  runs) runs ;;
  kill) kill_boxes ;;
  *) sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
