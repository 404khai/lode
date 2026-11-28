#!/usr/bin/env bash
# Phase 5 exit check: a 4+2 erasure-coded bucket on six veins survives two
# simultaneous vein losses. Override VEIN_STOP / VEIN_START (with __N__) and
# LODE (the CLI command) to drive setups other than the compose cluster.
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${LODE:=./bin/lode -dsn postgres://lode:lode@localhost:5432/lode?sslmode=disable}"
: "${VEIN_STOP:=docker compose -f deploy/docker-compose.yml kill vein__N__}"
: "${VEIN_START:=docker compose -f deploy/docker-compose.yml start vein__N__}"
: "${SETTLE:=3}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }
stop() { eval "${VEIN_STOP//__N__/$1}" >/dev/null; }
start() { eval "${VEIN_START//__N__/$1}" >/dev/null; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bucket="ec-$(date +%s)"
eval "$LODE bucket create $bucket -ec 4+2"
head -c $((60 * 1024 * 1024)) /dev/urandom >"$work/big"
head -c 5000 /dev/urandom >"$work/small"
aws s3 cp "$work/big" "s3://$bucket/big" >/dev/null
aws s3 cp "$work/small" "s3://$bucket/small" >/dev/null
echo "erasure: wrote 60 MiB + 5 KB to a 4+2 bucket"

for pair in "1 2" "3 6" "4 5"; do
  set -- $pair
  stop "$1"; stop "$2"
  for key in big small; do
    aws s3 cp "s3://$bucket/$key" "$work/$key.out" >/dev/null
    cmp -s "$work/$key" "$work/$key.out" || { echo "FAIL: $key differs with vein$1 and vein$2 down" >&2; exit 1; }
  done
  echo "  ok  reads with vein$1 and vein$2 down"
  start "$1"; start "$2"
  sleep "$SETTLE"
done
aws s3 rm --recursive "s3://$bucket" >/dev/null && aws s3 rb "s3://$bucket" >/dev/null
echo "erasure: all checks passed"
