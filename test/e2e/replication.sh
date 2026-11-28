#!/usr/bin/env bash
# Phase 3 exit check: kill a vein in the middle of an upload (which must
# still succeed), then read everything back with each vein down in turn.
# Defaults drive the compose cluster; override VEIN_STOP / VEIN_START (with
# __N__ for the vein number) to drive other setups.
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${VEINS:=1 2 3 4}"
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
bucket="rep-$(date +%s)"
head -c $((200 * 1024 * 1024)) /dev/urandom >"$work/big"
head -c $((3 * 1024 * 1024)) /dev/urandom >"$work/small"

aws s3 mb "s3://$bucket" >/dev/null
aws s3 cp "$work/small" "s3://$bucket/small" >/dev/null

echo "replication: uploading 200 MiB and killing vein2 mid-upload"
aws s3 cp "$work/big" "s3://$bucket/big" >"$work/upload.log" 2>&1 &
upload=$!
sleep 2
stop 2
if ! wait "$upload"; then
  cat "$work/upload.log" >&2
  echo "FAIL: upload failed after a vein died" >&2
  exit 1
fi
echo "  ok  upload survived losing vein2"
aws s3 cp "s3://$bucket/big" "$work/big.out" >/dev/null
cmp -s "$work/big" "$work/big.out" || { echo "FAIL: object differs" >&2; exit 1; }
echo "  ok  read back identical with vein2 down"
start 2
sleep "$SETTLE"

for v in $VEINS; do
  stop "$v"
  for key in big small; do
    aws s3 cp "s3://$bucket/$key" "$work/$key.out" >/dev/null
    cmp -s "$work/$key" "$work/$key.out" || { echo "FAIL: $key differs with vein$v down" >&2; exit 1; }
  done
  echo "  ok  reads with vein$v down"
  start "$v"
  sleep "$SETTLE"
done
aws s3 rm --recursive "s3://$bucket" >/dev/null && aws s3 rb "s3://$bucket" >/dev/null
echo "replication: all checks passed"
