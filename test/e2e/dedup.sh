#!/usr/bin/env bash
# Uploads the same file twice under different keys and checks the second
# upload stored nothing new (every replica of every nugget already existed).
# Usage: DEDUP_SIZE_MB=1024 test/e2e/dedup.sh
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${LODE_METRICS:=http://localhost:9090/metrics}"
: "${DEDUP_SIZE_MB:=1024}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }
written() { curl -sf "$LODE_METRICS" | awk '/^lode_cluster_bytes_stored_total /{printf "%d", $2} END {if (!NR) print 0}'; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bucket="dedup-$(date +%s)"
size=$((DEDUP_SIZE_MB * 1024 * 1024))
head -c "$size" /dev/urandom >"$work/blob"

aws s3 mb "s3://$bucket" >/dev/null
before=$(written)
aws s3 cp "$work/blob" "s3://$bucket/first" >/dev/null
mid=$(written)
aws s3 cp "$work/blob" "s3://$bucket/second" >/dev/null
after=$(written)

first=$((mid - before)); second=$((after - mid))
echo "dedup: file ${DEDUP_SIZE_MB} MiB; first upload wrote $first bytes, second wrote $second bytes"
[ "$first" -ge "$size" ] || { echo "FAIL: first upload wrote less than the file size" >&2; exit 1; }
[ "$second" -le $((size / 20)) ] || { echo "FAIL: second upload of identical content stored $second bytes" >&2; exit 1; }

aws s3 cp "s3://$bucket/second" "$work/back" >/dev/null
cmp -s "$work/blob" "$work/back" || { echo "FAIL: deduplicated object reads back wrong" >&2; exit 1; }
aws s3 rm --recursive "s3://$bucket" >/dev/null && aws s3 rb "s3://$bucket" >/dev/null
echo "dedup: ok (second upload stored $((second * 100 / size))% of the file; first stored $((first / size))x for replication)"
