#!/usr/bin/env bash
# End-to-end check of Adit with the real aws CLI.
# Usage: LODE_ENDPOINT=http://localhost:9000 test/e2e/e2e.sh
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true

aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }
pass() { printf '  ok  %s\n' "$*"; }
fail() { printf '  FAIL %s\n' "$*" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bucket="e2e-$(date +%s)-$RANDOM"

echo "lode e2e against $LODE_ENDPOINT ($(command aws --version 2>&1))"

aws s3 mb "s3://$bucket" >/dev/null && pass "mb"
aws s3 ls | grep -q "$bucket" && pass "ls buckets"

echo "hello lode" >"$work/small.txt"
# 7 MiB: below the CLI's multipart threshold but spans two nuggets.
head -c $((7 * 1024 * 1024)) /dev/urandom >"$work/big.bin"
mkdir -p "$work/tree/sub"
for i in 1 2 3; do echo "file $i" >"$work/tree/f$i.txt"; done
echo "nested" >"$work/tree/sub/n.txt"

aws s3 cp "$work/small.txt" "s3://$bucket/small.txt" >/dev/null && pass "cp small up"
aws s3 cp "$work/big.bin" "s3://$bucket/data/big.bin" >/dev/null && pass "cp 7MiB up"
aws s3 cp --recursive "$work/tree" "s3://$bucket/tree" >/dev/null && pass "cp --recursive up"

aws s3 cp "s3://$bucket/small.txt" "$work/small.out" >/dev/null
cmp -s "$work/small.txt" "$work/small.out" && pass "cp small down (identical)" || fail "small object differs"
aws s3 cp "s3://$bucket/data/big.bin" "$work/big.out" >/dev/null
cmp -s "$work/big.bin" "$work/big.out" && pass "cp 7MiB down (identical)" || fail "big object differs"

aws s3api get-object --bucket "$bucket" --key data/big.bin --range bytes=4194300-4194309 "$work/range.out" >/dev/null
cmp -s <(tail -c +4194301 "$work/big.bin" | head -c 10) "$work/range.out" && pass "ranged GET across nuggets" || fail "range differs"

aws s3api head-object --bucket "$bucket" --key small.txt | grep -q '"ContentLength": 11' && pass "head-object"

listing=$(aws s3 ls "s3://$bucket/")
grep -q "PRE data/" <<<"$listing" && grep -q "PRE tree/" <<<"$listing" && grep -q "small.txt" <<<"$listing" && pass "ls with prefixes" || fail "ls: $listing"
[ "$(aws s3 ls --recursive "s3://$bucket/tree/" | wc -l | tr -d ' ')" = 4 ] && pass "ls --recursive" || fail "recursive listing"

aws s3 sync "s3://$bucket/tree" "$work/synced" >/dev/null
diff -r "$work/tree" "$work/synced" >/dev/null && pass "sync down" || fail "sync differs"

aws s3 rm "s3://$bucket/small.txt" >/dev/null && pass "rm"
if aws s3api head-object --bucket "$bucket" --key small.txt >/dev/null 2>&1; then fail "object still exists after rm"; fi
pass "read-after-delete returns 404"

aws s3 rm --recursive "s3://$bucket" >/dev/null && pass "rm --recursive"
aws s3 rb "s3://$bucket" >/dev/null && pass "rb"

echo "e2e: all checks passed"
