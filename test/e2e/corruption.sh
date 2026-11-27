#!/usr/bin/env bash
# Corrupts a nugget on the compose vein's disk and checks Adit refuses to
# serve it. Requires `make up`.
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${VEIN_VOLUME:=lode_veindata}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bucket="corrupt-$(date +%s)"
head -c 100000 /dev/urandom >"$work/obj"

aws s3 mb "s3://$bucket" >/dev/null
aws s3 cp "$work/obj" "s3://$bucket/obj" >/dev/null
aws s3 cp "s3://$bucket/obj" "$work/before" >/dev/null
cmp -s "$work/obj" "$work/before" || { echo "FAIL: clean read differs" >&2; exit 1; }

# Flip bytes in the middle of the newest segment (the object we just wrote).
docker run --rm -v "$VEIN_VOLUME:/data" alpine sh -c '
  seg=$(ls /data/segments/*.seg | tail -1)
  size=$(stat -c %s "$seg")
  printf "\377\377\377\377" | dd of="$seg" bs=1 seek=$((size - 50000)) conv=notrunc 2>/dev/null'

if aws s3 cp "s3://$bucket/obj" "$work/after" >/dev/null 2>"$work/err"; then
  if cmp -s "$work/obj" "$work/after"; then
    echo "FAIL: corruption was not applied" >&2
  else
    echo "FAIL: corrupt bytes were served" >&2
  fi
  exit 1
fi
grep -q "InternalError\|500" "$work/err" || { echo "FAIL: unexpected error: $(cat "$work/err")" >&2; exit 1; }
echo "corruption: read of corrupted nugget was refused ($(head -c 120 "$work/err"))"
