#!/usr/bin/env bash
# Corrupts an object's bytes on the compose veins: with one corrupt replica
# Adit must serve the correct bytes from another; with every replica corrupt
# it must refuse to serve. Requires `make up`.
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${VEIN_VOLUMES:=lode_vein1 lode_vein2 lode_vein3 lode_vein4}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bucket="corrupt-$(date +%s)"
head -c 100000 /dev/urandom >"$work/obj"
aws s3 mb "s3://$bucket" >/dev/null
aws s3 cp "$work/obj" "s3://$bucket/obj" >/dev/null

# Overwrite 4 bytes inside every copy of the object (found by content).
corrupt() {
  docker run --rm -v "$1:/data" -v "$work:/w:ro" alpine sh -c '
    pat=$(od -An -tx1 -j 50000 -N 16 /w/obj | tr -d " \n")
    for seg in /data/segments/*.seg; do
      off=$(od -An -tx1 -v "$seg" | tr -d " \n" | grep -bo "$pat" | head -1 | cut -d: -f1)
      [ -n "$off" ] || continue
      printf "\377\377\377\377" | dd of="$seg" bs=1 seek=$((off / 2)) conv=notrunc 2>/dev/null
      echo hit
    done'
}

holders=()
for vol in $VEIN_VOLUMES; do
  [ -n "$(corrupt "$vol")" ] && holders+=("$vol") || true
  [ ${#holders[@]} -eq 1 ] && break
done
[ ${#holders[@]} -eq 1 ] || { echo "FAIL: no replica found to corrupt" >&2; exit 1; }
aws s3 cp "s3://$bucket/obj" "$work/one" >/dev/null
cmp -s "$work/obj" "$work/one" || { echo "FAIL: wrong bytes served with one corrupt replica" >&2; exit 1; }
echo "corruption: one corrupt replica (${holders[0]}) skipped, correct bytes served"

for vol in $VEIN_VOLUMES; do corrupt "$vol" >/dev/null; done
if aws s3 cp "s3://$bucket/obj" "$work/all" >/dev/null 2>"$work/err"; then
  echo "FAIL: an object with every replica corrupt was served" >&2
  exit 1
fi
grep -qiE "internal.?error|500" "$work/err" || { echo "FAIL: unexpected error: $(cat "$work/err")" >&2; exit 1; }
echo "corruption: every replica corrupt, read refused ($(head -c 100 "$work/err"))"
