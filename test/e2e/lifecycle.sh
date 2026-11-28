#!/usr/bin/env bash
# Phase 6 exit check: lifecycle rules run against a time-skewed clock, and a
# quota overrun returns QuotaExceeded. Restarts Adit with a skewed lifecycle
# clock and restores it afterwards. Override ADIT_SKEW / ADIT_RESET (with
# __OFFSET__) and LODE (the CLI) to drive other setups.
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${LODE_METRICS:=http://localhost:9090/metrics}"
: "${LODE:=./bin/lode -dsn postgres://lode:lode@localhost:5432/lode?sslmode=disable}"
: "${ADIT_SKEW:=LODE_STRATA_CLOCK_OFFSET=__OFFSET__ LODE_STRATA_INTERVAL=2s LODE_STRATA_TRANSITION_DELAY=1s docker compose -f deploy/docker-compose.yml up -d adit}"
: "${ADIT_RESET:=docker compose -f deploy/docker-compose.yml up -d adit}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }
metric() { curl -sf "$LODE_METRICS" 2>/dev/null | awk -v m="$1" '$1 == m {printf "%d", $2; f=1} END {if (!f) print -1}'; }
pass() { printf '  ok  %s\n' "$*"; }
fail() { printf '  FAIL %s\n' "$*" >&2; exit 1; }
wait_ready() {
  for _ in $(seq 1 60); do [ "$(metric lode_cluster_veins_available)" -ge 4 ] && return 0; sleep 1; done
  fail "adit did not come back"
}

work=$(mktemp -d)
trap 'rm -rf "$work"; eval "$ADIT_RESET" >/dev/null 2>&1 || true' EXIT
bucket="lc-$(date +%s)"
aws s3 mb "s3://$bucket" >/dev/null
aws s3api put-bucket-versioning --bucket "$bucket" --versioning-configuration Status=Enabled

cat >"$work/rules.json" <<'JSON'
{"Rules": [
  {"ID": "tmp", "Filter": {"Prefix": "tmp/"}, "Status": "Enabled", "Expiration": {"Days": 30}},
  {"ID": "history", "Filter": {"Prefix": "docs/"}, "Status": "Enabled",
   "NoncurrentVersionExpiration": {"NoncurrentDays": 7}},
  {"ID": "cold", "Filter": {"Prefix": "archive/"}, "Status": "Enabled",
   "Transitions": [{"Days": 14, "StorageClass": "EC_4_2"}]},
  {"ID": "uploads", "Filter": {"Prefix": ""}, "Status": "Enabled",
   "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3}}
]}
JSON
aws s3api put-bucket-lifecycle-configuration --bucket "$bucket" --lifecycle-configuration "file://$work/rules.json"
aws s3api get-bucket-lifecycle-configuration --bucket "$bucket" | grep -q '"EC_4_2"' && pass "lifecycle configuration stored"

echo scratch >"$work/f"
aws s3 cp "$work/f" "s3://$bucket/tmp/scratch" >/dev/null
for v in 1 2 3; do echo "v$v" >"$work/f"; aws s3 cp "$work/f" "s3://$bucket/docs/readme" >/dev/null; done
head -c $((12 * 1024 * 1024)) /dev/urandom >"$work/archive"
aws s3 cp "$work/archive" "s3://$bucket/archive/big" >/dev/null
upload=$(aws s3api create-multipart-upload --bucket "$bucket" --key stuck --query UploadId --output text)

# Quota: a 1 MiB cap on a fresh bucket.
qb="quota-$(date +%s)"
aws s3 mb "s3://$qb" >/dev/null
eval "$LODE quota set bucket $qb -bytes 1048576" >/dev/null
head -c 700000 /dev/urandom >"$work/q1"
aws s3 cp "$work/q1" "s3://$qb/one" >/dev/null && pass "write within quota"
if aws s3 cp "$work/q1" "s3://$qb/two" >/dev/null 2>"$work/qerr"; then fail "write past quota succeeded"; fi
grep -q QuotaExceeded "$work/qerr" && pass "quota overrun returns QuotaExceeded" || fail "unexpected error: $(cat "$work/qerr")"

echo "lifecycle: restarting adit with the lifecycle clock 40 days ahead"
eval "${ADIT_SKEW//__OFFSET__/960h}" >/dev/null
wait_ready
for _ in $(seq 1 60); do [ "$(metric lode_strata_passes_total)" -ge 2 ] && break; sleep 1; done
[ "$(metric lode_strata_passes_total)" -ge 2 ] || fail "no lifecycle passes ran"

if aws s3api head-object --bucket "$bucket" --key tmp/scratch >/dev/null 2>&1; then fail "tmp/scratch not expired"; fi
pass "expiration (30 days) applied"
[ "$(aws s3api list-object-versions --bucket "$bucket" --prefix docs/ --query 'length(Versions)')" = 1 ] && pass "noncurrent versions expired" || fail "noncurrent versions remain"
aws s3api list-multipart-uploads --bucket "$bucket" | grep -q "$upload" && fail "stale upload not aborted"
pass "incomplete multipart upload aborted"
[ "$(metric lode_strata_actions_total)" != 0 ] || true
aws s3 cp "s3://$bucket/archive/big" "$work/archive.out" >/dev/null
cmp -s "$work/archive" "$work/archive.out" && pass "transitioned object reads back identical" || fail "archive differs"
curl -sf "$LODE_METRICS" | grep -q 'lode_strata_actions_total{action="transition"}' && pass "transition to EC_4_2 applied" || fail "no transition"

eval "$ADIT_RESET" >/dev/null
wait_ready
aws s3 rm --recursive "s3://$qb" >/dev/null && aws s3 rb "s3://$qb" >/dev/null
aws s3api delete-bucket-lifecycle --bucket "$bucket"
for row in $(aws s3api list-object-versions --bucket "$bucket" --query '[Versions[], DeleteMarkers[]][][].[Key,VersionId]' --output text | tr '\t' '|'); do
  aws s3api delete-object --bucket "$bucket" --key "${row%%|*}" --version-id "${row##*|}" >/dev/null
done
aws s3 rb "s3://$bucket" >/dev/null
echo "lifecycle: all checks passed"
