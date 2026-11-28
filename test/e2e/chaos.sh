#!/usr/bin/env bash
# Phase 4 exit check. Loads data, keeps readers running, kills one vein for
# good and corrupts random bytes on another, then waits for Lode to restore
# full redundancy. Passes only with zero client-visible errors and zero lost
# objects. Defaults drive the compose cluster; override the commands to
# drive other setups (__N__ is the vein number).
set -euo pipefail

: "${LODE_ENDPOINT:=http://localhost:9000}"
: "${LODE_METRICS:=http://localhost:9090/metrics}"
: "${KILL_VEIN:=3}"
: "${CORRUPT_VEIN:=1}"
: "${VEIN_STOP:=docker compose -f deploy/docker-compose.yml kill vein__N__}"
: "${VEIN_START:=docker compose -f deploy/docker-compose.yml start vein__N__}"
: "${SQL:=docker compose -f deploy/docker-compose.yml exec -T postgres psql -U lode -tAc}"
: "${CORRUPT:=docker run --rm -v lode_vein__N__:/data python:3-alpine python3 -c}"
: "${TIMEOUT:=300}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-lode}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-lodesecret}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
aws() { command aws --endpoint-url "$LODE_ENDPOINT" "$@"; }
metric() { curl -sf "$LODE_METRICS" | awk -v m="$1" '$1 == m {printf "%d", $2; found=1} END {if (!found) print -1}'; }
sql() { eval "$SQL \"\$1\"" | tr -d '[:space:]'; }

work=$(mktemp -d)
reader=""
cleanup() { [ -n "$reader" ] && kill "$reader" 2>/dev/null || true; eval "${VEIN_START//__N__/$KILL_VEIN}" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT
bucket="chaos-$(date +%s)"
aws s3 mb "s3://$bucket" >/dev/null

echo "chaos: loading 24 objects"
mkdir -p "$work/src"
for i in $(seq 1 24); do
  head -c $(( (RANDOM % 8000 + 50) * 1024 )) /dev/urandom >"$work/src/obj$i"
done
aws s3 cp --recursive "$work/src" "s3://$bucket/" >/dev/null

# Readers: download random objects in a loop and count any failure or mismatch.
: >"$work/errors"
(
  while :; do
    i=$((RANDOM % 24 + 1))
    if ! aws s3 cp "s3://$bucket/obj$i" "$work/r$i" >/dev/null 2>>"$work/errors.log" || ! cmp -s "$work/src/obj$i" "$work/r$i"; then
      echo "obj$i $(date +%T)" >>"$work/errors"
    fi
    echo . >>"$work/reads"
  done
) &
reader=$!

sleep 3
echo "chaos: killing vein$KILL_VEIN and corrupting vein$CORRUPT_VEIN"
eval "${VEIN_STOP//__N__/$KILL_VEIN}" >/dev/null
eval "${CORRUPT//__N__/$CORRUPT_VEIN} '
import glob, os, random
segs = [s for s in glob.glob(os.environ.get(\"D\", \"/data\") + \"/segments/*.seg\") if os.path.getsize(s) > 0]
for _ in range(8):
    seg = random.choice(segs)
    with open(seg, \"r+b\") as f:
        f.seek(random.randrange(os.path.getsize(seg)))
        f.write(os.urandom(4))
print(\"corrupted 8 locations\")
'"

deadline=$((SECONDS + TIMEOUT))
phase=detect
while :; do
  down=$(sql "SELECT count(*) FROM veins WHERE id = 'vein$KILL_VEIN' AND state = 'down'")
  fixed=$(sql "SELECT count(*) FROM repair_jobs WHERE reason = 'corrupt' AND state = 'done'")
  under=$(metric lode_repair_under_replicated_nuggets)
  open=$(metric lode_repair_jobs_open)
  lost=$(metric lode_repair_jobs_lost)
  if [ "$phase" = detect ] && [ "$down" = 1 ] && [ "${fixed:-0}" -ge 1 ]; then
    phase=heal
    echo "chaos: vein$KILL_VEIN marked down and Assay-detected corruption repaired; waiting for full redundancy"
  fi
  if [ "$phase" = heal ] && [ "$under" = 0 ] && [ "$open" = 0 ]; then break; fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "FAIL: not healed within ${TIMEOUT}s (down=$down corrupt_repaired=$fixed under=$under open=$open lost=$lost)" >&2
    exit 1
  fi
  sleep 2
done
sleep 2
kill "$reader" 2>/dev/null || true; wait "$reader" 2>/dev/null || true; reader=""

reads=$(wc -l <"$work/reads" | tr -d ' ')
errors=$(wc -l <"$work/errors" | tr -d ' ')
echo "chaos: $reads reads during the failure, $errors errors"
if [ "$errors" -ne 0 ]; then cat "$work/errors" "$work/errors.log" >&2; echo "FAIL: client-visible errors" >&2; exit 1; fi
[ "$(metric lode_repair_jobs_lost)" = 0 ] || { echo "FAIL: repair reports lost nuggets" >&2; exit 1; }

for i in $(seq 1 24); do
  aws s3 cp "s3://$bucket/obj$i" "$work/final" >/dev/null
  cmp -s "$work/src/obj$i" "$work/final" || { echo "FAIL: obj$i differs after recovery" >&2; exit 1; }
done
echo "chaos: all 24 objects intact, full redundancy restored, zero lost"
