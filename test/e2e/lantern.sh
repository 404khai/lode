#!/usr/bin/env bash
# Phase 8 exit check. `lantern.sh start` records Lantern's live event stream
# (what the browser receives) in the background; run `make chaos`; then
# `lantern.sh check` asserts the stream showed the failure, the repair flows
# and recovery to green, in that order, while it happened.
set -euo pipefail

: "${LANTERN:=http://localhost:8080}"
: "${LANTERN_AUTH:=lode:lodesecret}"
: "${KILL_VEIN:=3}"
: "${STREAM:=/tmp/lantern-stream.txt}"
pidfile="$STREAM.pid"

case "${1:-}" in
start)
  for _ in $(seq 1 90); do curl -sf -u "$LANTERN_AUTH" -o /dev/null "$LANTERN/api/snapshot" && break; sleep 1; done
  rm -f "$STREAM"
  curl -sfN -u "$LANTERN_AUTH" "$LANTERN/api/events" >"$STREAM" &
  echo $! >"$pidfile"
  sleep 2
  grep -q '^event: snapshot' "$STREAM" || { echo "FAIL: no snapshot from Lantern" >&2; exit 1; }
  echo "lantern: recording the live stream to $STREAM"
  ;;
check)
  [ -f "$pidfile" ] && kill "$(cat "$pidfile")" 2>/dev/null || true
  python3 - "$STREAM" "vein$KILL_VEIN" <<'PY'
import json, sys
path, victim = sys.argv[1], sys.argv[2]
events, kind = [], None
for line in open(path):
    line = line.rstrip("\n")
    if line.startswith("event: "):
        kind = line[7:]
    elif line.startswith("data: ") and kind:
        events.append((kind, json.loads(line[6:])))
        kind = None
def find(pred, start=0):
    for i in range(start, len(events)):
        if pred(*events[i]):
            return i
    return None
down = find(lambda k, d: k == "event" and d["type"] == "vein.state" and d["attrs"].get("vein") == victim and d["attrs"].get("to") == "down")
if down is None:
    sys.exit(f"FAIL: Lantern never showed {victim} going down")
red = find(lambda k, d: k == "snapshot" and any(v["id"] == victim and v["state"] == "down" for v in d["veins"]), down)
copies = [d for k, d in events[down:] if k == "event" and d["type"] == "repair.copy" and d["attrs"].get("to")]
green = find(lambda k, d: k == "snapshot" and d["status"] == "green", down)
if red is None:
    sys.exit("FAIL: no snapshot showed the vein as down in the topology")
if not copies:
    sys.exit("FAIL: no repair flows (repair.copy events) reached Lantern")
if green is None:
    sys.exit("FAIL: Lantern never showed the cluster back to green")
snaps = sum(1 for k, _ in events if k == "snapshot")
print(f"lantern: {len(events)} stream messages ({snaps} snapshots): {victim} down -> {len(copies)} repair flows "
      f"(e.g. {copies[0]['attrs']['from'] or '?'} -> {copies[0]['attrs']['to']}) -> back to green")
PY
  ;;
*)
  echo "usage: $0 start|check" >&2
  exit 2
  ;;
esac
