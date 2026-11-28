# ADR 0004: Failure detection, repair queue and scrubbing

Status: accepted · Phase 4

## Context
Phase 3 tolerated failures but never restored redundancy. Phase 4 makes the
cluster heal itself: notice failed veins and corrupt copies, and bring every
referenced nugget back to its wanted number of copies on healthy veins.

## Decision
- **Canary** (in Adit, leader-elected by advisory lock) reads heartbeat
  timestamps every second and keeps a phi-accrual detector per vein (normal
  model over the last 100 inter-arrival times, 500 ms jitter floor).
  `healthy → suspect` when phi ≥ 8 (no new writes); `→ down` after
  `canary.down_after` (30 s) of silence, which queues a repair for every
  referenced nugget on the vein. Phi falling back below 4 returns a vein to
  healthy. Heartbeats no longer change state themselves. Every minute Canary
  also queues any under-replicated nugget the event paths missed.
- **Wanted copies** are stored per nugget (`nuggets.replication`, the highest
  replication of any bucket that wrote it), so repair does not need to
  find the buckets that reference a nugget.
- **Repair queue** (`repair_jobs`): at most one open job per nugget (partial
  unique index), priority = healthy copies at enqueue time (fewest first),
  leases with `FOR UPDATE SKIP LOCKED`, expired leases re-run, retries with
  linear backoff, `lost` after 20 attempts with no readable copy. Sources:
  Canary (vein down, drain, sweep), Adit (degraded writes, corrupt copies
  found on read) and Assay (corrupt copies found by scrubbing).
- **Smelter** workers (in Adit; any number of processes) handle a job by:
  forgetting and deleting copies listed as bad; reading a verified copy from
  any remaining replica; writing to Survey-ranked available veins until the
  nugget has its wanted copies on healthy veins; then releasing copies on
  draining veins. Writes share a bandwidth budget (`smelter.bandwidth_bytes`).
  Jobs are idempotent.
- **Assay** runs in each vein, re-reading and re-hashing every nugget at
  `vein.scrub_rate` with `vein.scrub_pause` between passes, and queues a
  `corrupt` repair naming itself as the bad vein.
- **Disk health**: veins count I/O errors and corrupt reads and report them in
  heartbeats (`lode veins` shows them).
- **Draining**: `lode vein drain <id>` stops writes to a vein and queues all
  its copies; once the Ledger records no copies of referenced nuggets on it,
  Canary retires it (`removed`), forgetting any unreferenced copies.
  `lode vein remove <id>` retires a dead vein, refusing while it holds the
  last copy of anything unless `-force`.

## Consequences
- A vein that returns after being marked down rejoins with its old copies;
  repairs that ran meanwhile leave some nuggets with extra copies, which
  are harmless and reclaimed with the nugget.
- Under-replication accounting counts only `healthy` veins: a `suspect`
  vein's copies trigger repair if it stays suspect past the sweep interval.
- SMART data is not read; disk health is inferred from I/O errors and
  verification failures.
