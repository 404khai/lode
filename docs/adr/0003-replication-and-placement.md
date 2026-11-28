# ADR 0003: Replicated writes, placement and replica tracking

Status: accepted · Phase 3

## Context
Lode moves from one embedded vein to a cluster of veins reached over gRPC.
Writes must survive a vein dying mid-upload, reads must survive any single
vein being down, and garbage collection must still find every copy.

## Decision
- **Topology.** `lode-vein` serves `VeinService` (PutShard, GetShard, StatShard,
  DeleteShard, Status), registers itself in `veins` and heartbeats every 2s.
  Adit loads the vein list every 2s. A vein is *available* for writes when its
  state is `healthy`, its heartbeat is newer than `cluster.stale_after` (10s),
  and Adit has not seen a transport failure to it in the last 5s.
- **Placement (Survey v1).** Rendezvous hashing (BLAKE3 of nugget ID + vein ID)
  over available veins, spreading across zones, then hosts, then anything.
  Removing a vein moves only the nuggets that ranked it first.
- **Replica tracking.** `nugget_replicas(nugget, vein_id)` records every copy
  that landed. Placement is therefore advisory: reads and GC use the recorded
  replicas, not the hash, so vein churn never strands data.
- **Writes.** Per nugget: touch (ADR 0002), read the recorded replicas, write
  in parallel to the best-ranked available veins that lack a copy until
  `replication` copies exist (bucket override via `x-lode-replication`), and
  succeed once `write_quorum` (default majority) are durable. Every copy that
  landed is recorded, even when the write fails. Fewer than `replication`
  copies is a *degraded* write, reported via `nugget.under_replicated` events
  and metrics, and topped up by repair (Smelter, Phase 4). Below quorum the
  request fails with 503 `ServiceUnavailable`.
- **Reads.** Try recorded replicas, available veins first and then lowest
  latency (EWMA). Every replica is re-verified against its BLAKE3 ID in Adit,
  so a corrupt or truncated copy causes failover, never wrong bytes. If every
  replica was unreachable, one more pass waits up to 2s for reconnecting
  veins (one that just restarted).
- **GC.** Tailings deletes a reclaimed nugget from every recorded replica
  while holding the nugget row lock. If any vein is unreachable the reclaim is
  deferred to a later pass; the row and replica rows stay.
- **Compaction** moves into each vein daemon (`vein.compact_interval`).

## Consequences
- Each nugget costs one extra Ledger write (replica rows) on upload.
- A vein that never comes back blocks reclaim of the nuggets it holds until it
  is drained or removed (Phase 4).
- Heartbeat staleness, not a failure detector, decides availability in this
  phase; Canary replaces it with phi-accrual suspicion in Phase 4.
