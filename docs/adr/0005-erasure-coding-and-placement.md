# ADR 0005: Erasure coding, shard format v1, placement strategies, rebalancing

Status: accepted · Phase 5

## Context
Three full replicas cost 3x storage. Large, cold or cheap data wants
Reed-Solomon `k+m` (e.g. 4+2 at 1.5x, surviving any two losses). Placement
also needs to be choosable per bucket, and the cluster must rebalance when
veins join without reshuffling everything.

## Decision
- **Storage class per bucket**: replicated (default, `x-lode-replication`) or
  `ec:K+M` (`x-lode-storage-class` on CreateBucket, or `lode bucket create -ec`).
  K ≥ 1, M ≥ 1, K+M ≤ 32.
- **Layout per nugget**, fixed at first write (`nuggets.ec_data/ec_parity`).
  Dedup still works across classes: content first written replicated stays
  replicated when a 4+2 bucket stores it, and vice versa. (Phase 6
  transitions re-encode deliberately.)
- **Encoding** at nugget level: the nugget (≤ 8 MiB) is split into K equal data
  shards (zero-padded) and M parity shards (`klauspost/reedsolomon`).
- **Shard blob format v1** (on-disk, stored as an ordinary vein blob):
  `"LDSH" | version=1 | index (1 byte) | nugget id (32 bytes) | shard bytes`,
  addressed by BLAKE3 of the whole blob. The header makes shard IDs unique
  per (nugget, index), so equal shard bytes of different nuggets (zero
  padding!) never share an ID, and Tailings deleting one nugget's shard can
  never remove another's. Readers check the header before using the bytes;
  the reconstructed nugget is verified against its ID. A future format bumps
  the version byte.
- **Ledger**: `nugget_shards(nugget, idx, shard, vein_id)`.
- **Writes** place each missing shard on a distinct available vein (Survey
  order) and acknowledge at **K+1** durable shards. Fewer than K+M is a
  degraded write that repair completes.
- **Reads** fetch the K data shards in parallel (no decoding on the happy
  path); missing or corrupt ones are replaced by parity shards and the nugget
  is reconstructed. Unreachable shards get one patient retry for veins that
  are reconnecting.
- **Repair**: redundancy is measured as *fault tolerance* (shards beyond K on
  healthy veins; copies beyond one when replicated). Smelter reads any K
  shards, reconstructs the missing ones, writes them to veins holding no
  other shard of the nugget, and only then drops corrupt shards or shards on
  draining veins. Assay reports a corrupt shard by its shard ID; the Ledger
  maps it back to its nugget.
- **Placement strategies** (`survey.Strategy`, per bucket via
  `x-lode-placement`, cluster default `adit.placement`):
  `hrw` (pure rendezvous), `zone-spread` (rendezvous order, spread over zones,
  then hosts; default), `capacity-weighted` (weighted rendezvous, score
  `-free/ln(u)`, so new data follows free space), `pinned:v1,v2,…` (restricted
  to listed veins). Repair uses zone-spread.
- **Rebalancer** (leader-elected, in Adit): when the set of available veins
  changes and stays stable for one interval, it walks every referenced
  nugget and moves copies/shards to where a *stable* strategy now places
  them, copying and recording first, then forgetting and deleting the old
  copy. It is disabled for `capacity-weighted`, whose targets drift with usage.

## Consequences
- A vein joining an existing failure domain takes about 1/N of copies
  (measured 0.160 for N = 6). A vein that brings a *new* zone rightly takes a
  copy of nearly every nugget under zone-spread.
- An erasure-coded nugget needs K+M distinct healthy veins for full
  redundancy and K+1 for writes; with fewer, writes fail with 503 and repair
  waits.
- Buckets with `pinned` or `hrw` placement are repaired and rebalanced with the
  cluster default strategy, not their own.
