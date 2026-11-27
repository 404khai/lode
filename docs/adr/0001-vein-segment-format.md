# ADR 0001: Vein on-disk format (v1)

Status: accepted · Phase 1

## Context
A vein stores content-addressed nuggets on local disk. We need durable writes,
cheap dedup, and corruption that is detected rather than served.

## Decision
- **Segments:** `segments/NNNNNNNN.seg`, append-only files of raw nugget bytes,
  rotated at 1 GiB. Segments are never rewritten in place.
- **Index:** Pebble DB at `index/`. Key `"n/" + blake3(nugget)` (32 bytes);
  value is 20 bytes big-endian: `segment u32 | offset u64 | length u32 | refs u32`.
- **Write order:** append → `fsync(segment)` → index `Set` with `pebble.Sync`.
  An index entry therefore always points at durable bytes.
- **Dedup:** a put of an existing ID only increments `refs`.
- **Reads:** every read re-hashes the bytes; a mismatch returns `ErrCorrupt`
  and emits `nugget.corrupt`. Corrupt bytes are never returned.
- **Release:** decrements `refs`; at zero the index entry is deleted. Bytes
  stay in the segment until compaction (Tailings, Phase 2).

## Consequences
- Space from deleted or crash-orphaned nuggets is only reclaimed by compaction.
- Writes are serialised per vein in v1 (one fsync per nugget); batching is a
  later optimisation.
- In Phase 1 the vein refcount is the only reference count; Phase 2 makes the
  Ledger's count authoritative, and the vein's becomes a cache that is reconciled.
- Any change to the key or value layout needs a new ADR and a format version.
