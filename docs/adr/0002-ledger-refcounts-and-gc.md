# ADR 0002: Ledger-owned reference counts, content-defined chunking and GC

Status: accepted · Phase 2 · supersedes the refcount part of ADR 0001

## Context
Phase 1 counted nugget references in each vein's Pebble index. With
multipart uploads, versioning and (soon) several veins, the count has to live
in one transactional place, and deleting bytes has to be safe against
uploads that are writing the same content at the same moment.

## Decision
- **Chunking.** Objects and parts are cut with FastCDC (normalised level 2):
  min 1 MiB, average 4 MiB, max 8 MiB, with a fixed gear table seeded from
  splitmix64(`"lode_cdc"`). These parameters are part of the format: changing
  them only hurts dedup against existing data, never correctness.
- **Refcounts.** Table `nuggets(id, length, refs, last_touched)` is
  authoritative. Inserting a manifest adds one reference per entry; dropping a
  manifest removes them and resets `last_touched`. Manifests belong to object
  versions or to multipart parts; completing an upload moves references from
  the parts' manifests to the object's manifest in one transaction.
- **In-flight uploads.** Before writing a nugget to a vein, Adit *touches* it
  (upsert with `last_touched = now()`). This replaces a separate
  `pending_uploads` table: the GC grace window protects bytes that are written
  but not yet committed.
- **Reclaim protocol (Tailings).** For each nugget with `refs = 0` and
  `last_touched < now() - grace`: lock the row (`FOR UPDATE`), re-check, delete
  the nugget from the vein, then delete the row and commit. A concurrent touch
  blocks on the row lock and, after the commit, re-inserts the row, and its
  `vein.Put` rewrites the bytes because they are now absent. A failed vein
  delete rolls back and keeps the row.
- **Vein.** `Put` of a present nugget is a no-op; `Delete` removes the index
  entry; `Compact` copies live nuggets out of segments that are at least
  `compact_ratio` dead (fsync + index batch) before removing the segment.
  The index value keeps its v1 layout; `refs` is always 1.
- **Versioning** follows S3: `null` versions for unversioned/suspended writes,
  delete markers, permanent deletes by version ID. Completed multipart uploads
  stay resolvable for an hour so a retried Complete returns the same result.
- **Conditional writes.** `If-Match` / `If-None-Match` on PutObject and
  CompleteMultipartUpload are checked inside the commit transaction while the
  object row is locked.

## Consequences
- The grace window (default 24h) must exceed the longest single PUT or part
  upload; an upload that touched a nugget more than `grace` ago before
  committing could lose it. Multipart parts are committed per part, so only a
  single part's upload time counts.
- Unreferenced bytes cost disk space until the grace window passes and a
  compaction runs.
- Each nugget costs one Ledger round trip on upload (the touch).
