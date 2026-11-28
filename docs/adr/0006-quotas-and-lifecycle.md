# ADR 0006: Quotas (Claim) and lifecycle (Strata)

Status: accepted · Phase 6

## Context
Operators need to cap what a bucket or access key may store, and buckets
need S3 lifecycle rules: expiry, noncurrent-version cleanup, aborting stale
uploads, and moving data between replicated and erasure-coded storage.

## Decision
### Claim
- Quotas are rows in `quotas(scope, subject, max_bytes, max_objects)` with
  scope `bucket` or `key` (access key). Usage counts logical bytes and
  object versions (current and noncurrent; delete markers are free).
- `usage_counters` change **inside the same transaction** as every version
  insert or delete. The insert path updates the counter row (locking it),
  compares against the quota and fails with `ErrQuotaExceeded`, rolling back
  the write. Concurrent writes therefore cannot jointly overshoot a quota.
- Adit also pre-checks the declared size before reading a body
  (`QuotaHeadroom`), crediting the null version an overwrite replaces, so an
  over-quota upload is refused without transferring it. S3 error:
  `403 QuotaExceeded`. Copies and multipart completions are checked too.
- A leader-elected reconciler recomputes counters from `object_versions`
  every 15 minutes and corrects drift.
- Each version records the access key that wrote it (`object_versions.access_key`).

### Strata
- `Put/Get/DeleteBucketLifecycleConfiguration`, subset: filters by prefix,
  single tag, or `And`; `Expiration` (Days, Date, ExpiredObjectDeleteMarker);
  `NoncurrentVersionExpiration` (NoncurrentDays, NewerNoncurrentVersions);
  `AbortIncompleteMultipartUpload`; `Transition` by Days to `STANDARD`
  (replicated) or `EC_K_M` (e.g. `EC_4_2`). Size filters, Date transitions and
  noncurrent transitions return `NotImplemented`.
- A leader-elected worker applies enabled rules every `strata.interval`.
  Expiration uses normal delete semantics (a delete marker in versioned
  buckets). Ages are measured from creation (or from when a version became
  noncurrent), without S3's round-up to midnight.
- **Transitions are per nugget.** Strata reads the nugget, writes the complete
  new layout (all K+M shards, or every replica), records it, then switches the
  nugget's layout in one transaction and schedules the old blobs in
  `deferred_deletes` for `strata.transition_delay` later. Readers that loaded
  the old layout keep working. A nugget shared with another object changes
  class for both.
- **Deferred deletes** (run by Tailings) delete a blob only if its nugget has
  not been touched since the delete was scheduled and the blob is not
  recorded as live again. A concurrent write that reused the blob, or a
  transition back, therefore never loses data (at worst a blob leaks).
- `strata.clock_offset` / `LODE_STRATA_CLOCK_OFFSET` shifts the lifecycle clock
  for tests (`make lifecycle` runs at +40 days). Adit logs a warning when it
  is set.

## Consequences
- Quota checks add one locked counter update per bucket and per key to each
  write transaction, which serialises writes per bucket at commit time.
- Transitions read and rewrite data; they share no bandwidth budget with
  Smelter yet.
