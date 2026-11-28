-- Who wrote each version (and started each upload), for per-access-key quotas.
ALTER TABLE object_versions ADD COLUMN access_key text NOT NULL DEFAULT '';
ALTER TABLE multipart_uploads ADD COLUMN access_key text NOT NULL DEFAULT '';

-- Quotas (Claim). scope is 'bucket' or 'key'; NULL limits are unlimited.
CREATE TABLE quotas (
    scope       text   NOT NULL CHECK (scope IN ('bucket', 'key')),
    subject     text   NOT NULL,
    max_bytes   bigint CHECK (max_bytes >= 0),
    max_objects bigint CHECK (max_objects >= 0),
    PRIMARY KEY (scope, subject)
);

-- Usage counters, maintained in the same transaction as every version
-- write or delete and reconciled from object_versions by a background job.
CREATE TABLE usage_counters (
    scope   text   NOT NULL CHECK (scope IN ('bucket', 'key')),
    subject text   NOT NULL,
    bytes   bigint NOT NULL DEFAULT 0,
    objects bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (scope, subject)
);
INSERT INTO usage_counters (scope, subject, bytes, objects)
SELECT 'bucket', b.name, coalesce(sum(v.size), 0), count(v.id)
FROM buckets b
LEFT JOIN objects o ON o.bucket_id = b.id
LEFT JOIN object_versions v ON v.object_id = o.id AND NOT v.is_delete_marker
GROUP BY b.name;

-- Lifecycle configuration (Strata), stored as parsed rules.
ALTER TABLE buckets ADD COLUMN lifecycle jsonb;

-- Blobs to delete from veins once no reader can still be using them
-- (after a storage-class transition or a rebalance move).
-- The blob is deleted only if its nugget has not been touched since the
-- delete was scheduled and the blob is not recorded as live again.
CREATE TABLE deferred_deletes (
    vein_id    text        NOT NULL,
    blob       bytea       NOT NULL,
    nugget     bytea       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    not_before timestamptz NOT NULL,
    PRIMARY KEY (vein_id, blob)
);
