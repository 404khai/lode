-- Ledger becomes the authoritative nugget refcount. A nugget may be reclaimed
-- only when refs = 0 and last_touched is older than the GC grace window;
-- uploads touch a nugget before writing it to a vein.
CREATE TABLE nuggets (
    id           bytea       PRIMARY KEY CHECK (length(id) = 32),
    length       integer     NOT NULL,
    refs         bigint      NOT NULL DEFAULT 0 CHECK (refs >= 0),
    last_touched timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX nuggets_unreferenced_idx ON nuggets (last_touched) WHERE refs = 0;

INSERT INTO nuggets (id, length, refs)
SELECT nugget, max(length), count(*) FROM manifest_nuggets GROUP BY nugget;

-- Versioning: '' = never enabled, 'Enabled', 'Suspended'.
ALTER TABLE buckets ADD COLUMN versioning text NOT NULL DEFAULT ''
    CHECK (versioning IN ('', 'Enabled', 'Suspended'));

ALTER TABLE object_versions
    ADD COLUMN version_id       text    NOT NULL DEFAULT 'null',
    ADD COLUMN is_delete_marker boolean NOT NULL DEFAULT false,
    ADD COLUMN parts_count      integer,
    ADD COLUMN part_sizes       bigint[],
    ALTER COLUMN manifest_id DROP NOT NULL;
ALTER TABLE object_versions ALTER COLUMN version_id DROP DEFAULT;
ALTER TABLE object_versions ADD CONSTRAINT object_versions_marker_manifest
    CHECK (is_delete_marker = (manifest_id IS NULL));
CREATE UNIQUE INDEX object_versions_object_version_idx ON object_versions (object_id, version_id);

CREATE TABLE multipart_uploads (
    upload_id          text PRIMARY KEY,
    bucket_id          bigint NOT NULL REFERENCES buckets (id) ON DELETE RESTRICT,
    key                text COLLATE "C" NOT NULL,
    headers            jsonb NOT NULL DEFAULT '{}',
    checksum_algorithm text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- Set once completed; the row is kept briefly so a retried Complete
    -- returns the same result, as S3 does.
    completed_etag     text,
    completed_version  text,
    completed_at       timestamptz
);
CREATE INDEX multipart_uploads_bucket_key_idx ON multipart_uploads (bucket_id, key, upload_id);
CREATE INDEX multipart_uploads_created_idx ON multipart_uploads (created_at);

CREATE TABLE multipart_parts (
    upload_id      text        NOT NULL REFERENCES multipart_uploads (upload_id) ON DELETE CASCADE,
    part_number    integer     NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    manifest_id    bigint      NOT NULL REFERENCES manifests (id),
    size           bigint      NOT NULL,
    etag           text        NOT NULL,
    checksum_value text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (upload_id, part_number)
);
