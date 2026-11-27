CREATE TABLE buckets (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE manifests (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chunk_size integer     NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE manifest_nuggets (
    manifest_id bigint  NOT NULL REFERENCES manifests (id) ON DELETE CASCADE,
    seq         integer NOT NULL,
    nugget      bytea   NOT NULL CHECK (length(nugget) = 32),
    length      integer NOT NULL CHECK (length > 0),
    PRIMARY KEY (manifest_id, seq)
);
CREATE INDEX manifest_nuggets_nugget_idx ON manifest_nuggets (nugget);

-- Keys use the "C" collation so listings are in S3's UTF-8 binary order.
CREATE TABLE objects (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bucket_id          bigint NOT NULL REFERENCES buckets (id) ON DELETE RESTRICT,
    key                text COLLATE "C" NOT NULL,
    current_version_id bigint,
    UNIQUE (bucket_id, key)
);

CREATE TABLE object_versions (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_id          bigint      NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    manifest_id        bigint      NOT NULL REFERENCES manifests (id),
    size               bigint      NOT NULL,
    etag               text        NOT NULL,
    checksum_algorithm text,
    checksum_value     text,
    headers            jsonb       NOT NULL DEFAULT '{}',
    created_at         timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE objects
    ADD CONSTRAINT objects_current_version_fk FOREIGN KEY (current_version_id)
    REFERENCES object_versions (id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED;
