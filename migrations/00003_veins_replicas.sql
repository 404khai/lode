-- Storage nodes. A vein registers itself and heartbeats; Adit treats veins
-- with a stale heartbeat as unavailable for placement.
CREATE TABLE veins (
    id             text PRIMARY KEY,
    addr           text        NOT NULL,
    zone           text        NOT NULL DEFAULT '',
    host           text        NOT NULL DEFAULT '',
    state          text        NOT NULL DEFAULT 'healthy'
        CHECK (state IN ('healthy', 'suspect', 'down', 'draining', 'removed')),
    capacity_bytes bigint      NOT NULL DEFAULT 0,
    used_bytes     bigint      NOT NULL DEFAULT 0,
    last_heartbeat timestamptz NOT NULL DEFAULT now(),
    registered_at  timestamptz NOT NULL DEFAULT now()
);

-- Where each nugget's replicas live.
CREATE TABLE nugget_replicas (
    nugget     bytea       NOT NULL REFERENCES nuggets (id) ON DELETE CASCADE,
    vein_id    text        NOT NULL REFERENCES veins (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (nugget, vein_id)
);
CREATE INDEX nugget_replicas_vein_idx ON nugget_replicas (vein_id);

-- Replication factor per bucket; 0 means the cluster default.
ALTER TABLE buckets ADD COLUMN replication integer NOT NULL DEFAULT 0 CHECK (replication BETWEEN 0 AND 16);
