-- Storage class and placement per bucket.
ALTER TABLE buckets
    ADD COLUMN ec_data     integer NOT NULL DEFAULT 0,
    ADD COLUMN ec_parity   integer NOT NULL DEFAULT 0,
    ADD COLUMN placement   text    NOT NULL DEFAULT '',
    ADD COLUMN pinned      text[]  NOT NULL DEFAULT '{}';

-- A nugget's layout is fixed when it is first written: 0/0 = replicated,
-- k/m = Reed-Solomon with k data and m parity shards.
ALTER TABLE nuggets
    ADD COLUMN ec_data   integer NOT NULL DEFAULT 0,
    ADD COLUMN ec_parity integer NOT NULL DEFAULT 0;

-- Where each shard of an erasure-coded nugget lives. shard is the ID the
-- vein stores it under (BLAKE3 of a header naming nugget and index, plus
-- the shard bytes), so shards of different nuggets never collide.
CREATE TABLE nugget_shards (
    nugget     bytea       NOT NULL REFERENCES nuggets (id) ON DELETE CASCADE,
    idx        integer     NOT NULL,
    shard      bytea       NOT NULL CHECK (length(shard) = 32),
    vein_id    text        NOT NULL REFERENCES veins (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (nugget, idx, vein_id)
);
CREATE INDEX nugget_shards_vein_idx ON nugget_shards (vein_id);
CREATE INDEX nugget_shards_shard_idx ON nugget_shards (shard);
