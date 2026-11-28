-- Desired copies per nugget: the highest replication of any bucket that has
-- written it.
ALTER TABLE nuggets ADD COLUMN replication integer NOT NULL DEFAULT 0;

-- Disk health reported by veins in heartbeats.
ALTER TABLE veins
    ADD COLUMN io_errors       bigint NOT NULL DEFAULT 0,
    ADD COLUMN corrupt_nuggets bigint NOT NULL DEFAULT 0,
    ADD COLUMN state_changed   timestamptz NOT NULL DEFAULT now();

-- Repair work for Smelter. At most one open job per nugget; workers lease
-- jobs with SKIP LOCKED, and an expired lease returns the job to the queue.
CREATE TABLE repair_jobs (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nugget      bytea       NOT NULL,
    reason      text        NOT NULL,
    bad_veins   text[]      NOT NULL DEFAULT '{}',
    priority    integer     NOT NULL DEFAULT 0, -- healthy copies when queued; lower runs first
    state       text        NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'leased', 'done', 'lost')),
    lease_owner text,
    lease_until timestamptz,
    not_before  timestamptz NOT NULL DEFAULT now(),
    attempts    integer     NOT NULL DEFAULT 0,
    last_error  text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX repair_jobs_open_idx ON repair_jobs (nugget) WHERE state IN ('queued', 'leased');
CREATE INDEX repair_jobs_queue_idx ON repair_jobs (priority, created_at) WHERE state IN ('queued', 'leased');
