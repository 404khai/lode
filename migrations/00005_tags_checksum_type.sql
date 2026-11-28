-- Object and bucket tags; checksum type (COMPOSITE or FULL_OBJECT) for
-- multipart checksums.
ALTER TABLE object_versions
    ADD COLUMN tags          jsonb NOT NULL DEFAULT '{}',
    ADD COLUMN checksum_type text;
ALTER TABLE buckets ADD COLUMN tags jsonb;
ALTER TABLE multipart_uploads
    ADD COLUMN tags          jsonb NOT NULL DEFAULT '{}',
    ADD COLUMN checksum_type text;
