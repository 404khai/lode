# Lode

Self-hostable, S3-compatible distributed object store in Go, with an MCP server (Foreman)
for agents and a visual cluster explorer (Lantern).

Status: **single-node object store with multipart, versioning, dedup and GC.** See [AGENTS.md](AGENTS.md) for architecture and roadmap.

## Quick start

```bash
make up                       # postgres + adit on :9000 (metrics/health on :9090)
export AWS_ACCESS_KEY_ID=lode AWS_SECRET_ACCESS_KEY=lodesecret AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://photos
aws --endpoint-url http://localhost:9000 s3 cp ./cat.jpg s3://photos/
aws --endpoint-url http://localhost:9000 s3 ls s3://photos/
```

## Supported S3 operations

`ListBuckets`, `CreateBucket`, `DeleteBucket`, `HeadBucket`, `GetBucketLocation`,
`PutObject`, `GetObject` (single `Range`, `partNumber`, conditional headers), `HeadObject`,
`DeleteObject`, `DeleteObjects`, `ListObjects`, `ListObjectsV2`,
`CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`,
`ListParts`, `ListMultipartUploads`, `PutBucketVersioning`, `GetBucketVersioning`,
`ListObjectVersions`. Conditional writes (`If-Match` / `If-None-Match`) on PUT and Complete.

- Auth: SigV4 (header and presigned), including signed and unsigned `aws-chunked` streaming with trailers.
- Integrity: BLAKE3 per nugget on every read, MD5 ETags, `Content-MD5`, `x-amz-content-sha256`,
  and `x-amz-checksum-{crc32,crc32c,crc64nvme,sha1,sha256}`.
- Storage: content-defined chunking (FastCDC, ~4 MiB) with dedup across objects and versions;
  unreferenced data is reclaimed by Tailings after a 24h grace window.
- Path-style addressing only. Not yet: copy, ACLs/policies, tagging, lifecycle, SSE,
  composite multipart checksums. Compatibility is tracked in `test/s3compat/tests.txt`.

## Development

```bash
make build test lint
LODE_TEST_DSN=postgres://localhost/lode_test make test   # include Postgres-backed tests
make up && make e2e && make corruption
```

Licensed under the [Apache License 2.0](LICENSE).
