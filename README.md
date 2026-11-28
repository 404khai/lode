# Lode

Self-hostable, S3-compatible distributed object store in Go, with an MCP server (Foreman)
for agents and a visual cluster explorer (Lantern).

Status: **self-healing replicated object store: 3 copies across zones, failure detection, automatic repair and scrubbing, with multipart, versioning, dedup and GC.** See [AGENTS.md](AGENTS.md) for architecture and roadmap.

## Quick start

```bash
make up                       # postgres + 6 veins + adit on :9000 (metrics/health on :9090)
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
`ListObjectVersions`, `CopyObject`, `UploadPartCopy`, `Get/Put/DeleteObjectTagging`,
`Get/Put/DeleteBucketTagging`, `Get/PutBucketAcl`, `Get/PutObjectAcl` (owner-only, see below).
Conditional writes (`If-Match` / `If-None-Match`) on PUT, Copy and Complete.

- Auth: SigV4 (header and presigned), including signed and unsigned `aws-chunked` streaming with trailers.
- Integrity: BLAKE3 per nugget on every read, MD5 ETags, `Content-MD5`, `x-amz-content-sha256`,
  and `x-amz-checksum-{crc32,crc32c,crc64nvme,sha1,sha256}`.
- Cluster: `lode-vein` storage nodes over gRPC. Each nugget is written to 3 veins (spread over
  zones, then hosts) and acknowledged after 2 are durable; reads fail over between replicas and
  re-verify every copy. Per-bucket replication: `x-lode-replication: N` on CreateBucket.
- Erasure coding: `lode bucket create photos -ec 4+2` (or `x-lode-storage-class: ec:4+2` on
  CreateBucket) stores each nugget as 4 data + 2 parity shards on 6 veins: 1.5x overhead, any two
  veins can be lost. Placement per bucket: `hrw`, `zone-spread`, `capacity-weighted`, `pinned:...`.
  The rebalancer moves ~1/N of data when a vein joins.
- Quotas: `lode quota set bucket photos -bytes 10737418240` (or `key <access-key>`); writes past a
  quota fail with `QuotaExceeded`. Lifecycle: `PutBucketLifecycleConfiguration` with expiration,
  noncurrent-version expiration, abort-incomplete-multipart and transitions to `STANDARD` or
  `EC_K_M` (e.g. `EC_4_2`).
- Healing: Canary (phi-accrual) marks silent veins suspect then down; Smelter re-replicates
  everything they held; Assay scrubs every vein and repairs corrupt copies. Operate with
  `lode veins`, `lode vein drain <id>`, `lode vein remove <id>`, `lode repairs`.
- Storage: content-defined chunking (FastCDC, ~4 MiB) with dedup across objects and versions;
  unreferenced data is reclaimed by Tailings after a 24h grace window.
- Copies are metadata-only: the new version shares the source's nuggets, so no bytes move.
- Multipart checksums: `COMPOSITE` and `FULL_OBJECT` (CRCs combined server-side) for
  crc32, crc32c, crc64nvme, sha1 and sha256.
- Access: single-tenant and owner-only. ACL reads return the owner with `FULL_CONTROL`; the canned
  `private` ACL is accepted; public ACLs, grants and bucket policies are refused with `NotImplemented`.
- Not yet: server-side encryption (requests asking for it are refused rather
  than silently stored in plaintext), bucket policies, CORS, object lock, virtual-hosted addressing.
  Compatibility is tracked in `test/s3compat/tests.txt`.

## Lantern (cluster explorer)

`make up` serves Lantern on http://localhost:8080 (sign in with an access key and secret). It shows
zones → hosts → veins coloured by health with capacity, repair copies animated between veins, a
per-bucket fault-tolerance heatmap with the under-replicated trend, a live event timeline, an
object's shard placement map, and an "Ask Foreman" panel that runs Foreman's MCP tools. Locally:
`make lantern-ui && ./bin/lode-lantern -ui web/lantern/dist`.

## Agents (MCP)

Foreman exposes the cluster to agents over MCP. Read-only by default; `-allow-write` adds
`drain_vein`, `trigger_scrub`, `set_quota`, `put_lifecycle_rule` and `retry_repair` (annotated
destructive). Every tool returns structured JSON and a one-line summary.

```bash
# stdio, e.g. for Claude Code:
claude mcp add lode -- ./bin/lode-foreman -config deploy/adit.example.yaml
# or streamable HTTP (compose runs this on :9100):
./bin/lode-foreman -http :9100   # Authorization: Bearer <access-key>:<secret-key>
```

Tools: `cluster_status`, `list_veins`, `vein_detail`, `disk_health`, `locate_object`,
`objects_at_risk`, `list_repairs`, `explain_repair`, `bucket_usage`, `list_lifecycle_rules`,
`simulate_placement`. Resources: `lode://cluster`, `lode://repairs`, `lode://veins/{id}`,
`lode://buckets/{name}`, `lode://objects/{bucket}/{key}`.

## Development

```bash
make build test lint
LODE_TEST_DSN=postgres://localhost/lode_test make test   # include Postgres-backed tests
make up && make e2e && make corruption && make replication && make erasure && make lifecycle && make chaos
```

Licensed under the [Apache License 2.0](LICENSE).
