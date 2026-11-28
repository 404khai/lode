# AGENTS.md — Lode

Lode is a self-hostable, S3-compatible distributed object store written in Go. It is
designed to be **agent-native**: every operation an operator can do through the CLI or
the Survey explorer, an agent can do through the MCP server (`lode-foreman`).

Repo: https://github.com/404khai/lode

This file is the contract for any AI agent (or human) working in this repo. Read it
fully before making changes. When a phase is completed, tick its exit criteria here in
the same PR.

---

## 1. Vocabulary (component names)

Lode uses a mining vocabulary. Use these names consistently in code, logs, metrics and docs.

| Name        | What it is                                                        | Go package / binary        |
|-------------|-------------------------------------------------------------------|----------------------------|
| **Adit**    | S3-compatible HTTP gateway (the entrance to the mine)             | `cmd/lode-adit`, `internal/adit` |
| **Vein**    | Storage node daemon: holds chunks on local disks (Pebble index + blob files) | `cmd/lode-vein`, `internal/vein` |
| **Ledger**  | Metadata service: buckets, objects, versions, manifests (PostgreSQL) | `internal/ledger`          |
| **Nugget**  | A content-addressed chunk (BLAKE3 hash = ID)                      | `internal/nugget`          |
| **Manifest**| Ordered list of nuggets + EC layout that make up one object version | `internal/manifest`       |
| **Survey**  | Placement engine: decides which veins/disks hold which shards     | `internal/survey`          |
| **Canary**  | Failure detector: heartbeats, phi-accrual suspicion, disk health  | `internal/canary`          |
| **Assay**   | Integrity scrubber: re-reads and re-hashes nuggets in the background | `internal/assay`        |
| **Smelter** | Repair worker: rebuilds missing/corrupt shards from replicas or parity | `internal/smelter`    |
| **Tailings**| Garbage collector: reclaims unreferenced nuggets                  | `internal/tailings`        |
| **Strata**  | Lifecycle policy engine: expiration, noncurrent-version cleanup, tier transitions | `internal/strata` |
| **Claim**   | Quotas: per-bucket / per-tenant byte and object limits            | `internal/claim`           |
| **Foreman** | MCP server exposing the cluster to agents                         | `cmd/lode-foreman`, `internal/foreman` |
| **Lantern** | Web cluster explorer UI (topology, placement, repairs)            | `web/lantern`              |
| `lode`      | Operator CLI                                                      | `cmd/lode`                 |

---

## 2. Architecture at a glance

```
 S3 clients / aws-cli / SDKs           Agents (Claude, etc.)        Browser
            │                                   │                      │
            ▼                                   ▼                      ▼
      ┌───────────┐  gRPC   ┌───────────┐  ┌──────────┐  SSE/WS  ┌──────────┐
      │   Adit    │────────▶│  Ledger   │◀─│ Foreman  │          │ Lantern  │
      │ (S3 HTTP) │         │ (Postgres)│  │  (MCP)   │──────────│  (web)   │
      └─────┬─────┘         └─────┬─────┘  └────┬─────┘          └──────────┘
            │ gRPC stream         │ events      │
            ▼                     ▼             ▼
   ┌────────┬────────┬────────┐  Survey · Canary · Assay · Smelter · Tailings · Strata
   │ Vein 1 │ Vein 2 │ Vein N │  (control-plane workers, leader-elected via Postgres advisory locks)
   │ Pebble │ Pebble │ Pebble │
   │ + disks│ + disks│ + disks│
   └────────┴────────┴────────┘
```

**Data path:** Adit splits uploads into nuggets (content-defined chunking, FastCDC,
target 4 MiB), hashes them (BLAKE3), asks Survey for placement, streams shards to veins
over gRPC, then commits the manifest to Ledger in a single Postgres transaction.
An object version is visible **only after** the manifest commit.

**Control path:** Canary marks veins/disks `healthy → suspect → down`. Down events
enqueue repair jobs; Smelter executes them. Assay finds silent corruption and enqueues
the same jobs. Tailings deletes nuggets whose refcount has been zero past a grace period.

### Consistency guarantees (do not weaken without an ADR)
- **Read-after-write** for PUT/DELETE of a key (strong, via Ledger as source of truth).
- **Strong LIST** consistency (lists are served from Ledger, not veins).
- Manifests are immutable; a new write = a new version row.
- A nugget is never deleted while any committed or in-flight manifest references it
  (Ledger refcount + touch-before-write + GC grace window ≥ 24h by default; ADR 0002).
- Writes ack only after `W` shards are durable (fsync) — default: replication `W=2 of 3`,
  EC `k+1 of k+m`.

---

## 3. Repo layout

```
reports/           graft usage reports (graft_report_lode_<topic>.md)
cmd/               binaries (lode, lode-adit, lode-vein, lode-foreman)
internal/          all packages listed in §1
proto/             gRPC definitions (buf-managed)
migrations/        Postgres migrations (embedded; applied by internal/ledger under an advisory lock), numbered, never edited after merge
web/lantern/       explorer UI (TypeScript, Vite, D3/react-flow)
deploy/            docker-compose, Dockerfiles, example configs
test/e2e/          multi-node tests using docker compose + chaos helpers
test/s3compat/     runs ceph/s3-tests subset against Adit
docs/adr/          architecture decision records
```

## 4. Commands

```bash
make build            # all binaries
make test             # unit tests (go test ./... -race); set LODE_TEST_DSN=postgres://... to run Ledger/Adit integration tests
make lint             # golangci-lint + buf lint
make proto            # regenerate gRPC code (buf generate)
make up               # docker compose: postgres + 1 adit + 6 veins in 2 zones (foreman and lantern join in later phases)
make e2e              # aws CLI e2e suite against `make up` (LODE_ENDPOINT overrides the target)
make corruption       # corrupts a nugget on the compose vein and checks it is never served
make dedup            # uploads one file twice and checks it is stored once (DEDUP_SIZE_MB, default 1024)
make replication      # kills a vein mid-upload, then reads with each vein down (VEIN_STOP/VEIN_START with __N__ override the compose commands)
make s3compat         # ceph/s3-tests cases in test/s3compat/tests.txt against an Adit on :9000 (see test/s3compat/adit.yaml)
make erasure          # 4+2 bucket on 6 veins: reads with each pair of veins down
make chaos            # kills a vein and corrupts another under read load; waits for full redundancy, zero client errors
./bin/lode veins      # operator CLI (LODE_LEDGER_DSN): veins, vein drain|undrain|remove, repairs, bucket create -ec K+M
```

## 5. Rules for agents

1. **Run `make lint test` before declaring done.** If you touched the data path, also run `make e2e`.
2. **Never edit a merged migration.** Add a new one.
3. **Proto changes must be backward-compatible** (no field renumbering/removal). `buf breaking` enforces this.
4. **Every background worker must be idempotent and resumable** — jobs are rows in Postgres with leases.
5. **No silent data loss paths.** Any code that deletes bytes on disk needs a test proving a referenced nugget survives.
6. **Errors on the S3 surface use real S3 error codes** (`NoSuchKey`, `InvalidPart`, …) with XML bodies.
7. **Observability is part of the feature:** each component exposes Prometheus metrics and emits structured events (`internal/events`) that Lantern and Foreman consume.
8. Keep dependencies minimal. Allowed core: `pebble`, `pgx`, `grpc-go`, `blake3`, `klauspost/reedsolomon`, `mcp-go` (or official Go MCP SDK), `prometheus/client_golang`, `gopkg.in/yaml.v3`. Ask before adding others.
9. Write an ADR in `docs/adr/` for any change to consistency, placement, or on-disk format.
10. Don't push to `main`; open PRs from short, conventional branch names (`feat/s3-multipart`, `fix/range-reads`, `chore/ci-cache`). Never put phase names or numbers in branch names — reference the phase in the commit body instead (e.g. `Phase: 2`).
11. **Parallel work uses graft, never git worktrees.** If a task is split across parallel agents or sessions, create and control them with graft via its MCP server. Do not use `git worktree` or worktree isolation.
12. **Every graft session ends with a report.** The agent writes `reports/graft_report_lode_<topic>.md` (e.g. `graft_report_lode_multipart.md`) covering: what it loved about graft, pain points, and improvements it would like to see. Commit the report with the work.

---

## 6. Build phases

Each phase ends with a working, demoable system. Don't start a phase until the previous
phase's exit criteria pass in CI.

### Phase 0 — Skeleton
- Go module `github.com/404khai/lode`, Makefile, golangci-lint, buf, GitHub Actions.
- Config loading (YAML + env), structured logging (`slog`), Prometheus endpoint.
- `docker-compose.yml` with Postgres.
- **Exit:** `make build lint test` green in CI; `make up` boots Postgres and an empty Adit returning 501s. ✅

### Phase 1 — Single-node object store
- Vein with one disk: nugget write/read/delete, Pebble index (`hash → file, offset, len, refcount`), append-only blob segment files.
- Ledger schema: `buckets`, `objects`, `object_versions`, `manifests`, `manifest_nuggets`.
- Adit: `CreateBucket`, `DeleteBucket`, `ListBuckets`, `PutObject`, `GetObject` (incl. `Range`), `HeadObject`, `DeleteObject`, `ListObjectsV2`.
- SigV4 auth with static access keys.
- Checksums: BLAKE3 per nugget, MD5 ETag, honour `Content-MD5` and `x-amz-checksum-sha256`.
- **Exit:** `aws s3 cp/ls/rm` work against Adit; corrupted nugget on disk returns an error, never wrong bytes. ✅

### Phase 2 — Chunking, dedup, multipart, versioning
- FastCDC content-defined chunking; identical nuggets stored once (refcount in Ledger, authoritative).
- Multipart: `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`; S3-style multipart ETag.
- Versioning: `PutBucketVersioning`, `ListObjectVersions`, `GetObject?versionId`, delete markers.
- Tailings v1: refcount-zero sweep with grace period, plus abandoned multipart cleanup.
- **Exit:** uploading the same 1 GiB file twice uses ~1 GiB on disk; s3-tests multipart + versioning subset passes. ✅

### Phase 3 — Multi-node replication
- gRPC `VeinService` (streaming PutShard/GetShard/Stat/Delete); vein registration in Ledger.
- Survey v1: rendezvous (HRW) hashing over veins, failure-domain aware (`zone`, `host`, `disk` labels).
- Replication factor per bucket (default 3), write quorum `W`, read from fastest healthy replica.
- **Exit:** 4-vein cluster; kill one vein mid-upload and the upload still succeeds; reads succeed with any one vein down. ✅

### Phase 4 — Failure detection and repair
- Canary: heartbeats, phi-accrual detector, disk SMART/IO-error reporting, states `healthy/suspect/down/draining`.
- Repair job queue in Postgres (leased, prioritised by remaining redundancy).
- Smelter: re-replicates under-replicated nuggets; throttled bandwidth.
- Assay: continuous scrub at configurable rate; enqueues repair on hash mismatch.
- Draining a vein for decommission.
- **Exit:** `make chaos` — kill a vein, corrupt random files on another — cluster returns to full redundancy with zero client-visible errors and zero lost objects. ✅

### Phase 5 — Erasure coding & placement strategies
- Reed–Solomon (`k+m`, e.g. 4+2, 8+3) per bucket/storage class; EC applied at nugget-stripe level.
- Degraded reads (reconstruct on the fly); Smelter rebuilds lost shards from parity.
- Placement strategies pluggable in Survey: `hrw`, `zone-spread`, `capacity-weighted`, `pinned`; rebalancer when veins join.
- On-disk format ADR, versioned.
- **Exit:** 6-vein cluster with 4+2 survives two simultaneous vein losses; rebalance after adding a vein moves ≤ ~1/N of data. ✅

### Phase 6 — Quotas & lifecycle
- Claim: per-bucket and per-access-key quotas (bytes, objects), enforced at Adit before data is accepted; usage counters reconciled by background job.
- Strata: `PutBucketLifecycleConfiguration` subset — expiration, noncurrent-version expiration, abort-incomplete-multipart, transition between replication and EC classes.
- **Exit:** lifecycle rules verified by e2e with time-skew test clock; quota overrun returns `QuotaExceeded`.

### Phase 7 — Foreman (MCP server)
Agent-native surface. Read-only tools are default; mutating tools require `--allow-write` and are tagged destructive.
- Resources: `lode://cluster`, `lode://veins/{id}`, `lode://buckets/{name}`, `lode://objects/{bucket}/{key}`, `lode://repairs`.
- Tools:
  - `cluster_status`, `list_veins`, `vein_detail`, `disk_health`
  - `locate_object` (which veins/disks hold each shard, redundancy left)
  - `list_repairs`, `explain_repair` (why this job exists, ETA)
  - `bucket_usage`, `list_lifecycle_rules`, `simulate_placement`
  - write: `drain_vein`, `trigger_scrub`, `set_quota`, `put_lifecycle_rule`, `retry_repair`
- Transports: stdio and streamable HTTP; auth via the same access keys.
- Every tool returns structured JSON plus a one-line human summary.
- **Exit:** an agent can answer "which objects are at risk if vein-3 dies right now?" and safely drain a vein end to end, verified in an MCP integration test.

### Phase 8 — Lantern (cluster explorer)
- Live topology: zones → hosts → veins → disks, coloured by health; capacity bars.
- Object view: search a key, see its manifest, nuggets, and the shard placement map.
- Replication/EC status heatmap; under-replicated counts trending.
- Failed disks highlighted; repair operations animated as flows between veins with progress.
- Event timeline (Canary state changes, repairs, GC sweeps); data fed by SSE from `internal/events`.
- "Ask Foreman" panel: optional, calls the MCP server for natural-language questions.
- **Exit:** during `make chaos`, Lantern shows the failure, the repair flows, and recovery to green in real time.

### Phase 9 — Hardening & release
- Full s3-tests subset report published in `docs/s3-compat.md`.
- Benchmarks (`warp`) and a perf regression job.
- Fuzzing for manifest/segment parsers; crash-consistency tests (kill -9 during writes).
- TLS everywhere, key rotation, signed release images, Helm chart (optional).
- **Exit:** tagged `v0.1.0` release with docs, compose quick-start, and a published compatibility matrix.

---

## 7. Definition of done (any PR)
- Tests added/updated; `make lint test` green (plus `e2e` for data-path changes).
- Metrics/events emitted for new behaviour.
- Docs updated (this file if vocabulary/phase status changes; ADR if format/consistency changes).
- No TODOs left in data-path code without a linked issue.
