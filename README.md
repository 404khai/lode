# Lode

Self-hostable, S3-compatible distributed object store in Go, with an MCP server (Foreman)
for agents and a visual cluster explorer (Lantern).

Status: **Phase 0 — skeleton.** See [AGENTS.md](AGENTS.md) for architecture and roadmap.

```bash
make build test
make up            # postgres + adit on :9000 (metrics on :9090)
curl -i localhost:9000/   # 501 NotImplemented (for now)
```

Licensed under the [Apache License 2.0](LICENSE).
