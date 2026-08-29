# XferDB

**Universal database transfer tool** — migrate data between any databases with a single command.

```bash
xferdb migrate --from postgres://source/db --to mysql://target/db
```

---

## 🎯 Mission

Moving data between different database systems is painful. Each DB has its own migration tooling, quirks, and incompatibilities. **XferDB aims to be the universal translator for database transfers.**

> **Goal**: One command to migrate from *any* supported database to *any* other — SQL, NoSQL, and vector databases alike.

---

## Why XferDB?

| Problem | Solution |
|---------|----------|
| No universal DB migration tool | XferDB handles PostgreSQL, MySQL, MongoDB, Cassandra, and more |
| Vendor lock-in | Escape to any target DB without rewriting your migration scripts |
| Complex ETL pipelines | Simple CLI — no pipeline builder needed |
| Vendor-specific vector DBs | First-class support for Pinecone, Qdrant, Weaviate, Chroma |

---

## Supported Databases

| Database | Source | Target | Notes |
|----------|--------|--------|-------|
| SQLite | ✅ | ✅ | Fully working |
| PostgreSQL | 🔜 | 🔜 | Implemented, pending live DB testing |
| MySQL | 🔜 | 🔜 | Implemented, pending live DB testing |
| MongoDB | 🔜 | 🔜 | Coming soon |
| Cassandra | 🔜 | 🔜 | Coming soon |
| **Pinecone** | 🔜 | 🔜 | Vector DB |
| **Qdrant** | 🔜 | 🔜 | Vector DB |
| **Weaviate** | 🔜 | 🔜 | Vector DB |
| **Chroma** | 🔜 | 🔜 | Vector DB |

> ✅ = Working &nbsp;|&nbsp; 🔜 = Planned or implemented but not yet verified against a live instance

---

## Features

- 🚀 **Single binary** — no dependencies, works anywhere
- 🔄 **Incremental sync** — resume interrupted migrations
- 📊 **Progress tracking** — real-time byte/row counts
- ✅ **Validation** — data integrity checks post-transfer
- 🔒 **Secure** — TLS support, credentials never logged
- 🧩 **Pluggable drivers** — add your own source/target adapters
- 🐳 **Docker ready** — run anywhere containerized

---

## Installation

### Binary (recommended)

> **Note:** Binaries are published to GitHub on official releases. For development builds, build from source.

```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/nextdevops/XferDB/releases/latest/download/xferdb-darwin-arm64 | tar -xz
sudo mv xferdb /usr/local/bin/

# macOS (Intel)
curl -fsSL https://github.com/nextdevops/XferDB/releases/latest/download/xferdb-darwin-amd64 | tar -xz
sudo mv xferdb /usr/local/bin/

# Linux
curl -fsSL https://github.com/nextdevops/XferDB/releases/latest/download/xferdb-linux-amd64 | tar -xz
sudo mv xferdb /usr/local/bin/

# Windows
iwr https://github.com/nextdevops/XferDB/releases/latest/download/xferdb-windows-amd64.exe -OutFile xferdb.exe
```

### Docker

```bash
docker run -v ~/.xferdb:/data xferdb migrate --from postgres://source/db --to mysql://target/db
```

### From source

```bash
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB
go install ./cmd/xferdb
```

---

## Quick Start

XferDB runs as an API server. The CLI talks to it.

### 1. Start the server

```bash
xferdb server
# Listening on :8080, state at ~/.xferdb/state.db
```

### 2. Create a project

```bash
xferdb project create \
  --name my-migration \
  --source sqlite:///path/to/source.db \
  --target sqlite:///path/to/target.db
```

### 3. Run a migration

```bash
xferdb project use my-migration
xferdb migrate
# Migration started: <id>
# phase=complete       table=                     rows=50000/50000  rate=0/s  eta=0s
```

### SQLite example (working today)

```bash
# Start server
xferdb server &

# Create project
xferdb project create \
  --name sqlite-test \
  --source sqlite:///source.db \
  --target sqlite:///target.db

# Migrate
xferdb project use sqlite-test
xferdb migrate
```

### Via the REST API directly

```bash
# Create project
curl -X POST http://localhost:8080/api/v1/projects \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-migration",
    "source_config": {"type": "sqlite", "dsn": "/path/to/source.db"},
    "target_config": {"type": "sqlite", "dsn": "/path/to/target.db"},
    "transfer_config": {"batch_size": 1000}
  }'

# Run preflight
curl -X POST http://localhost:8080/api/v1/projects/<id>/preflight

# Start migration
curl -X POST http://localhost:8080/api/v1/projects/<id>/migrations

# Check progress
curl http://localhost:8080/api/v1/migrations/<id>/stats
```

---

## Development

Development happens on the internal Gitea instance. Source code is eventually released to GitHub with official releases.

### Building from source

```bash
# Clone from internal Gitea
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB

# Download dependencies
go mod download

# Build
go build -o xferdb ./cmd/xferdb

# Run
./xferdb version
```

---

## Roadmap

- [x] SQLite source & target adapter
- [x] REST API with projects, migrations, preflight, schema analysis
- [x] Pause/resume with checkpointing and crash recovery
- [x] Multi-project support
- [ ] PostgreSQL adapter — live DB testing
- [ ] MySQL adapter — live DB testing
- [ ] MongoDB adapter
- [ ] Cassandra adapter
- [ ] Vector DB adapters (Pinecone, Qdrant, Weaviate, Chroma)
- [ ] Web UI for migration configuration
- [ ] WebSocket live progress
- [ ] Scheduled/repeated migrations
- [ ] Data transformation pipeline (column mapping, type casting)

---

## Versioning

XferDB follows [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`).

| Version | Milestone |
|---------|-----------|
| `v0.1.0` | PostgreSQL source & target (CLI) |
| `v0.2.0` | MySQL adapter |
| `v0.3.0` | SQLite adapter |
| `v0.4.0` | Web UI |
| `v1.0.0` | Stable release |

**Pre-1.0 convention:**

```
v0.MINOR.PATCH
     |     └── Bug fixes
     └── New features (adapters, UI, etc.)
```

The `v1.0.0` release will be tagged once the CLI interface and config format are considered stable.

---

## Documentation

Full documentation available at: [https://gitea.homelab.local/nextdevops/XferDB/wiki](https://gitea.homelab.local/nextdevops/XferDB/wiki)

---

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

All contributions require a Signed-off-by line (DCO).

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
