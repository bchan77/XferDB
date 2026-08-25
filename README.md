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
| PostgreSQL | ✅ | ✅ | Full support |
| MySQL | ✅ | ✅ | Full support |
| SQLite | ✅ | ✅ | Full support |
| MongoDB | 🔜 | 🔜 | Coming soon |
| Cassandra | 🔜 | 🔜 | Coming soon |
| **Pinecone** | 🔜 | 🔜 | Vector DB |
| **Qdrant** | 🔜 | 🔜 | Vector DB |
| **Weaviate** | 🔜 | 🔜 | Vector DB |
| **Chroma** | 🔜 | 🔜 | Vector DB |

> 🔜 = Planned support

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

```bash
# macOS (Apple Silicon)
curl -fsSL https://gitea.homelab.local/nextdevops/XferDB/releases/latest/download/xferdb-darwin-arm64 | tar -xz
sudo mv xferdb /usr/local/bin/

# macOS (Intel)
curl -fsSL https://gitea.homelab.local/nextdevops/XferDB/releases/latest/download/xferdb-darwin-amd64 | tar -xz
sudo mv xferdb /usr/local/bin/

# Linux
curl -fsSL https://gitea.homelab.local/nextdevops/XferDB/releases/latest/download/xferdb-linux-amd64 | tar -xz
sudo mv xferdb /usr/local/bin/

# Windows
iwr https://gitea.homelab.local/nextdevops/XferDB/releases/latest/download/xferdb-windows-amd64.exe -OutFile xferdb.exe
```

### Docker

```bash
docker run -v ~/.xferdb:/data xferdb migrate --from postgres://source/db --to mysql://target/db
```

### From source

```bash
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB
go install
```

---

## Quick Start

### Using connection strings

```bash
xferdb migrate \
  --from "postgres://user:password@localhost:5432/source_db" \
  --to "mysql://user:password@localhost:3306/target_db"
```

### Using a config file

```bash
xferdb migrate --config migration.yaml
```

### Example config file

```yaml
source:
  type: postgres
  host: localhost
  port: 5432
  database: source_db
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}

target:
  type: mysql
  host: localhost
  port: 3306
  database: target_db
  user: ${MYSQL_USER}
  password: ${MYSQL_PASSWORD}

transfer:
  batch_size: 1000
  workers: 4
  validate: true
```

---

## Roadmap

- [ ] PostgreSQL source & target adapter
- [ ] MySQL source & target adapter
- [ ] SQLite source & target adapter
- [ ] Incremental sync with cursor-based pagination
- [ ] MongoDB adapter
- [ ] Cassandra adapter
- [ ] Vector DB adapters (Pinecone, Qdrant, Weaviate, Chroma)
- [ ] Web UI for migration configuration
- [ ] Scheduled/repeated migrations
- [ ] Data transformation pipeline (column mapping, type casting)

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
