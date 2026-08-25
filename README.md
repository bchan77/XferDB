# XferDB

**Universal database transfer tool** — migrate data between any databases with a single command.

```bash
xferdb migrate --from postgres://source/db --to mysql://target/db
```

## Supported Sources & Targets

| Database | Status | Notes |
|----------|--------|-------|
| PostgreSQL | ✅ | Full support |
| MySQL | ✅ | Full support |
| SQLite | ✅ | Full support |
| MongoDB | 🔜 | Coming soon |
| Cassandra | 🔜 | Coming soon |
| **Pinecone** | 🔜 | Vector DB support |
| **Qdrant** | 🔜 | Vector DB support |
| **Weaviate** | 🔜 | Vector DB support |
| **Chroma** | 🔜 | Vector DB support |

> 🔜 = Planned support

## Features

- 🚀 **Single binary** — no dependencies, works anywhere
- 🔄 **Incremental sync** — resume interrupted migrations
- 📊 **Progress tracking** — real-time byte/row counts
- ✅ **Validation** — data integrity checks post-transfer
- 🔒 **Secure** — TLS support, credentials never logged
- 🧩 **Pluggable drivers** — add your own source/target adapters

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

### From source

```bash
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB
go install
```

## Quick Start

### Connect to source and target

```bash
# Using connection strings
xferdb migrate \
  --from "postgres://user:pass@localhost:5432/source_db" \
  --to "mysql://user:pass@localhost:3306/target_db"

# Using config file
xferdb migrate --config xferdb.yaml
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

## Documentation

Full documentation available at: [https://gitea.homelab.local/nextdevops/XferDB/wiki](https://gitea.homelab.local/nextdevops/XferDB/wiki)

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
