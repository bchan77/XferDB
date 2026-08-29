# XferDB

**Universal database transfer tool** — migrate data between any databases with schema, indexes, foreign keys, and parallel table migration.

---

## Supported Databases

| Database | Source | Target | Notes |
|----------|--------|--------|-------|
| PostgreSQL / YugabyteDB | ✅ | ✅ | Tested against YugabyteDB 2024.2 |
| MySQL | ✅ | ✅ | Implemented, pending live DB testing |
| SQLite | ✅ | ✅ | Fully working |
| MongoDB | 🔜 | 🔜 | Coming soon |
| Cassandra | 🔜 | 🔜 | Coming soon |

> ✅ = Working &nbsp;|&nbsp; 🔜 = Planned

---

## Features

- **3-phase migration** — schema creation, bulk data transfer (upsert), then indexes and constraints
- **Parallel table migration** — migrate N tables concurrently with `--workers`
- **Preflight checks** — verify connectivity, SSL, and permissions before committing to a migration
- **Pause / resume / cancel** — full lifecycle control; checkpoints survive server restarts
- **Live progress display** — per-table status (done / in-progress / pending), rows, rate, and human-readable ETA for the whole migration
- **Flexible reload modes** — default upsert (delta sync), `--truncate` (wipe and reload), or `--recreate-schema` (drop and recreate)
- **Multi-project** — named projects with their own source/target config and migration history
- **API-first** — REST API is the core; CLI is a thin client

---

## Installation

### From source

```bash
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB
go build -o xferdb ./cmd/xferdb
sudo mv xferdb /usr/local/bin/
```

### Binary releases

Binaries are published to Gitea on tagged releases. Download and move to your `PATH`.

---

## Quick Start

XferDB runs as an API server. The CLI talks to it.

### 1. Start the server

```bash
xferdb server
# XferDB API server listening on :8080 (state: ~/.xferdb/state.db)
```

### 2. Create a project

```bash
xferdb project create \
  --name my-migration \
  --source postgres://user:pass@source-host:5432/mydb?sslmode=require \
  --target postgres://user:pass@target-host:5432/mydb?sslmode=require \
  --batch-size 5000 \
  --workers 3
```

### 3. Run preflight

```bash
xferdb project use my-migration
xferdb migrate --preflight
```

```
Project:  my-migration
Status:   ready

Source:
  can_read:         true
  can_write:        true
  can_create_table: true

Target:
  can_read:         true
  can_write:        true
  can_create_table: true
```

### 4. Migrate

```bash
xferdb migrate
```

```
Migration started: 7fba746e-...
Tracking progress (Ctrl+C to detach)...
Phase: in_progress     Elapsed: 51s  ETA: 22m 10s
Rows:  110,000 / 2,100,000   Rate: 1,506/s
────────────────────────────────────────────────────────────
  ✓  customers                       100,000 rows
  ●  order_items                     10,000 / 2,000,000 rows  (0%)
  ○  orders                          pending
  ○  products                        pending
```

---

## CLI Reference

### Project commands

```bash
# Create a project
xferdb project create \
  --name <name> \
  --source <dsn> \
  --target <dsn> \
  [--description <desc>] \
  [--batch-size 1000] \
  [--workers 1]

# List all projects
xferdb project list

# Set the active project (stored in ~/.xferdb/current_project)
xferdb project use <name>

# Show the active project config
xferdb project show

# Check connectivity and permissions
xferdb project preflight [name]

# Delete a project
xferdb project delete <name>
```

### Migration commands

```bash
# Run a migration (uses current project context)
xferdb migrate

# Specify a project explicitly
xferdb migrate --project <name>

# Check connectivity before migrating
xferdb migrate --preflight

# Re-attach to a running migration
xferdb migrate --status

# Cancel the running migration (keeps history, preserves checkpoints)
xferdb migrate --cancel

# Migrate N tables in parallel (inter-table)
xferdb migrate --workers 3

# Split each table into N parallel segments (intra-table, PK-range by default)
xferdb migrate --batch-workers 4

# Force OFFSET-based segment splitting instead of PK range
xferdb migrate --batch-workers 4 --offset-segments

# Combine both: 2 tables at once, each split across 4 workers
xferdb migrate --workers 2 --batch-workers 4

# Truncate target tables before loading (keeps schema)
xferdb migrate --truncate

# Drop and recreate target tables (schema changed on source)
xferdb migrate --recreate-schema

# Migrate specific tables only (comma-separated; schema.table notation supported)
xferdb migrate --tables orders,customers
xferdb migrate --tables public.orders,public.customers
```

### Other commands

```bash
xferdb server [--addr :8080]   # Start the API server
xferdb version                  # Print version
xferdb list                     # List supported adapters
```

---

## Migration modes

| Mode | Command | Behaviour |
|------|---------|-----------|
| Delta sync (default) | `xferdb migrate` | Upsert — new rows inserted, changed rows updated, nothing deleted. Safe to re-run. |
| Truncate reload | `xferdb migrate --truncate` | Wipes target table data first, then loads fresh. Schema is kept. |
| Recreate schema | `xferdb migrate --recreate-schema` | Drops and recreates target tables, then loads. Use when source schema has changed. |
| Selective tables | `xferdb migrate --tables t1,t2` | Only migrate the named tables; all others are skipped. Supports `schema.table` notation. |
| Parallel per-table | `xferdb migrate --batch-workers 4` | Split each table into N segments; workers read/write in parallel. Uses PK ranges by default (no OFFSET scan penalty); falls back to OFFSET for tables without an integer PK. Add `--offset-segments` to force OFFSET mode. |
| Schema only | API: `schema_only: true` | Creates tables, indexes, constraints — no data transfer. |
| Data only | API: `data_only: true` | Skips schema creation — target schema must already exist. |

---

## DSN formats

```bash
# PostgreSQL / YugabyteDB
postgres://user:pass@host:5432/dbname?sslmode=require
postgres://user:pass@host:5433/dbname?sslmode=require   # YugabyteDB default port

# MySQL
mysql://user:pass@host:3306/dbname

# SQLite
sqlite:///absolute/path/to/file.db
sqlite://relative/path.db
```

> **Tip:** If your password contains `!`, use single quotes or `set +H` in zsh to avoid history expansion.

---

## Preflight error hints

XferDB detects common connection problems and gives actionable hints:

| Error | Hint |
|-------|------|
| `database does not exist` | Create the database first: `CREATE DATABASE name;` |
| `SSL off` / `pg_hba.conf` | Add `?sslmode=require` to the DSN |
| `authentication failed` | Check username and password |
| `connection refused` | Check host, port, and that the server is running |
| `i/o timeout` | Check firewall rules or network connectivity |

---

## Migration lifecycle

```
xferdb migrate              → starts a new migration (blocked if one is already running)
xferdb migrate --status     → re-attach to the running migration
xferdb migrate --cancel     → stop the migration (history kept, checkpoints preserved)
xferdb migrate              → restart (resumes from last checkpoint)
```

On server restart, any interrupted migrations are automatically marked as failed. The next `xferdb migrate` resumes from the last saved checkpoint.

---

## REST API

The CLI is a thin wrapper. All operations are available directly:

```bash
# Projects
POST   /api/v1/projects
GET    /api/v1/projects
GET    /api/v1/projects/:id
DELETE /api/v1/projects/:id
POST   /api/v1/projects/:id/preflight
POST   /api/v1/projects/:id/analyze

# Migrations
POST   /api/v1/projects/:id/migrations      # body: {"workers":3,"truncate":true,"tables":["orders","public.customers"],...}
GET    /api/v1/projects/:id/migrations
GET    /api/v1/migrations/:id
PATCH  /api/v1/migrations/:id               # body: {"action":"pause"|"resume"|"cancel"}
DELETE /api/v1/migrations/:id               # cancel + delete record
GET    /api/v1/migrations/:id/stats

GET    /api/v1/health
```

---

## Building from source

```bash
git clone https://gitea.homelab.local/nextdevops/XferDB.git
cd XferDB
go mod download
go build -o xferdb ./cmd/xferdb
./xferdb version
```

---

## Roadmap

- [x] PostgreSQL / YugabyteDB source and target
- [x] MySQL source and target
- [x] SQLite source and target
- [x] 3-phase migration (schema → data → post-schema)
- [x] Parallel table migration (`--workers`)
- [x] Preflight checks with actionable error hints
- [x] Pause / resume / cancel
- [x] Checkpoint-based crash recovery
- [x] Per-table live progress display
- [x] Selective table migration (`--tables`)
- [x] Intra-table parallel workers (`--batch-workers`) with PK-range splitting and OFFSET fallback
- [ ] MongoDB adapter
- [ ] Web UI
- [ ] WebSocket live progress
- [ ] Native tool integration (pg_dump/mysqldump for same-family migrations)
- [ ] Scheduled / repeated migrations
- [ ] Data transformation pipeline (column mapping, type casting)

---

## Versioning

XferDB follows [Semantic Versioning](https://semver.org/).

| Version | Milestone |
|---------|-----------|
| `v0.2.0` | Current — PostgreSQL/MySQL/SQLite, parallel migration, preflight |
| `v0.3.0` | MongoDB adapter |
| `v0.4.0` | Web UI |
| `v1.0.0` | Stable release |

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
