# XferDB

**Universal database transfer tool** — migrate data between any databases with schema, indexes, foreign keys, and parallel table migration.

---

## Supported Databases

| Database | Source | Target | Tested Versions |
|----------|--------|--------|-----------------|
| PostgreSQL | ✅ | ✅ | 14, 15, 16, 17 |
| YugabyteDB | ✅ | ✅ | 2024.2 |
| MySQL | ✅ | ✅ | 8.4 |
| SQLite | ✅ | ✅ | 3.x |
| MongoDB | ✅ | 🔜 | 6.0, 7.0, 8.0, 8.2, 8.3 |
| Cassandra | 🔜 | 🔜 | — |

> ✅ = Working &nbsp;|&nbsp; 🔜 = Planned

### Tested Migration Paths

| Source | Target | Status |
|--------|--------|--------|
| MongoDB (6.0, 7.0, 8.0, 8.2, 8.3) | PostgreSQL (14, 15, 16, 17) | ✅ Tested — 20/20 passing |
| MySQL 8.4 | PostgreSQL 17 | ✅ Tested |
| PostgreSQL (14, 15, 16, 17) | PostgreSQL (14, 15, 16, 17) | ✅ Tested |
| PostgreSQL | YugabyteDB 2024.2 | ✅ Tested |
| SQLite | PostgreSQL | ✅ Tested |

---

## Features

- **3-phase migration** — schema creation, bulk data transfer (upsert), then indexes and constraints
- **Native schema transfer** — postgres→postgres migrations use `pg_dump`/`psql` to preserve extensions, custom types, vector indexes (`ivfflat`, `hnsw`), and exact index definitions
- **Post-schema progress** — shows each index and constraint as it builds so the display never appears hung
- **Parallel table migration** — migrate N tables concurrently with `--table-workers`
- **Intra-table parallel segments** — split each table into N PK-range segments with `--segment-workers`; falls back to OFFSET for tables without an integer PK
- **Bulk copy mode** — `--bulk-copy` uses the PostgreSQL `COPY` protocol for high-throughput writes (combine with `--truncate` or `--recreate-schema`)
- **Preflight checks** — verify connectivity, SSL, and permissions before committing to a migration
- **Pause / resume / cancel** — full lifecycle control; checkpoints survive server restarts
- **Live progress display** — per-table status, rows/s, read/write rates, ETA, and live resource usage (goroutines, heap, CPU%)
- **Detailed table status** — shows "counting rows...", "waiting for schema", "waiting for indexes" so you always know what's happening
- **Async pipeline mode** — `--async-pipeline` decouples reading and writing with buffered channels for higher throughput
- **Stats history** — migration statistics recorded every 5 seconds; view with `--history` after completion
- **Structured server logging** — JSON log file + text stderr with configurable log level; captures project/migration lifecycle, table schemas, and per-table progress
- **Flexible reload modes** — default upsert (delta sync), `--truncate` (wipe and reload), or `--recreate-schema` (drop and recreate)
- **Multi-project** — named projects with their own source/target config and migration history
- **API-first** — REST API is the core; CLI is a thin client
- **Database info in preflight** — shows type, version, host, database name, table count, size, and SSL status for both source and target
- **Support bundle** — `xferdb support-bundle [project]` generates a redacted diagnostic archive for troubleshooting

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

# With structured logging to a file
xferdb server --log-file /var/log/xferdb.log --log-level info
```

### 2. Create a project

```bash
xferdb project create \
  --name my-migration \
  --source postgres://user:pass@source-host:5432/mydb?sslmode=require \
  --target postgres://user:pass@target-host:5432/mydb?sslmode=require \
  --batch-size 5000 \
  --table-workers 3
```

### 3. Run preflight

```bash
xferdb project use my-migration
xferdb migrate --preflight
```

```
Project:  my-migration
Status:   ready

Source: postgres 16
  Host:     source-host:5432
  Database: mydb
  Tables:   14
  Size:     2.3 GB
  SSL:      require
  Permissions:
    can_read:         true
    can_write:        true
    can_create_table: true

Target: postgres 16
  Host:     target-host:5432
  Database: mydb
  Tables:   14
  Size:     0 B
  SSL:      require
  Permissions:
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
Rows:  110,000 / 2,100,000   Rate: 11,959/s
Read:  6,430/s   Write: 1,667/s
Batch: 50,000 rows   Table workers: 5   Segment workers: 10
Goroutines: 62     Heap: 227.8 MiB  Sys: 281.1 MiB  CPU: 312.4%  GC: 16
────────────────────────────────────────────────────────────
  ✓  customers                       100,000 rows
  ●  order_items                     1,500,000 / 2,000,000 rows  (75%)
  ○  orders                          pending
  ○  products                        pending
```

Post-schema phase shows each object as it builds:

```
Phase: post_schema     Elapsed: 4m 12s  ETA: 0s
  → index:           idx_orders_customer_id
  ✓  customers                       100,000 rows
  ✓  orders                          1,000,000 rows
  ...
```

---

## CLI Reference

### Server command

```bash
xferdb server \
  [--addr :8080] \
  [--log-file /var/log/xferdb.log] \
  [--log-level debug|info|warn|error]
```

`--log-file` writes structured JSON logs to the given path. Text logs always go to stderr. `--log-level` applies to both outputs (default: `info`).

### Project commands

```bash
# Create a project
xferdb project create \
  --name <name> \
  --source <dsn> \
  --target <dsn> \
  [--description <desc>] \
  [--batch-size 1000] \
  [--table-workers 1]

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

### Schema analysis (MongoDB → PostgreSQL)

When migrating from MongoDB to PostgreSQL, XferDB can sample your collections and infer a schema plan.

```bash
# Analyze collections and infer Postgres schema
xferdb project analyze [name]

# With options
xferdb project analyze \
  --sample-size 5000 \         # docs to sample per collection (default 2000)
  --sample-pct 1.0 \           # percentage for large collections (default 1%)
  --sample-threshold 100000 \  # switch to percentage above this count
  --ai                         # enable AI annotations for ambiguous fields
```

Review and override the inferred schema:

```bash
# View the inferred schema plan
xferdb project schema

# View a single collection
xferdb project schema --collection users

# Override a field's Postgres type
xferdb project schema --set users.age=integer
xferdb project schema --set orders.total=numeric

# Override field strategy (direct, skip, flatten, as_jsonb)
xferdb project schema --set users.metadata=as_jsonb
xferdb project schema --set users.internal_flags=skip

# Reset schema plan and re-analyze
xferdb project schema --reset
xferdb project schema --reset --collection users  # reset one collection
```

**Field strategies:**

| Strategy | Behaviour |
|----------|-----------|
| `direct` | Map to a Postgres column (default for scalar types) |
| `as_jsonb` | Store as JSONB (for nested objects/arrays) |
| `flatten` | Flatten nested object fields into separate columns |
| `skip` | Exclude field from migration |

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

# View recorded stats history after migration completes
xferdb migrate --history

# Cancel the running migration (keeps history, preserves checkpoints)
xferdb migrate --cancel

# Migrate N tables in parallel (inter-table)
xferdb migrate --table-workers 5

# Split each table into N parallel segments (intra-table, PK-range by default)
xferdb migrate --segment-workers 10

# Force OFFSET-based segment splitting instead of PK range
xferdb migrate --segment-workers 4 --offset-segments

# Combine both: 5 tables at once, each split across 10 workers
xferdb migrate --table-workers 5 --segment-workers 10

# Truncate target tables before loading (keeps schema)
xferdb migrate --truncate

# Drop and recreate target tables (schema changed on source)
xferdb migrate --recreate-schema

# Use PostgreSQL COPY protocol for faster writes (combine with --truncate or --recreate-schema)
xferdb migrate --bulk-copy --recreate-schema

# Async pipeline mode — decouples read/write for higher throughput
xferdb migrate --async-pipeline --bulk-copy --truncate

# Use exact MongoDB document counts instead of estimates (slower but accurate)
xferdb migrate --accurate-counts

# Override batch size for this run (project default is used when not set)
xferdb migrate --batch-size 50000

# Migrate specific tables only (comma-separated; schema.table notation supported)
xferdb migrate --tables orders,customers
xferdb migrate --tables public.orders,public.customers
```

### Support bundle

Generate a diagnostic archive for troubleshooting — system info, project config, source/target database metadata, preflight results, migration history, checkpoints, table schemas, and MongoDB schema plan (if applicable). Passwords and API keys are automatically redacted.

```bash
xferdb support-bundle [project-name]
xferdb support-bundle [project-name] --output /tmp/bundle.tar.gz
```

### Other commands

```bash
xferdb server [--addr :8080] [--log-file <path>] [--log-level <level>]
xferdb support-bundle [project-name] [--output <path>]
xferdb version
xferdb list
```

---

## Migration modes

| Mode | Command | Behaviour |
|------|---------|-----------|
| Delta sync (default) | `xferdb migrate` | Upsert — new rows inserted, changed rows updated, nothing deleted. Safe to re-run. |
| Truncate reload | `xferdb migrate --truncate` | Wipes target table data first, then loads fresh. Schema is kept. |
| Recreate schema | `xferdb migrate --recreate-schema` | Drops and recreates target tables, then loads. Use when source schema has changed. |
| Bulk copy | `xferdb migrate --bulk-copy` | Uses PostgreSQL `COPY` for maximum write throughput. Requires empty target tables — combine with `--truncate` or `--recreate-schema`. |
| Selective tables | `xferdb migrate --tables t1,t2` | Only migrate the named tables; all others are skipped. Supports `schema.table` notation. |
| Parallel per-table | `xferdb migrate --segment-workers 4` | Split each table into N segments; workers read/write in parallel. Uses PK ranges by default (no OFFSET scan penalty); falls back to OFFSET for tables without an integer PK. Add `--offset-segments` to force OFFSET mode. |
| Async pipeline | `xferdb migrate --async-pipeline` | Decouples reading and writing with buffered channels. Readers fetch the next batch while writers flush the previous one. Combine with `--bulk-copy` for best throughput. |
| Accurate counts | `xferdb migrate --accurate-counts` | Use exact MongoDB `CountDocuments` instead of `EstimatedDocumentCount`. Slower but gives accurate row totals and ETA. |
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

# MongoDB
mongodb://user:pass@host:27017/dbname
mongodb://user:pass@host:27017/dbname?authSource=admin
mongodb+srv://user:pass@cluster.mongodb.net/dbname  # Atlas SRV format
```

> **Tip:** If your password contains special characters (`!`, `@`, `#`, etc.), URL-encode them: `!` → `%21`, `@` → `%40`. Use single quotes around the DSN to prevent shell expansion.

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

## Native schema transfer (postgres→postgres)

When both source and target are PostgreSQL (or YugabyteDB), XferDB uses `pg_dump` and `psql` instead of introspection-based `CREATE TABLE` statements. This preserves:

- Extensions (`pgvector`, `PostGIS`, `pg_trgm`, etc.)
- Custom types and enums
- Sequences with their current values
- Vector indexes (`ivfflat`, `hnsw`) and other extension-specific index types
- Table storage options and tablespace assignments

`pg_dump` and `psql` must be on your `PATH`. If they are not found, XferDB falls back to introspection-based schema transfer automatically.

Indexes and constraints (post-data phase) are executed one at a time so that YugabyteDB's serializable DDL concurrency control is not triggered.

---

## Server logging

The server writes two log streams simultaneously:

| Stream | Format | Flag |
|--------|--------|------|
| stderr | Human-readable text | always on |
| log file | Structured JSON | `--log-file <path>` |

```bash
xferdb server --log-file /var/log/xferdb.log --log-level debug
```

Logged events include: server start, project create/delete, preflight checks, migration start/pause/resume/cancel/complete/fail, table schema (column names and types), and per-table start/complete/fail with row counts and elapsed time.

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
GET    /api/v1/projects/:id/support-bundle  # tar.gz diagnostic archive, credentials redacted

# Migrations
POST   /api/v1/projects/:id/migrations      # body: {"table_workers":5,"segment_workers":10,"batch_size":50000,"truncate":true,"bulk_copy":true,"tables":["orders","customers"],...}
GET    /api/v1/projects/:id/migrations
GET    /api/v1/migrations/:id
PATCH  /api/v1/migrations/:id               # body: {"action":"pause"|"resume"|"cancel"}
DELETE /api/v1/migrations/:id               # cancel + delete record
GET    /api/v1/migrations/:id/stats
GET    /api/v1/migrations/:id/stats/history # recorded snapshots (every 5s during migration)

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
- [x] Native schema transfer via `pg_dump`/`psql` (postgres→postgres)
- [x] Post-schema progress display (per-index / per-constraint visibility)
- [x] Parallel table migration (`--table-workers`)
- [x] Intra-table parallel workers (`--segment-workers`) with PK-range splitting and OFFSET fallback
- [x] Bulk copy mode (`--bulk-copy`) using PostgreSQL `COPY` protocol
- [x] Preflight checks with actionable error hints
- [x] Pause / resume / cancel
- [x] Checkpoint-based crash recovery
- [x] Per-table live progress display with read/write rates
- [x] Live resource monitoring (goroutines, heap, CPU%)
- [x] Selective table migration (`--tables`)
- [x] Structured server logging (`--log-file`, `--log-level`)
- [x] MongoDB source adapter with schema inference
- [x] Async pipeline mode (`--async-pipeline`)
- [x] Accurate MongoDB counts (`--accurate-counts`)
- [x] Migration stats history (`--history`)
- [x] Database info in preflight (type, version, host, table count, size, SSL)
- [x] Support bundle (`xferdb support-bundle`)
- [ ] MongoDB target adapter
- [ ] Web UI
- [ ] WebSocket live progress
- [ ] Scheduled / repeated migrations
- [ ] Data transformation pipeline (column mapping, type casting)

---

## Versioning

XferDB follows [Semantic Versioning](https://semver.org/). See [CHANGELOG.md](CHANGELOG.md) for release history.

---

## Sponsorship

If XferDB saves you time on a database migration, consider sponsoring the project. Funds go toward the things that keep development moving:

- **AI assistance** — coding copilots and inference costs during development
- **Testing infrastructure** — managed PostgreSQL, MySQL, YugabyteDB, and MongoDB instances across versions and platforms for CI
- **JetBrains license** — IDE for ongoing development
- **Coffee** — because the other three need fuel

[Sponsor this project on GitHub](https://github.com/sponsors/bchan77)

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
