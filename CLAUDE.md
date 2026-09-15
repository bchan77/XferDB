# XferDB — Agent Context

## What This Project Is

XferDB is a universal database migration tool written in Go. It moves data between databases (PostgreSQL, MySQL, SQLite, MongoDB, Vector DBs) with pause/resume, checkpointing, schema analysis, and AI-assisted suggestions.

**Design principles (non-negotiable):**
- **API-First** — The REST API is the core. CLI and Web UI are clients of it, not direct engine callers.
- **Multi-project** — A Project is a named container with a fixed source/target config that owns a history of migrations. Projects are a first-class concept baked into the state layer and API from day one.
- **Resilient** — Upsert mode + checkpointing ensures at-least-once delivery and crash recovery.
- **Pluggable** — Database adapters are interchangeable. AI is optional.

See `ARCHITECTURE.md` for full system design, endpoint list, SQL schema, and package layout.

---

## Current State of the Codebase

**ALL 11 TASKS COMPLETE. The system is fully built and compiles clean (`go build ./...`, `go vet ./...`).**

| Package | Status | Notes |
|---------|--------|-------|
| `adapters/` | Done | Core types + SourceAdapter/TargetAdapter interfaces |
| `adapters/sqlite/` | Done | Full source + target with INSERT OR REPLACE upsert |
| `adapters/postgres/` | Done | Full source + target with ON CONFLICT DO UPDATE |
| `adapters/mysql/` | Done | Full source + target with ON DUPLICATE KEY UPDATE |
| `registry/` | Done | Adapter registry + connection string parser (own package to avoid import cycle) |
| `state/` | Done | SQLite metadb: projects, migrations, migration_tables, checkpoints |
| `engine/` | Done | Migration engine with pause/resume, checkpointing, progress events |
| `stats/` | Done | Stats collector consuming engine events; thread-safe Snapshot() |
| `analyzer/` | Done | Schema diff engine + rule-based suggestions + AI stub interface |
| `api/` | Done | REST API (net/http ServeMux): projects, migrations, preflight, analyze, stats |
| `cmd/xferdb/` | Done | CLI rewritten as thin API client; `server`, `project`, `migrate` commands |
| `cmd/xferdb-web/` | Done (relational schema editing not yet supported) | Separate process serving the web UI (vanilla JS SPA, hash routing, no build step); reverse-proxies `/api/*` to `xferdb server` so the UI survives an API/engine crash (e.g. OOM) |

Legacy scaffolding in `internal/` is superseded — do not extend it.

---

## Build Plan

Build bottom-up. Projects are baked in from Task #6 onward.

### Task #1 — Core types and interfaces — READY TO START
**Package:** `adapters/adapter.go`

Define all shared types:
- `Project` (id, name, description, SourceConfig, TargetConfig, TransferConfig, CreatedAt)
- `Migration` (id, project_id, status MigrationStatus, config, timestamps, error)
- `MigrationStatus` enum: pending | in_progress | paused | completed | failed
- `TableProgress` (table_name, status, rows_total, rows_transferred)
- `Checkpoint` (migration_id, table_name, batch_id, last_pk, rows_in_batch)
- `TableSchema` (name, columns []ColumnDef)
- `ColumnDef` (name, type, nullable, primary_key, default_value)
- `SchemaChange` (type ChangeType, column ColumnDef)
- `Batch` (records []map[string]interface{}, size int)
- `BatchOptions` (offset int, limit int, last_pk interface{})
- `PermissionCheck` (can_read, can_write, can_create_table bool, errors []string)
- `ConnectionConfig` (type, host, port, database, username, password, ssl_mode, dsn string)

Define split interfaces (per ARCHITECTURE.md §8). See `adapters/adapter.go` for the current
`SourceAdapter`/`TargetAdapter` definitions — they've grown since this plan was written (e.g.
`GetPKRange`, `GetInfo`, `DropTable`, `TruncateTable`, `CreateIndexes`, `CreateConstraints`), so
treat the source file as authoritative, not this doc.

---

### Task #2 — SQLite adapter — blocked by #1
**Package:** `adapters/sqlite/source.go`, `adapters/sqlite/target.go`

Implement both interfaces for SQLite. Start here — no server needed, easy to test. WriteBatch uses `INSERT OR REPLACE INTO` for upsert.

### Task #3 — PostgreSQL adapter — blocked by #1
**Package:** `adapters/postgres/source.go`, `adapters/postgres/target.go`

Use `lib/pq` (already in go.mod). WriteBatch uses `INSERT ... ON CONFLICT DO UPDATE`. GetSchema queries `information_schema.columns`.

### Task #4 — MySQL adapter — blocked by #1
**Package:** `adapters/mysql/source.go`, `adapters/mysql/target.go`

Use `go-sql-driver/mysql` (already in go.mod). WriteBatch uses `INSERT ... ON DUPLICATE KEY UPDATE`.

### Task #5 — Adapter registry — DONE
**Package:** `registry/registry.go`

Maps type strings ("postgres", "mysql", "sqlite") to constructor functions. Parses connection strings (`postgres://user:pass@host/db`) into `ConnectionConfig`. Lives in its own package (not inside `adapters/`) to avoid the import cycle that would arise from `adapters` importing its own sub-packages.

### Task #6 — State Manager with projects — blocked by #1
**Package:** `state/metadb.go`, `state/projects.go`, `state/migrations.go`, `state/checkpoints.go`

SQLite-backed state store. See `state/metadb.go` for the current schema — it now has more tables
than the original plan (also `project_tables`, `schema_plans`, `migration_stats`), so treat that
file as authoritative, not this doc.

### Task #7 — Migration Engine — blocked by #5, #6
**Package:** `engine/engine.go`, `engine/checkpoint.go`, `engine/batch.go`, `engine/worker.go`

Orchestrates data transfer. Takes a project's source/target config, instantiates adapters via registry. Batch loop: read → write (upsert) → save checkpoint. Pause via channel signal. Resume by loading last checkpoint from state manager. Emits progress events on a channel for stats. Multiple engines can run concurrently (one per active migration).

### Task #8 — Stats Collector — blocked by #7
**Package:** `stats/collector.go`, `stats/types.go`

Subscribes to engine progress events. Tracks rows/bytes transferred, rate, ETA, table breakdown. Exposes snapshot for the API stats handler. Scoped per migration (and by extension per project).

### Task #9 — Schema Analyzer — blocked by #5
**Package:** `analyzer/analyzer.go`, `analyzer/introspect.go`, `analyzer/diff.go`, `analyzer/suggest.go`, `analyzer/ai/`

Introspects source + target schemas via GetSchema. Diff engine flags type_mismatch and missing_column. Rule-based suggestions in suggest.go. AI suggester interface in analyzer/ai/suggester.go with openai.go and anthropic.go providers (only active when configured).

### Task #10 — REST API — blocked by #7, #8, #9
**Package:** `api/server.go`, `api/routes.go`, `api/handlers/`, `api/middleware/`, `api/websocket/`

Full endpoint set with projects as top-level resource:
```
POST   /api/v1/projects
GET    /api/v1/projects
GET    /api/v1/projects/:id
DELETE /api/v1/projects/:id
POST   /api/v1/projects/:id/preflight
POST   /api/v1/projects/:id/analyze
POST   /api/v1/projects/:id/migrations
GET    /api/v1/projects/:id/migrations
GET    /api/v1/migrations/:id
PATCH  /api/v1/migrations/:id        (pause/resume)
DELETE /api/v1/migrations/:id
GET    /api/v1/migrations/:id/stats
GET    /api/v1/migrations/:id/ws     (WebSocket)
GET    /api/v1/health
```

API manages a pool of running engine goroutines. Auth via API key header.

### Task #11 — CLI rewrite — blocked by #10
**Package:** `cmd/xferdb/`

Rewrite as thin HTTP client calling the REST API. New project subcommands:
```
xferdb project create --name prod-to-staging --config project.yaml
xferdb project list
xferdb project use prod-to-staging   # stores context in ~/.xferdb/context
xferdb migrate                        # uses current project context
xferdb migrate --project prod-to-staging
```

---

## Dependency Graph

```
#1 Core types/interfaces
    ├── #2 SQLite adapter ──┐
    ├── #3 Postgres adapter ─┼── #5 Adapter registry ──┐
    ├── #4 MySQL adapter ───┘                           │
    └── #6 State Manager (projects) ───────────────────┤
                                                        │
                                          #7 Migration Engine
                                              ├── #8 Stats Collector ──┐
                                              └── #9 Schema Analyzer ──┤
                                                                        │
                                                          #10 REST API ──▶ #11 CLI
```

---

## Key Decisions

- **Multi-project from day one** — `projects` table in state, migrations scoped to a project, API routes nested under projects. Not retrofitted later.
- **Upsert mode default** — All WriteBatch implementations use upsert (not plain INSERT) to handle crash recovery without duplicates.
- **Split SourceAdapter/TargetAdapter** — Not a single combined interface. Source has read-only methods; Target adds CreateTable/AlterTable/WriteBatch.
- **API-first, CLI is a client** — CLI must not call engine code directly. It calls the REST API. This enforces the architecture boundary.
- **Start with SQLite adapter (#2)** — Easiest to test locally, validates the interface before touching Postgres/MySQL.
- **`internal/` is legacy scaffolding** — Do not extend it. New code goes into the top-level packages (`adapters/`, `engine/`, `state/`, `api/`, `analyzer/`, `stats/`).
- **Web UI runs in its own process (`cmd/xferdb-web/`)** — It holds no migration state and no source/target DB credentials; it only serves static assets and reverse-proxies `/api/*` (REST + the future WebSocket at `/api/v1/migrations/:id/ws`) to `xferdb server`. This means an OOM crash in the engine (which buffers batches in memory) doesn't take the UI down, the browser needs no CORS config since it only ever talks to one origin, and `xferdb server` can live on a private network never exposed to the browser directly. Source/target databases must be network-reachable from wherever `xferdb server` runs, not from the client machine.
