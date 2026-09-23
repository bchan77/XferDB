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
| `adapters/sqlite/` | Done | Full source + target with INSERT OR REPLACE upsert. `--bulk-copy` mode batches multi-row `INSERT OR REPLACE` under the bound-parameter limit plus `PRAGMA synchronous = OFF` for the write (`target_bulkcopy_test.go` covers it, runs unconditionally — no server needed) |
| `adapters/postgres/` | Done | Full source + target with ON CONFLICT DO UPDATE. `--bulk-copy` mode uses the COPY protocol (`writeBatchCopy`); requires an empty target (no ON CONFLICT support) and converts `[]byte` values to string for any non-bytea column, since lib/pq's COPY encoder otherwise hex-mangles them as bytea regardless of the real column type (`target_bulkcopy_test.go`, opt-in via `XFERDB_TEST_TARGET_DSN`) |
| `adapters/mysql/` | Done | Full source + target with ON DUPLICATE KEY UPDATE. `--bulk-copy` mode uses `LOAD DATA LOCAL INFILE` via a registered `io.Reader`, with `REPLACE INTO TABLE` (tolerates a non-empty target, unlike postgres) and `CHARACTER SET binary` (without it the server rejects genuine binary column values as invalid multi-byte utf8mb4) (`target_bulkcopy_test.go`, opt-in via `XFERDB_TEST_TARGET_DSN`) |
| `registry/` | Done | Adapter registry + connection string parser (own package to avoid import cycle) |
| `state/` | Done | SQLite metadb: projects, migrations, migration_tables, checkpoints |
| `engine/` | Done | Migration engine with pause/resume, checkpointing, progress events |
| `stats/` | Done | Stats collector consuming engine events; thread-safe Snapshot() |
| `analyzer/` | Done | Schema diff engine + rule-based suggestions + AI stub interface |
| `api/` | Done | REST API (net/http ServeMux): projects, migrations, preflight, analyze, stats |
| `cmd/xferdb/` | Done | CLI rewritten as thin API client; `server`, `project`, `migrate` commands |
| `cmd/xferdb-web/` | Done | Separate process serving the web UI (vanilla JS SPA, hash routing, no build step); reverse-proxies `/api/*` to `xferdb server` so the UI survives an API/engine crash (e.g. OOM). A project's default page (`#/projects/:id`) is its table list with live migration status/controls (locks tables while a migration is active); connection settings moved to `#/projects/:id/settings`. The split-view schema editor's type-override dropdown works for every adapter pairing, not just MongoDB→PostgreSQL — see `state/schema_plan.go` (`OverridePlan` upserts, keyed by table/collection + field name) and `engine/engine.go` (`applyPlanOverrides`, applied before `CreateTable` for relational sources too) |

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
- **`--bulk-copy` trades the upsert guarantee for speed, per adapter** — `adapters.BulkCopyWriter` is an opt-in interface (`EnableCopy()`) all three target adapters implement, each with its own fast bulk-load mechanism (postgres: COPY protocol; mysql: `LOAD DATA LOCAL INFILE`; sqlite: batched `INSERT OR REPLACE` + `PRAGMA synchronous = OFF`). The engine only calls `EnableCopy()` when `--truncate` or `--recreate-schema` also guarantees an empty target — postgres's raw COPY has no conflict handling at all, so a non-empty target fails outright; mysql's `REPLACE INTO TABLE` and sqlite's `INSERT OR REPLACE` degrade gracefully instead, but the precondition is still required for consistency and because sqlite's `synchronous = OFF` trades away crash durability regardless of conflicts.
- **Split SourceAdapter/TargetAdapter** — Not a single combined interface. Source has read-only methods; Target adds CreateTable/AlterTable/WriteBatch.
- **API-first, CLI is a client** — CLI must not call engine code directly. It calls the REST API. This enforces the architecture boundary.
- **Start with SQLite adapter (#2)** — Easiest to test locally, validates the interface before touching Postgres/MySQL.
- **`internal/` is legacy scaffolding** — Do not extend it. New code goes into the top-level packages (`adapters/`, `engine/`, `state/`, `api/`, `analyzer/`, `stats/`).
- **Web UI runs in its own process (`cmd/xferdb-web/`)** — It holds no migration state and no source/target DB credentials; it only serves static assets and reverse-proxies `/api/*` (REST + the future WebSocket at `/api/v1/migrations/:id/ws`) to `xferdb server`. This means an OOM crash in the engine (which buffers batches in memory) doesn't take the UI down, the browser needs no CORS config since it only ever talks to one origin, and `xferdb server` can live on a private network never exposed to the browser directly. Source/target databases must be network-reachable from wherever `xferdb server` runs, not from the client machine.
- **CI gates on the real compatibility matrix, not just `go test`** — `.gitea/workflows/go-build.yaml` runs `go build`/`go test` in-runner; `.gitea/workflows/xferdb-matrix-gate.yaml` additionally triggers the Jenkins `xferdb-matrix` job (real Postgres/MySQL/MongoDB sources in Kubernetes, `SIZE=SMOKE`) on every push/PR touching `**.go`, and posts an APPROVED or REQUEST_CHANGES review on the PR via a bot token once it finishes. An "approved" review on a PR here may be that bot, not a human — check who posted it. Needs repo secrets/vars `JENKINS_URL`, `JENKINS_USER`, `JENKINS_TOKEN` (Jenkins auth) and the existing `GH_TOKEN` (Gitea review API) to function; see `/Users/bchan/workspace/NextDevOps/XferDB-jenkins` for the Jenkins pipeline itself. The review-posting step's `curl` call checks the HTTP status and fails loudly on anything outside 2xx — it used to send an invalid `event` value (`APPROVE` instead of Gitea's actual `APPROVED`) silently swallowed by an unchecked `curl -s`, so every approval attempt failed without anyone noticing until a PR sat unreviewed with green CI.
- **UI smoke tests on web changes** — `.gitea/workflows/ui-smoke-test.yaml` triggers the Jenkins `xferdb-ui` job when PRs touch `cmd/xferdb-web/**`. Runs Playwright E2E tests against the web UI (project CRUD, smoke tests). Same approval/request-changes pattern as the matrix gate. Also supports `workflow_dispatch` for manual runs.
