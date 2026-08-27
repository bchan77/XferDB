# XferDB Architecture

This document describes the system design, component responsibilities, and key decisions for XferDB. It reflects the current state of the codebase and the planned native-tool integration for schema migration.

---

## Design Principles

1. **API-First** — The REST API is the core. CLI and Web UI are thin clients of it, never direct engine callers.
2. **Multi-project** — A Project is a named container with a fixed source/target config that owns a history of migrations. Projects are a first-class concept baked into the state layer and API.
3. **Resilient** — Upsert mode + checkpointing ensures at-least-once delivery and crash recovery.
4. **Pluggable** — Database adapters are interchangeable. AI is optional.
5. **Schema-complete** — Schema migration is a first-class feature. For same-family migrations (PG→PG, MySQL→MySQL), native tools (`pg_dump`, `mysqldump`) are invoked transparently. For cross-family migrations (PG→MySQL, etc.), XferDB's native Go schema engine handles it.
6. **Zero manual steps** — Users interact only with XferDB. They never run `pg_dump` or `psql` themselves.

---

## System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                          Clients                             │
├───────────────────┬──────────────────┬───────────────────────┤
│       CLI         │     Web UI       │    SDK / Programmatic  │
└────────┬──────────┴────────┬─────────┴───────────┬───────────┘
         │                   │                     │
         └───────────────────┴─────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                        REST API                              │
│  /projects  /migrations  /preflight  /analyze  /stats        │
└────────────────────────────┬─────────────────────────────────┘
                             │
         ┌───────────────────┼─────────────────────┐
         ▼                   ▼                     ▼
┌─────────────────┐ ┌─────────────────┐ ┌──────────────────────┐
│  Migration      │ │  Schema         │ │  Stats               │
│  Engine         │ │  Analyzer       │ │  Collector           │
│                 │ │                 │ │                      │
│  Phase 1:Schema │ │  Diff engine    │ │  Live progress       │
│  Phase 2:Data   │ │  Suggestions    │ │  Rate / ETA          │
│  Phase 3:Post   │ │  AI integration │ │  Per-table breakdown │
└────────┬────────┘ └────────┬────────┘ └──────────────────────┘
         │                   │
         ▼                   │
┌─────────────────┐          │
│  Tool Resolver  │◀─────────┘
│  (planned)      │
│                 │
│  settings.yaml  │
│  pg_dump path   │
│  mysqldump path │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│                   State Manager (metadb)                     │
│       Embedded SQLite: projects, migrations, checkpoints     │
└──────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────┐
│                 Database Abstraction Layer                   │
├─────────────┬─────────────┬─────────────┬────────────────────┤
│  PostgreSQL │    MySQL    │   SQLite    │  (future adapters) │
│   Adapter   │   Adapter   │   Adapter   │                    │
└─────────────┴─────────────┴─────────────┴────────────────────┘
```

---

## Core Components

### 1. Projects

A **Project** is the top-level unit of configuration. It binds a source database, a target database, and transfer settings into a named entity that persists across multiple migration runs.

- A project is created once and reused for every migration against that source/target pair.
- Migrations are scoped to a project — you cannot create a migration without a project.
- The project's source/target config (including `version`) is used to resolve native tools.

```json
{
  "id": "uuid",
  "name": "prod-to-staging",
  "source_config": {
    "type": "postgres",
    "version": "15",
    "host": "prod.db.internal",
    "database": "appdb"
  },
  "target_config": {
    "type": "postgres",
    "version": "17",
    "host": "staging.db.internal",
    "database": "appdb"
  },
  "transfer_config": {
    "batch_size": 1000,
    "data_only": false,
    "schema_only": false
  }
}
```

---

### 2. API Layer

All clients interact exclusively through the REST API. The CLI and Web UI are thin HTTP clients.

#### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/projects` | Create a project |
| `GET` | `/api/v1/projects` | List all projects |
| `GET` | `/api/v1/projects/:id` | Get project details |
| `DELETE` | `/api/v1/projects/:id` | Delete a project |
| `POST` | `/api/v1/projects/:id/preflight` | Verify connectivity and permissions |
| `POST` | `/api/v1/projects/:id/analyze` | Diff schemas, generate suggestions |
| `POST` | `/api/v1/projects/:id/migrations` | Start a new migration |
| `GET` | `/api/v1/projects/:id/migrations` | List migrations for a project |
| `GET` | `/api/v1/migrations/:id` | Get migration details |
| `PATCH` | `/api/v1/migrations/:id` | Pause or resume |
| `DELETE` | `/api/v1/migrations/:id` | Cancel |
| `GET` | `/api/v1/migrations/:id/stats` | Live statistics |
| `GET` | `/api/v1/migrations/:id/ws` | WebSocket: live progress stream |
| `GET` | `/api/v1/health` | Health check |

---

### 3. Migration Engine

The engine orchestrates a full migration for a single project. Multiple engines run concurrently, one per active migration.

#### Three-Phase Execution

```
Phase 1 — Schema
    For each source table:
        IF same-family (PG→PG, MySQL→MySQL) AND native tool available:
            shell out to pg_dump --schema-only | psql  (or mysqldump | mysql)
        ELSE:
            call target.CreateTable(schema)   ← native Go path
    (CREATE TABLE IF NOT EXISTS — safe to re-run on resume)

Phase 2 — Data
    For each table not already completed:
        loop:
            batch ← source.ReadBatch(offset, limit)
            target.WriteBatch(batch)           ← upsert
            state.SaveCheckpoint()
            emit ProgressEvent
        mark table complete

Phase 3 — Post-schema
    IF same-family AND native tool was used in Phase 1:
        (schema already complete — no-op)
    ELSE (native Go path):
        For each table:
            target.CreateIndexes(indexes)
            target.CreateConstraints(fks, checks)
    (idempotent: IF NOT EXISTS / existence check before ALTER)
```

**Why indexes and constraints are deferred to Phase 3:**
- Bulk inserts without indexes are significantly faster on large tables.
- Foreign keys must be created after all tables exist to avoid reference ordering failures.
- Both operations are idempotent so a crash mid-Phase 3 is safe to resume.

#### Pause / Resume

Pause and resume use buffered channels so callers never block:

```
Pause()  → send to pauseCh (buffered 1)
Resume() → send to resumeCh (buffered 1)

Engine checks for pause signal between every batch.
On pause: persists StatusPaused to state, blocks on resumeCh.
On resume: persists StatusInProgress, continues batch loop.
```

Crash recovery uses the same mechanism — the engine reads existing table progress on startup and skips completed tables.

#### TransferConfig Flags

| Flag | Effect |
|------|--------|
| `data_only: true` | Skip Phases 1 and 3. Target schema must already exist. Use when pg_dump --schema-only was run manually or by another system. |
| `schema_only: true` | Run Phases 1 and 3 only. No data is transferred. |

---

### 4. Native Tool Integration (planned)

For same-family migrations, XferDB delegates schema work to the database's own dump/restore tools. This covers the full schema surface area (sequences, custom types, views, partitioned tables, etc.) without XferDB having to reimplement it.

#### Settings File

Located at `~/.xferdb/settings.yaml` by default; override with `--config`.

```yaml
tools:
  postgres:
    "14": /usr/lib/postgresql/14/bin
    "15": /usr/lib/postgresql/15/bin
    "17": /usr/lib/postgresql/17/bin
  mysql:
    "8.0": /usr/local/mysql-8.0/bin
    "8.4": /usr/local/mysql-8.4/bin
```

XferDB looks for `pg_dump` / `psql` (or `mysqldump` / `mysql`) inside the mapped directory. If the version is not in the settings file, it falls back to `PATH`. If no binary is found, the engine errors clearly before the migration starts — it does not silently fall back to the native Go schema path.

#### Tool Resolution Order

```
1. settings.yaml lookup for (type, version)
2. PATH fallback  (which pg_dump)
3. Error: "pg_dump not found — add version '15' to settings.yaml
          or install postgresql-client"
```

#### What Each Tool Covers

| DB family | Schema dump | Schema apply | Covers |
|-----------|-------------|--------------|--------|
| PostgreSQL | `pg_dump --schema-only` | `psql` | Sequences, custom types, views, partitions, triggers, RLS |
| MySQL | `mysqldump --no-data` | `mysql` | Views, triggers, stored procedures, events |
| SQLite | Native Go | Native Go | Tables, indexes, FKs (SQLite schema is simple enough) |

Plain SQL output format is used (not `-Fc` custom format) so `pg_restore` is not required — `psql` is sufficient.

#### Cross-Family Migrations

When source and target are different database families (e.g. PostgreSQL → MySQL), no native tool can bridge them. XferDB's native Go schema engine handles it — tables, indexes, foreign keys, and check constraints. Database-specific objects (views, triggers, stored procedures) are explicitly out of scope and documented as such.

---

### 5. Schema Analyzer

Compares source and target schemas and generates suggestions. Used by the `/analyze` endpoint before a migration starts.

```
Introspect source and target schemas
        │
        ▼
Diff engine — detects:
    missing_table   (error)
    missing_column  (error)
    type_mismatch   (warning)
        │
        ▼
Rule-based suggestions
        │
        ▼ (optional, when AI configured)
AI suggester — context-aware recommendations
```

---

### 6. Stats Collector

Consumes the engine's `ProgressEvent` channel in a background goroutine. Maintains a thread-safe snapshot exposed via `Snapshot()`.

#### Progress Phases

| Phase value | Meaning |
|-------------|---------|
| `schema` | Phase 1 in progress (creating tables) |
| `in_progress` | Phase 2 in progress (transferring data) |
| `post_schema` | Phase 3 in progress (creating indexes / constraints) |
| `paused` | Migration paused between batches |
| `complete` | All three phases finished |
| `failed` | Engine encountered an unrecoverable error |

#### Rate Calculation

Uses exponential smoothing (α = 0.3) over 1-second sample windows. Per-table row counts are tracked separately and summed to produce accurate cross-table totals.

---

### 7. State Manager

Persists all migration state to an embedded SQLite database at `~/.xferdb/state.db`.

#### Schema

```sql
CREATE TABLE projects (
    id              TEXT PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    description     TEXT,
    source_config   JSON NOT NULL,
    target_config   JSON NOT NULL,
    transfer_config JSON,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE migrations (
    id           TEXT PRIMARY KEY,
    project_id   TEXT REFERENCES projects(id),
    status       TEXT NOT NULL,
    config       JSON NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at   TIMESTAMP,
    completed_at TIMESTAMP,
    error        TEXT
);

CREATE TABLE migration_tables (
    migration_id     TEXT REFERENCES migrations(id),
    table_name       TEXT NOT NULL,
    status           TEXT NOT NULL,
    rows_total       INTEGER,
    rows_transferred INTEGER DEFAULT 0,
    started_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    PRIMARY KEY (migration_id, table_name)
);

CREATE TABLE checkpoints (
    migration_id TEXT REFERENCES migrations(id),
    table_name   TEXT NOT NULL,
    batch_id     INTEGER NOT NULL,
    last_pk      TEXT,
    rows_in_batch INTEGER,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (migration_id, table_name, batch_id)
);
```

---

### 8. Database Adapters

Each database implements two split interfaces. Split interfaces enforce that source adapters are read-only and target adapters carry all write operations.

#### SourceAdapter

```go
type SourceAdapter interface {
    Connect(ctx context.Context, config ConnectionConfig) error
    Close() error
    Ping(ctx context.Context) error
    ListTables(ctx context.Context) ([]TableSchema, error)
    GetSchema(ctx context.Context, table string) (*TableSchema, error)
    GetRowCount(ctx context.Context, table string) (int64, error)
    ReadBatch(ctx context.Context, table string, opts BatchOptions) (*Batch, error)
    CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}
```

#### TargetAdapter

```go
type TargetAdapter interface {
    Connect(ctx context.Context, config ConnectionConfig) error
    Close() error
    Ping(ctx context.Context) error
    ListTables(ctx context.Context) ([]TableSchema, error)
    GetSchema(ctx context.Context, table string) (*TableSchema, error)
    CreateTable(ctx context.Context, schema *TableSchema) error
    AlterTable(ctx context.Context, table string, changes []SchemaChange) error
    // Phase 3 — called after all data is loaded
    CreateIndexes(ctx context.Context, table string, indexes []IndexDef) error
    CreateConstraints(ctx context.Context, table string, fks []ForeignKey, checks []CheckConstraint) error
    WriteBatch(ctx context.Context, table string, batch *Batch) error
    CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}
```

#### TableSchema

```go
type TableSchema struct {
    Name        string            // table name
    Columns     []ColumnDef       // columns with types, nullability, defaults, PKs
    Indexes     []IndexDef        // non-PK indexes (unique and non-unique)
    ForeignKeys []ForeignKey      // FK constraints with ON DELETE/UPDATE rules
    Checks      []CheckConstraint // CHECK constraints
}
```

#### Adapter Behaviour Notes

| Adapter | WriteBatch upsert | FK handling | Index creation |
|---------|-------------------|-------------|----------------|
| SQLite | `INSERT OR REPLACE` | Inline in `CREATE TABLE` (no ALTER TABLE ADD FK) | `CREATE INDEX IF NOT EXISTS` |
| PostgreSQL | `INSERT … ON CONFLICT DO UPDATE` | `ALTER TABLE ADD CONSTRAINT FOREIGN KEY` | `CREATE INDEX IF NOT EXISTS` |
| MySQL | `INSERT … ON DUPLICATE KEY UPDATE` | `ALTER TABLE ADD CONSTRAINT FOREIGN KEY` | Existence check then `CREATE INDEX` |

SQLite auto-generated UNIQUE indexes (`sqlite_autoindex_*`) are renamed to portable names (`uq_{table}_{cols}`) during introspection so they can be recreated on the target.

---

## Package Structure

```
xferdb/
├── cmd/xferdb/
│   ├── main.go
│   └── commands/
│       ├── root.go          # server command, --server flag
│       ├── project.go       # project create/list/use/show
│       └── migrate.go       # migrate command (thin API client)
│
├── api/
│   ├── server.go            # Server struct, ListenAndServe
│   ├── routes.go            # Go 1.22 ServeMux with method+path patterns
│   └── handlers/
│       ├── projects.go      # CreateProject, ListProjects, GetProject,
│       │                    # DeleteProject, Preflight, Analyze
│       └── migrations.go    # StartMigration, GetMigration, ListMigrations,
│                            # PatchMigration, DeleteMigration, GetStats
│
├── engine/
│   ├── engine.go            # Engine struct, 3-phase Run, Pause/Resume
│   ├── table.go             # transferTable, resumeOffset
│   └── progress.go          # ProgressEvent, EventKind constants
│
├── analyzer/
│   ├── analyzer.go          # Analyze(source, target) → Analysis
│   ├── diff.go              # Compare schemas → []Issue
│   ├── suggest.go           # Rule-based suggestions
│   └── ai/
│       └── suggester.go     # Suggester interface + NoopSuggester
│
├── adapters/
│   ├── adapter.go           # All shared types + SourceAdapter/TargetAdapter interfaces
│   ├── postgres/
│   │   ├── postgres.go      # buildDSN, getSchema (columns+indexes+FKs+checks)
│   │   ├── source.go
│   │   └── target.go        # CreateTable, CreateIndexes, CreateConstraints, WriteBatch
│   ├── mysql/
│   │   ├── mysql.go         # buildDSN, getSchema (columns+indexes+FKs)
│   │   ├── source.go
│   │   └── target.go
│   └── sqlite/
│       ├── sqlite.go        # getSchema (PRAGMA table_info/index_list/foreign_key_list)
│       ├── source.go
│       └── target.go        # CreateTable (FKs inline), CreateIndexes, CreateConstraints (no-op)
│
├── registry/
│   └── registry.go          # NewSource/NewTarget, ParseConnectionString
│                            # (own package to avoid import cycle with adapters/)
│
├── state/
│   ├── metadb.go            # MetaDB struct, Open, migrate (DDL)
│   ├── projects.go          # CreateProject, GetProject, ListProjects, DeleteProject
│   ├── migrations.go        # SetMigrationStatus, UpsertTableProgress, GetTableProgress
│   └── checkpoints.go       # SaveCheckpoint, GetLastCheckpoint, DeleteCheckpoints
│
├── stats/
│   ├── collector.go         # Collector, Start, Snapshot, apply, updateRate
│   └── types.go             # StatsSnapshot, TableStats, RowStats
│
└── tools/                   # planned
    ├── resolver.go          # read settings.yaml, resolve binary path for (type, version)
    └── runner.go            # shell out to pg_dump/psql, capture errors
```

---

## Configuration

### Project Connection Config

The `version` field tells the tool resolver which binary path to use from `settings.yaml`.

```json
{
  "type": "postgres",
  "version": "15",
  "host": "localhost",
  "port": 5432,
  "database": "mydb",
  "username": "user",
  "password": "secret",
  "ssl_mode": "require"
}
```

### settings.yaml

Global tool registry. Located at `~/.xferdb/settings.yaml`; override with `xferdb --config /path/to/settings.yaml`.

```yaml
tools:
  postgres:
    "14": /usr/lib/postgresql/14/bin
    "15": /usr/lib/postgresql/15/bin
    "17": /usr/lib/postgresql/17/bin
  mysql:
    "8.0": /usr/local/mysql/bin
    "8.4": /usr/local/mysql-8.4/bin
```

If a version is not listed, XferDB falls back to `PATH`. If no binary is found anywhere, the migration fails before it starts with a clear error pointing to this file.

### Transfer Config

```json
{
  "batch_size": 1000,
  "workers": 1,
  "validate": false,
  "on_error": "abort",
  "data_only": false,
  "schema_only": false
}
```

---

## Crash Recovery

XferDB checkpoints after every successfully written batch. If the process crashes:

1. Restart the server (`xferdb server`)
2. Resume the migration via `PATCH /api/v1/migrations/:id` with `{"action":"resume"}`
3. The engine reads existing table progress, skips completed tables, and continues from the last batch offset

Because `WriteBatch` uses upsert semantics, a batch that was written but not checkpointed before the crash is safely re-sent — no duplicate rows are created.

---

## Key Decisions

| Decision | Rationale |
|----------|-----------|
| Projects as first-class concept | Migrations belong to a project; the source/target config is defined once and reused across runs |
| Split SourceAdapter / TargetAdapter | Enforces read-only source, prevents accidental writes to source |
| Upsert-default WriteBatch | At-least-once delivery without duplicates; crash-safe |
| Phase 3 indexes/constraints deferred | Bulk insert without indexes is faster; FK ordering requires all tables to exist first |
| Native tools for same-family schema | pg_dump covers sequences, custom types, views, triggers — XferDB cannot maintain parity |
| Native Go for cross-family schema | No native tool bridges database families; tables + indexes + FKs covers the common case |
| Plain SQL dump format (not -Fc) | Avoids pg_restore dependency; psql is universally available |
| registry/ as separate package | Placing the registry inside adapters/ would cause an import cycle (adapters → adapters/postgres → adapters) |
| CLI is a thin HTTP client | Enforces the API-first boundary; all logic lives in the server |
