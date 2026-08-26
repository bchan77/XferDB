# XferDB Architecture

This document describes the high-level architecture and design decisions for XferDB.

---

## Design Principles

1. **API-First** — Core engine is accessed exclusively through APIs; CLI and UI are clients
2. **Resilient** — Migrations can be paused, resumed, and recovered from failures
3. **Pluggable** — Database adapters are interchangeable; AI integration is optional
4. **Observable** — Statistics available before, during, and after migration
5. **Safe** — Pre-flight checks verify connectivity and permissions before any data moves

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Clients                                  │
├──────────────────┬──────────────────┬───────────────────────────────┤
│       CLI        │      Web UI      │      SDK / Programmatic       │
└────────┬─────────┴────────┬─────────┴───────────────┬───────────────┘
         │                  │                         │
         ▼                  ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          REST API                                   │
│   /migrations  /preflight  /analyze  /stats  /health  /ai/suggest  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
┌─────────────────┐   ┌───────────────────┐   ┌───────────────────────┐
│  Migration      │   │  Schema           │   │  Stats                │
│  Engine         │   │  Analyzer         │   │  Collector            │
│                 │   │                   │   │                       │
│ - Pause/Resume  │   │ - Diff engine     │   │ - Pre-migration       │
│ - Checkpointing │   │ - Suggestions     │   │ - Live progress       │
│ - Batch control │   │ - AI integration  │   │ - Post-migration      │
└────────┬────────┘   └─────────┬─────────┘   └───────────┬───────────┘
         │                      │                         │
         ▼                      ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     State Manager (metadb)                          │
│          SQLite for migration state, checkpoints, history           │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Database Abstraction Layer                       │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────────┤
│  PostgreSQL │    MySQL    │   SQLite    │   MongoDB   │  Vector DBs │
│   Adapter   │   Adapter   │   Adapter   │   Adapter   │   Adapters  │
└─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
```

---

## Core Components

### 1. API Layer

The API layer is the single entry point to the core engine. All clients (CLI, Web UI, SDK) interact with XferDB through this layer.

#### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/preflight` | Verify source/target connectivity and permissions |
| `POST` | `/api/v1/analyze` | Analyze schemas and generate suggestions |
| `POST` | `/api/v1/migrations` | Start a new migration |
| `GET` | `/api/v1/migrations` | List all migrations |
| `GET` | `/api/v1/migrations/:id` | Get migration details |
| `PATCH` | `/api/v1/migrations/:id` | Pause or resume a migration |
| `DELETE` | `/api/v1/migrations/:id` | Cancel a migration |
| `GET` | `/api/v1/migrations/:id/stats` | Get migration statistics |
| `POST` | `/api/v1/ai/suggest` | Get AI-powered schema suggestions |
| `GET` | `/api/v1/health` | Health check |

#### Live Updates

For real-time progress during migrations:
- **WebSocket** at `/api/v1/migrations/:id/ws` for UI clients
- **Polling** via `/api/v1/migrations/:id/stats` for CLI

---

### 2. Migration Engine

The migration engine orchestrates data transfer between source and target databases.

#### Responsibilities

- Execute batch-based data transfer
- Maintain checkpoints for pause/resume capability
- Handle errors with configurable policies (abort vs. skip)
- Emit progress events for statistics collection

#### Pause/Resume Architecture

Migrations are checkpointed at multiple granularities:

```
Migration
    └── Tables
            └── Batches
                    └── Rows

Each level maintains its own checkpoint, enabling:
- Resume from exact failure point
- Skip completed tables on restart
- Recover from process crashes
```

#### Checkpoint Data

```yaml
migration_id: "uuid"
status: "in_progress"  # pending | in_progress | paused | completed | failed
started_at: "2024-01-15T10:00:00Z"
tables:
  - name: "users"
    status: "completed"
    rows_transferred: 50000
  - name: "orders"
    status: "in_progress"
    rows_transferred: 12500
    last_checkpoint:
      batch_id: 25
      last_pk: 12500
      timestamp: "2024-01-15T10:05:00Z"
```

---

### 3. Schema Analyzer

The schema analyzer compares source and target schemas, identifies incompatibilities, and generates suggestions.

#### Analysis Pipeline

```
┌─────────────────┐
│  Introspection  │  Query source/target information_schema
└────────┬────────┘
         ▼
┌─────────────────┐
│  Diff Engine    │  Compare schemas, detect mismatches
└────────┬────────┘
         ▼
┌─────────────────┐
│  Rule-based     │  Generate suggestions from known patterns
│  Suggestions    │
└────────┬────────┘
         ▼ (optional)
┌─────────────────┐
│  AI Suggester   │  Context-aware recommendations
└─────────────────┘
```

#### Analysis Output

```yaml
analysis:
  source_type: "postgres"
  target_type: "mysql"
  compatible: false
  tables:
    - name: "users"
      status: "compatible"
    - name: "orders"
      status: "incompatible"
      issues:
        - type: "type_mismatch"
          column: "metadata"
          source_type: "JSONB"
          target_type: "JSON"
          severity: "warning"
          suggestion: "JSONB will be converted to JSON. Binary operations will not be available."
        - type: "missing_column"
          column: "tsv_search"
          source_type: "TSVECTOR"
          severity: "error"
          suggestion: "TSVECTOR has no MySQL equivalent. Consider removing or using FULLTEXT index."
          ai_suggestion: "..."  # if AI enabled
```

#### UI Integration

When using the Web UI, schema analysis results display in a split panel:
- **Left panel**: Source schema
- **Right panel**: Target schema with highlighted differences
- **Bottom panel**: Suggestions (rule-based and AI)

---

### 4. AI Integration

AI integration is **optional** and **pluggable**. XferDB works without AI; AI enhances suggestions when configured.

#### Architecture

```
┌─────────────────────────────────────┐
│         AI Suggester Interface      │
├─────────────────────────────────────┤
│  + Suggest(context SchemaContext)   │
│  + Available() bool                 │
└──────────────┬──────────────────────┘
               │
       ┌───────┴───────┐
       ▼               ▼
┌─────────────┐ ┌─────────────┐
│  OpenAI     │ │  Anthropic  │  ... (extensible)
│  Provider   │ │  Provider   │
└─────────────┘ └─────────────┘
```

#### Configuration

```yaml
# config.yaml
ai:
  enabled: true
  provider: "openai"  # openai | anthropic | custom
  api_key: "${OPENAI_API_KEY}"
  model: "gpt-4"
```

When AI is not configured, the analyzer returns only rule-based suggestions.

---

### 5. Statistics Collector

Statistics are collected at three phases:

#### Pre-Migration Statistics

Collected during preflight/analysis:
- Row counts per table
- Table sizes (bytes)
- Index counts
- Estimated transfer duration
- Schema compatibility summary

#### Live Statistics

Collected during migration:
- Rows transferred (total and per table)
- Bytes transferred
- Current table being processed
- Transfer rate (rows/sec, bytes/sec)
- Estimated time remaining
- Error count

#### Post-Migration Statistics

Collected after migration completes:
- Total rows transferred
- Total duration
- Validation results (row count match, checksum verification)
- Error summary

#### Statistics Response

```json
{
  "migration_id": "uuid",
  "phase": "in_progress",
  "started_at": "2024-01-15T10:00:00Z",
  "elapsed_seconds": 300,
  "tables": {
    "total": 15,
    "completed": 8,
    "in_progress": 1,
    "pending": 6
  },
  "rows": {
    "total": 1500000,
    "transferred": 850000,
    "rate_per_second": 2833
  },
  "bytes": {
    "total": 2147483648,
    "transferred": 1288490189,
    "rate_per_second": 4294967
  },
  "current_table": "orders",
  "eta_seconds": 230,
  "errors": []
}
```

---

### 6. Pre-flight Checks

Before any migration starts, pre-flight checks verify:

| Check | Description |
|-------|-------------|
| Source connectivity | TCP connection to source database |
| Source authentication | Valid credentials |
| Source permissions | SELECT access on specified tables |
| Target connectivity | TCP connection to target database |
| Target authentication | Valid credentials |
| Target permissions | CREATE, INSERT, UPDATE access |
| Schema compatibility | Basic type compatibility (detailed analysis separate) |
| Resource estimation | Disk space, memory requirements |

#### Pre-flight Response

```json
{
  "status": "ready",  // ready | warnings | failed
  "checks": [
    { "name": "source_connectivity", "status": "pass" },
    { "name": "source_auth", "status": "pass" },
    { "name": "source_permissions", "status": "pass" },
    { "name": "target_connectivity", "status": "pass" },
    { "name": "target_auth", "status": "pass" },
    { "name": "target_permissions", "status": "pass" },
    { "name": "schema_compatibility", "status": "warning", "message": "3 type mismatches detected" }
  ],
  "estimates": {
    "rows": 1500000,
    "bytes": 2147483648,
    "tables": 15
  }
}
```

---

### 7. State Manager

The state manager persists all migration state to an embedded SQLite database.

#### Responsibilities

- Store migration configuration and status
- Persist checkpoints for pause/resume
- Track table-level and batch-level progress
- Maintain migration history

#### Schema

```sql
CREATE TABLE migrations (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    config JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error TEXT
);

CREATE TABLE migration_tables (
    migration_id TEXT REFERENCES migrations(id),
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_total INTEGER,
    rows_transferred INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    PRIMARY KEY (migration_id, table_name)
);

CREATE TABLE checkpoints (
    migration_id TEXT REFERENCES migrations(id),
    table_name TEXT NOT NULL,
    batch_id INTEGER NOT NULL,
    last_pk TEXT,
    rows_in_batch INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (migration_id, table_name, batch_id)
);
```

---

### 8. Database Adapters

Each supported database implements source and target interfaces.

#### Source Interface

```go
type SourceAdapter interface {
    // Connection
    Connect(ctx context.Context, config ConnectionConfig) error
    Close() error
    Ping(ctx context.Context) error

    // Introspection
    ListTables(ctx context.Context) ([]TableInfo, error)
    GetSchema(ctx context.Context, table string) (*TableSchema, error)
    GetRowCount(ctx context.Context, table string) (int64, error)

    // Data extraction
    ReadBatch(ctx context.Context, table string, opts BatchOptions) (*Batch, error)

    // Permissions
    CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}
```

#### Target Interface

```go
type TargetAdapter interface {
    // Connection
    Connect(ctx context.Context, config ConnectionConfig) error
    Close() error
    Ping(ctx context.Context) error

    // Introspection
    ListTables(ctx context.Context) ([]TableInfo, error)
    GetSchema(ctx context.Context, table string) (*TableSchema, error)

    // Schema management
    CreateTable(ctx context.Context, schema *TableSchema) error
    AlterTable(ctx context.Context, changes []SchemaChange) error

    // Data loading
    WriteBatch(ctx context.Context, table string, batch *Batch) error

    // Permissions
    CheckPermissions(ctx context.Context) (*PermissionCheck, error)
}
```

---

## Package Structure

```
xferdb/
├── cmd/
│   └── xferdb/
│       └── main.go                 # CLI entry point
│
├── api/
│   ├── server.go                   # HTTP server setup
│   ├── routes.go                   # Route definitions
│   ├── handlers/
│   │   ├── migrations.go           # Migration CRUD handlers
│   │   ├── preflight.go            # Pre-flight check handler
│   │   ├── analyze.go              # Schema analysis handler
│   │   ├── stats.go                # Statistics handler
│   │   └── health.go               # Health check handler
│   ├── middleware/
│   │   ├── logging.go
│   │   └── recovery.go
│   └── websocket/
│       └── progress.go             # Live progress updates
│
├── engine/
│   ├── engine.go                   # Migration orchestrator
│   ├── checkpoint.go               # Checkpoint management
│   ├── batch.go                    # Batch processing
│   └── worker.go                   # Worker pool
│
├── analyzer/
│   ├── analyzer.go                 # Schema analysis orchestrator
│   ├── introspect.go               # Schema introspection
│   ├── diff.go                     # Schema diff engine
│   ├── suggest.go                  # Rule-based suggestions
│   └── ai/
│       ├── suggester.go            # AI suggester interface
│       ├── openai.go               # OpenAI provider
│       └── anthropic.go            # Anthropic provider
│
├── adapters/
│   ├── adapter.go                  # Source/Target interfaces
│   ├── registry.go                 # Adapter registry
│   ├── postgres/
│   │   ├── source.go
│   │   └── target.go
│   ├── mysql/
│   │   ├── source.go
│   │   └── target.go
│   └── sqlite/
│       ├── source.go
│       └── target.go
│
├── state/
│   ├── metadb.go                   # SQLite state manager
│   ├── migrations.go               # Migration state operations
│   └── checkpoints.go              # Checkpoint operations
│
├── stats/
│   ├── collector.go                # Statistics collector
│   └── types.go                    # Statistics types
│
├── config/
│   ├── config.go                   # Configuration parsing
│   └── validate.go                 # Configuration validation
│
├── internal/
│   └── testutil/                   # Test utilities
│
└── ui/                             # Web UI (separate build)
    ├── src/
    └── ...
```

---

## Migration Flow

```
                    ┌──────────────────┐
                    │  User Request    │
                    │  (CLI or UI)     │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ POST /preflight  │
                    │                  │
                    │ - Test source    │
                    │ - Test target    │
                    │ - Check perms    │
                    └────────┬─────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
              ┌──────────┐      ┌──────────┐
              │  Pass    │      │  Fail    │──▶ Return errors
              └────┬─────┘      └──────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  POST /analyze   │
          │                  │
          │ - Introspect     │
          │ - Diff schemas   │
          │ - Generate       │
          │   suggestions    │
          │ - AI suggestions │
          │   (if enabled)   │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  User Review     │
          │                  │
          │  UI: Split panel │
          │  CLI: Summary    │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │ POST /migrations │
          │                  │
          │ - Create record  │
          │ - Start engine   │
          └────────┬─────────┘
                   │
                   ▼
    ┌──────────────────────────────────┐
    │       Migration Engine           │
    │                                  │
    │  ┌────────────────────────────┐  │
    │  │  For each table:           │  │
    │  │   - Read batch from source │  │
    │  │   - Write batch to target  │  │
    │  │   - Update checkpoint      │  │
    │  │   - Emit progress          │  │
    │  └────────────────────────────┘  │
    │                                  │
    │  Handles:                        │
    │   - PATCH /pause                 │
    │   - PATCH /resume                │
    │   - Process crash (checkpoint)   │
    └──────────────┬───────────────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Validation      │
          │                  │
          │ - Row counts     │
          │ - Checksums      │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Complete        │
          │                  │
          │ - Final stats    │
          │ - Cleanup        │
          └──────────────────┘
```

---

## Configuration

### Connection String Format

```
<type>://<user>:<password>@<host>:<port>/<database>?<options>
```

Examples:
```
postgres://user:pass@localhost:5432/mydb?sslmode=require
mysql://user:pass@localhost:3306/mydb
sqlite:///path/to/database.db
mongodb://user:pass@localhost:27017/mydb
```

### Config File Format

```yaml
source:
  type: postgres
  host: localhost
  port: 5432
  database: source_db
  user: ${POSTGRES_USER}
  password: ${POSTGRES_PASSWORD}
  ssl_mode: require

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
  on_error: abort  # abort | skip

ai:
  enabled: false
  provider: openai
  api_key: ${OPENAI_API_KEY}
```

---

## Error Handling

### Error Policies

| Policy | Behavior |
|--------|----------|
| `abort` | Stop migration on first error (default) |
| `skip` | Log error, skip row/batch, continue |

### Recoverable vs Non-Recoverable Errors

| Recoverable | Non-Recoverable |
|-------------|-----------------|
| Network timeout | Invalid credentials |
| Connection reset | Permission denied |
| Lock wait timeout | Schema incompatibility |
| Batch write failure | Disk full |

Recoverable errors trigger automatic retry with exponential backoff.

---

## Crash Recovery

XferDB is designed to handle sudden termination (kill signal, power loss, OOM) gracefully.

### What Happens on Crash

```
Program running                    Program crashes
      │                                  │
      ▼                                  ▼
┌─────────────────┐              ┌─────────────────┐
│ Read batch      │              │ State preserved │
│ Write batch     │              │ in SQLite       │
│ ▶ Checkpoint ◀──┼──────────────│                 │
│ Read next batch │              │ Last checkpoint │
│ ...             │              │ is recovery     │
└─────────────────┘              │ point           │
                                 └─────────────────┘
```

### Recovery Flow

When the program restarts after a crash:

1. **Load state from SQLite** — Read migration status, table progress, last checkpoint
2. **Identify incomplete tables** — Find tables with status = `in_progress`
3. **Resume from last checkpoint** — Use `last_pk` to query source starting from that point
4. **Skip completed tables** — Tables marked `completed` are not re-processed
5. **Continue migration** — Proceed as normal from recovery point

### State Preservation

| Preserved (in SQLite) | Lost (in memory) |
|-----------------------|------------------|
| Migration config | Current batch being written |
| Completed tables | Uncommitted rows in batch |
| Last checkpoint per table | Live statistics |
| Error history | WebSocket connections |

### Handling Duplicate Rows

If a crash occurs after writing to target but before saving the checkpoint:

```
Write batch to target  ──▶  CRASH  ──▶  Checkpoint NOT saved
                                              │
                                              ▼
                              On restart, batch is re-sent
                              (potential duplicates)
```

**Mitigation strategies:**

| Strategy | Description | Trade-off |
|----------|-------------|-----------|
| **Upsert mode** (default) | Use `INSERT ... ON CONFLICT UPDATE` | Slight write overhead |
| **Idempotency keys** | Track batch IDs in target table | Adds complexity |
| **Two-phase checkpoint** | Checkpoint before AND after write | Slower, safer |

### Default Behavior

XferDB uses **upsert mode** by default to ensure:

- **At-least-once delivery** — Every row reaches the target
- **No duplicates** — Upsert handles re-sent batches after crash
- **Minimal overhead** — Single checkpoint per batch

```
for each batch:
    1. Write to target using UPSERT
    2. Save checkpoint AFTER successful write

    // If crash occurs between steps 1 and 2:
    // - Batch is re-sent on restart
    // - UPSERT prevents duplicate rows
```

### Delivery Guarantees

| Guarantee | Supported | Notes |
|-----------|-----------|-------|
| At-least-once | Yes | Default behavior with upsert |
| Exactly-once | Partial | Requires target DB with transaction support |
| At-most-once | No | Not supported (data loss risk) |

### Manual Recovery

If automatic recovery fails, manual intervention options:

```bash
# Check migration status
xferdb migrations list

# View checkpoint details
xferdb migrations inspect <migration-id>

# Force restart from specific table
xferdb migrations resume <migration-id> --from-table <table-name>

# Discard migration and start fresh
xferdb migrations delete <migration-id>
```

---

## Security Considerations

- **Credentials**: Never logged, stored encrypted in state DB
- **TLS**: Supported for all database connections
- **API**: Authentication required for all endpoints (API key or JWT)
- **Secrets**: Environment variable substitution in config files

---

## Future Considerations

Items intentionally deferred:

- **gRPC API**: Add alongside REST if performance requires it
- **Distributed mode**: Multiple workers across machines
- **CDC/streaming**: Real-time replication (vs. batch migration)
- **Custom transformations**: Column mapping, type casting pipeline
- **Scheduling**: Cron-based repeated migrations
