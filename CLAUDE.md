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
| `adapters/postgres/` | Done | Full source + target with ON CONFLICT DO UPDATE. **YugabyteDB** (#158) is adapter type `yugabyte` (source and target, `yugabyte://` DSNs, port 5433) running this same adapter; code that branches on `"postgres"` should use `adapters.PostgresFamily` unless the behaviour really is PostgreSQL-only. Live readiness (`livecheck.go`) detects YugabyteDB from `version()` (`-YB-`): it skips `pg_stat_replication` (waits up to 2 minutes for `system.cdc_state`, which only the first slot creates), checks `yb_enable_replication_commands`, fails replica identity `CHANGE` (YugabyteDB's default, which pgoutput refuses) and `NOTHING`, and reports `cdc_intent_retention_ms` instead of WAL limits; a type that doesn't match the server is a failed check. Each check query runs through `queryRow`, which stops waiting after `liveCheckTimeout` even if the server never answers (a cancelled context alone doesn't bound lib/pq). Live from `yugabyte` (`yugabyte_cdc.go`): slots are created with `yb_lsn_type = HYBRID_TIME` (by SQL; the replication protocol can't ask for it), so slot LSNs are hybrid times comparable with `yb_get_current_hybrid_time_lsn()`, which `CurrentPosition` returns; an idle YugabyteDB stream's keepalives don't advance (unlike PostgreSQL's), so `CurrentPosition` then commits a heartbeat (this stream's row in `xferdb_heartbeat`, in the publication; `Stream` drops its changes, so the commit only moves `Confirmed`). `cdc.TimePositions` marks the source: `Behind` is microseconds, the manager reports `behind_seconds` instead of `lag_bytes` and no retention. Sequences are read from each sequence (YugabyteDB's `pg_sequences.last_value` is always NULL). Teardown deletes the heartbeat row first, drops the slot (waits up to 75s: a stopped stream's slot stays active for `ysql_cdc_active_replication_slot_window_ms`), then the publication, then the heartbeat table once empty (YugabyteDB won't drop a published table). `listTables` and pg_dump (`--exclude-table`) leave the heartbeat table out. The pg_dump schema copy covers the whole postgres family. On a `yugabyte` target the post-data primary keys (`isPrimaryKey`) run right after pre-data, while the tables are empty: added after the copy, YugabyteDB rewrites the table (rows are stored in key order); later post-data stages skip them (`alreadyExists` 42P16). **Leftover streams** (`api/handlers/live_streams.go`, `adapters.LiveStreamLister` in `adapters/postgres/livestreams.go`): slots/publications/heartbeat rows named `xferdb_[a-z0-9]+` that no running (pending/in_progress/paused) live migration on this server owns, matched by `postgres.SlotName(migration id)`; a readiness warning, `GET`/`DELETE /projects/:id/live-streams[/:name]`, the settings page's *Drop* buttons and `xferdb leftovers`. Never dropped automatically (another server's streams look the same), and never an active slot. `XFERDB_TEST_YB` (a YugabyteDB DSN) runs the YugabyteDB tests: readiness, `cdc/worker` `TestLiveMigrationConvergesYugabyte`, `api` `TestLiveMigrationYugabyte` (SQLite and YugabyteDB targets) and `TestLiveMigrationSchemaChangeYugabyte`. **Schemas:** with `ConnectionConfig.Schema` empty (the default) a source migrates every non-system schema; set, only that one (passed as the session's `search_path` by `withSearchPath`, keyword or URL DSN). Table names stay bare for the connection's own schema (`current_schema()`: Schema, or public), so existing projects/checkpoints keep their names, and are `schema.table` for any other; `splitName`/`qualify` turn them back into SQL, and every catalog query filters on `schemaExpr` (the name's schema, else `current_schema()`). A target given a `schema.table` name creates that schema and writes there (`CREATE SCHEMA IF NOT EXISTS`, `pq.CopyInSchema`); a mysql/sqlite target gets a table literally named `schema.table`, and a dotted MongoDB collection name now lands in a schema on a postgres target. pg→pg schema copy (`engine/native.go`, `pg_dump`) keeps objects in their source schema, so `pgDumpSchemaArgs` uses it only when both sides' own schema match (`--schema=` for a single-schema source) and otherwise falls back to `CreateTable`, which can't recreate custom enum types. Preflight shows `schemas` (all mode) or, for an empty single schema, `other_schemas` |
| `adapters/mysql/` | Done | Full source + target with ON DUPLICATE KEY UPDATE. `--bulk-copy` mode uses `LOAD DATA LOCAL INFILE` via a registered `io.Reader`, with `REPLACE INTO TABLE` (tolerates a non-empty target, unlike postgres) and `CHARACTER SET binary` (without it the server rejects genuine binary column values as invalid multi-byte utf8mb4) (`target_bulkcopy_test.go`, opt-in via `XFERDB_TEST_TARGET_DSN`) |
| `registry/` | Done | Adapter registry + connection string parser (own package to avoid import cycle) |
| `state/` | Done | SQLite metadb: projects, migrations, migration_tables, checkpoints. Opened with **one connection** (`SetMaxOpenConns(1)`): every writer is in-process, and a pool let table workers race for SQLite's file lock and fail tables with `database is locked` (#124). Close rows before the next query on this handle, or it waits on itself. The SQLite target adapter does the same for its write handle |
| `engine/` | Done | Migration engine with pause/resume, checkpointing, progress events |
| `stats/` | Done | Stats collector consuming engine events; thread-safe Snapshot() |
| `metrics/` | Done (#105) | Prometheus `GET /metrics` on `xferdb server` (outside `/api/`, so the web process doesn't proxy it, and outside the request log). `client_golang` is pinned to v1.24.x: v1.25 needs Go 1.26. Fed through `stats.Observer` (`Collector.SetObserver`), one `metrics.Run` per migration: counters from events, with rows counted from each event's per-table running total (only increases), so dropped batch events and out-of-order segment events don't skew it; per-migration gauges read from active migrations' `Snapshot()` at scrape time and dropped when the run ends (`Observer.Done`) |
| `analyzer/` | Done | Schema diff engine + rule-based suggestions + AI stub interface |
| `api/` | Done | REST API (net/http ServeMux): projects, migrations, preflight, analyze, stats, validations |
| `validate/` | Done (#103 phases 1-4) | Post-migration data validation: L1 row counts, L2 order-independent row fingerprint, and L3 primary-key row diff (`diff.go`), for every source (MongoDB included). Re-reads the source through its own `ReadBatch` + the schema plan (so flattening/renames/`_extra` reproduce themselves) and reads the target through the *source* adapter of the target's type, then compares values after `canonicalize` (`canonical.go`) by the intended column's logical type. Runs automatically after a completed migration unless `validation: off` / `--skip-validation`. Web UI: Validation/Level selects on the migration start form (per run) and in project settings (project default, saved through `PUT /projects/:id` `transfer_config`), per-table badges on the table list (from `GET /projects/:id/validation-status`), and a results card with *Validate now* on the migration page. L3 runs only on a table that failed L1/L2: a single integer key is diffed by key range on both sides, any other key in the safe set (int, text/char, decimal, uuid, date, Mongo `_id`, composite) by `adapters.RowLookup` (`WHERE pk IN (…)` on the target) plus a reverse pass over a sorted set of source-key hashes; other tables keep their L2 result with the reason in `level_note`. Up to 100 example rows per table go to `validation_diffs` (`GET /validations/:id/tables/:table/diffs`; the UI's *View rows* drawer); `sample` (percent) limits the diff. **Lossy by plan** (`lossy.go`): a relational column narrowed by a plan override is converted by the target as it stores it, so the validator applies the same conversion to the source value. A table whose only differences are those conversions is `expected_lossy` (not a failure; `lossy_rows`, examples of kind `lossy_by_plan`). Only conversions a target performs *silently* are modelled, as measured on PostgreSQL 16 / MySQL strict mode: numeric scale reduction (both, half away from zero), float/decimal→integer (MySQL only; a float rounds half to even, decimal text half away), timestamp→date (MySQL rounds to whole seconds first). PostgreSQL refuses 1.5 in an integer, strict targets refuse over-long VARCHAR (the migration fails), and SQLite stores values as given. Live check: `XFERDB_TEST_LOSSY_PG`/`XFERDB_TEST_LOSSY_MYSQL` run `TestLossyByPlanLive` over every pair. **`per_table` mode** (`api/handlers/validations_per_table.go`): each table is validated as soon as the engine records it completed, while the rest still copy, with as many workers as the migration's table workers (each a `validate.Session` holding one source/target connection pair). It hooks `engine.OnTableDone`, not `EventTableDone`, so it doesn't depend on an `Events()` subscriber (since #137 only `EventBatch` is dropped when the channel is full, and only once something subscribes are the rest guaranteed), and `finish` re-sweeps `migration_tables` at the end. The run is created when the first table completes, so a migration that completes nothing leaves no validation, as with `after`. If the migration fails or is cancelled, the completed tables are still validated and the rest are recorded as skipped. **Progress:** `Request.OnProgress` gets a `validate.Progress` per table (step counting → reading_source → reading_target, or row_diff; L2 rows read of both counts summed), at each step and once per batch; `ValidationsHandler` keeps the latest per running table in memory only (dropped once the result is saved, and when the run ends) and returns it as `in_progress` from `GET /validations/:id`, which the UI shows with a bar, rate and ETA (also in `GET /projects/:id/validation-status` `in_progress`, for the tables page's badges and banner). **Reads:** `scan` streams a whole table in one `SELECT *` through `adapters.TableStreamer` (postgres, mysql, sqlite) since the fingerprint and diff lookups are order-independent; paging cost a full scan + sort per page on a YugabyteDB hash-sharded key (12s/page, 1.2M rows ≈ 8h vs 2m15s streamed) |
| `cdc/` | Done (#102 phases 1-4) | Live migration, engine-neutral. `Change` (one row change; values must be exactly what the source's `ReadBatch` returns for that column, so streamed and copied rows reach the target alike) and `ChangeSource` (`adapters/postgres/cdc.go`: pgoutput via `jackc/pglogrepl`, decoding mirroring lib/pq's `textDecode`, domains mapped to base types; `TestDecodeMatchesSnapshot` checks it type by type). `queue.go`: segment files + a `commit` seq, so readers see only fsynced whole transactions; `OpenWriter` drops a torn or unfinished tail. `Capturer` acks the source only after fsync, and reports `Confirmed` (the source's flushed position, which moves past idle WAL) read under the same lock as `QueuedSeq`. `Applier`: micro-batches, per table in seq order, tables in parallel, each table's part committed with its seq via `adapters.ChangeApplier` (`xferdb_cdc.applied` on postgres, `xferdb_cdc_applied` on mysql/sqlite), then a stream high-water mark (`HighWaterTable`). A key-changing update missing columns is resolved against what the target holds, else re-read from the source. `cdc/worker`: `xferdb cdc capture|apply --dir` processes (hidden commands), loopback status endpoint with a token (not a Unix socket: macOS limits their paths to ~104 bytes), a `<role>.lock` flock so two of a role never run on one directory, never writing the metadb. **`cdc/live` (phase 2)** is the server side: `Manager.Setup` (engine `Live.Setup` hook, before any row is copied) creates the slot/publication, saves `cdc_streams`, writes the spec and starts supervising; the engine's live snapshot (`engine/live.go`) pages each table by its whole primary key (`adapters.KeysetSource`, `BatchOptions.Keyset`, row-value comparison on text-bound params) up to `MaxKey` at the table's start, and creates only plain indexes (and, on a pg_dump schema, the primary keys); `SnapshotDone` records `catchup_pos`; supervision (every second) starts/restarts workers with backoff (`ExecLauncher`: `xferdb cdc …` in its own process group; `InProcessLauncher` in tests), computes lag, and ends catch-up once capture has confirmed `catchup_pos` and apply has reached the queue seq it had then, creating the unique indexes (`engine.DeferredSchema`, `StageCatchUp`; FKs/checks/triggers are `StageCutover`, for phase 3). Phases in `migrations.phase`: setup → snapshot → catch_up → streaming → cutover → completed. **Cutover (phase 3, `cdc/live/cutover.go`)** runs in its own goroutine, reading the status supervision refreshes: drain (record the source's position, wait for capture to confirm it and apply to reach the queue seq capture had then), quiet check (after `QuietWait`, any change capture queued past that seq means writes continue: stop, back to streaming with `writes_after`, unless `ignore_writes`, which drains once more), sequences (`adapters.SequenceSyncer`, postgres target only: `setval` for each source sequence the target has), `engine.DeferredSchema(StageCutover)`, then `forget` (supervision stops first, or it would restart the workers) and `teardown`, status completed, and `OnCompleted` starts validation by the run's config (`ValidationsHandler.AfterCutover`). A step that fails leaves the stream running for another try; a restart in phase cutover reruns it from the start. `POST /migrations/:id/cutover` (409 when not streaming, paused or already cutting over), `xferdb cutover [--yes] [--ignore-writes]`, and the UI's *Cut over* button with *Cut over again* / *Cut over anyway* when writes are detected. **Schema changes and WAL (phase 4):** the stream sends an `OpSchema` change whenever pgoutput describes a table (a Relation message: once per session and after DDL); the capturer compares its columns (name + `format_type`, generated columns left out, as `ChangeSource.Columns` reads them at setup into `Spec.Schema`) with what it knows (`schema.json`, saved after the queue sync and before the ack, so a crash can at worst queue one twice) and queues only a real difference, or a table name it doesn't know (a rename). An `OpSchema` change never carries `Pos`. Apply ends a batch before one, applies everything earlier, then waits with `ApplyStatus.Blocked` until `schema.accepted` reaches its seq; supervision then pauses the migration and keeps the change in `cdc_streams.schema_change`, and `Manager.Resume` accepts it (XferDB never applies DDL: the user changes the target first; `xferdb resume` asks). `ChangeSource.Retention` (restart_lsn distance, `wal_status`, `safe_wal_size` on 13+) feeds `Status.Retention` and `Status.Warnings` (capture down over `CaptureDownWarning`, slot unreserved or within a quarter of its limit, the queue's disk under 10% or 1 GiB free); a lost or missing slot fails the migration and tears it down (`failLost`). Live runs are skipped by `MarkInterruptedMigrations`; `Server.ResumeLive` reruns an interrupted snapshot (from checkpoints, with the run's `migrations.run_config`) or reattaches. A failed or cancelled snapshot, a cancel and a delete all tear the stream down (`Manager.Stop`); there is no resuming a failed live migration. Live tests need `XFERDB_TEST_CDC_PG` (a `wal_level=logical` server): `cdc/worker` `TestLiveMigrationConverges`, `api` `TestLiveMigration` (writer running, apply killed, server restarted; SQLite and PostgreSQL targets), `TestLiveMigrationResumesSnapshot`, `TestLiveMigrationSchemaChange` and `TestLiveMigrationSlotLost` **MongoDB source (#179, `adapters/mongo/cdc.go`):** a change stream on the database (replica set, or a sharded cluster through one mongos: a stream's cursor lives on the mongos that opened it), nothing created on the source (`Prepare` records the cluster time, `Teardown` is a no-op), so `Manager.Setup` reuses `cdc_streams.start_pos` on a rerun. Positions are `<t>.<i>[/<resume token>]` cluster times (`cdc.TimePositions`; a token's `_data` starts with 0x82 + t + i); `Confirmed` follows the stream's high-water mark, which `Capturer.ConfirmedFile` saves so a quiet stream resumes from it rather than from a last change that may have left the oplog; lost is checked by `cdc.PositionRetention` (oldest oplog entry after that resume point). Documents are converted by the plan (`Spec.Plan`, `SetPlan`) with every column present (missing fields NULL, unknown ones in `_extra`, never an `OpSchema`); updates use post-images where the collection keeps them (6.0+, `whenAvailable`), else the document is read back from the primary. The live snapshot is `KeysetSource` on `_id`, read from the primary, a BSON type group at a time (`readKeyset`), and a live run from MongoDB creates every non-key column nullable (`nullableColumns`: the plan's NOT NULL is only what the sample saw). **Sharded:** a shard key change that moves the document is streamed as an insert (new shard key) and a delete (old one) in one transaction (same `lsid` + `txnNumber`, cluster times of their own) in either order, so `Stream` buffers a transaction's events and sends them whole with `Pos` on the last (`txnBuffer`), and drops a delete of an `_id` its transaction inserted under another `documentKey` (`moved`; the inserts of the last `txnMemory` transactions are remembered, in case another event comes between). Deletes are keyed by `_id` only: matching the shard key too (#181) left rows behind when updates without post-images were skipped. A same-shard shard key change is an update whose `documentKey` has the old value, so a lookup that finds nothing by it retries by `_id`; the oplog can't be read through mongos, so readiness only warns about it and `RetentionAt` opens a stream at the resume point instead (at most once a minute). mongos passes a change on only once every shard is past it (an idle shard's no-op write comes every 10s), so the cutover's quiet check waits for capture to confirm the source's position *after* its wait before counting writes (`awaitCaptured`). `XFERDB_TEST_MONGO_RS` (a replica set URI) and `XFERDB_TEST_MONGO_SHARDED` (a mongos URI) run `TestLiveMigrationMongo` (SQLite, plus `XFERDB_TEST_MONGO_PG` / `XFERDB_TEST_YB` targets; sharded: users by hashed `_id`, things by a range of `v` split over two shards), `TestStreamResumesFromHighWaterMark` and `TestLiveChecksMongo` |
| `cmd/xferdb/` | Done | CLI rewritten as thin API client; `server`, `project`, `migrate` commands |
| `cmd/xferdb-web/` | Done | Separate process serving the web UI (vanilla JS SPA, hash routing, no build step); reverse-proxies `/api/*` to `xferdb server` so the UI survives an API/engine crash (e.g. OOM). A project's default page (`#/projects/:id`) is its table list with live migration status/controls (locks tables while a migration is active); connection settings moved to `#/projects/:id/settings`. The connection editor asks for host/port/username/password (and SSL mode) before the database, whose *Load databases* button calls `POST /api/v1/connections/databases` (`adapters.DatabaseLister`; postgres connects to the `postgres` maintenance DB, then `template1`, when no database is given) and swaps the text box for a dropdown, or shows the connection error under the field. The settings form passes `project_id`/`side` so the stored password is reused, but only while type/host/port/username still match it. When preflight fails only because the target database doesn't exist (postgres 3D000, mysql 1049), it returns `target_missing_database` and the settings page offers a *Create database* button (`POST /api/v1/projects/:id/target-database`, `adapters.DatabaseCreator`) — deliberately a click, never automatic, so a typo'd name doesn't silently become a new database. The connection editor's target side offers the same when *Load databases* finds the typed name missing (`POST /api/v1/connections/create-database`, unsaved fields). The migration start form opens with the project's `transfer_config` (workers, batch size, segment workers, performance checkboxes, validation) and *Run Migration* saves those choices back to it (`run_options_saved` marks that, since an older project's `async_pipeline: false` means unset while the form's default is on); schema mode, bulk copy and table selection are deliberately not remembered. The split-view schema editor's type-override dropdown works for every adapter pairing, not just MongoDB→PostgreSQL — see `state/schema_plan.go` (`OverridePlan` upserts, keyed by table/collection + field name) and `state.ApplyPlanOverrides` (applied by the engine before `CreateTable` for relational sources too, and by `validate/` to learn each column's intended type) |

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
- **Live migration is labelled beta** (decided 2026-11-04) — a *Beta* badge wherever the web UI offers or shows it (`betaBadge()`), "beta" in the CLI's `--live`, `cutover` and `resume` help plus a note when one starts (`liveBetaNote`), and in the README, CHANGELOG and the user guide `docs/LIVE_MIGRATION.md` (keep that guide in step with the UI, CLI and API). Drop all of it together when it leaves beta.
- **Column defaults are translated, not copied** — each source records the dialect of its defaults (`ColumnDef.DefaultDialect`; the MySQL source turns information_schema's unquoted/escaped text into real SQL, `mysqlDefaultSQL`), and each target's `CreateTable`/`AlterTable` goes through `adapters.TranslateDefault`: the same dialect keeps the expression as written, another gets literals, booleans, current timestamp/date and UUID generators translated, and anything else (sequences, arbitrary expressions) dropped, which never changes copied data. `engine` `TestColumnDefaultsLive` checks every pair (needs `XFERDB_TEST_PAIRS_PG` / `XFERDB_TEST_PAIRS_MYSQL`, host:port with password-less logins), and the matrix's `column_defaults` suite does the same in the cluster.
- **Validation is on by default** — a completed migration is validated at L2 (`validate/`) unless the project's `transfer.validation` is `off` or the run passes `--skip-validation`/`"validation":"off"`. `xferdb migrate` waits for it and exits non-zero on a failing table, so the matrix gate exercises validation on every pair without any harness change. Only tables whose `migration_tables.status` is `completed` are validated; a result is `stale` once a later migration re-migrates that table.
- **Split SourceAdapter/TargetAdapter** — Not a single combined interface. Source has read-only methods; Target adds CreateTable/AlterTable/WriteBatch.
- **API-first, CLI is a client** — CLI must not call engine code directly. It calls the REST API. This enforces the architecture boundary.
- **Start with SQLite adapter (#2)** — Easiest to test locally, validates the interface before touching Postgres/MySQL.
- **`internal/` is legacy scaffolding** — Do not extend it. New code goes into the top-level packages (`adapters/`, `engine/`, `state/`, `api/`, `analyzer/`, `stats/`).
- **Web UI runs in its own process (`cmd/xferdb-web/`)** — It holds no migration state and no source/target DB credentials; it only serves static assets and reverse-proxies `/api/*` (REST + the future WebSocket at `/api/v1/migrations/:id/ws`) to `xferdb server`. This means an OOM crash in the engine (which buffers batches in memory) doesn't take the UI down, the browser needs no CORS config since it only ever talks to one origin, and `xferdb server` can live on a private network never exposed to the browser directly. Source/target databases must be network-reachable from wherever `xferdb server` runs, not from the client machine.
- **CI gates on the real compatibility matrix, not just `go test`** — `.gitea/workflows/go-build.yaml` runs `go build`/`go test` in-runner; `.gitea/workflows/xferdb-matrix-gate.yaml` additionally triggers the Jenkins `xferdb-matrix` job (real Postgres/MySQL/MongoDB sources in Kubernetes, `SIZE=SMOKE`) on every push/PR touching `**.go`, and posts an APPROVED or REQUEST_CHANGES review on the PR via a bot token once it finishes. An "approved" review on a PR here may be that bot, not a human — check who posted it. Needs repo secrets/vars `JENKINS_URL`, `JENKINS_USER`, `JENKINS_TOKEN` (Jenkins auth) and the existing `GH_TOKEN` (Gitea review API) to function; see `/Users/bchan/workspace/NextDevOps/XferDB-jenkins` for the Jenkins pipeline itself. The review-posting step's `curl` call checks the HTTP status and fails loudly on anything outside 2xx — it used to send an invalid `event` value (`APPROVE` instead of Gitea's actual `APPROVED`) silently swallowed by an unchecked `curl -s`, so every approval attempt failed without anyone noticing until a PR sat unreviewed with green CI.
- **UI smoke tests on web changes** — `.gitea/workflows/ui-smoke-test.yaml` triggers the Jenkins `xferdb-ui` job when PRs touch `cmd/xferdb-web/**`. Runs Playwright E2E tests against the web UI (project CRUD, smoke tests). Same approval/request-changes pattern as the matrix gate. Also supports `workflow_dispatch` for manual runs.
