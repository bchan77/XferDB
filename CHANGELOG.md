# Changelog

All notable changes to XferDB are documented here.

## [0.6.0] - 2026-09-09

### Added
- **Database info in preflight** — shows database type, version, host, database name, table count, size, and SSL status for both source and target
- **Support bundle** — `xferdb support-bundle [project]` generates a tar.gz diagnostic archive for troubleshooting (credentials automatically redacted)

### Fixed
- MySQL adapter no longer fails on URL-format DSNs (`mysql://user:pass@host:port/db`) — rebuilds them into the native `go-sql-driver/mysql` format instead of passing the URL straight to `sql.Open()`
- MySQL → PostgreSQL migrations now map MySQL types to their PostgreSQL equivalents (`datetime`→`timestamp`, `tinyint(1)`→`boolean`, `json`→`jsonb`, etc.) instead of failing with `pq: type "datetime" does not exist`
- Compatibility test matrix expanded to MongoDB 6.0/7.0/8.0/8.2/8.3 × PostgreSQL 14/15/16/17, MySQL 8.4 → PostgreSQL 17, and full PostgreSQL 14–17 cross-version migration — 27/27 passing

### Support Bundle Contents
- `system.json` — XferDB version, Go version, OS, architecture
- `project.json` — project configuration (passwords/DSN credentials redacted)
- `source_info.json` / `target_info.json` — database metadata
- `preflight.json` — permissions check results
- `migrations.json` — migration history with table progress
- `checkpoints.json` — resume checkpoints
- `source_schema.json` / `target_schema.json` — table schemas
- `schema_plan.json` — MongoDB schema plan (if applicable)

## [0.4.0] - 2024-09-06

### Added
- **MongoDB source adapter** with schema inference
- Schema analysis for MongoDB → PostgreSQL migrations (`xferdb project analyze`)
- Schema plan review and override (`xferdb project schema`)
- Percentage-based sampling for large MongoDB collections
- AI annotation interface for ambiguous field types
- Active project indicator in `project list`
- **Async pipeline mode** (`--async-pipeline`) — decouples reading and writing with buffered channels for higher throughput
- **Accurate MongoDB counts** (`--accurate-counts`) — use exact `CountDocuments` instead of estimates for accurate ETA
- **Stats history recording** — migration statistics recorded every 5 seconds; view with `--history` after completion
- **Stats history API** — `GET /api/v1/migrations/:id/stats/history` returns all recorded snapshots
- **Detailed table status** — shows "counting rows...", "waiting for schema", "waiting for indexes" during migration

### Fixed
- Row totals now correct when MongoDB over-estimates document counts
- ETA recalculated using corrected totals (not stale estimates)
- Rate decays when no batches arrive (e.g., during bulk copy setup) instead of staying frozen

## [0.3.0] - 2024

### Added
- **Native schema transfer** — postgres→postgres uses `pg_dump`/`psql` to preserve extensions, custom types, vector indexes
- **Bulk copy mode** (`--bulk-copy`) — PostgreSQL `COPY` protocol for high-throughput writes
- **Resource monitoring** — live display of goroutines, heap, CPU%, GC cycles
- **Structured server logging** — JSON log file with `--log-file`, configurable level with `--log-level`
- Post-schema progress display (shows each index/constraint as it builds)

## [0.2.0] - 2024

### Added
- PostgreSQL, MySQL, SQLite source and target adapters
- Parallel table migration (`--table-workers`)
- Intra-table parallel segments (`--segment-workers`)
- Preflight checks with actionable error hints
- Pause / resume / cancel lifecycle
- Checkpoint-based crash recovery
- Selective table migration (`--tables`)
- Multi-project support with named projects
