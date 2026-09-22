# Changelog

All notable changes to XferDB are documented here.

## [Unreleased]

### Added
- **Bulk copy for MySQL and SQLite targets** — `--bulk-copy` previously only did something on postgres targets (COPY protocol); it's now implemented for mysql (`LOAD DATA LOCAL INFILE` via a registered `io.Reader`, using `REPLACE INTO TABLE` so a non-empty target degrades gracefully instead of erroring) and sqlite (multi-row `INSERT OR REPLACE` chunked under the bound-parameter limit, plus `PRAGMA synchronous = OFF` for the duration of the write). Still gated by the engine on `--truncate`/`--recreate-schema`. Web UI's "Bulk copy" option is no longer hidden for non-postgres targets.
- **Adapter test coverage** — `adapters/postgres`, `adapters/mysql`, and `adapters/sqlite` had zero unit tests; each now has a bulk-copy regression suite (postgres/mysql are opt-in via `XFERDB_TEST_TARGET_DSN` against a live database, sqlite runs unconditionally against a temp file) that would have caught the two adapter-specific bugs found while building this: postgres's COPY encoder bytea-mangling non-bytea `[]byte` values (e.g. a MySQL-sourced `DECIMAL` scanned as `[]byte`), and sqlite's `PRAGMA synchronous` rejecting changes inside an active transaction.
- **Web UI: tables-first project workflow** — clicking a project now lands directly on its
  table/collection list instead of a connection-settings page (moved to `#/projects/:id/settings`).
  That screen shows live migration status with Run/Pause/Resume/Cancel, and locks tables (with an
  explanatory banner) while a migration is active so schema edits can't race a running transfer.
- **Web UI: schema editor for every adapter pairing** — the split-view type-override editor
  (dropdown + Save with confirmation) previously only worked end-to-end for MongoDB→PostgreSQL;
  it's now generalized to Postgres/MySQL/SQLite pairings too. `OverridePlan` upserts instead of
  requiring a pre-seeded row, and the engine applies saved overrides to the target `CREATE TABLE`
  for relational sources (falling back off the postgres→postgres `pg_dump` fast path when
  overrides exist, since that path bypasses `CreateTable` entirely).
- **CI: xferdb-matrix compatibility gate** — `.gitea/workflows/xferdb-matrix-gate.yaml` triggers
  the Jenkins `xferdb-matrix` job (real Postgres/MySQL/MongoDB sources, `SIZE=S`) on every push/PR
  touching Go code, and approves or requests changes on the PR based on the result.
- **Structured config files (xferdb.yaml / xferdb.json)** — server settings, migration defaults, and named projects can now live in a single file under version control. Loaded from `--config`, `$XFERDB_CONFIG`, `./xferdb.{yaml,yml,json}`, or `~/.xferdb/config.{yaml,yml,json}` in that order. Precedence is CLI flag → env var → config file → built-in default.
- **`xferdb config` subcommand** — `init` writes a starter file, `show` prints the resolved config, `validate` checks syntax and semantics, `path` prints the resolved path.
- **DSN safety via `${VAR}` expansion** — DSN strings in config files can reference environment variables, so credentials never have to live in the file. `${VAR}` requires the var to be set; `${VAR:-default}` falls back. Missing required vars fail the load with a clear error pointing at the field.
- **Three example configs** in `examples/` (`xferdb.yaml`, `xferdb.json`, `xferdb.toml`) showing realistic project setups and the env-var DSN pattern.
- **Config-aware defaults** — `config.ResolveTransferConfig` lets `migrate`/`project create` honour file-defined defaults before consulting flags.
- **Tests** — `config/` covers YAML/JSON load, env expansion (set, default, missing), missing-file fallback, validation errors (duplicate names, unknown adapters, bad log level, negative values), and precedence (CLI > env > file > default).

### Notes
- **TOML is not yet supported** in v0.6.0 (kept the dependency surface minimal). It returns a clear error explaining how to convert with `yq` / `toml2json`. Tracked for v0.6.1.

## [0.5.0] - 2026-09-09

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
