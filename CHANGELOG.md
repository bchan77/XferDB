# Changelog

All notable changes to XferDB are documented here.

## [Unreleased]

Live migration (#102) ships as a **beta**: labelled so in the web UI, the CLI and the README until it has run against real workloads.
How to use it, step by step: [docs/LIVE_MIGRATION.md](docs/LIVE_MIGRATION.md). `xferdb migrate --preflight --live` runs the live readiness checks from the CLI.

### Added
- **Live migration readiness check (#102, phase 1; beta).** The project settings page has a *Live
  Migration* section, and `POST /projects/:id/preflight` takes `{"mode": "live"}`, checking
  whether a PostgreSQL source can stream changes (wal_level, replication privilege, free slot
  and WAL sender, primary keys, publication permissions, WAL retention) and whether the target
  can apply them.
- **Live migration, streaming (#102, phase 2; beta).** A PostgreSQL source can now be migrated live:
  `POST /projects/:id/migrations` with `{"mode": "live"}`, `xferdb migrate --live`, or the run
  form's *Live migration* checkbox. XferDB creates a replication slot and publication, copies
  the tables while capturing every change made meanwhile, replays those changes (catch-up), and
  then keeps the target following the source (streaming). The server runs one capture and one
  apply process per live migration; they outlive a server restart, which reattaches to them, and
  are restarted when they fail. Pause stops applying while capture keeps queueing, so the source
  still frees its WAL. `GET /migrations/:id` reports `phase` and a `live` object (lag in changes,
  seconds and WAL bytes, queue size, worker errors); the migration page shows a phase strip and
  the lag, and `/metrics` has `xferdb_cdc_*` gauges. The live copy pages each table by its
  primary key up to the largest key at its start, so rows changing under it are never skipped.
  Unique indexes are added once catch-up is done; foreign keys, checks and triggers are left for
  cutover, which comes with phase 3 (until then a live migration streams until cancelled).
  Starting one runs the live readiness check first and refuses `data_only`, `truncate` and
  `schema_only`; cancelling or deleting it drops the slot and publication.
- **Live migration cutover (#102, phase 3; beta).** A streaming live migration is ended with *Cut over*
  in the web UI, `xferdb cutover`, or `POST /migrations/:id/cutover`. Stop writing to the source
  first; the cutover then applies every change up to the source's position, checks for a few
  seconds that no more changes to the migrated tables arrive, sets the target's sequences to the
  source's values (PostgreSQL targets), adds the foreign keys, checks and triggers held back so
  far, drops the replication slot and publication, and completes the migration, which is then
  validated by the project's settings. If changes keep arriving, the cutover stops and the
  migration keeps streaming: cut over again once writes have stopped, or with `ignore_writes`
  (`--ignore-writes`, *Cut over anyway*) to drain once more and leave later writes behind. A
  cutover interrupted by a server restart starts over. `xferdb migrate --live --until-streaming`
  returns once a migration is streaming, for scripts. Until this release a live migration
  streamed until cancelled.
- **Live migration: schema changes and WAL safety (#102, phase 4; beta).** A table's columns changing
  on the source (a column added, dropped or retyped, or the table renamed) pauses the live
  migration where the stream reaches the change: every change before it is applied, none after
  it, and the migration page (`live.schema_change` in the API) lists what changed. XferDB doesn't
  apply DDL; make the same change on the target, then *Resume* (or `xferdb resume`, new), which
  accepts it. Cutover is refused meanwhile. The migration page also shows the WAL the source
  keeps for the replication slot, with a warning when capture has been stopped for over a
  minute, the slot nears or passes `max_slot_wal_keep_size`, or the server's disk holding the
  change queue runs low; a slot PostgreSQL has invalidated, or that was dropped, fails the migration with the reason instead of retrying forever. New
  metrics `xferdb_cdc_source_retained_bytes` and `xferdb_cdc_schema_change_paused`.

### Fixed
- **Re-running a PostgreSQL→PostgreSQL migration into an existing schema.** Without *Recreate
  schema*, a second run failed after copying its rows with `multiple primary keys for table ...
  are not allowed`: the post-data step (keys, indexes, constraints, triggers) stopped at the
  first object the target already had, while the pre-data step already skipped existing ones.
  Existing objects are now skipped in both.
- **Column defaults across databases.** A source column's default went into the target's
  `CREATE TABLE` exactly as the source wrote it, so the migration failed before copying a row
  whenever the target spoke another SQL dialect: PostgreSQL `now()`, `'x'::text` or
  `nextval(...)` on SQLite or MySQL. MySQL→MySQL failed as well, because MySQL reports string
  defaults unquoted (`DEFAULT new`). Defaults are now translated between dialects: literals,
  booleans, the current timestamp and date, and UUID generators. A default with no
  equivalent on the target (a sequence, a UUID function on SQLite, an arbitrary expression)
  is left off. The data copied is the same either way; this only affects rows inserted later
  that leave the column out. MySQL `datetime(p)` now maps to `timestamp(p)` on PostgreSQL.

## [1.1.0] - 2026-10-21

Post-migration data validation, Prometheus metrics, multi-schema PostgreSQL, and a set of fixes
for migrations that finished "successfully" while losing or mangling data.

### Behavior changes
Read these before upgrading from 1.0.0.
- **Validation runs by default.** A completed migration is now validated at L2 (row count +
  row fingerprint), and `xferdb migrate` waits for it and **exits non-zero when a table fails**.
  Scripts that wrap `xferdb migrate` can start failing where they used to pass. Turn it off per
  run with `--skip-validation` (`"validation": "off"`), or per project with
  `transfer.validation: off`. The old `validate` bool in transfer config was never read and is
  replaced by `validation` / `validation_level` / `validation_sample`.
- **A PostgreSQL source with no `Schema` migrates every non-system schema**, not just `public`.
  Tables in the connection's own schema keep bare names (so existing projects, checkpoints and
  plans are unaffected); tables in other schemas are named `schema.table` and land in the same
  schema on a PostgreSQL target. Set `Schema: public` to keep the 1.0.0 behaviour.
- **Engine table filter**: `loadtest.orders` now matches only that table; it used to also match
  a bare `orders`. `public.orders` still matches `orders`.
- **Async pipeline is on by default** in the web UI's run form.

### Added
- **Post-migration validation (#103)** — for every source, MongoDB included. The validator
  re-reads the source through the schema plan (so flattening, skipped fields and `_extra`
  reproduce themselves) and compares canonicalized values by the intended column type, so
  `true` vs `1`, `1.50` vs `1.5`, JSON key order and timestamp precision don't read as
  differences.
  - **L1** row counts; **L2** order-independent row fingerprint (also covers tables without a
    primary key); **L3** primary-key row diff that names missing, extra and changed rows (with
    the changed columns), for integer, text, decimal, uuid, date, Mongo `_id` and composite
    keys. `--sample N` limits L3 to a percentage. Up to 100 example rows per table are kept.
  - **Lossy by plan** — when a schema-plan override narrows a column (e.g. `numeric(10,4)` →
    `numeric(10,2)`, `double` → `integer` on MySQL, `timestamp` → `date`), the validator applies
    the same conversion the target does. A table whose only differences are those conversions is
    `expected_lossy`, not a failure.
  - **`per_table` mode** validates each table as soon as it finishes copying, while the rest
    are still running, so a bad table shows up early.
  - Live progress per table (step, rows read, rate, ETA) on the migration page and the tables
    page.
  - CLI: `xferdb validate [--level L1|L2|L3] [--sample N]`; `migrate --skip-validation`,
    `--validation after|per_table|off`, `--validation-level`, `--validation-sample`.
  - API: `POST/GET /migrations/:id/validations`, `GET/PATCH /validations/:id`,
    `GET /validations/:id/tables/:table/diffs`, `GET /projects/:id/validation-status`.
  - Web UI: validation controls on the run form and in project settings, per-table badges, a
    results card with *Validate now*, and a *View rows* drawer for L3 differences.
- **Prometheus metrics (#105)** — `xferdb server` serves `GET /metrics` (outside `/api/`, so the
  web process doesn't proxy it): rows/tables/migrations counters, batch read/write duration
  histograms, per-active-migration gauges (phase, progress, rows/s, ETA, tables by state), and
  Go runtime/process collectors.
- **PostgreSQL schemas** — a `Schema` setting on PostgreSQL connections (empty = all schemas on
  a source, one schema when set). Targets create the schema as needed (`CREATE SCHEMA IF NOT
  EXISTS`), COPY included. Preflight lists the schemas it found. Verified against YugabyteDB
  2025.2.5.2.
- **Database picker** — the connection editor asks for host, port, credentials and SSL mode
  first, then *Load databases* turns the Database field into a dropdown of the server's
  databases (`POST /api/v1/connections/databases`).
- **Create a missing target database** — when preflight fails only because the target database
  doesn't exist, the settings page (and the connection editor's target side) offers a
  *Create database* button. It is never automatic, so a mistyped name doesn't quietly become a
  new database.
- **Run form remembers its options per project** — workers, batch size, segment workers,
  performance options and validation choices are saved back to the project on *Run Migration*.
  Schema mode, bulk copy and table selection are deliberately not remembered.
- **Finished migrations list their tables** — `GET /api/v1/migrations/:id` returns per-table
  outcomes, and the migration page shows them instead of "No tables yet".

### Fixed
- **PostgreSQL OFFSET paging could skip and repeat rows** — with segment workers at 0 (the
  default) pages had no `ORDER BY`, so on large tables consecutive pages overlapped. A 2M-row
  copy wrote 2,000,000 rows holding only 1,242,222 distinct ids. Pages are now ordered by the
  primary key.
- **WriteBatch dropped fields missing from a batch's first record (#96)** — all three targets
  now take the column list from the table schema; absent fields are written as NULL.
- **MongoDB `_id` shapes that failed or were silently truncated (#97)** — a polymorphic `_id`
  (e.g. mostly Int64 with a few strings) now widens to `text` (or `bigint` for Int32/Int64)
  instead of taking the majority type; a compound (embedded-document) `_id` uses the `as_jsonb`
  strategy; a Binary `_id` overridden to `uuid` is written as canonical UUID text, and Binary
  values in any non-`bytea` column are written as UUID text or hex instead of invalid UTF-8.
  Keyset paging now stores the last `_id` as Extended JSON so it keeps its BSON type, and moves
  on to the next BSON type once one runs out. Previously every batch after the first used a
  string key for compound, Binary, and floating-point `_id`s. Because `$gt` only matches values
  of its own type, the migration stopped after the first 1000 rows with no error. Polymorphic
  `_id`s could drop every type after the first. Resume keys checkpointed by older versions
  still decode.
- **MongoDB nested flattening and long collection names (#98)** — flattening now recurses to
  every depth (grandchild columns were created but never written), and collection names longer
  than PostgreSQL's 63-byte identifier limit get a deterministic hashed suffix instead of two
  collections silently sharing one table.
- **MongoDB dotted field names (#117)** — a literal `"a.b"` key colliding with a nested
  `a: {b: …}` path is now reported as a warning in analyze and the CLI instead of merging
  silently.
- **MongoDB URIs the driver accepts but `url.Parse` doesn't** (e.g. a backslash in the
  password) no longer fail with "database name cannot be empty".
- **MySQL target passed source type names straight into DDL** — a PostgreSQL source failed
  `CREATE TABLE`. Types are now translated; unqualified `numeric` becomes `decimal(65,30)`
  (MySQL's bare `DECIMAL` drops every fraction digit) and timestamps become `datetime`
  (MySQL `TIMESTAMP` stops at 2038). TEXT/BLOB/JSON primary keys get a prefix length.
- **SQLite target type mapping and BLOB binding (#119)** — source types are mapped to SQLite
  affinities (wide numerics stay exact as TEXT), `[]byte` values are no longer stored as BLOB in
  non-BLOB columns (a MySQL → SQLite copy stored even `varchar` as BLOB), and composite primary
  keys are declared at table level.
- **SQLite "database is locked" failing tables under parallel writes (#124)** — the state store
  and the SQLite target each use a single write connection.
- **`sqlite://` DSNs from a config file** now open instead of failing with "unable to open
  database file".
- **PostgreSQL connection strings are quoted (#130)** — an empty password no longer swallows the
  next setting (which turned `sslmode=disable` into the password).
- **Schema-plan overrides from `project schema --set` blanked the column name and primary key
  (#113)** — overrides are now partial updates.
- **Engine lifecycle events could be dropped (#137)** and **table status write errors were
  ignored on segment-parallel paths (#138)**, leaving finished tables shown as in progress or
  not recorded as completed.
- **Stats snapshot shared its table slice with the collector (#125).**
- **Web UI**: loading tables no longer wipes values typed into the run form; static assets are
  sent with `Cache-Control: no-cache` so a browser picks up a new build after an upgrade.

### Changed
- Release binaries are no longer committed under `releases/`; they are attached to the Gitea
  release only.
- CI posts matrix results as PR comments rather than reviews (Gitea refuses self-approval).
- `github.com/prometheus/client_golang` v1.24.1 added (pinned: v1.25 requires Go 1.26).

## [1.0.0] - 2026-09-23

First stable release. Every adapter (PostgreSQL, MySQL, SQLite, MongoDB), the migration engine
(pause/resume/checkpointing), schema analysis, the REST API, CLI, and Web UI are complete. The
write path — default upsert and `--bulk-copy` — is implemented and has regression test coverage
for all three target adapters, and the full compatibility matrix (45 cases across every
supported source/target pairing, including dedicated bulk-copy cases) passes against real
infrastructure.

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
  the Jenkins `xferdb-matrix` job (real Postgres/MySQL/MongoDB sources, `SIZE=SMOKE`) on every
  push/PR touching Go code, and posts an APPROVED or REQUEST_CHANGES review on the PR based on
  the result.
- **Structured config files (xferdb.yaml / xferdb.json)** — server settings, migration defaults, and named projects can now live in a single file under version control. Loaded from `--config`, `$XFERDB_CONFIG`, `./xferdb.{yaml,yml,json}`, or `~/.xferdb/config.{yaml,yml,json}` in that order. Precedence is CLI flag → env var → config file → built-in default.
- **`xferdb config` subcommand** — `init` writes a starter file, `show` prints the resolved config, `validate` checks syntax and semantics, `path` prints the resolved path.
- **DSN safety via `${VAR}` expansion** — DSN strings in config files can reference environment variables, so credentials never have to live in the file. `${VAR}` requires the var to be set; `${VAR:-default}` falls back. Missing required vars fail the load with a clear error pointing at the field.
- **Three example configs** in `examples/` (`xferdb.yaml`, `xferdb.json`, `xferdb.toml`) showing realistic project setups and the env-var DSN pattern.
- **Config-aware defaults** — `config.ResolveTransferConfig` lets `migrate`/`project create` honour file-defined defaults before consulting flags.
- **Tests** — `config/` covers YAML/JSON load, env expansion (set, default, missing), missing-file fallback, validation errors (duplicate names, unknown adapters, bad log level, negative values), and precedence (CLI > env > file > default).

### Notes
- **TOML is not yet supported** as of v1.0.0 (kept the dependency surface minimal). It returns a clear error explaining how to convert with `yq` / `toml2json`.

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
