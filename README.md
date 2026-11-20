# XferDB

**Universal database transfer tool** — migrate data between any databases with schema, indexes, foreign keys, parallel table migration, and built-in validation that the target matches the source.

---

## Supported Databases

| Database | Source | Target | Tested Versions |
|----------|--------|--------|-----------------|
| PostgreSQL | ✅ | ✅ | 14, 15, 16, 17 |
| YugabyteDB | ✅ | ✅ | 2025.2, 2026.1 (type `yugabyte`; live migration source too) |
| MySQL | ✅ | ✅ | 8.0, 8.4, 9.7, 26.7 |
| SQLite | ✅ | ✅ | 3.x |
| MongoDB | ✅ | 🔜 | 6.0, 7.0, 8.0, 8.2, 8.3 (live migration source too: replica set or sharded cluster) |
| Oracle | ✅ | — | 21c, 23ai (type `oracle`; 12c+ expected to work; live migration source too; see [Oracle](#oracle)) |
| Cassandra | 🔜 | 🔜 | — |

> ✅ = Working &nbsp;|&nbsp; 🔜 = Planned

### Tested Migration Paths

Most of these are the pairs covered by the automated compatibility matrix (75 cases against real
Postgres/MySQL/MongoDB/YugabyteDB instances in Kubernetes, run on every push/PR touching Go code — see
[`XferDB-jenkins`](https://gitea.homelab.local/nextdevops/XferDB-jenkins)). Every case also
validates the result (L2, below), and each target has at least one case exercising
`--bulk-copy`'s write path, not just the default one. Rows noted "manually verified" aren't
automated yet.

| Source | Target | Status |
|--------|--------|--------|
| MongoDB (6.0, 7.0, 8.0, 8.2, 8.3) | PostgreSQL (14, 15, 16, 17) | ✅ Tested — 26 cases (incl. polymorphic/compound `_id`, nested documents, and live migrations from an 8.0 replica set and a 7.0 sharded cluster) |
| MongoDB (7.0, 8.0) | YugabyteDB 2026.1 | ✅ Tested — 2 cases (live migrations from a 7.0 replica set and an 8.0 sharded cluster) |
| MySQL (8.0, 8.4, 9.7, 26.7) | PostgreSQL (14, 15, 16, 17) | ✅ Tested — 18 cases |
| PostgreSQL (14, 15, 16, 17) | PostgreSQL (14, 15, 16, 17) | ✅ Tested — 10 cases |
| PostgreSQL (14, 17) | MySQL 8.4 | ✅ Tested — 4 cases |
| PostgreSQL (15, 17) | SQLite | ✅ Tested — 4 cases |
| MySQL 26.7 | SQLite | ✅ Tested — 2 cases |
| MySQL 26.7 | MySQL 8.4 | ✅ Tested — 1 case |
| YugabyteDB (2025.2, 2026.1) | PostgreSQL 17 | ✅ Tested — 2 cases |
| YugabyteDB (2025.2, 2026.1) | SQLite | ✅ Tested — 2 cases (incl. a live migration from 2026.1) |
| YugabyteDB (2025.2, 2026.1) | YugabyteDB 2026.1 | ✅ Tested — 2 cases (incl. a live migration from 2025.2); also manually verified with a 1.2M-row table validated at L2 |
| PostgreSQL (16, 17) | YugabyteDB (2025.2, 2026.1) | ✅ Tested — 2 cases |
| SQLite | PostgreSQL | Implemented; not yet covered by the automated compatibility matrix or an e2e test |
| Oracle (21c, 23ai) | PostgreSQL 17, MySQL 8.4, SQLite | ✅ Tested by the `e2e` test `TestOracleSource` (L3 validation on every table); matrix cases are on their way |

---

## Features

- **3-phase migration** — schema creation, bulk data transfer (upsert), then indexes and constraints
- **Native schema transfer** — postgres→postgres migrations use `pg_dump`/`psql` to preserve extensions, custom types, vector indexes (`ivfflat`, `hnsw`), and exact index definitions
- **Post-schema progress** — shows each index and constraint as it builds so the display never appears hung
- **Parallel table migration** — migrate N tables concurrently with `--table-workers`
- **Intra-table parallel segments** — split each table into N PK-range segments with `--segment-workers`; falls back to OFFSET for tables without an integer PK
- **Bulk copy mode** — `--bulk-copy` uses each target's fast bulk-load path for high-throughput writes (postgres: `COPY`, mysql: `LOAD DATA LOCAL INFILE`, sqlite: batched inserts); combine with `--truncate` or `--recreate-schema`
- **Data validation** — after a migration, compare source and target: row counts (L1), every row's content (L2, the default), or L2 plus a primary-key diff listing the differing rows (L3); for every source, MongoDB included. See [Validation](#validation)
- **All PostgreSQL schemas** — a PostgreSQL source migrates every non-system schema by default; the optional *Schema* setting narrows it to one
- **Preflight checks** — verify connectivity, SSL, and permissions before committing to a migration; a missing target database can be created from the web UI
- **Live migration (beta)** — from a PostgreSQL, YugabyteDB, MongoDB or Oracle source, copy the tables and then keep streaming the source's changes to the target until you cut over, so applications keep running during the copy. See [Live migration (beta)](#live-migration-beta)
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
go build -o xferdb-web ./cmd/xferdb-web  # optional: the web UI, see below
sudo mv xferdb xferdb-web /usr/local/bin/
```

### Binary releases

Binaries are published to Gitea on tagged releases. Download and move to your `PATH`.

---

## Configuration file (xferdb.yaml) — new in v1.0.0

XferDB now reads a structured config file (YAML or JSON) so projects, server settings, and migration defaults live in version control instead of on the command line.

**Search order** — first match wins:
1. `--config /path/to/xferdb.yaml` flag
2. `$XFERDB_CONFIG` environment variable
3. `./xferdb.{yaml,yml,json}` in the current working directory
4. `~/.xferdb/config.{yaml,yml,json}`

**Precedence** — highest wins:
1. CLI flag (e.g. `--table-workers=4`)
2. Environment variable (`XFERDB_TABLE_WORKERS=4`)
3. Config file value
4. The project's saved transfer settings (`xferdb migrate` only; `project create --table-workers`, the web UI's run form)
5. Built-in default

**DSN safety** — use `${VAR}` or `${VAR:-default}` references in DSN strings; values are expanded from the process environment at load time. Missing variables (no default) cause the load to fail.

```bash
# Start a config from scratch
xferdb config init ./xferdb.yaml

# Validate, show, and resolve
xferdb config validate --config ./xferdb.yaml
xferdb config show --config ./xferdb.yaml
xferdb config path --config ./xferdb.yaml
```

Example (`examples/xferdb.yaml`):

```yaml
server:
  addr: ":8080"
  log_level: info

defaults:
  table_workers: 2
  batch_size: 5000
  bulk_copy: true
  on_error: abort

projects:
  - name: prod-to-staging
    source:
      type: postgres
      dsn: ${PROD_DSN}
    target:
      type: postgres
      dsn: ${STAGING_DSN}
    transfer:
      tables: [orders, customers]
      batch_size: 2500
```

JSON is also supported (`examples/xferdb.json`). TOML isn't parsed yet (`examples/xferdb.toml` shows the equivalent structure for reference); convert it with `yq` or `toml2json`.

Full field-by-field reference: [`docs/CONFIG.md`](docs/CONFIG.md).

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

When the migration completes, `xferdb migrate` waits for its validation and exits non-zero if
any table's source and target differ.

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

# Live migration (beta, PostgreSQL, YugabyteDB, MongoDB and Oracle sources): copy, then stream changes until cutover.
# Guide: docs/LIVE_MIGRATION.md
xferdb migrate --preflight --live         # check the source and target can do it
xferdb migrate --live
xferdb migrate --live --until-streaming   # return once it streams, for scripts
xferdb cutover                            # stop writes to the source first
xferdb resume                             # after a pause, e.g. at a schema change
xferdb leftovers [drop NAME]              # streams left on the source by ended migrations

# Cancel the running migration (keeps history, preserves checkpoints)
xferdb migrate --cancel

# Resume a failed or cancelled migration (snapshot mode): completed tables are skipped,
# the rest continue from their checkpoints, nothing is dropped or truncated, and only the
# indexes/constraints not created yet are run. Also the web UI's Resume button.
xferdb resume [--table-workers N]

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

# Use the target's fast bulk-load path for writes (combine with --truncate or --recreate-schema)
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

# Validation (default: after the migration, at L2)
xferdb migrate --validation per_table        # validate each table as soon as it is copied
xferdb migrate --validation-level L3 --validation-sample 10
xferdb migrate --skip-validation
```

### Validate command

Re-run validation on a migration that already finished:

```bash
xferdb validate                              # the current project's latest migration
xferdb validate --project <name> --migration <id>
xferdb validate --level L3 [--sample 10]     # list differing rows of failing tables
xferdb validate --tables orders,customers
```

### Support bundle

Generate a diagnostic archive for troubleshooting. Start with `timeline.json`: one time-ordered list of what happened — migrations and tables starting and ending (with each failed table's error), indexes and constraints created after the copy, schema type overrides being saved, progress phase changes, validations, and the server's warnings and errors. Alongside it: `logs.jsonl` (the server's recent log lines for this project, kept in memory, so no `--log-file` is needed; lines from before the last restart are only in a log file), migration history with the settings each run used, progress samples, validation results, the schema plan, checkpoints, project config, source/target database metadata, preflight results and table schemas. `README.txt` in the archive lists every file. Passwords and API keys are automatically redacted.

```bash
xferdb support-bundle [project-name]
xferdb support-bundle [project-name] --output /tmp/bundle.tar.gz
```

### Other commands

```bash
xferdb server [--addr :8080] [--log-file <path>] [--log-level <level>]
xferdb support-bundle [project-name] [--output <path>]
xferdb validate [--project <name>] [--migration <id>] [--level L1|L2|L3]
xferdb config init|validate|show|path
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
| Bulk copy | `xferdb migrate --bulk-copy` | Uses each target's fast bulk-load path (postgres: `COPY`, mysql: `LOAD DATA LOCAL INFILE`, sqlite: batched inserts) for maximum write throughput. Combine with `--truncate` or `--recreate-schema`. |
| Selective tables | `xferdb migrate --tables t1,t2` | Only migrate the named tables; all others are skipped. Supports `schema.table` notation. |
| Parallel per-table | `xferdb migrate --segment-workers 4` | Split each table into N segments; workers read/write in parallel. Uses PK ranges by default (no OFFSET scan penalty); falls back to OFFSET for tables without an integer PK. Add `--offset-segments` to force OFFSET mode. |
| Async pipeline | `xferdb migrate --async-pipeline` | Decouples reading and writing with buffered channels. Readers fetch the next batch while writers flush the previous one. Combine with `--bulk-copy` for best throughput. |
| Accurate counts | `xferdb migrate --accurate-counts` | Use exact MongoDB `CountDocuments` instead of `EstimatedDocumentCount`. Slower but gives accurate row totals and ETA. |
| Schema only | API: `schema_only: true` | Creates tables, indexes, constraints — no data transfer. |
| Data only | API: `data_only: true` | Skips schema creation — target schema must already exist. |

---

## Live migration (beta)

> **Beta.** Live migration works end to end and is tested on every change, but it hasn't yet run against real production workloads. Try it on a copy of your data before relying on it.

**Step-by-step guide: [docs/LIVE_MIGRATION.md](docs/LIVE_MIGRATION.md)**: requirements and how to set them, the UI, CLI and API for each step, schema changes, cutover, WAL, and troubleshooting.

A live migration copies the tables and then keeps streaming the source's changes to the target, so applications keep writing to the source during the copy. When you are ready to switch, stop the writes and cut over. The source must be **PostgreSQL** with `wal_level = logical`, **YugabyteDB** (source type `yugabyte`), **MongoDB** 4.2+ as a replica set or a sharded cluster (through one `mongos`; documents go through the schema plan as in the copy, and fields it doesn't know land in `_extra`), or **Oracle** 21c/23ai in ARCHIVELOG mode with primary key supplemental logging (read with LogMiner); see the guide for each one's requirements. The target can be PostgreSQL, YugabyteDB, MySQL or SQLite. Every migrated table needs a primary key.

1. **Readiness.** *Settings → Check live migration readiness*, `xferdb migrate --preflight --live`, or `POST /projects/:id/preflight` with `{"mode":"live"}`; checks `wal_level`, the replication privilege, free slots and WAL senders, primary keys, and permission to create the publication (MongoDB: a replica set or sharded cluster, 4.2+, that the login can open a change stream, and the oplog window; Oracle: ARCHIVELOG mode, supplemental logging, the LogMiner and flashback privileges, key types, and on 21c `archive_lag_target`).
2. **Start.** Tick *Live migration* on the migration form, or run `xferdb migrate --live`. XferDB creates a replication slot and publication (nothing on MongoDB, whose change stream reads the oplog), copies the tables, catches up on what was written meanwhile, then streams. The capture and apply workers run as their own processes and survive a server restart.
3. **Stream.** The migration page shows the lag and the WAL the source keeps for the slot. *Pause* stops applying changes; they are still captured.
4. **Cut over.** Stop every application writing to the source, then *Cut over* (`xferdb cutover`). The remaining changes are applied, sequences set (PostgreSQL targets), and foreign keys, checks and triggers added; then the slot is dropped and the migration validated. If writes are still arriving, the cutover stops and streaming goes on.

**Schema changes.** XferDB doesn't apply DDL. When a table's columns change on the source, the migration pauses at that point and shows what changed. Make the same change on the target, then *Resume* (`xferdb resume`).

**WAL.** The source keeps WAL for the slot until XferDB has captured it. The migration page warns when capture has stopped, the slot nears `max_slot_wal_keep_size`, or the server's queue disk runs low; a slot the source has invalidated fails the migration. Cancelling or deleting a live migration drops the slot. A MongoDB source keeps nothing for XferDB: if capture is stopped longer than the oplog's window, the migration fails, so keep the window longer than any outage you expect.

---

## Validation

A completed migration is validated automatically unless the project's `validation` setting is
`off` or the run passes `--skip-validation`. Only tables that completed in that migration are
checked; a result becomes *stale* once a later migration copies the table again.

| Level | Checks | Cost |
|-------|--------|------|
| L1 | Row counts | Cheap |
| L2 (default) | Every row, as an order-independent fingerprint of both sides | Reads both databases in full again |
| L3 | L2, then for a failing table a primary-key diff: up to 100 example rows (missing, extra, different) | L3 sample % limits the diff |

- **When:** `after` (default) validates once the whole migration completes; `per_table`
  validates each table as soon as it is copied; `off` skips it. *Validate now* in the web UI or
  `xferdb validate` runs it on demand. Either way, as many tables are validated at once as the
  migration's table workers (largest first, each on its own source and target connections), and
  L2 reads a table's source and target at the same time. When no whole table is left to start,
  idle workers help with a large one (500k+ rows, one integer key), read in key-range parts.
- **Any pairing:** the source is re-read through its own adapter and schema plan (so MongoDB
  flattening, renames and `_extra` are reproduced), and values are compared by the column's
  logical type, so e.g. MySQL `DECIMAL` and PostgreSQL `numeric` match.
- **Lossy by plan:** if you narrowed a column with a schema-plan override (e.g. a scale
  reduction), the validator applies the same conversion the target does. A table whose only
  differences are those conversions is reported as *lossy (plan)*, not failed.
- **L3 needs a usable key:** integer, text, decimal, uuid, date, MongoDB `_id`, or a composite
  of those. Other tables keep their L2 result, with the reason shown.
- **Progress:** each table's step and rows read are shown live on the migration and tables
  pages.

---

## PostgreSQL schemas

With the project's *Schema* setting empty (the default), a PostgreSQL source migrates every
non-system schema. Tables in the connection's own schema (`public`, normally) keep their bare
name; others are named `schema.table`, and a PostgreSQL target creates the schema and writes
there. Set *Schema* to migrate only that one schema. postgres→postgres native schema transfer
(`pg_dump`) is used only when both sides' default schema match; otherwise schema is created by
introspection, which can't recreate custom enum types.

---

## Oracle

Oracle is a source (12c and later; tested on 21c XE and 23ai Free). XferDB connects with a pure-Go
driver, so no Oracle client software is needed next to it.

- **Connection:** host, port (1521), the **service name** (e.g. `FREEPDB1`, not a SID) and a login.
  In the web UI's connection editor that's *Service name*; as a DSN,
  `oracle://user:pass@host:1521/SERVICE`.
- **Schema:** a project reads one schema: *Schema* (`?schema=HR` in a DSN) when set, else the
  login's own. The login needs `SELECT` on the tables (and sees them in `ALL_TABLES`).
- **Names:** Oracle stores unquoted names in upper case; XferDB folds them to lower case, so
  `CUSTOMERS.ORDER_ID` arrives as `customers.order_id`. A name created quoted in mixed case keeps
  its case, and so do two names that would fold into one.
- **Types:**

  | Oracle | Arrives as (PostgreSQL target) | Notes |
  |---|---|---|
  | `NUMBER(p,0)` | `smallint` / `integer` / `bigint` (p ≤ 4 / 9 / 18), else `numeric(p)` | |
  | `NUMBER(p,s)`, `NUMBER` | `numeric(p,s)`, `numeric` | exact; a MySQL target keeps at most 30 decimals (`DECIMAL(65,30)`) |
  | `FLOAT`, `BINARY_DOUBLE`, `BINARY_FLOAT` | `double precision`, `real` | |
  | `VARCHAR2`/`NVARCHAR2(n)`, `CHAR`/`NCHAR(n)` | `varchar(n)`, `char(n)` | lengths in characters |
  | `CLOB`, `NCLOB`, `LONG` | `text` | |
  | `BLOB`, `RAW`, `LONG RAW` | `bytea` | |
  | `DATE` | `timestamp(0)` | an Oracle `DATE` has a time of day |
  | `TIMESTAMP(n)`, `… WITH [LOCAL] TIME ZONE` | `timestamp(n)`, `timestamp(n) with time zone` | |
  | `INTERVAL … TO …` | `interval` | ISO 8601 text on targets without an interval type |
  | `JSON` (21c), `XMLTYPE`, `BOOLEAN` (23ai) | `jsonb`, `xml`, `boolean` | |

  Other targets get these through their usual mapping (MySQL `DATETIME(6)`, `LONGTEXT`, …). The
  schema editor can override any of them. Object types, `VARRAY`, nested tables, `SDO_GEOMETRY` and
  `BFILE` aren't supported: the table fails at CREATE TABLE naming the type, so leave it out.
- **Empty strings:** Oracle stores `''` as NULL, so they arrive as NULL.
- **What's copied:** tables and their rows, primary keys, plain indexes, and foreign keys between
  tables of the schema; `SYSDATE`/`SYSTIMESTAMP` defaults become the target's current timestamp.
  Not copied: function-based indexes, CHECK constraints, views, sequences, triggers and PL/SQL.
  An identity column arrives as a plain integer column with its values.
- **Left out of the table list:** the recycle bin, temporary, external and nested tables,
  materialized views' tables and logs.
- **Live migration** (beta) from Oracle 21c and 23ai reads changes with LogMiner, which every
  edition has; nothing is created on the source. It needs ARCHIVELOG mode, primary key
  supplemental logging and the LogMiner privileges the readiness check lists. See
  [docs/LIVE_MIGRATION.md](docs/LIVE_MIGRATION.md).

---

## DSN formats

```bash
# PostgreSQL
postgres://user:pass@host:5432/dbname?sslmode=require

# YugabyteDB (YSQL): type yugabyte, default port 5433
yugabyte://user:pass@host:5433/dbname?sslmode=require

# MySQL
mysql://user:pass@host:3306/dbname

# SQLite
sqlite:///absolute/path/to/file.db
sqlite://relative/path.db

# MongoDB
mongodb://user:pass@host:27017/dbname
mongodb://user:pass@host:27017/dbname?authSource=admin
mongodb+srv://user:pass@cluster.mongodb.net/dbname  # Atlas SRV format

# Oracle (source only): the service name, optionally a schema other than the login's
oracle://user:pass@host:1521/FREEPDB1
oracle://user:pass@host:1521/ORCLPDB1?schema=HR
oracle://user:pass@host:2484/ORCLPDB1?SSL=true      # TLS
```

> **Tip:** If your password contains special characters (`!`, `@`, `#`, etc.), URL-encode them: `!` → `%21`, `@` → `%40`. Use single quotes around the DSN to prevent shell expansion — and don't carry shell escapes such as `\!` into the web UI, where they become part of the password.
>
> **MongoDB:** when the URI names a database but the user is defined in `admin`, add `?authSource=admin`, or authentication fails.

---

## Preflight error hints

XferDB detects common connection problems and gives actionable hints:

| Error | Hint |
|-------|------|
| `database does not exist` | Create the database first: `CREATE DATABASE name;` — or, for the target, click *Create database* in the web UI |
| `SSL off` / `pg_hba.conf` | Add `?sslmode=require` to the DSN |
| `authentication failed` | Check username and password |
| `connection refused` | Check host, port, and that the server is running |
| `i/o timeout` | Check firewall rules or network connectivity |
| `ORA-01017` (Oracle) | Check username and password (case-sensitive) |
| `ORA-12514` / `ORA-12505` (Oracle) | Check the service name (`FREEPDB1`, `ORCLPDB1`, …; a SID won't do) |
| `ORA-28000` / `ORA-01045` (Oracle) | Unlock the account / grant `CREATE SESSION` |

---

## Migration lifecycle

```
xferdb migrate              → starts a new migration (blocked if one is already running)
xferdb migrate --status     → re-attach to the running migration
xferdb migrate --cancel     → stop the migration (history kept, checkpoints preserved)
xferdb migrate              → restart (resumes from last checkpoint)
```

On server restart, any interrupted migrations (and validations) are automatically marked as failed. The next `xferdb migrate` resumes from the last saved checkpoint.

---

## Native schema transfer (postgres→postgres)

When both source and target are PostgreSQL (or YugabyteDB), and both use the same default schema (see [PostgreSQL schemas](#postgresql-schemas)), XferDB uses `pg_dump` and `psql` instead of introspection-based `CREATE TABLE` statements. This preserves:

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

## Metrics (Prometheus)

`xferdb server` serves Prometheus metrics at `GET /metrics` on the same address as the API (not under `/api/`, so the web UI process doesn't proxy it: point Prometheus at the server directly). Scrapes are left out of the request log.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: xferdb
    static_configs:
      - targets: ["xferdb-server:8080"]
```

| Metric | Type | Labels |
|--------|------|--------|
| `xferdb_rows_transferred_total` | counter | `project`, `table` |
| `xferdb_tables_finished_total` | counter | `project`, `status` (completed, failed, cancelled) |
| `xferdb_migrations_finished_total` | counter | `project`, `status` (completed, failed, cancelled) |
| `xferdb_batch_read_duration_seconds`, `xferdb_batch_write_duration_seconds` | histogram | `project` |
| `xferdb_migrations_active` | gauge | `phase` |
| `xferdb_migration_phase` | gauge (1) | `project`, `migration`, `phase` |
| `xferdb_migration_progress_ratio`, `_rows_expected`, `_rows_transferred`, `_rows_per_second`, `_eta_seconds` | gauge | `project`, `migration` |
| `xferdb_migration_tables` | gauge | `project`, `migration`, `state` |

The `xferdb_migration_*` gauges exist only while that migration runs, so a `migration` label never outlives its run. The batch-duration histograms are a sample: batch progress events may be dropped under load (row counts are not affected). Go runtime (`go_*`) and process (`process_*`) metrics are included. If the server dies, the scrape target goes down (`up == 0`), which is the signal to alert on.

---

## REST API

The CLI is a thin wrapper. All operations are available directly:

```bash
# Projects
POST   /api/v1/projects
GET    /api/v1/projects
GET    /api/v1/projects/:id
PUT    /api/v1/projects/:id                 # edit connection config; blank password/dsn keeps the stored value
DELETE /api/v1/projects/:id
POST   /api/v1/projects/:id/preflight
POST   /api/v1/projects/:id/analyze
GET    /api/v1/projects/:id/tables/:table   # source + target column schema for one table (split view)
GET    /api/v1/projects/:id/schema-plan     # saved column-type overrides, keyed by table/collection
PUT    /api/v1/projects/:id/schema-plan     # body: [{"collection":"orders","field_name":"total","pg_type":"numeric",...}]
DELETE /api/v1/projects/:id/schema-plan     # wipe overrides for one table (?collection=) or the whole project
GET    /api/v1/projects/:id/support-bundle  # tar.gz diagnostic archive, credentials redacted
GET    /api/v1/projects/:id/validation-status  # latest validation result per table
POST   /api/v1/projects/:id/target-database # create the missing target database

# Connections (unsaved form fields)
POST   /api/v1/connections/databases        # list databases on a server
POST   /api/v1/connections/schemas          # list schemas in a database
POST   /api/v1/connections/create-database  # create a target database

# Migrations
POST   /api/v1/projects/:id/migrations      # body: {"table_workers":5,"segment_workers":10,"batch_size":50000,"truncate":true,"bulk_copy":true,"tables":["orders","customers"],...}
GET    /api/v1/projects/:id/migrations
GET    /api/v1/migrations/:id
PATCH  /api/v1/migrations/:id               # body: {"action":"pause"|"resume"|"cancel"}
POST   /api/v1/migrations/:id/cutover       # live migrations (beta); body: {"ignore_writes":false}
DELETE /api/v1/migrations/:id               # cancel + delete record
GET    /api/v1/migrations/:id/stats
GET    /api/v1/migrations/:id/stats/history # recorded snapshots (every 5s during migration)
GET    /api/v1/migrations/:id/schema-steps  # indexes/constraints created after the copy, with timings

# Validations
POST   /api/v1/migrations/:id/validations   # body: {"level":"L3","sample":10,"tables":["orders"]} (all optional)
GET    /api/v1/migrations/:id/validations
GET    /api/v1/validations/:id              # per-table results, plus live progress while running
PATCH  /api/v1/validations/:id              # body: {"action":"cancel"}
GET    /api/v1/validations/:id/tables/:table/diffs  # L3 example rows

GET    /api/v1/health
GET    /metrics                             # Prometheus metrics (see above)
```

---

## Web UI

`xferdb-web` is a separate process serving a vanilla-JS single-page app (hash routing, no
build step) that reverse-proxies `/api/*` to `xferdb server`. The browser only ever talks to
one origin, and the UI stays up even if the API/engine process crashes (e.g. OOM during a
large migration).

```bash
xferdb server --addr :8080 &
xferdb-web --addr :3000 --api-addr http://localhost:8080
# open http://localhost:3000
```

### Screens

- **Projects** (`#/`) — list, create, delete projects with live migration status badges
- **Tables** (`#/projects/:id`) — a project's default landing page: its tables/collections,
  live migration status with Run / Pause / Resume / Cancel, migration history, and each table's
  validation badge (with live progress while it validates). While a migration is active,
  tables are locked (not clickable) with a banner explaining why
- **Table detail** (`#/projects/:id/tables/:table`) — split view: source columns on the left,
  suggested target schema on the right. The target type is an editable dropdown (per-dialect
  choices for Postgres/MySQL/SQLite/Mongo→Postgres) with Save-with-confirmation, backed by the
  `schema-plan` endpoints above — available for every adapter pairing, not just MongoDB→Postgres
- **Settings** (`#/projects/:id/settings`) — connection config view/edit, preflight/test
  connection, project validation defaults, download support bundle, reset schema plans, delete
  project. The connection editor asks for host, port, credentials and SSL mode first; *Load
  databases* then lists the server's databases (or shows the connection error), and a missing
  target database can be created with *Create database*
- **Migration progress** (`#/migrations/:id`) — full live progress page with real-time stats,
  per-table breakdown, resource usage, pause/resume/cancel controls, stats history, and the
  validation results card (*Validate now*, per-table progress, *View rows* for L3 diffs)

### Migration options (Web UI)

The "Run a migration" form exposes all CLI migration options. It opens with the options of
the project's last run from the form; schema mode, bulk copy and table selection always start
from their safe defaults.

| Option | Description |
|--------|-------------|
| **Schema mode** | Keep existing, Truncate first, or Drop & recreate target tables |
| **Table workers** | Number of tables to migrate concurrently |
| **Batch size** | Rows per INSERT batch |
| **Segment workers** | Parallel workers per table (splits by PK range) |
| **Validation** | After migration, as each table finishes, or skip |
| **Validation level** | L1, L2 or L3 (see [Validation](#validation)) |
| **L3 sample (%)** | Share of a failing table the L3 diff checks; 0 checks every row |
| **Tables to migrate** | Select/deselect individual tables (appears after analyze) |

**Performance options** (expandable section):

| Option | Description |
|--------|-------------|
| **Async pipeline** | Overlap reading and writing for faster throughput |
| **Bulk copy** | Fast bulk-load write path (postgres: COPY, mysql: LOAD DATA LOCAL INFILE, sqlite: batched inserts); only available with truncate or drop & recreate |
| **Accurate row counts** | Use exact counts instead of estimates (slower) |
| **Force OFFSET segments** | Use OFFSET-based splitting instead of PK range (when segment workers > 0) |

### MongoDB sampling options

For MongoDB source projects, additional options appear:

| Option | Description |
|--------|-------------|
| **Sample size** | Documents to sample per small collection (default: 2000) |
| **Sample %** | Percentage to sample for large collections (default: 1%) |
| **Threshold** | Document count above which percentage sampling kicks in |
| **AI annotations** | Enable AI-powered field type inference |

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
- [x] Bulk copy mode (`--bulk-copy`) — postgres `COPY`, mysql `LOAD DATA LOCAL INFILE`, sqlite batched inserts
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
- [x] Prometheus metrics (`GET /metrics`)
- [x] Post-migration data validation (L1 counts, L2 row fingerprint, L3 row diff)
- [x] All PostgreSQL schemas by default
- [x] Oracle source adapter (21c, 23ai)
- [x] Live migration from Oracle (LogMiner; 21c, 23ai)
- [ ] Live migration from Oracle 19c (mining from the root container, #196)
- [ ] MongoDB target adapter
- [x] Web UI (projects, live table browser, migration status/controls, schema editor for every adapter pairing)
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
