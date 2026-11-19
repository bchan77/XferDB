# Changelog

All notable changes to XferDB are documented here.

## [Unreleased]

### Added
- **Schema and database pickers for every connection type.** PostgreSQL, YugabyteDB and Oracle
  connections get a *Load schemas* button that offers the database's schemas as suggestions in
  the Schema field, which stays free text so a target can name a new schema (Oracle: the schemas
  holding tables, Oracle's own left out), and MongoDB
  connections a Database field with *Load databases* (admin, config and local left out; a
  database chosen there takes the place of the one in the URI). An empty field still means every schema (PostgreSQL source), each table's source schema
  (PostgreSQL target), the login's own schema (Oracle) or the URI's database (MongoDB). New
  endpoint `POST /api/v1/connections/schemas`; `/connections/databases` now takes MongoDB.
- **Live migration from Oracle (beta, #195).** Oracle 21c and 23ai can be the source of a live
  migration to PostgreSQL, YugabyteDB, MySQL or SQLite. Changes are read with LogMiner from inside
  the pluggable database (every edition has it), so nothing is created on the source. Each
  transaction's changed rows are read back as of its commit, so streamed rows arrive exactly as
  copied ones. Readiness checks ARCHIVELOG mode, supplemental logging, the LogMiner and flashback
  privileges, primary keys, and on 21c `archive_lag_target` (21c mines archived logs only, so it is
  that far behind; 23ai streams within seconds). A stream that falls behind across an `ALTER TABLE`
  fails rather than skip what it can't decode. 19c, which mines from the root container, is #196.
  See `docs/LIVE_MIGRATION.md`.
- **Oracle as a migration source (#190).** Oracle 12c and later (tested on 21c XE and 23ai Free)
  can be migrated to PostgreSQL, YugabyteDB, MySQL or SQLite, through a pure-Go driver, so no
  Oracle client software is needed. A project reads one schema (the login's own unless *Schema*
  names another) and connects to a service name: `oracle://user:pass@host:1521/FREEPDB1`.
  Unquoted upper-case names arrive in lower case. Oracle's types map to their nearest equivalent
  on each target (`NUMBER(10)` to `bigint`, `DATE` to a timestamp, `CLOB` to text, ...), keeping
  `NUMBER` values exact; primary keys, plain indexes and foreign keys come along. The web UI offers
  `oracle` as a source type with a *Service name* field, and `xferdb list` now lists every adapter
  with what it can be used for. See the README's *Oracle* section.
- **Live migration from MongoDB (beta, #179).** A MongoDB 4.2+ replica set, or a sharded cluster
  through one `mongos`, can be the source of a live migration to PostgreSQL, YugabyteDB, MySQL or
  SQLite. Changes are read from a change stream, so nothing is created on the source. Documents
  go through the project's schema plan exactly as in the copy, and fields the plan doesn't know,
  also ones that first appear while streaming, land in `_extra`, so the stream never pauses for a
  schema change. Updates use the collection's post-images where it keeps them (6.0+), and are
  otherwise read back from the primary. A shard key change that moves a document between shards
  is applied as the one change it is. Readiness checks the topology, the version, that the login
  can open a change stream, and the oplog window; a stream whose resume point has left the oplog
  fails the migration. See `docs/LIVE_MIGRATION.md`.

### Changed
- **Validation runs tables in parallel and reads both sides at once (#200).** `after` mode and
  *Validate now* validate as many tables at once as the migration's table workers, largest first
  (`per_table` already did; for *Validate now* / `xferdb validate`, the project's `table_workers`,
  else the count the migration ran with, now saved with every migration); L2 reads a table's source and target concurrently instead of one
  after the other; and a MongoDB source is read through one cursor rather than a sorted `find`
  per batch (#202, validation only). Once no whole table is left to start, idle workers help with
  a large one (500k+ rows keyed by one integer column, a MongoDB numeric `_id` included): it is
  read in key-range parts of ~250k rows that any worker can take, and the parts' fingerprints add
  up to the table's. YugabyteDB tables are always read in one pass. Results are unchanged. The
  progress step `reading_source` / `reading_target` is now a single `reading` step.

### Fixed
- **Oracle tables with an XMLTYPE column can be migrated.** Reading one failed with
  `TTC error: received code … during response reading`, since the Oracle driver can't decode
  XMLTYPE; the column is now read as its serialized text (compact, not re-indented) and
  lands in an `xml` column as before.
- **A sharded MongoDB source no longer fails a table when a document moves shards during the copy.**
  Read through mongos while its shard key changed, a document could come back from both shards,
  side by side in `_id` order; when both copies landed in one batch, a PostgreSQL or YugabyteDB
  target's upsert failed with `ON CONFLICT DO UPDATE command cannot affect row a second time`
  (seen once in 15 runs of the matrix's `mongo80-sh-yb2026-live`). A batch now keeps the first
  copy of a repeated `_id` (numbers compared by value); a live migration's stream, started before
  the copy, replays the move.
- **Analyze no longer calls every table of a first migration incompatible.** A table the target
  doesn't have yet was an error (`INCOMPATIBLE · 1 ISSUE(S)` on every row of a new project), though
  the migration creates it. It is now status `new` (issue severity `info`), shown as
  *new · will be created*. Other badges name their first issue (`no column x on the target`,
  `amount: numeric vs double precision`) and list every suggestion on hover.
- **Identity and auto-increment columns arrived as plain columns (#194).** An Oracle or
  PostgreSQL identity column, a PostgreSQL `serial` and a MySQL `AUTO_INCREMENT` column now
  generate keys on the target too (`GENERATED BY DEFAULT AS IDENTITY` on PostgreSQL and
  YugabyteDB, `AUTO_INCREMENT` on MySQL; a SQLite `INTEGER PRIMARY KEY` already does), so an
  application's inserts that leave the key out keep working after the switch-over. Once the
  rows are copied (at cutover for a live migration), each column's next value is set past the
  largest copied key and past the source's own next value. This also fixes PostgreSQL to
  PostgreSQL copies through `pg_dump`, whose identity sequences started over at 1. A MySQL target
  session now sets `NO_AUTO_VALUE_ON_ZERO`, so a copied key of 0 stays 0. MySQL allows only the
  first column of the primary key to be `AUTO_INCREMENT`; other identity columns stay plain there.
- **Interval columns failed validation unless both sides spelled them alike.** Validation now
  compares intervals as months, days and microseconds, so `P1Y2M` and PostgreSQL's
  `1 year 2 mons` match. A MySQL target, which has no interval type, gets `VARCHAR(64)` instead of
  a CREATE TABLE that failed.
- **The cutover's quiet check could miss writes still on their way (#181).** It waited a few
  seconds and then counted the changes capture had queued since the cutover point, but a change
  made during that wait could still be on its way to capture, and the check then passed and the
  cutover completed while the source was being written, leaving those writes behind. It showed
  on sharded MongoDB, whose `mongos` holds a change until every shard is past it; any source
  could do it under a slow enough network. The check now waits until capture has confirmed the
  source's position after the wait before counting.
- **Live migration readiness fails a table with a `DEFERRABLE` primary key (#176).** PostgreSQL
  can't identify a row by a deferrable key, so such a table has no usable replica identity, and
  once XferDB's publication included it every `UPDATE` and `DELETE` on it failed on the source
  (*cannot update table … because it does not have a replica identity and publishes updates*)
  until the live migration was cancelled. The readiness check passed it, since it read only the
  replica identity setting. A new check, *a primary key that isn't DEFERRABLE*, now fails it and
  names the key, so a live migration can't start with one. `REPLICA IDENTITY FULL` isn't offered
  as the fix: XferDB's upserts can't use the deferrable key the schema copy gives a PostgreSQL
  target either.
- **`xferdb migrate` uses the project's saved transfer settings (#172).** The CLI sent its own
  defaults for table workers, segment workers, async pipeline, OFFSET segments and accurate counts
  with every run, and the server takes any value it is sent as an override for that run, so the
  project's saved settings never applied: after `xferdb project create --table-workers 3` (as in
  the README's quick start), `xferdb migrate` still ran with 1 table worker. A setting is now sent
  only when a flag, an `XFERDB_*` environment variable or the config file gives it; otherwise the
  project's value is used, as the web UI's run form already did. Schema mode (`--recreate-schema`,
  `--truncate`) and `--bulk-copy` stay per run and are never taken from the project.

## [1.2.0] - 2026-11-09

Live migration (beta) and YugabyteDB as a database type of its own: copy a PostgreSQL or
YugabyteDB source and keep the target following it until a guided cutover. Upgrading from 1.1.0
needs no changes; projects that reach YugabyteDB as `postgres` keep working.

Live migration (#102) ships as a **beta**: labelled so in the web UI, the CLI and the README until it has run against real workloads.
How to use it, step by step: [docs/LIVE_MIGRATION.md](docs/LIVE_MIGRATION.md). `xferdb migrate --preflight --live` runs the live readiness checks from the CLI.

### Added
- **Hash-sharded YugabyteDB tables are copied by hash range, in parallel.** A table whose primary
  key starts with a `HASH` column was paged in key order, which YugabyteDB answers with a full scan
  and sort per page, so a large table copied slowly and one at a time. Such a table is now split
  into ranges of `yb_hash_code()` (which YugabyteDB reads efficiently), copied by the run's segment
  workers in parallel, each range checkpointed, so a resumed migration skips the ranges already
  done.
- **A live migration shows what its stream is doing.** Once the copy is done, each table in the
  migration page's list shows the inserts, updates and deletes the stream has applied to it and how
  long ago the last one was; *Applied* shows the rate over the last minute, and a line names the
  table last written to. The copy's cards are labelled *Initial copy*, *Copy elapsed*, *Copy rate*
  and *Rows copied*, so they aren't read as the stream's. The counts survive an apply restart
  (`activity.json` in the worker directory), so *Applied* no longer drops to 0 when apply is
  restarted. API: `live.apply.tables` and `live.changes_per_sec` in `GET /migrations/:id`; the CLI
  status line shows the rate. After a server restart, when the copy's live stats are gone, the page
  shows its recorded stats and table list instead of dropping them, so the per-table lines stay.
- **Leftover live migration streams are found and can be dropped.** A replication slot or
  publication named like a live migration's that no migration still running on this server owns
  (its cancel couldn't reach the source, the server stopped mid-teardown, or the migration was
  deleted) is a warning in the live readiness check and is listed under *Settings → Live
  Migration* with a *Drop* button; `xferdb leftovers [drop NAME]` and
  `GET`/`DELETE /api/v1/projects/:id/live-streams[/:name]` do the same. Never dropped
  automatically, since another XferDB server on the same source owns streams this one doesn't
  know, and never while a running migration owns it or its slot is in use.
- **Live migration from YugabyteDB (#158; beta).** A `yugabyte` source can now be migrated live, to
  any target. XferDB creates its replication slot with hybrid-time positions
  (`yb_lsn_type = HYBRID_TIME`), compares them with `yb_get_current_hybrid_time_lsn()` to tell
  when catch-up and cutover are done, and commits a heartbeat (one row per migration in a small
  `xferdb_heartbeat` table in the publication, never copied) right after recording a position, since
  an idle YugabyteDB stream doesn't advance on its own. Lag is reported in time (`behind_seconds`)
  rather than WAL bytes; sequences are read from the sequences themselves (YugabyteDB's
  `pg_sequences.last_value` is always NULL); dropping the slot waits out YugabyteDB's one-minute
  active window. The readiness check adds hybrid-time slot support and `CREATE` on the schema for
  the heartbeat table. The pg_dump schema copy now covers `yugabyte` sources and targets too
  (leaving out the heartbeat table), as it did for YugabyteDB reached as `postgres`.
- **YugabyteDB live migration: rows changed twice in one transaction.** The guide
  ([docs/LIVE_MIGRATION.md](docs/LIVE_MIGRATION.md)) lists the tserver gflag
  `cdc_enable_intra_transactional_before_image=true` (2025.2.4+) as a requirement when the
  application updates or deletes a row it already changed in the same transaction; without it
  capture stops at *Failed to get the beforeimage for tablet_id*. Readiness can't check it (SQL
  can't read gflags).
- **YugabyteDB adapter type (#158).** `yugabyte` is a source and target type of its own (web UI,
  `yugabyte://` DSNs, default port 5433), running the postgres adapter. Projects that reach
  YugabyteDB as `postgres` keep working unchanged. Its live readiness check shows what the source
  needs: replication commands
  enabled (`yb_enable_replication_commands`), a replica identity other than YugabyteDB's default
  `CHANGE`, and how long the tservers keep changes (`cdc_intent_retention_ms`) in place of WAL
  limits. A `postgres` project pointing at a YugabyteDB server is told to switch type.
- **Replica identity check (#158).** The live readiness check fails a table whose replica identity
  pgoutput can't use: `NOTHING` (on PostgreSQL, updates and deletes on it fail once it's
  published) or YugabyteDB's `CHANGE`, with the `ALTER TABLE … REPLICA IDENTITY DEFAULT` fix.
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
- **Negative workers or batch size are refused.** A project's transfer settings accepted negative
  table workers, segment workers or batch size (the web UI's number fields also changed when
  scrolled over, which is how a `-75` got saved; a run treated it as the default). Creating or
  updating a project now rejects them, and a focused number field no longer changes when the page
  is scrolled.
- **Validation retries a table that failed on a transient database error.** YugabyteDB reports a
  read on a table whose schema just changed under the connection's cache (its indexes and
  constraints had just been added) as SQLSTATE 40001, *schema version mismatch*, an error to retry.
  The validator took it for a verdict: a YugabyteDB → YugabyteDB migration whose every row had
  copied failed validation on one table. A table that fails on 40001 (or a 40P01 deadlock) is now
  validated again, up to three more times, 1, 3 and 10 seconds apart.
- **A YugabyteDB target gets its primary keys before the copy, not after.** The pg_dump schema copy
  added primary keys with the rest of post-data, after the rows were in. YugabyteDB stores a
  table's rows in primary key order, so adding one to a loaded table rewrites the whole table (a
  1M-row table spent minutes in *post-schema*). On a `yugabyte` target the primary keys now go in
  right after the tables are created, while they are empty; the later post-data step skips them.
- **Tables gone from the source no longer stay in a project's table list, or in a run's total.** A
  project keeps a record of every source table with its last result; a table dropped from the
  source (or a project pointed at a new database) stayed listed forever, as *pending* in every
  later migration, and its rows were added to the migration's total, so progress stopped short of
  100% (48% for a fully copied run) and the ETA was too long. Each run now drops the records of
  tables the source no longer has, and a migration's row total counts only its own tables: those
  outside its scope are still listed, but no longer counted, so a `--tables` run also reaches 100%.
- **The live readiness check no longer hangs or hides results (#158).** Each check runs on its own
  with a 15-second limit per query: one the source fails or doesn't answer fails that check, and
  the others are still reported (before, the first failing query replaced every result with a
  single *source checks* error, after however long the source took). On YugabyteDB it no longer
  reads `pg_stat_replication`, which waits up to two minutes for a table only the first
  replication slot creates.
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
