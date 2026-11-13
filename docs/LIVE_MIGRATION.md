# Live migration (beta)

A live migration copies your tables to the target and then keeps streaming
every change made on the source, so the applications using the source keep
running while the data moves. When you are ready to switch, you stop the
writes, *cut over*, and point the applications at the target.

> **Beta.** Live migration works end to end and runs in XferDB's test matrix
> on every change, but it hasn't yet been used on real production workloads.
> Rehearse it on a copy of your data first, and keep the source until you have
> checked the target.

## Contents

- [What you need](#what-you-need)
- [1. Check readiness](#1-check-readiness)
- [2. Start the live migration](#2-start-the-live-migration)
- [3. Watch it stream](#3-watch-it-stream)
- [4. Pause and resume](#4-pause-and-resume)
- [5. Schema changes on the source](#5-schema-changes-on-the-source)
- [6. Cut over](#6-cut-over)
- [Cancelling](#cancelling)
- [WAL and disk space](#wal-and-disk-space)
- [The oplog (MongoDB)](#the-oplog-mongodb)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)

## What you need

**Source: PostgreSQL 10 or later**, with:

| Requirement | How to set it |
|---|---|
| `wal_level = logical` | `ALTER SYSTEM SET wal_level = logical;` then restart PostgreSQL. On Amazon RDS / Aurora set the parameter `rds.logical_replication = 1` and reboot. |
| A login with the replication privilege | `ALTER ROLE xferdb REPLICATION;` (on RDS: `GRANT rds_replication TO xferdb;`) |
| A free replication slot and WAL sender | XferDB uses one of each for the whole migration. Raise `max_replication_slots` / `max_wal_senders` (restart needed) if none are free. |
| A primary key on every migrated table | Changes are applied by key. Leave tables without one out with `--tables`. |
| A primary key that isn't `DEFERRABLE` | PostgreSQL can't identify rows by a deferrable key, so once the table is published its updates and deletes fail **on the source**, and XferDB's upserts can't use the key on a PostgreSQL target. Recreate it as an ordinary key: `ALTER TABLE <table> DROP CONSTRAINT <key>, ADD PRIMARY KEY (<columns>);` (drop and re-add any foreign keys that reference it), or leave the table out with `--tables`. A `DEFERRABLE` unique constraint on other columns is fine. |
| A replica identity other than `NOTHING` | `DEFAULT` (the primary key, PostgreSQL's default) or `FULL`. Under `NOTHING`, updates and deletes on a published table fail on the source. Fix: `ALTER TABLE <table> REPLICA IDENTITY DEFAULT;` |
| Permission to create a publication | `CREATE` on the database, and the login must own the migrated tables (or be a member of the owning role). |
| Recommended: a WAL limit for slots | `ALTER SYSTEM SET max_slot_wal_keep_size = '50GB'; SELECT pg_reload_conf();` (PostgreSQL 13+). See [WAL and disk space](#wal-and-disk-space). |

**Or source: YugabyteDB** (YSQL; source type `yugabyte`), with:

| Requirement | How to set it |
|---|---|
| Replication commands and hybrid-time slots | The tserver gflags `ysql_yb_enable_replication_commands=true` and `ysql_yb_allow_replication_slot_lsn_types=true`, on a release that has `yb_get_current_hybrid_time_lsn()` (2025.2 has all three). |
| Old values for rows changed twice in one transaction | The tserver gflag `cdc_enable_intra_transactional_before_image=true` (2025.2.4 and later), if the application updates or deletes a row it already inserted or changed earlier in the same transaction (for example a delete and re-insert under the same key). Without it, capture stops at *Failed to get the beforeimage for tablet_id* when the stream reaches such a transaction. Readiness can't check this flag: SQL can't read it. |
| A replica identity pgoutput can stream | New YugabyteDB tables get `CHANGE`, which pgoutput refuses: `ALTER TABLE <table> REPLICA IDENTITY DEFAULT;` for each, **before** starting (a slot fixes the identity it streams with when it is created). |
| `CREATE` on the connection's schema | XferDB creates a small `xferdb_heartbeat` table there for the migration and drops it at the end (see below). |
| The login, slot, primary key and publication requirements above | As for PostgreSQL, except WAL senders, which YugabyteDB doesn't have. |

How it differs from PostgreSQL:

- **Heartbeats.** An idle YugabyteDB stream doesn't move forward the way an
  idle PostgreSQL one does, so XferDB couldn't otherwise tell that the target
  has caught up while nothing is written. During the migration it updates one
  row in `xferdb_heartbeat` about once a second; those changes are never
  copied to the target.
- **Lag in time, not bytes.** *Behind source* is shown as a time, and there is
  no *WAL held on source*.
- **Retention.** YugabyteDB keeps a stream's changes for the tservers'
  `cdc_intent_retention_ms` (8 hours unless changed). If capture is stopped
  for longer, the stream expires and the migration must start over.
- **Sequences** are set at cutover from each sequence's cached block, so the
  target may skip up to `cache_size` values: it never reuses one.
- **Dropping the slot** waits up to a minute after the stream stops, so
  cutover and cancel take that long to finish.
- **Hash-sharded tables** (a primary key starting with a `HASH` column, as
  YugabyteDB makes by default) are copied in chunks of their hash space rather
  than in key order, which would cost a scan of the table for every page. As
  many chunks are copied at once as the run's segment workers
  (`--segment-workers`, 1 by default), and each is checkpointed, so a resumed
  snapshot skips the chunks already done. Range-sharded keys are paged in key
  order, which YugabyteDB reads efficiently.

**Or source: MongoDB 4.2 or later**, as a **replica set** or a **sharded
cluster**, with:

| Requirement | How to set it |
|---|---|
| A replica set or a sharded cluster | A standalone `mongod` keeps no oplog, so there is no change stream to read. A single-member replica set is enough: start `mongod` with `--replSet rs0` and run `rs.initiate()` once. |
| A login that can open a change stream | The `changeStream` and `find` actions on the migrated database. The built-in `read` role has both. |
| A schema plan | As for any MongoDB migration: run *Analyze* (`xferdb project analyze`) first. It fixes each collection's columns. |
| On a sharded cluster: one `mongos` | Connect to a single `mongos`, not a load balancer in front of several: a change stream's cursor lives on the `mongos` that opened it. |
| Recommended: an oplog window longer than any outage | XferDB keeps nothing on the source, so its stream can only resume from what the oplog still holds. See [The oplog (MongoDB)](#the-oplog-mongodb). |
| Recommended (6.0+): post-images | `db.runCommand({collMod: '<collection>', changeStreamPreAndPostImages: {enabled: true}})`. With them, each update streams the document as it was after the change; without them, XferDB reads the document back from the primary when it streams the update. Both are correct; post-images save a read per update. |

How it differs from PostgreSQL:

- **Nothing is created on the source.** No slot, no publication: the change
  stream reads the oplog, which the replica set (or each shard) keeps anyway.
  Cancelling or cutting over leaves nothing behind, and there are no
  leftover streams.
- **Documents go through the schema plan**, exactly as in the copy. A field
  the plan doesn't know, also one that first appears while the migration
  streams, goes into the `_extra` column. MongoDB has no schema changes, so a
  live migration from it never pauses for one. A field removed from a
  document (`$unset`) becomes `NULL`, so every column but `_id` is created
  nullable on a live migration's target (the plan's *not null* is only what
  its sample saw).
- **Lag in time, not bytes**, as for YugabyteDB.
- **The copy reads from the primary**, not a secondary: a secondary behind the
  stream's start would copy a document older than a change the stream never
  replays.
- **Transactions** are queued whole, and their changes applied per document,
  as for every source.
- **Sharded clusters.** `mongos` merges the shards' changes in cluster-time
  order, and passes a change on only once every shard is past it. An idle
  shard moves forward by a no-op write every 10 seconds, so the end of
  catch-up and the cutover each take up to about 10 seconds longer than on a
  replica set. An update that changes a document's shard key (and so may move
  it to another shard) is applied as one change. Chunk migrations between
  shards aren't changes and are never copied.

**Or source: Oracle 21c or 23ai** (12c to 19c need mining from the root
container, which isn't supported yet: #196), with:

| Requirement | How to set it |
|---|---|
| ARCHIVELOG mode | As SYSDBA in the root container: `SHUTDOWN IMMEDIATE; STARTUP MOUNT; ALTER DATABASE ARCHIVELOG; ALTER DATABASE OPEN;` (then `ALTER PLUGGABLE DATABASE ALL OPEN;`). |
| Supplemental logging: minimal, and the primary key | `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;` and `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;` (or the latter per table: `ALTER TABLE <table> ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;`). |
| LogMiner privileges, in the pluggable database | `GRANT LOGMINING, SELECT ANY TRANSACTION TO xferdb; GRANT EXECUTE ON DBMS_LOGMNR TO xferdb;` and `SELECT` on `V_$DATABASE`, `V_$LOGMNR_CONTENTS`, `V_$LOGMNR_LOGS`, `V_$ARCHIVED_LOG`, `V_$LOG`, `V_$TRANSACTION` and `V_$PARAMETER`. Readiness prints the full list. |
| Flashback queries on the migrated tables | Free on the login's own tables; otherwise `GRANT FLASHBACK ON <table> TO xferdb;` (or `FLASHBACK ANY TABLE`). |
| A primary key on every migrated table, of a type the stream can carry | `NUMBER`, `FLOAT`, `CHAR`, `VARCHAR2`, `DATE`, `TIMESTAMP` (without time zone) or `RAW` key columns. |
| On 21c: `archive_lag_target` | 21c mines only archived logs from inside a pluggable database, so changes arrive when the current log is archived. `ALTER SYSTEM SET archive_lag_target = 60;` (in the root container) bounds that to a minute. 23ai reads the online log, so changes arrive within seconds. |
| Recommended: archived logs kept longer than any outage | XferDB keeps nothing on the source, so it can only resume from redo still there. Don't let RMAN delete archived logs the migration hasn't read: readiness and the migration page show how far back the redo goes. |

How it differs from PostgreSQL:

- **Nothing is created on the source.** Changes are read with LogMiner,
  which every edition has (no GoldenGate licence), from the redo Oracle
  writes anyway. Cancelling or cutting over leaves nothing behind.
- **Rows are read back.** LogMiner tells XferDB which rows each transaction
  changed (by primary key); once the transaction commits, XferDB reads those
  rows as they were at its commit (a flashback query, `AS OF SCN`), so a
  streamed row reaches the target exactly as a copied one would, LOBs
  included. If Oracle's undo no longer goes back that far (`undo_retention`,
  15 minutes by default), the row is read as it is now instead, and the
  changes made since are replayed too, so the target still ends up right.
- **Schema changes need the stream to be caught up.** LogMiner reads the
  redo with today's dictionary, so it can't decode a table's changes from
  before an `ALTER TABLE` if the stream hadn't read them yet. XferDB then
  fails the migration rather than skip them, and it must start over. On 23ai
  the stream is a few seconds behind; on 21c it is up to `archive_lag_target`
  behind, so make schema changes when nothing has been written to the table
  for longer than that.
- **Lag in time, not bytes**, as for YugabyteDB.
- **Rows changed while the stream was down** are found again from the redo
  when capture restarts, as long as the archived logs are still there.

**Target:** PostgreSQL, YugabyteDB, MySQL or SQLite.

**The XferDB server:** the capture and apply workers run as processes on the
machine running `xferdb server`, and queue changes on its disk under
`~/.xferdb/cdc/<migration-id>/` (next to the state database). It needs to reach
both databases for the whole migration, and enough free disk for the changes
made while the target is behind or paused.

## 1. Check readiness

Run the readiness check before you start; it lists each requirement and how to
fix what fails.

**Web UI:** open the project, *Settings* → *Live Migration* → *Check live
migration readiness*.

**CLI:**

```bash
xferdb project use my-project
xferdb migrate --preflight --live
```

```
Live migration (beta): ready
  ✓ PostgreSQL 10 or later (server_version_num = 160015)
  ✓ wal_level = logical (wal_level = logical)
  ✓ replication privilege (role xferdb)
  ✓ a free replication slot (9 of max_replication_slots = 10 free)
  ✓ a free WAL sender (9 of max_wal_senders = 10 free)
  ✓ a primary key on every table (all 14 tables)
  ✓ permission to create the publication (CREATE on the database)
  ! WAL a slot may hold (max_slot_wal_keep_size = -1 (unlimited))
      fix: if XferDB's capture stops for long, the slot keeps WAL until the source's disk fills; ...
  ✓ a target that can apply changes (target is postgres)
```

A `✗` blocks the migration; a `!` is a warning worth reading. When the
project's settings name tables, only those are checked. Each check runs on
its own with a 15-second limit: a query the source doesn't answer in time
fails that check only, and the others are still shown.

**API:** `POST /api/v1/projects/:id/preflight` with `{"mode": "live"}`; the
checks are in the response's `live` field.

## 2. Start the live migration

**Web UI:** on the project page, tick **Live migration** in the migration form
and click **Start Live Migration**. *Keep existing target schema/data* and
*Drop & recreate target tables* both work; *Truncate* can't be used, because
XferDB creates the target's schema itself (it holds back unique indexes,
foreign keys and checks until replaying changes can no longer break them).

**CLI:**

```bash
xferdb migrate --live
xferdb migrate --live --tables orders,customers   # only some tables
xferdb migrate --live --recreate-schema           # drop and recreate target tables first
xferdb migrate --live --until-streaming           # return once it streams (for scripts)
```

`xferdb migrate --live` follows the migration until it ends. Ctrl+C only stops
following: the migration goes on in the server, and `xferdb migrate --status`
attaches to it again.

**API:** `POST /api/v1/projects/:id/migrations` with `{"mode": "live"}` plus
the usual options (`tables`, `recreate_schema`, `table_workers`, ...).

## 3. Watch it stream

A live migration moves through these phases, shown as a strip on its page and
in the CLI's status line:

| Phase | What happens |
|---|---|
| **Setup** | XferDB creates a replication slot and a publication on the source, then the target tables. From here on every change on the source is kept for the migration. |
| **Snapshot** | The tables are copied, while the applications go on writing. |
| **Catch-up** | The changes made during the copy are applied. Unique indexes are added at the end. |
| **Streaming** | The target follows the source, usually within a second or two. It stays here until you cut over. |
| **Cutover** | The last changes are applied and the stream is closed (step 6). |
| **Completed** | The target has every change; the slot is gone. |

While it streams, the page shows:

- **Changes to apply** and **Apply lag**: how far the target is behind.
- **Behind source**: how far capture is behind the source's WAL.
- **WAL held on source**: the WAL the source keeps for this migration.
- **Queue on disk**: changes captured but not yet applied, on the XferDB server.
- **Applied**: changes the stream has written to the target, with the rate over
  the last minute, and the table it last wrote to and when.
- Under each table in the table list: the inserts, updates and deletes the
  stream has applied to it, and how long ago the last one was. The rows and
  progress bars above them count the initial copy only, which is why the copy
  cards are labelled *Initial copy*, *Copy elapsed* and *Copy rate*.

If the XferDB server restarts, the workers keep running and the server picks
the migration up again when it starts. A worker that crashes is restarted
automatically; its error is shown until it recovers.

## 4. Pause and resume

**Pause** stops applying changes to the target; they are still captured and
queued on the XferDB server, so the source doesn't keep extra WAL. **Resume**
applies the queue and carries on. Use it to give the target a break (an index
build, a backup), not as a long-term state: the queue grows meanwhile.

In the CLI: `xferdb resume`. (Pause is in the web UI and the API:
`PATCH /api/v1/migrations/:id` with `{"action": "pause"}` or `"resume"`.)

## 5. Schema changes on the source

XferDB doesn't apply schema changes (DDL). When a migrated table's columns
change on the source — a column added, dropped or retyped, or the table renamed
— the migration **pauses at that point**:

- every change made before the schema change is applied;
- nothing after it is applied, for any table, until you resume;
- the migration page shows the table and what changed, for example
  `column loyalty_tier added (text)`.

To carry on:

1. Make the same change on the target yourself, for example
   `ALTER TABLE customers ADD COLUMN loyalty_tier text DEFAULT 'bronze';`.
2. Click **Resume** (it asks you to confirm the change is on the target), or
   run `xferdb resume`, which shows the change and asks the same.

The changes after it are then applied to the target as it is now. If you
resume without changing the target, a change that needs the new column fails
to apply, and apply keeps retrying, with the error shown, until the target has
it.

Changes that don't touch columns (new indexes, constraints, new tables) don't
pause the migration and aren't copied. A table created after the migration
started isn't part of it.

## 6. Cut over

When you're ready to switch the applications to the target:

1. **Stop every application writing to the source.** Writes made after the
   cutover don't reach the target.
2. **Cut over:** *Cut over* on the migration page, or `xferdb cutover`.
3. **Point the applications at the target** once the cutover has completed.

The cutover runs these steps, shown as they go:

1. Apply every change up to the source's current position.
2. Check, for a few seconds, that no more changes to the migrated tables
   arrive.
3. Set the target's sequences to the source's values (PostgreSQL targets), and
   move every identity or `AUTO_INCREMENT` column past the migrated keys and
   the source's own next value (PostgreSQL, YugabyteDB and MySQL targets), so
   new rows don't collide with copied ones.
4. Add the foreign keys, checks and triggers held back until now.
5. Stop the stream and remove it from the source (the replication slot and
   publication; nothing on MongoDB).

The migration is then completed and validated by the project's validation
settings (`xferdb cutover` waits for the result and exits non-zero if a table
differs).

**If writes are still arriving**, the cutover stops at step 2 and the migration
goes on streaming, with the number of changes that arrived. Find and stop the
writer, then *Cut over again*. *Cut over anyway* (`xferdb cutover
--ignore-writes`) applies what arrived once more and finishes, leaving any later
writes behind.

If a step fails (a foreign key that the data doesn't satisfy, say), the
migration also goes on streaming with the reason shown; fix it and cut over
again.

## Cancelling

*Cancel* on the migration page, `xferdb migrate --cancel`, or deleting the
migration stops the stream and drops the replication slot and publication on
the source (and, on YugabyteDB, its heartbeat row; on MongoDB there is nothing
to drop). Rows already on the target
stay. A cancelled or failed live migration can't be resumed: start a new one.

### Leftover streams

A stream can still be left on the source: the source was unreachable when the
migration was cancelled (the cancel then fails, and can be repeated), the
server stopped in the middle of dropping it, or someone deleted XferDB's state.
The source keeps every change for a leftover slot, so XferDB looks for them:

- The readiness check warns about **leftover live migration streams**: those
  named like a live migration's (`xferdb_` plus an id) that no migration still
  running on this server owns, and lists them under *Leftover streams on the
  source*, each with a **Drop** button.
- `xferdb leftovers` lists the streams on the project's source, leftovers
  marked `!`; `xferdb leftovers drop NAME` drops one.
- API: `GET /api/v1/projects/:id/live-streams`,
  `DELETE /api/v1/projects/:id/live-streams/:name`.

Nothing is dropped automatically. Another XferDB server using the same source
owns streams this one doesn't know, which look like leftovers from here: drop
one only if no other server uses it. A stream a running migration owns, or
whose slot is in use, can't be dropped this way.

## WAL and disk space

PostgreSQL keeps the WAL a replication slot hasn't confirmed. XferDB confirms
changes as soon as they are queued on its own disk, so in normal running,
pausing included, the source keeps very little. It keeps more when capture
can't run: the XferDB server is down or can't reach the source.

The migration page warns when:

- capture has been stopped for more than a minute;
- the slot is near, or past, `max_slot_wal_keep_size`;
- the disk holding the queue on the XferDB server is running out of space.

With `max_slot_wal_keep_size` set, PostgreSQL drops a slot that falls too far
behind instead of filling its disk. XferDB then **fails the migration** with
the reason, since the changes in that WAL are gone; start it again. The same
happens if someone drops the slot by hand. Without a limit (the default), a
long capture outage can fill the source's disk, which is why the readiness
check warns about it.

The queue on the XferDB server grows while the target is behind or the
migration is paused, including at a schema change; it is deleted as changes
are applied.

Prometheus metrics (`GET /metrics`): `xferdb_cdc_phase`,
`xferdb_cdc_lag_bytes`, `xferdb_cdc_lag_changes`, `xferdb_cdc_lag_seconds`,
`xferdb_cdc_queue_bytes`, `xferdb_cdc_source_retained_bytes`,
`xferdb_cdc_schema_change_paused`.

### The oplog (MongoDB)

A MongoDB source keeps no changes for XferDB: the oplog is a capped log that
drops its oldest entries as new writes arrive, however far behind a reader
is. XferDB queues changes on its own disk as it reads them, so the oplog only
matters while capture isn't running (the XferDB server down, or unable to
reach the source). If capture is stopped for longer than the oplog's window,
the changes it would resume from are gone, and XferDB **fails the migration**
with the reason; start it again.

The readiness check shows a replica set's window (`rs.printReplicationInfo()`
shows it too) and warns under 24 hours; raise it with `replSetResizeOplog`, or
set a minimum retention with `oplogMinRetentionHours` (4.4+). Through `mongos`
the shards' oplogs can't be read, so readiness only reminds you to check each
shard. On a quiet migration (the migrated collections rarely written while
the rest of the database is busy) XferDB still moves its resume point forward
with the oplog, so it doesn't fall out of the window just by waiting.

## Limitations

- The source must be PostgreSQL, YugabyteDB, MongoDB (replica set or sharded
  cluster) or Oracle 21c/23ai (not RAC, and not 19c yet: #196). MySQL sources
  are planned.
- Every migrated table needs a primary key (a MongoDB collection always has
  one, `_id`).
- Schema changes are not applied; they pause the migration (step 5).
- Standalone sequences are set at cutover on PostgreSQL targets only; identity
  and `AUTO_INCREMENT` columns are set on PostgreSQL, YugabyteDB and MySQL
  targets (SQLite needs nothing: it takes the largest key plus one).
- The cutover doesn't stop the writes for you: you stop the applications.
- The tables are fixed when the migration starts.

## Troubleshooting

| Symptom | What to do |
|---|---|
| Readiness: `wal_level = replica` | Set `wal_level = logical` and restart PostgreSQL. |
| Readiness: replica identity `NOTHING` or `CHANGE` | `ALTER TABLE <table> REPLICA IDENTITY DEFAULT;` for each table listed. `CHANGE` is YugabyteDB's default for new tables. |
| Readiness: *source type matches the server* | The project's source type is `postgres` but the server is YugabyteDB (or the reverse). Change the type in the project's settings. |
| Readiness: no free replication slot | Drop an unused slot (`SELECT slot_name, active FROM pg_replication_slots;`) or raise `max_replication_slots`. |
| `permission denied` creating the publication | Connect as the owner of the tables, or grant the login membership in the owning role; it also needs `CREATE` on the database. |
| *Capture worker: ... (restarting)* | The worker lost the source; it retries with a growing delay. Check the source is reachable from the XferDB server. |
| *Capture worker: ... Failed to get the beforeimage for tablet_id ... (SQLSTATE XX000)* (YugabyteDB) | A transaction changed the same row more than once, and the source can't send its old values. Set the tserver gflag `cdc_enable_intra_transactional_before_image=true` on every tserver (2025.2.4+) and restart them. Set it before starting: a transaction the stream has already reached may still fail, in which case cancel and start a new live migration. |
| Readiness: *the source is a standalone server* (MongoDB) | Run it as a replica set: restart `mongod` with `--replSet rs0` and run `rs.initiate()`. Even one member is enough. |
| Readiness: *can open a change stream* fails (MongoDB) | Grant the login the `read` role (or the `changeStream` and `find` actions) on the migrated database. |
| Migration failed: *the source's oplog no longer has the changes this stream needs* (MongoDB) | Capture was stopped for longer than the oplog's window (see [The oplog](#the-oplog-mongodb)). Raise the window and start a new live migration. |
| *Capture worker: ... dropped / renamed on the source* (MongoDB) | A migrated collection (or the database) was dropped or renamed while streaming. The migration can't go on: start a new one. |
| *Apply worker: ... (restarting)* | A change couldn't be applied to the target, often a schema difference. The error names the table. |
| *writes are still happening on the source* at cutover | Something still writes to a migrated table. Stop it and cut over again. |
| Migration failed: *the source removed WAL ...* | The slot went past `max_slot_wal_keep_size` while capture was down. Start a new live migration. |
| Readiness: *no leftover live migration streams* (!) | A slot or publication no running migration owns: see [Leftover streams](#leftover-streams). Drop it in *Settings → Live Migration* or with `xferdb leftovers drop NAME`, once you know no other XferDB server uses it. |
