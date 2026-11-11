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
3. Set the target's sequences to the source's values (PostgreSQL targets), so
   new rows don't collide with copied ones.
4. Add the foreign keys, checks and triggers held back until now.
5. Stop the stream and drop the replication slot and publication.

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
the source (and, on YugabyteDB, its heartbeat row). Rows already on the target
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

## Limitations

- The source must be PostgreSQL or YugabyteDB. MySQL and MongoDB sources are planned.
- Every migrated table needs a primary key.
- Schema changes are not applied; they pause the migration (step 5).
- Sequences are set at cutover on PostgreSQL targets only.
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
| *Apply worker: ... (restarting)* | A change couldn't be applied to the target, often a schema difference. The error names the table. |
| *writes are still happening on the source* at cutover | Something still writes to a migrated table. Stop it and cut over again. |
| Migration failed: *the source removed WAL ...* | The slot went past `max_slot_wal_keep_size` while capture was down. Start a new live migration. |
| Readiness: *no leftover live migration streams* (!) | A slot or publication no running migration owns: see [Leftover streams](#leftover-streams). Drop it in *Settings → Live Migration* or with `xferdb leftovers drop NAME`, once you know no other XferDB server uses it. |
