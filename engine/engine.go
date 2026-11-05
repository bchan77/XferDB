package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/mongo"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Engine orchestrates data transfer for a single migration.
// Multiple engines may run concurrently, one per active migration.
type Engine struct {
	migrationID string
	project     *adapters.Project
	db          *state.MetaDB

	source adapters.SourceAdapter
	target adapters.TargetAdapter

	events chan ProgressEvent
	// subscribed is set once Events() is called. From then on emit blocks
	// rather than drop anything but EventBatch, since that caller must drain
	// the channel until it is closed (#137).
	subscribed atomic.Bool
	pauseCh    chan struct{} // buffered(1): send to signal pause
	resumeCh   chan struct{} // buffered(1): send to signal resume

	// onTableDone, when set, is called with a table's name right after
	// its completed status is saved. Unlike EventTableDone it doesn't depend
	// on an Events() subscriber, so per-table validation can rely on it.
	onTableDone func(table string)

	// live, when set, makes this run a live migration's snapshot.
	live *Live
}

// OnTableDone registers fn to be called, from the engine's worker goroutine,
// each time a table finishes transferring and is recorded as completed. fn
// must not block. Call before Run.
func (e *Engine) OnTableDone(fn func(table string)) {
	e.onTableDone = fn
}

// tableDone notifies the OnTableDone callback, if any.
func (e *Engine) tableDone(table string) {
	if e.onTableDone != nil {
		e.onTableDone(table)
	}
}

// New creates a new Engine. Call Run to start the migration.
func New(migrationID string, project *adapters.Project, db *state.MetaDB) *Engine {
	return &Engine{
		migrationID: migrationID,
		project:     project,
		db:          db,
		events:      make(chan ProgressEvent, 128),
		pauseCh:     make(chan struct{}, 1),
		resumeCh:    make(chan struct{}, 1),
	}
}

// Events returns the channel of progress events. Call it before Run; the
// caller must then drain the channel until it is closed, because the engine
// blocks on it to deliver every event except EventBatch, which is dropped
// when the consumer falls behind (the next batch supersedes it).
func (e *Engine) Events() <-chan ProgressEvent {
	e.subscribed.Store(true)
	return e.events
}

// Pause signals the engine to pause after the current batch completes.
// Safe to call from any goroutine.
func (e *Engine) Pause() {
	select {
	case e.pauseCh <- struct{}{}:
	default:
	}
}

// Resume unpauses a paused engine.
func (e *Engine) Resume() {
	select {
	case e.resumeCh <- struct{}{}:
	default:
	}
}

// Run executes the full migration in three phases:
//  1. Schema   — create all target tables (idempotent; skipped when DataOnly=true)
//  2. Data     — transfer rows table by table (resumes from last checkpoint)
//  3. Post-schema — create indexes and constraints after data load (skipped when DataOnly=true)
//
// Run blocks until the migration completes, fails, or ctx is cancelled.
// The events channel is closed when Run returns.
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.events)

	if e.live != nil {
		if err := CheckLive(e.project.TransferConfig); err != nil {
			return e.fail(ctx, err)
		}
	}
	if err := e.connect(ctx); err != nil {
		return e.fail(ctx, err)
	}
	defer e.disconnect()

	if err := e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusInProgress); err != nil {
		return e.fail(ctx, err)
	}

	allTables, err := e.source.ListTables(ctx)
	if err != nil {
		return e.fail(ctx, fmt.Errorf("list source tables: %w", err))
	}

	// Register every source table in the project registry (INSERT OR IGNORE) so
	// --status always shows the full DB picture regardless of which tables this
	// specific migration touches.
	{
		names := make([]string, len(allTables))
		for i, t := range allTables {
			names[i] = t.Name
		}
		e.db.RegisterProjectTables(ctx, e.project.ID, names) //nolint:errcheck
	}

	tables := allTables
	cfg := e.project.TransferConfig

	// Filter to only the requested tables when --tables is specified.
	if len(cfg.Tables) > 0 {
		filtered := filterTables(tables, cfg.Tables)
		// Fail fast if any requested name has no match in the source.
		if missing := missingRequestedTables(filtered, cfg.Tables); len(missing) > 0 {
			return e.fail(ctx, fmt.Errorf("tables not found in source: %s", strings.Join(missing, ", ")))
		}
		if len(filtered) == 0 {
			return e.fail(ctx, fmt.Errorf("no matching tables found for filter: %v", cfg.Tables))
		}
		tables = filtered
	}

	// Fetch row counts for all tables upfront so the ETA covers the whole migration.
	tableNames := make([]string, len(tables))
	tableCounts := make(map[string]int64, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
		if n, err := e.source.GetRowCount(ctx, t.Name); err == nil {
			tableCounts[t.Name] = n
		}
	}
	e.emit(ProgressEvent{Kind: EventMigrationStart, TableNames: tableNames, TableCounts: tableCounts, TableSchemas: tables, Timestamp: time.Now()})

	dataOnly := cfg.DataOnly
	schemaOnly := cfg.SchemaOnly

	// Relational sources have no automatic schema-plan seeding (only the Mongo
	// analyze path calls SavePlan), but a user can still save column-type
	// overrides directly from the table split view via schema-plan overrides.
	// Load them once so both the pg_dump gating and the CreateTable loop below
	// can apply them.
	var relationalPlan []state.SchemaPlanRow
	if e.project.SourceConfig.Type != "mongodb" {
		relationalPlan, _ = e.db.GetPlanForProject(ctx, e.project.ID)
	}

	// nativeSchema is true when pg_dump handled Phase 1; Phase 3 must then use
	// pg_dump --section=post-data instead of the introspection-based index loop.
	var nativeSchema bool

	// Phase 1: Schema.
	// For postgres→postgres migrations, prefer pg_dump --section=pre-data so that
	// extensions (pgvector, PostGIS, …), custom types, and sequences are transferred
	// exactly as they exist on the source. Falls back to introspection-based
	// CreateTable when pg_dump/psql are not on PATH, or when the user has saved
	// column-type overrides for this project — pg_dump bypasses CreateTable
	// entirely, so overrides would otherwise be silently ignored.
	// Skipped entirely for --truncate (schema already exists on target).
	if !dataOnly && !cfg.Truncate {
		e.emit(ProgressEvent{Kind: EventSchemaPhase, Timestamp: time.Now()})

		if adapters.PostgresFamily(e.project.SourceConfig.Type) && adapters.PostgresFamily(e.project.TargetConfig.Type) && len(relationalPlan) == 0 {
			ok, err := e.pgDumpPreData(ctx, e.project.SourceConfig, e.project.TargetConfig, cfg.RecreateSchema)
			if err != nil {
				return e.fail(ctx, fmt.Errorf("pg_dump pre-data: %w", err))
			}
			nativeSchema = ok
		}

		if !nativeSchema {
			for _, schema := range tables {
				if len(relationalPlan) > 0 {
					state.ApplyPlanOverrides(&schema, relationalPlan)
				}
				if cfg.RecreateSchema {
					if err := e.target.DropTable(ctx, schema.Name); err != nil {
						return e.fail(ctx, fmt.Errorf("drop table %s: %w", schema.Name, err))
					}
				}
				if err := e.target.CreateTable(ctx, &schema); err != nil {
					return e.fail(ctx, fmt.Errorf("create table %s: %w", schema.Name, err))
				}
			}
		}
	}

	if e.live != nil {
		// The stream starts before any row is copied: every change from here
		// on is captured, whatever the copy reads.
		if err := e.live.Setup(ctx, tables); err != nil {
			return e.fail(ctx, fmt.Errorf("live migration setup: %w", err))
		}
	}

	if !schemaOnly {
		// Phase 2: Data — transfer rows for each table, skipping already-complete ones.
		existingProgress, err := e.db.GetTableProgress(ctx, e.migrationID)
		if err != nil {
			return e.fail(ctx, err)
		}
		doneSet := completedTableSet(existingProgress)

		// Truncate requested: clear existing data before loading.
		// With native pg_dump schema (--recreate-schema), tables are dropped and
		// recreated so they're already empty. With introspection-based recreate,
		// DropTable+CreateTable also leaves tables empty. Either way, skip truncate.
		if cfg.Truncate && !cfg.RecreateSchema {
			for _, schema := range tables {
				if !doneSet[schema.Name] {
					if err := e.target.TruncateTable(ctx, schema.Name); err != nil {
						return e.fail(ctx, fmt.Errorf("truncate table %s: %w", schema.Name, err))
					}
				}
			}
		}

		// BulkCopy uses the COPY protocol, which has no ON CONFLICT support and
		// therefore only produces correct results when every target table is
		// guaranteed empty going in. Silently honoring it otherwise turns any
		// pre-existing row into a duplicate-key failure on every single table.
		if cfg.BulkCopy && (cfg.Truncate || cfg.RecreateSchema) {
			if bw, ok := e.target.(adapters.BulkCopyWriter); ok {
				bw.EnableCopy()
			}
		}

		workers := cfg.TableWorkers
		if workers < 1 {
			workers = 1
		}

		// Feed pending tables into a work channel; workers drain it concurrently.
		work := make(chan adapters.TableSchema, len(tables))
		for _, schema := range tables {
			if !doneSet[schema.Name] {
				work <- schema
			}
		}
		close(work)

		var wg sync.WaitGroup
		var mu sync.Mutex
		var tableErrors []error

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for schema := range work {
					if err := e.transferTable(ctx, schema); err != nil {
						// Mark only this table as failed (or cancelled, when this
						// stems from ctx being cancelled by an explicit user
						// Cancel/Delete rather than a real error); other tables
						// keep going. Status writes use a ctx that ignores that
						// same cancellation so they still land.
						now := time.Now()
						writeCtx := context.WithoutCancel(ctx)
						status, kind, regStatus := adapters.StatusFailed, EventTableFailed, "failed"
						if ctx.Err() != nil {
							status, kind, regStatus = adapters.StatusCancelled, EventTableCancelled, "cancelled"
						}
						// A lost status write leaves the table's previous status
						// in migration_tables, which resume and validation go by,
						// so report it with the table's error (#138).
						if werr := e.db.UpsertTableProgress(writeCtx, e.migrationID, adapters.TableProgress{
							TableName:   schema.Name,
							Status:      status,
							CompletedAt: &now,
						}); werr != nil {
							err = fmt.Errorf("%w (recording table as %s also failed: %v)", err, status, werr)
						}
						ev := ProgressEvent{Kind: kind, TableName: schema.Name, Timestamp: now}
						if kind == EventTableFailed {
							ev.Err = err
						}
						e.emit(ev)
						if werr := e.db.UpdateProjectTableStatus(writeCtx, e.project.ID, schema.Name,
							regStatus, e.migrationID, 0, 0); werr != nil {
							slog.Warn("engine: update project table status", "migration_id", e.migrationID,
								"table", schema.Name, "status", regStatus, "error", werr)
						}
						mu.Lock()
						tableErrors = append(tableErrors, fmt.Errorf("%s: %w", schema.Name, err))
						mu.Unlock()
					} else {
						// Persist final counts to the project registry.
						transferred, total, _ := e.db.GetTableFinalProgress(ctx, e.migrationID, schema.Name)
						if werr := e.db.UpdateProjectTableStatus(ctx, e.project.ID, schema.Name,
							"done", e.migrationID, transferred, total); werr != nil {
							slog.Warn("engine: update project table status", "migration_id", e.migrationID,
								"table", schema.Name, "status", "done", "error", werr)
						}
					}
				}
			}()
		}
		wg.Wait()

		if len(tableErrors) > 0 {
			msgs := make([]string, len(tableErrors))
			for i, te := range tableErrors {
				msgs[i] = te.Error()
			}
			return e.fail(ctx, fmt.Errorf("%d table(s) failed: %s", len(tableErrors), strings.Join(msgs, "; ")))
		}
	}

	// Phase 3: Post-schema — indexes and FK constraints after all data is loaded.
	// Building indexes on a populated table is faster than maintaining them during inserts.
	// When native pg_dump schema was used in Phase 1, pg_dump --section=post-data
	// handles this (so it picks up ivfflat/hnsw vector indexes and other extension-specific
	// index types that our introspection doesn't know how to recreate).
	// Skipped for --truncate when native schema wasn't used (indexes already exist).
	if !dataOnly && (!cfg.Truncate || nativeSchema) {
		e.emit(ProgressEvent{Kind: EventPostSchemaPhase, Timestamp: time.Now()})

		postWorkers := cfg.TableWorkers
		if postWorkers < 1 {
			postWorkers = 1
		}

		// A live migration creates only what replay can't break now; the
		// rest follows at catch-up and cutover (DeferredSchema).
		var keep func(ddlBlock) bool
		post := tables
		if e.live != nil {
			keep = func(b ddlBlock) bool { return stageOfBlock(b) == StageSnapshot }
			post = stageSchema(tables, StageSnapshot)
		}
		if nativeSchema {
			if _, err := e.pgDumpPostData(ctx, e.project.SourceConfig, e.project.TargetConfig, keep); err != nil {
				return e.fail(ctx, fmt.Errorf("pg_dump post-data: %w", err))
			}
		} else if err := e.createPostSchema(ctx, post, postWorkers); err != nil {
			return e.fail(ctx, err)
		}
	}

	if e.live != nil {
		// The snapshot is done; the stream carries the migration on.
		e.emit(ProgressEvent{Kind: EventSnapshotDone, Timestamp: time.Now()})
		return nil
	}

	if err := e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusCompleted); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventComplete, Timestamp: time.Now()})
	return nil
}

// createPostSchema creates tables' indexes and constraints, tables in
// parallel across workers.
func (e *Engine) createPostSchema(ctx context.Context, tables []adapters.TableSchema, workers int) error {
	work := make(chan adapters.TableSchema, len(tables))
	for _, schema := range tables {
		if len(schema.Indexes) > 0 || len(schema.ForeignKeys) > 0 || len(schema.Checks) > 0 {
			work <- schema
		}
	}
	close(work)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var postErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for schema := range work {
				if len(schema.Indexes) > 0 {
					e.emit(ProgressEvent{
						Kind:          EventPostSchemaItem,
						PostSchemaMsg: fmt.Sprintf("%-16s %s (%d)", "indexes:", schema.Name, len(schema.Indexes)),
						Timestamp:     time.Now(),
					})
					if err := e.target.CreateIndexes(ctx, schema.Name, schema.Indexes); err != nil {
						mu.Lock()
						if postErr == nil {
							postErr = fmt.Errorf("create indexes for %s: %w", schema.Name, err)
						}
						mu.Unlock()
						return
					}
				}
				if len(schema.ForeignKeys) > 0 || len(schema.Checks) > 0 {
					e.emit(ProgressEvent{
						Kind:          EventPostSchemaItem,
						PostSchemaMsg: fmt.Sprintf("%-16s %s (%d fk, %d check)", "constraints:", schema.Name, len(schema.ForeignKeys), len(schema.Checks)),
						Timestamp:     time.Now(),
					})
					if err := e.target.CreateConstraints(ctx, schema.Name, schema.ForeignKeys, schema.Checks); err != nil {
						mu.Lock()
						if postErr == nil {
							postErr = fmt.Errorf("create constraints for %s: %w", schema.Name, err)
						}
						mu.Unlock()
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	return postErr
}

// connect instantiates and connects source and target adapters using the project config.
func (e *Engine) connect(ctx context.Context) error {
	src, err := registry.NewSource(e.project.SourceConfig.Type)
	if err != nil {
		return fmt.Errorf("source adapter: %w", err)
	}
	if err := src.Connect(ctx, e.project.SourceConfig); err != nil {
		return fmt.Errorf("connect source: %w", err)
	}

	// MongoDB adapters need the schema plan injected after connect.
	if mongoSrc, ok := src.(*mongo.Source); ok {
		// MongoDB uses keyset pagination, not offset-based. Segment workers
		// split tables by offset ranges which doesn't work with MongoDB.
		if e.project.TransferConfig.SegmentWorkers > 1 {
			return fmt.Errorf("--segment-workers is not supported for MongoDB sources (use --table-workers instead)")
		}

		plan, err := e.db.GetPlanForProject(ctx, e.project.ID)
		if err != nil {
			return fmt.Errorf("load schema plan: %w", err)
		}
		if len(plan) == 0 {
			return fmt.Errorf("no schema plan found — run 'xferdb project analyze' first")
		}
		if err := mongoSrc.SetPlan(plan); err != nil {
			return fmt.Errorf("set schema plan: %w", err)
		}
		// Enable accurate counts if requested (slower but reliable).
		if e.project.TransferConfig.AccurateCounts {
			mongoSrc.SetAccurateCounts(true)
		}
	}

	e.source = src

	tgt, err := registry.NewTarget(e.project.TargetConfig.Type)
	if err != nil {
		return fmt.Errorf("target adapter: %w", err)
	}
	if err := tgt.Connect(ctx, e.project.TargetConfig); err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	e.target = tgt
	return nil
}

func (e *Engine) disconnect() {
	if e.source != nil {
		e.source.Close()
	}
	if e.target != nil {
		e.target.Close()
	}
}

// fail marks the migration as failed and returns err — unless ctx has
// already been cancelled (the only way that happens mid-Run is an explicit
// user Cancel/Delete, since this ctx is never wired to any request deadline
// or shutdown signal), in which case it's marked cancelled instead so
// history shows "cancelled" rather than the driver-level error text
// produced by stopping mid-transfer (e.g. a Postgres "canceling statement
// due to user request"). The DB write uses context.WithoutCancel so it
// still lands even though ctx may be the very thing that was cancelled.
func (e *Engine) fail(ctx context.Context, err error) error {
	writeCtx := context.WithoutCancel(ctx)
	if ctx.Err() != nil {
		e.db.SetMigrationCancelled(writeCtx, e.migrationID) //nolint:errcheck
		e.emit(ProgressEvent{Kind: EventCancelled, Timestamp: time.Now()})
		return err
	}
	e.db.SetMigrationError(writeCtx, e.migrationID, err) //nolint:errcheck
	e.emit(ProgressEvent{Kind: EventError, Err: err, Timestamp: time.Now()})
	return err
}

// filterTables returns the subset of schemas whose names match the filter list,
// case-insensitively. A postgres source names a table outside its own schema
// "schema.table", so such a filter matches only that table. "public.orders"
// also matches a bare "orders", for filters written before postgres sources
// named schemas.
func filterTables(tables []adapters.TableSchema, filters []string) []adapters.TableSchema {
	want := make(map[string]bool, len(filters))
	for _, f := range filters {
		want[filterKey(f)] = true
	}
	out := tables[:0:0] // nil-safe empty slice
	for _, t := range tables {
		if want[strings.ToLower(t.Name)] {
			out = append(out, t)
		}
	}
	return out
}

// filterKey normalises a table filter: lower case, "public." dropped.
func filterKey(f string) string {
	f = strings.ToLower(f)
	return strings.TrimPrefix(f, "public.")
}

// missingRequestedTables returns the names from the requested filter list that
// have no corresponding entry in the found (post-filter) tables slice.
func missingRequestedTables(found []adapters.TableSchema, requested []string) []string {
	foundSet := make(map[string]bool, len(found))
	for _, t := range found {
		foundSet[strings.ToLower(t.Name)] = true
	}
	var missing []string
	for _, req := range requested {
		if !foundSet[filterKey(req)] {
			missing = append(missing, req)
		}
	}
	return missing
}

// checkPause blocks if a pause signal is pending, updating state accordingly.
// Returns a non-nil error only if ctx is cancelled while paused.
func (e *Engine) checkPause(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.pauseCh:
		e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusPaused) //nolint:errcheck
		e.emit(ProgressEvent{Kind: EventPaused, Timestamp: time.Now()})
		// Block until resumed or cancelled.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.resumeCh:
			e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusInProgress) //nolint:errcheck
			e.emit(ProgressEvent{Kind: EventResumed, Timestamp: time.Now()})
		}
	default:
	}
	return nil
}

// emit sends a progress event. Only EventBatch may be dropped when the
// consumer is behind: every other kind is delivered once a consumer has
// subscribed, since the collector never sees a lost table-done, failed or
// terminal event again (#137). With no consumer (tests running the engine
// directly), nothing would drain a blocking send, so everything may drop.
func (e *Engine) emit(ev ProgressEvent) {
	ev.MigrationID = e.migrationID
	if ev.Kind != EventBatch && e.subscribed.Load() {
		e.events <- ev
		return
	}
	select {
	case e.events <- ev:
	default:
	}
}

// completedTableSet builds a set of table names that are already fully transferred.
func completedTableSet(progress []adapters.TableProgress) map[string]bool {
	set := make(map[string]bool, len(progress))
	for _, tp := range progress {
		if tp.Status == adapters.StatusCompleted {
			set[tp.TableName] = true
		}
	}
	return set
}
