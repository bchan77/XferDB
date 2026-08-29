package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
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

	events   chan ProgressEvent
	pauseCh  chan struct{} // buffered(1): send to signal pause
	resumeCh chan struct{} // buffered(1): send to signal resume
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

// Events returns the channel of progress events. The caller should drain
// this channel promptly; slow consumers cause events to be dropped (not blocked).
func (e *Engine) Events() <-chan ProgressEvent {
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

	if err := e.connect(ctx); err != nil {
		return e.fail(ctx, err)
	}
	defer e.disconnect()

	if err := e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusInProgress); err != nil {
		return e.fail(ctx, err)
	}

	tables, err := e.source.ListTables(ctx)
	if err != nil {
		return e.fail(ctx, fmt.Errorf("list source tables: %w", err))
	}

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
	e.emit(ProgressEvent{Kind: EventMigrationStart, TableNames: tableNames, TableCounts: tableCounts, Timestamp: time.Now()})

	dataOnly := cfg.DataOnly
	schemaOnly := cfg.SchemaOnly

	// Phase 1: Schema — create all target tables before any data is transferred.
	if !dataOnly {
		e.emit(ProgressEvent{Kind: EventSchemaPhase, Timestamp: time.Now()})
		for _, schema := range tables {
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

	if !schemaOnly {
		// Phase 2: Data — transfer rows for each table, skipping already-complete ones.
		existingProgress, err := e.db.GetTableProgress(ctx, e.migrationID)
		if err != nil {
			return e.fail(ctx, err)
		}
		doneSet := completedTableSet(existingProgress)

		// Truncate requested: clear existing data before loading.
		// Skip when RecreateSchema is set — the table was just recreated, already empty.
		if cfg.Truncate && !cfg.RecreateSchema {
			for _, schema := range tables {
				if !doneSet[schema.Name] {
					if err := e.target.TruncateTable(ctx, schema.Name); err != nil {
						return e.fail(ctx, fmt.Errorf("truncate table %s: %w", schema.Name, err))
					}
				}
			}
		}

		if cfg.BulkCopy {
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

		workerCtx, cancelWorkers := context.WithCancel(ctx)
		defer cancelWorkers()

		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for schema := range work {
					if err := e.transferTable(workerCtx, schema); err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
							cancelWorkers() // stop other workers on first error
						}
						mu.Unlock()
						return
					}
				}
			}()
		}
		wg.Wait()

		if firstErr != nil {
			return e.fail(ctx, firstErr)
		}
	}

	// Phase 3: Post-schema — indexes and FK constraints after all data is loaded.
	// Creating indexes after bulk insert is faster; FKs must come after all tables exist.
	if !dataOnly {
		e.emit(ProgressEvent{Kind: EventPostSchemaPhase, Timestamp: time.Now()})
		for _, schema := range tables {
			if len(schema.Indexes) > 0 {
				if err := e.target.CreateIndexes(ctx, schema.Name, schema.Indexes); err != nil {
					return e.fail(ctx, fmt.Errorf("create indexes for %s: %w", schema.Name, err))
				}
			}
			if len(schema.ForeignKeys) > 0 || len(schema.Checks) > 0 {
				if err := e.target.CreateConstraints(ctx, schema.Name, schema.ForeignKeys, schema.Checks); err != nil {
					return e.fail(ctx, fmt.Errorf("create constraints for %s: %w", schema.Name, err))
				}
			}
		}
	}

	if err := e.db.SetMigrationStatus(ctx, e.migrationID, adapters.StatusCompleted); err != nil {
		return err
	}
	e.emit(ProgressEvent{Kind: EventComplete, Timestamp: time.Now()})
	return nil
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

// fail marks the migration as failed, emits an error event, and returns err.
func (e *Engine) fail(ctx context.Context, err error) error {
	e.db.SetMigrationError(ctx, e.migrationID, err) //nolint:errcheck
	e.emit(ProgressEvent{Kind: EventError, Err: err, Timestamp: time.Now()})
	return err
}

// filterTables returns the subset of schemas whose names match the filter list.
// Entries in filters may be bare table names ("orders") or schema-qualified
// ("public.orders"); the schema prefix is stripped before comparison.
func filterTables(tables []adapters.TableSchema, filters []string) []adapters.TableSchema {
	want := make(map[string]bool, len(filters))
	for _, f := range filters {
		// Normalise: strip schema prefix so "public.orders" → "orders".
		bare := f
		if idx := strings.LastIndex(f, "."); idx >= 0 {
			bare = f[idx+1:]
		}
		want[strings.ToLower(bare)] = true
		want[strings.ToLower(f)] = true // keep full name too for forward compat
	}
	out := tables[:0:0] // nil-safe empty slice
	for _, t := range tables {
		if want[strings.ToLower(t.Name)] {
			out = append(out, t)
		}
	}
	return out
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
		bare := req
		if idx := strings.LastIndex(req, "."); idx >= 0 {
			bare = req[idx+1:]
		}
		if !foundSet[strings.ToLower(bare)] && !foundSet[strings.ToLower(req)] {
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

// emit sends a progress event, dropping it if the consumer is behind.
func (e *Engine) emit(ev ProgressEvent) {
	ev.MigrationID = e.migrationID
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
