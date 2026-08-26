package engine

import (
	"context"
	"fmt"
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

// Run executes the full migration. It blocks until the migration completes,
// fails, or ctx is cancelled. The events channel is closed when Run returns.
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

	// Load existing table progress to skip completed tables on resume.
	existingProgress, err := e.db.GetTableProgress(ctx, e.migrationID)
	if err != nil {
		return e.fail(ctx, err)
	}
	doneSet := completedTableSet(existingProgress)

	for _, schema := range tables {
		if doneSet[schema.Name] {
			continue // already completed in a previous run
		}
		if err := e.transferTable(ctx, schema); err != nil {
			return e.fail(ctx, err)
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
