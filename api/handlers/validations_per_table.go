package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/validate"
	"github.com/google/uuid"
)

// perTableRun validates a running migration's tables one by one as each
// reaches completed (validation mode per_table), with as many workers as the
// migration has table workers. The validation run is created when the first
// table finishes, so a migration that completes no table leaves none behind,
// as in mode after. When the migration ends, however it ends, the tables
// that completed are still validated and every other table is recorded as
// skipped.
type perTableRun struct {
	h           *ValidationsHandler
	migrationID string
	project     *adapters.Project
	level       string
	sample      int
	workers     int
	log         *slog.Logger

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []string
	seen   map[string]bool
	closed bool

	once     sync.Once
	v        *state.Validation // nil until the first table completes
	plan     []state.SchemaPlanRow
	startErr error // the run could not be created; the migration falls back to mode after
	ctx      context.Context
	cancel   context.CancelFunc

	wg       sync.WaitGroup
	failed   int
	checked  int
	openErr  error // a worker could not connect
	resultMu sync.Mutex
}

// beginPerTable prepares per-table validation for a migration about to run.
// Hand its tableDone to the engine's OnTableDone and call finish once the
// engine's Run returns.
func (h *ValidationsHandler) beginPerTable(migrationID string, p *adapters.Project, level string, sample, workers int, log *slog.Logger) *perTableRun {
	if level == "" {
		level = validate.DefaultLevel
	}
	if level != validate.LevelL3 || sample == 100 {
		sample = 0
	}
	if workers < 1 {
		workers = 1
	}
	r := &perTableRun{
		h: h, migrationID: migrationID, project: p, level: level, sample: sample, workers: workers,
		log: log, seen: map[string]bool{},
	}
	r.cond = sync.NewCond(&r.mu)
	r.ctx, r.cancel = context.WithCancel(context.Background())
	for i := 0; i < workers; i++ {
		r.wg.Add(1)
		go r.work()
	}
	return r
}

// tableDone queues a table the engine just recorded as completed. It never
// blocks, as the engine requires.
func (r *perTableRun) tableDone(table string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.seen[table] {
		return
	}
	r.seen[table] = true
	r.queue = append(r.queue, table)
	r.cond.Signal()
}

// next blocks until a table is queued, or returns "" once the queue is closed
// and empty.
func (r *perTableRun) next() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.queue) == 0 && !r.closed {
		r.cond.Wait()
	}
	if len(r.queue) == 0 {
		return ""
	}
	t := r.queue[0]
	r.queue = r.queue[1:]
	return t
}

// ensure creates the validation run on first use. It fails when the project
// already has a validation running (for example one started by hand).
func (r *perTableRun) ensure() error {
	r.once.Do(func() {
		ctx := context.Background()
		plan, err := r.h.migrationPlan(ctx, r.migrationID, r.project)
		if err != nil {
			r.startErr = err
			return
		}
		r.plan = plan

		h := r.h
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, busy := h.byProject[r.project.ID]; busy {
			r.startErr = errValidationBusy
			return
		}
		now := time.Now()
		v := &state.Validation{
			ID:          uuid.New().String(),
			MigrationID: r.migrationID,
			Level:       r.level,
			Status:      adapters.StatusInProgress,
			Options:     state.ValidationOptions{Sample: r.sample, Mode: validate.ModePerTable},
			CreatedAt:   now,
		}
		if err := h.DB.CreateValidation(ctx, v); err != nil {
			r.startErr = err
			return
		}
		h.DB.SetValidationStatus(ctx, v.ID, adapters.StatusInProgress, "") //nolint:errcheck
		h.cancels[v.ID] = r.cancel
		h.byProject[r.project.ID] = v.ID
		r.v = v
		r.log = r.log.With("validation_id", v.ID, "level", r.level)
		r.log.Info("validation.started", "mode", validate.ModePerTable, "workers", r.workers)
	})
	return r.startErr
}

// work validates queued tables until the queue closes, holding one
// connection pair to the source and target for its whole life.
func (r *perTableRun) work() {
	defer r.wg.Done()
	var s *validate.Session
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	writeCtx := context.Background()
	for {
		table := r.next()
		if table == "" {
			return
		}
		if r.ensure() != nil {
			continue // finish() falls back to mode after
		}
		skip := func(reason string) {
			r.h.DB.UpsertValidationTable(writeCtx, state.ValidationTable{ //nolint:errcheck
				ValidationID: r.v.ID, TableName: table, Result: state.ValidationSkipped, Detail: reason,
			})
		}
		if r.ctx.Err() != nil {
			skip("validation was cancelled")
			continue
		}
		if s == nil {
			var err error
			s, err = validate.Open(r.ctx, validate.Request{
				Project: r.project, Plan: r.plan, Level: r.level,
				BatchSize: r.project.TransferConfig.BatchSize, Sample: r.sample,
				OnProgress: r.h.onProgress(r.v.ID),
			})
			if err != nil {
				r.resultMu.Lock()
				if r.openErr == nil {
					r.openErr = err
				}
				r.resultMu.Unlock()
				s = nil
				skip("validation could not connect: " + err.Error())
				continue
			}
		}
		res, err := s.Table(r.ctx, table)
		if err != nil {
			skip("validation was cancelled")
			continue
		}
		failed := r.h.saveTable(writeCtx, r.v.ID, res, r.log)
		r.resultMu.Lock()
		r.checked++
		if failed {
			r.failed++
		}
		r.resultMu.Unlock()
	}
}

// finish runs once the migration has ended. It queues any completed table
// the run has not seen, waits for the workers, records every other table of
// the migration as skipped, and closes the run.
func (r *perTableRun) finish() {
	ctx := context.Background()
	progress, err := r.h.DB.GetTableProgress(ctx, r.migrationID)
	if err != nil {
		r.log.Error("validation.per_table_progress_failed", "error", err)
	}
	for _, tp := range progress {
		if tp.Status == adapters.StatusCompleted {
			r.tableDone(tp.TableName)
		}
	}
	r.mu.Lock()
	r.closed = true
	r.cond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()

	if r.startErr != nil {
		// Validate the way mode after would, if the project allows it.
		r.log.Warn("validation.per_table_unavailable", "error", r.startErr)
		r.cancel()
		r.h.startAfterMigration(r.migrationID, r.project, validate.ModeAfter, r.level, r.sample, r.log)
		return
	}
	if r.v == nil {
		r.cancel()
		return // no table completed; nothing was validated
	}
	defer func() {
		r.cancel()
		r.h.mu.Lock()
		delete(r.h.cancels, r.v.ID)
		delete(r.h.byProject, r.project.ID)
		delete(r.h.progress, r.v.ID)
		r.h.mu.Unlock()
	}()

	for _, tp := range progress {
		if tp.Status != adapters.StatusCompleted {
			r.h.DB.UpsertValidationTable(ctx, state.ValidationTable{ //nolint:errcheck
				ValidationID: r.v.ID, TableName: tp.TableName, Result: state.ValidationSkipped,
				Detail: fmt.Sprintf("table did not complete in this migration (status %s)", tp.Status),
			})
		}
	}
	switch {
	case r.ctx.Err() != nil:
		r.h.DB.SetValidationStatus(ctx, r.v.ID, adapters.StatusCancelled, "cancelled by user") //nolint:errcheck
		r.log.Info("validation.cancelled")
	case r.openErr != nil:
		r.h.DB.SetValidationStatus(ctx, r.v.ID, adapters.StatusFailed, r.openErr.Error()) //nolint:errcheck
		r.log.Error("validation.failed", "error", r.openErr)
	default:
		r.h.DB.SetValidationStatus(ctx, r.v.ID, adapters.StatusCompleted, "") //nolint:errcheck
		r.log.Info("validation.completed", "tables", r.checked, "failed_tables", r.failed)
	}
}
