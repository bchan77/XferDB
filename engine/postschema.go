package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// schemaJob is one post-schema DDL statement: an index, a foreign key or a
// check constraint.
type schemaJob struct {
	kind  string   // index, foreign_key, check
	table string   // the table it is created on
	name  string   // the index's or constraint's name
	locks []string // the tables it changes: its own, and a foreign key's referenced table
	run   func(ctx context.Context) error
}

// schemaJobs returns tables' post-schema DDL in two stages: every index
// first (a foreign key may need a unique index on the table it references),
// then the foreign keys and check constraints.
func (e *Engine) schemaJobs(tables []adapters.TableSchema) (indexes, constraints []schemaJob) {
	for _, t := range tables {
		table := t.Name
		for _, ix := range t.Indexes {
			ix := ix
			indexes = append(indexes, schemaJob{kind: "index", table: table, name: ix.Name, locks: []string{table},
				run: func(ctx context.Context) error {
					return e.target.CreateIndexes(ctx, table, []adapters.IndexDef{ix})
				}})
		}
		for _, fk := range t.ForeignKeys {
			fk := fk
			locks := []string{table}
			if fk.RefTable != "" && fk.RefTable != table {
				locks = append(locks, fk.RefTable)
			}
			constraints = append(constraints, schemaJob{kind: "foreign_key", table: table, name: fk.Name, locks: locks,
				run: func(ctx context.Context) error {
					return e.target.CreateConstraints(ctx, table, []adapters.ForeignKey{fk}, nil)
				}})
		}
		for _, ck := range t.Checks {
			ck := ck
			constraints = append(constraints, schemaJob{kind: "check", table: table, name: ck.Name, locks: []string{table},
				run: func(ctx context.Context) error {
					return e.target.CreateConstraints(ctx, table, nil, []adapters.CheckConstraint{ck})
				}})
		}
	}
	return indexes, constraints
}

// runSchemaJobs runs tables' post-schema DDL on workers goroutines, never
// two statements on one table at once: YugabyteDB refuses DDL on a table
// another DDL transaction is changing or still verifying
// (TABLE_SCHEMA_CHANGE_IN_PROGRESS), and a foreign key changes the table it
// references too. Each statement is a progress event, a log line and, for a
// migration, a migration_schema_steps row with its timing.
func (e *Engine) runSchemaJobs(ctx context.Context, tables []adapters.TableSchema, workers int) error {
	indexes, constraints := e.schemaJobs(tables)
	total := len(indexes) + len(constraints)
	if total == 0 {
		return nil
	}
	skipped := 0
	if e.migrationID != "" {
		// Steps a restart left running are over; steps an earlier run of
		// this migration (resumed) finished aren't run again.
		_ = e.db.MarkSchemaStepsInterrupted(ctx, e.migrationID, time.Now())
		if steps, err := e.db.ListSchemaSteps(ctx, e.migrationID); err == nil {
			done := map[string]bool{}
			for _, st := range steps {
				if st.Status == "done" {
					done[st.Kind+"\x00"+st.Table+"\x00"+st.Name] = true
				}
			}
			keep := func(jobs []schemaJob) []schemaJob {
				var out []schemaJob
				for _, j := range jobs {
					if done[j.kind+"\x00"+j.table+"\x00"+j.name] {
						skipped++
						continue
					}
					out = append(out, j)
				}
				return out
			}
			indexes, constraints = keep(indexes), keep(constraints)
		}
	}
	if workers < 1 {
		workers = 1
	}
	p := &schemaProgress{total: total, done: skipped, locks: map[string]*sync.Mutex{}}
	for _, stage := range [][]schemaJob{indexes, constraints} {
		if err := e.runSchemaStage(ctx, stage, workers, p); err != nil {
			return err
		}
	}
	return nil
}

// schemaProgress is runSchemaJobs's shared state.
type schemaProgress struct {
	mu      sync.Mutex
	total   int
	done    int
	running map[*schemaJob]time.Time
	locks   map[string]*sync.Mutex
}

func (p *schemaProgress) tableLock(name string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.locks[name]
	if !ok {
		l = &sync.Mutex{}
		p.locks[name] = l
	}
	return l
}

// status is the snapshot's one-line post-schema status: what is running,
// for how long, and how many statements are done.
func (p *schemaProgress) status() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var oldest *schemaJob
	var since time.Time
	for j, at := range p.running {
		if oldest == nil || at.Before(since) {
			oldest, since = j, at
		}
	}
	head := fmt.Sprintf("%d/%d done", p.done, p.total)
	if oldest == nil {
		return head
	}
	more := ""
	if n := len(p.running) - 1; n > 0 {
		more = fmt.Sprintf(" (+%d more running)", n)
	}
	return fmt.Sprintf("%s · %s %s on %s, %s%s", head, strings.ReplaceAll(oldest.kind, "_", " "), oldest.name, oldest.table,
		time.Since(since).Round(time.Second), more)
}

func (e *Engine) runSchemaStage(ctx context.Context, jobs []schemaJob, workers int, p *schemaProgress) error {
	if len(jobs) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	work := make(chan *schemaJob, len(jobs))
	for i := range jobs {
		work <- &jobs[i]
	}
	close(work)

	// The status line's elapsed time moves while a long index builds.
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	stopTick := make(chan struct{})
	go func() {
		for {
			select {
			case <-tick.C:
				e.emit(ProgressEvent{Kind: EventPostSchemaItem, PostSchemaMsg: p.status(), Timestamp: time.Now()})
			case <-stopTick:
				return
			}
		}
	}()
	defer close(stopTick)

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				if ctx.Err() != nil {
					return
				}
				if err := e.runSchemaJob(ctx, j, p); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// ddlRetryFor is how long a statement YugabyteDB refuses for another DDL on
// the same table (one not run by this migration, or one it still verifies
// after commit) is retried.
var ddlRetryFor = 2 * time.Minute

func (e *Engine) runSchemaJob(ctx context.Context, j *schemaJob, p *schemaProgress) error {
	locks := append([]string(nil), j.locks...)
	sort.Strings(locks) // one order, so two jobs never wait on each other
	for _, name := range locks {
		l := p.tableLock(name)
		l.Lock()
		defer l.Unlock()
	}

	started := time.Now()
	var stepID int64
	if e.migrationID != "" {
		id, err := e.db.StartSchemaStep(ctx, state.SchemaStep{MigrationID: e.migrationID, Kind: j.kind,
			Table: j.table, Name: j.name, StartedAt: started})
		if err != nil {
			slog.Warn("engine: record schema step", "migration_id", e.migrationID, "step", j.name, "error", err)
		}
		stepID = id
	}
	p.mu.Lock()
	if p.running == nil {
		p.running = map[*schemaJob]time.Time{}
	}
	p.running[j] = started
	p.mu.Unlock()
	e.emit(ProgressEvent{Kind: EventPostSchemaItem, PostSchemaMsg: p.status(), Timestamp: started})

	attempts := 0
	var err error
	for wait := time.Second; ; wait = min(2*wait, 10*time.Second) {
		attempts++
		if err = j.run(ctx); err == nil || !isDDLConflict(err) || time.Since(started)+wait > ddlRetryFor {
			break
		}
		slog.Info("migration.post_schema_retry", "migration_id", e.migrationID, "kind", j.kind,
			"table", j.table, "name", j.name, "attempt", attempts, "error", err)
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-time.After(wait):
			continue
		}
		break
	}

	finished := time.Now()
	status, errText := "done", ""
	if err != nil {
		status, errText = "failed", err.Error()
	}
	if stepID != 0 {
		if ferr := e.db.FinishSchemaStep(context.WithoutCancel(ctx), stepID, status, attempts, errText, finished); ferr != nil {
			slog.Warn("engine: record schema step", "migration_id", e.migrationID, "step", j.name, "error", ferr)
		}
	}
	p.mu.Lock()
	delete(p.running, j)
	if err == nil {
		p.done++
	}
	p.mu.Unlock()
	slog.Info("migration.post_schema_step", "migration_id", e.migrationID, "kind", j.kind, "table", j.table,
		"name", j.name, "status", status, "seconds", finished.Sub(started).Seconds(), "attempts", attempts)
	e.emit(ProgressEvent{Kind: EventPostSchemaItem, PostSchemaMsg: p.status(), Timestamp: finished})
	if err != nil {
		what := map[string]string{"index": "create index", "foreign_key": "add foreign key", "check": "add check constraint"}[j.kind]
		return fmt.Errorf("%s %s on %s: %w", what, j.name, j.table, err)
	}
	return nil
}

// isDDLConflict reports a YugabyteDB refusal of DDL on a table another DDL
// transaction is changing or verifying, which goes away once it finishes.
func isDDLConflict(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "TABLE_SCHEMA_CHANGE_IN_PROGRESS") ||
		strings.Contains(msg, "undergoing DDL transaction verification") ||
		strings.Contains(msg, "Catalog Version Mismatch")
}
