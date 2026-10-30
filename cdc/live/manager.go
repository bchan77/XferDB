// Package live runs live migrations (#102) for the server: it sets up a
// migration's change stream before the snapshot copy, then supervises its
// capture and apply worker processes, moves the migration through its
// phases, and reports lag.
//
// Phases, recorded in migrations.phase:
//
//	setup      the engine is resolving tables and creating the target schema
//	snapshot   the stream exists and capture runs; the engine copies rows
//	catch_up   apply replays what was written during the copy
//	streaming  the target follows the source; unique indexes exist
//
// Cutover, which ends a live migration, comes in a later phase of #102.
package live

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Phases.
const (
	PhaseSetup     = "setup"
	PhaseSnapshot  = "snapshot"
	PhaseCatchUp   = "catch_up"
	PhaseStreaming = "streaming"
)

// Status is a live migration's stream as the API reports it.
type Status struct {
	Phase string `json:"phase"`
	// Capture and Apply are the workers' own reports; nil while a worker
	// isn't running, with the reason it stopped in the matching error.
	Capture      *cdc.CaptureStatus `json:"capture,omitempty"`
	Apply        *cdc.ApplyStatus   `json:"apply,omitempty"`
	CaptureError string             `json:"capture_error,omitempty"`
	ApplyError   string             `json:"apply_error,omitempty"`
	// LagBytes is how far capture is behind the source's log (PostgreSQL:
	// WAL bytes). LagChanges is how many queued changes apply hasn't
	// applied, and LagSeconds how old the oldest of them is, when known.
	LagBytes   int64    `json:"lag_bytes"`
	LagChanges uint64   `json:"lag_changes"`
	LagSeconds *float64 `json:"lag_seconds,omitempty"`
	// SourcePos is the source's current position, StartPos and CatchUpPos
	// where the stream began and where catch-up ends.
	SourcePos  string `json:"source_pos,omitempty"`
	StartPos   string `json:"start_pos,omitempty"`
	CatchUpPos string `json:"catch_up_pos,omitempty"`
	// QueueBytes is the size of the queue on the server's disk.
	QueueBytes int64     `json:"queue_bytes"`
	Paused     bool      `json:"paused,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Manager runs the server's live migrations.
type Manager struct {
	DB *state.MetaDB
	// Dir holds a directory per live migration (<data-dir>/cdc).
	Dir      string
	Launcher Launcher
	Log      *slog.Logger
	// Interval is how often a migration's workers are checked; 0 means 1s.
	Interval time.Duration

	mu   sync.Mutex
	runs map[string]*run
}

// run is one supervised live migration.
type run struct {
	id      string
	project *adapters.Project // with the run's effective transfer config
	dir     string
	cancel  context.CancelFunc
	done    chan struct{}

	mu     sync.Mutex
	status Status
	// caughtUpSeq is the queue seq catch-up ends at, once capture has
	// confirmed the catch-up position (reached tells it is set).
	caughtUpSeq uint64
	reached     bool
	backoff     map[string]*backoff
	// lastApply is apply's last report, for the lag while it is paused or
	// restarting: what it had applied then is still applied.
	lastApply *cdc.ApplyStatus
}

type backoff struct {
	next  time.Time
	delay time.Duration
}

// WorkerDir is a live migration's worker directory.
func (m *Manager) WorkerDir(id string) string { return filepath.Join(m.Dir, id) }

func (m *Manager) interval() time.Duration {
	if m.Interval > 0 {
		return m.Interval
	}
	return time.Second
}

func (m *Manager) log() *slog.Logger {
	if m.Log != nil {
		return m.Log
	}
	return slog.Default()
}

// Setup creates a live migration's change stream and starts capturing, for
// engine.Live.Setup: before the snapshot copies any row. Run again for an
// interrupted snapshot, it keeps the stream that exists.
func (m *Manager) Setup(ctx context.Context, id string, project *adapters.Project, tables []adapters.TableSchema) error {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	src, err := registry.NewChangeSource(ctx, project.SourceConfig, id)
	if err != nil {
		return err
	}
	defer src.Close()
	// Prepare returns the existing slot's position when there is one.
	start, err := src.Prepare(ctx, names)
	if err != nil {
		return err
	}
	stream, err := m.DB.GetCDCStream(ctx, id)
	if err != nil {
		return err
	}
	if stream == nil {
		slot := id
		if n, ok := src.(interface{ SlotName() string }); ok {
			slot = n.SlotName()
		}
		stream = &state.CDCStream{MigrationID: id, SlotName: slot, Publication: slot, StartPos: src.FormatPosition(start)}
		if err := m.DB.SaveCDCStream(ctx, stream); err != nil {
			return err
		}
	}
	dir := m.WorkerDir(id)
	if err := worker.WriteSpec(dir, &worker.Spec{
		MigrationID: id,
		Source:      project.SourceConfig,
		Target:      project.TargetConfig,
		Tables:      names,
		StartPos:    start,
		Apply:       cdc.ApplyOptions{Workers: project.TransferConfig.TableWorkers},
	}); err != nil {
		return err
	}
	if err := m.DB.SetMigrationPhase(ctx, id, PhaseSnapshot); err != nil {
		return err
	}
	m.Supervise(id, project)
	return nil
}

// SnapshotDone records where catch-up ends, the source's position now, and
// moves the migration on to catch-up; supervision then starts apply.
func (m *Manager) SnapshotDone(ctx context.Context, id string) error {
	r := m.get(id)
	if r == nil {
		return fmt.Errorf("live migration %s isn't running", id)
	}
	src, err := registry.NewChangeSource(ctx, r.project.SourceConfig, id)
	if err != nil {
		return err
	}
	defer src.Close()
	pos, err := src.CurrentPosition(ctx)
	if err != nil {
		return err
	}
	stream, err := m.DB.GetCDCStream(ctx, id)
	if err != nil || stream == nil {
		return fmt.Errorf("live migration %s has no stream record: %v", id, err)
	}
	stream.CatchupPos = src.FormatPosition(pos)
	if err := m.DB.SaveCDCStream(ctx, stream); err != nil {
		return err
	}
	return m.DB.SetMigrationPhase(ctx, id, PhaseCatchUp)
}

// Supervise starts supervising a live migration, if it isn't already.
func (m *Manager) Supervise(id string, project *adapters.Project) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runs == nil {
		m.runs = map[string]*run{}
	}
	if _, ok := m.runs[id]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{id: id, project: project, dir: m.WorkerDir(id), cancel: cancel, done: make(chan struct{}), backoff: map[string]*backoff{}}
	m.runs[id] = r
	go m.supervise(ctx, r)
}

func (m *Manager) get(id string) *run {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runs[id]
}

// Active reports whether a live migration is supervised here.
func (m *Manager) Active(id string) bool { return m.get(id) != nil }

// Status returns a supervised live migration's latest status.
func (m *Manager) Status(id string) (Status, bool) {
	r := m.get(id)
	if r == nil {
		return Status{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, true
}

// Statuses returns every supervised live migration's latest status, by id.
func (m *Manager) Statuses() map[string]Status {
	m.mu.Lock()
	runs := make([]*run, 0, len(m.runs))
	for _, r := range m.runs {
		runs = append(runs, r)
	}
	m.mu.Unlock()
	out := make(map[string]Status, len(runs))
	for _, r := range runs {
		r.mu.Lock()
		out[r.id] = r.status
		r.mu.Unlock()
	}
	return out
}

// Pause stops apply; capture keeps queueing, so the source keeps freeing
// its log while the target stands still.
func (m *Manager) Pause(ctx context.Context, id string) error {
	r := m.get(id)
	if r == nil {
		return fmt.Errorf("live migration %s isn't running", id)
	}
	if err := m.DB.SetMigrationStatus(ctx, id, adapters.StatusPaused); err != nil {
		return err
	}
	return m.Launcher.Stop(ctx, r.dir, worker.RoleApply)
}

// Resume restarts apply after Pause.
func (m *Manager) Resume(ctx context.Context, id string) error {
	if m.get(id) == nil {
		return fmt.Errorf("live migration %s isn't running", id)
	}
	// Supervision starts apply on its next check.
	return m.DB.SetMigrationStatus(ctx, id, adapters.StatusInProgress)
}

// Stop ends a live migration's stream: supervision and both workers stop,
// the source's slot and publication are dropped, the target's applied
// positions cleared and the worker directory removed. The migration's
// status is the caller's to set. project is used when the migration isn't
// supervised (nil: only what is supervised is stopped).
func (m *Manager) Stop(ctx context.Context, id string, project *adapters.Project) error {
	m.mu.Lock()
	r := m.runs[id]
	delete(m.runs, id)
	m.mu.Unlock()
	if r != nil {
		r.cancel()
		<-r.done
		project = r.project
	}
	if project == nil {
		return nil
	}
	dir := m.WorkerDir(id)
	var errs []error
	for _, role := range []string{worker.RoleApply, worker.RoleCapture} {
		if err := m.Launcher.Stop(ctx, dir, role); err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", role, err))
		}
	}
	if src, err := registry.NewChangeSource(ctx, project.SourceConfig, id); err != nil {
		errs = append(errs, err)
	} else {
		if err := src.Teardown(ctx); err != nil {
			errs = append(errs, err)
		}
		src.Close()
	}
	if err := clearTarget(ctx, project.TargetConfig, id); err != nil {
		errs = append(errs, err)
	}
	if err := os.RemoveAll(dir); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func clearTarget(ctx context.Context, cfg adapters.ConnectionConfig, id string) error {
	tgt, err := registry.NewTarget(cfg.Type)
	if err != nil {
		return err
	}
	ca, ok := tgt.(adapters.ChangeApplier)
	if !ok {
		return nil
	}
	if err := tgt.Connect(ctx, cfg); err != nil {
		return fmt.Errorf("connect to target: %w", err)
	}
	defer tgt.Close()
	return ca.ClearChangeState(ctx, id)
}

// Shutdown stops supervising every migration and leaves their workers
// running, as a server shutdown does.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	runs := m.runs
	m.runs = nil
	m.mu.Unlock()
	for _, r := range runs {
		r.cancel()
		<-r.done
	}
}

// supervise checks a live migration's workers every interval until ctx is
// done: it (re)starts whichever should run and doesn't, moves the phase on
// once catch-up is reached, and keeps the status the API reports.
func (m *Manager) supervise(ctx context.Context, r *run) {
	defer close(r.done)
	log := m.log().With("migration_id", r.id)
	var src cdc.ChangeSource
	defer func() {
		if src != nil {
			src.Close()
		}
	}()
	tick := time.NewTicker(m.interval())
	defer tick.Stop()
	for {
		if src == nil {
			s, err := registry.NewChangeSource(ctx, r.project.SourceConfig, r.id)
			if err != nil {
				log.Warn("live.source", "error", err)
			} else {
				src = s
			}
		}
		err := m.check(ctx, r, src, log)
		if errors.Is(err, errEnded) {
			m.mu.Lock()
			if m.runs[r.id] == r {
				delete(m.runs, r.id)
			}
			m.mu.Unlock()
			return
		}
		if err != nil && ctx.Err() == nil {
			log.Warn("live.check", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// errEnded stops supervising a migration that has ended or is gone.
var errEnded = errors.New("live migration ended")

func (m *Manager) check(ctx context.Context, r *run, src cdc.ChangeSource, log *slog.Logger) error {
	mig, err := m.DB.GetMigration(ctx, r.id)
	if errors.Is(err, sql.ErrNoRows) {
		return errEnded // deleted
	}
	if err != nil {
		return err
	}
	switch mig.Status {
	case adapters.StatusCompleted, adapters.StatusFailed, adapters.StatusCancelled:
		return errEnded
	}
	stream, err := m.DB.GetCDCStream(ctx, r.id)
	if err != nil {
		return err
	}
	paused := mig.Status == adapters.StatusPaused
	applying := mig.Phase == PhaseCatchUp || mig.Phase == PhaseStreaming
	st := Status{Phase: mig.Phase, Paused: paused, UpdatedAt: time.Now()}
	if stream != nil {
		st.StartPos, st.CatchUpPos = stream.StartPos, stream.CatchupPos
	}

	capSt, capErr := m.ensure(ctx, r, worker.RoleCapture, true, log)
	if capSt != nil {
		st.Capture = capSt.Capture
	} else if capErr != "" {
		st.CaptureError = capErr
	}
	appSt, appErr := m.ensure(ctx, r, worker.RoleApply, applying && !paused, log)
	if appSt != nil {
		st.Apply = appSt.Apply
	} else if appErr != "" {
		st.ApplyError = appErr
	}

	if src != nil && st.Capture != nil {
		if cur, err := src.CurrentPosition(ctx); err == nil {
			st.SourcePos = src.FormatPosition(cur)
			if len(st.Capture.Confirmed) > 0 {
				st.LagBytes = src.Behind(st.Capture.Confirmed, cur)
			}
		}
	}
	r.mu.Lock()
	if st.Apply != nil {
		r.lastApply = st.Apply
	}
	applied := r.lastApply
	r.mu.Unlock()
	if st.Capture != nil && applied != nil {
		if st.Capture.QueuedSeq > applied.AppliedSeq {
			st.LagChanges = st.Capture.QueuedSeq - applied.AppliedSeq
			if !applied.AppliedCommit.IsZero() {
				s := time.Since(applied.AppliedCommit).Seconds()
				st.LagSeconds = &s
			}
		} else {
			zero := 0.0
			st.LagSeconds = &zero
		}
	}
	if qs, err := cdc.QueueStats(worker.QueueDir(r.dir)); err == nil {
		st.QueueBytes = qs.Bytes
	}

	r.mu.Lock()
	r.status = st
	r.mu.Unlock()

	if mig.Phase == PhaseCatchUp && stream != nil && src != nil {
		return m.catchUp(ctx, r, src, stream, st, log)
	}
	return nil
}

// catchUp ends catch-up once every change up to the catch-up position is
// applied: capture must first confirm that position (an idle source
// confirms it without any change), which fixes the queue seq apply must
// reach. Then the unique indexes go in and the migration is streaming.
func (m *Manager) catchUp(ctx context.Context, r *run, src cdc.ChangeSource, stream *state.CDCStream, st Status, log *slog.Logger) error {
	if stream.CatchupPos == "" || st.Capture == nil {
		return nil
	}
	target, err := src.ParsePosition(stream.CatchupPos)
	if err != nil {
		return err
	}
	r.mu.Lock()
	if !r.reached && len(st.Capture.Confirmed) > 0 && src.Behind(st.Capture.Confirmed, target) == 0 {
		r.caughtUpSeq, r.reached = st.Capture.QueuedSeq, true
	}
	reached, seq := r.reached, r.caughtUpSeq
	r.mu.Unlock()
	if !reached || st.Apply == nil || st.Apply.AppliedSeq < seq {
		return nil
	}
	log.Info("live.caught_up", "catch_up_pos", stream.CatchupPos, "seq", seq)
	cfg := r.project
	tables, err := m.tables(r)
	if err != nil {
		return err
	}
	if err := engine.DeferredSchema(ctx, cfg, m.DB, tables, engine.StageCatchUp); err != nil {
		return fmt.Errorf("unique indexes after catch-up: %w", err)
	}
	if err := m.DB.SetMigrationPhase(ctx, r.id, PhaseStreaming); err != nil {
		return err
	}
	log.Info("live.streaming")
	return nil
}

// tables returns the tables a live migration streams, from its spec.
func (m *Manager) tables(r *run) ([]string, error) {
	spec, err := worker.ReadSpec(r.dir)
	if err != nil {
		return nil, err
	}
	return spec.Tables, nil
}

// ensure makes the worker of role run when want is set, or stop when not,
// and returns its status while it runs, or why it stopped. A worker that
// keeps failing is restarted with a growing delay, up to a minute.
func (m *Manager) ensure(ctx context.Context, r *run, role string, want bool, log *slog.Logger) (*worker.Status, string) {
	st, err := worker.ReadStatus(ctx, r.dir, role)
	if err == nil {
		if !want {
			if err := m.Launcher.Stop(ctx, r.dir, role); err != nil {
				log.Warn("live.stop_worker", "role", role, "error", err)
			}
			return nil, ""
		}
		if b := r.backoff[role]; b != nil && time.Now().After(b.next.Add(time.Minute)) {
			delete(r.backoff, role) // up for a while: reset the delay
		}
		return st, ""
	}
	reason := ""
	if errors.Is(err, worker.ErrNotRunning) {
		if msg := err.Error(); msg != worker.ErrNotRunning.Error() {
			reason = msg
		}
	} else {
		reason = err.Error()
	}
	if !want {
		return nil, ""
	}
	b := r.backoff[role]
	if b != nil && time.Now().Before(b.next) {
		return nil, reason // started recently: give it time, or wait out the delay
	}
	if b == nil {
		b = &backoff{delay: 2 * time.Second}
		r.backoff[role] = b
	} else {
		b.delay = min(b.delay*2, time.Minute)
	}
	b.next = time.Now().Add(b.delay)
	if reason != "" {
		log.Warn("live.restart_worker", "role", role, "reason", reason)
	}
	if err := m.Launcher.Start(r.dir, role); err != nil {
		return nil, err.Error()
	}
	return nil, reason
}

// ProjectFor rebuilds the project a live migration runs with: the stored
// project with the run's own transfer config.
func ProjectFor(ctx context.Context, db *state.MetaDB, mig *adapters.Migration) (*adapters.Project, error) {
	p, err := db.GetProject(ctx, mig.ProjectID)
	if err != nil {
		return nil, err
	}
	cfg, ok, err := db.GetRunConfig(ctx, mig.ID)
	if err != nil {
		return nil, err
	}
	if ok {
		p.TransferConfig = cfg
	}
	return p, nil
}
