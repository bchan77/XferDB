package live

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

// Phases a cutover moves a live migration through.
const (
	PhaseCutover   = "cutover"
	PhaseCompleted = "completed"
)

// Cutover steps, in order.
const (
	StepDrain       = "drain"
	StepQuiet       = "quiet_check"
	StepSequences   = "sequences"
	StepConstraints = "constraints"
	StepFinish      = "finish"
)

var cutoverSteps = []struct{ name, label string }{
	{StepDrain, "Apply every change up to the cutover point"},
	{StepQuiet, "Check the source has stopped writing"},
	{StepSequences, "Set the target's sequences to the source's values"},
	{StepConstraints, "Add foreign keys, checks and triggers"},
	{StepFinish, "Stop the stream and remove it from the source"},
}

// Errors Cutover refuses with.
var (
	ErrNotRunning     = errors.New("live migration isn't running")
	ErrNotStreaming   = errors.New("a live migration can be cut over only once it is streaming")
	ErrPaused         = errors.New("the live migration is paused: resume it before cutting over")
	ErrCutoverRunning = errors.New("a cutover is already running")
)

// CutoverOptions are the user's choices for a cutover.
type CutoverOptions struct {
	// IgnoreWrites goes on although changes to the migrated tables kept
	// arriving after the cutover point: they are drained once more, and
	// anything written after that is not migrated.
	IgnoreWrites bool `json:"ignore_writes"`
}

// CutoverStep is one step of a cutover as the API reports it.
type CutoverStep struct {
	Name   string `json:"name"`
	Label  string `json:"label"`
	State  string `json:"state"` // pending, running, done, skipped, failed
	Detail string `json:"detail,omitempty"`
}

// CutoverStatus is a cutover's progress.
type CutoverStatus struct {
	Running bool          `json:"running"`
	Steps   []CutoverStep `json:"steps"`
	Error   string        `json:"error,omitempty"`
	// WritesAfter is how many changes to the migrated tables arrived after
	// the cutover point, when the quiet check stopped the cutover.
	WritesAfter uint64     `json:"writes_after,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

func newCutoverStatus() *CutoverStatus {
	st := &CutoverStatus{Running: true, StartedAt: time.Now().UTC()}
	for _, s := range cutoverSteps {
		st.Steps = append(st.Steps, CutoverStep{Name: s.name, Label: s.label, State: "pending"})
	}
	return st
}

func (m *Manager) quietWait() time.Duration {
	if m.QuietWait > 0 {
		return m.QuietWait
	}
	return 3 * time.Second
}

// Cutover ends a live migration: it drains the stream up to the source's
// position now, checks the source has stopped writing, sets the target's
// sequences, adds the constraints held back until now, then stops the
// stream, drops the slot and marks the migration completed (OnCompleted
// then validates it). It returns once the cutover has started; progress is
// in Status. A cutover that stops short (writes still arriving, a
// constraint that won't go in) leaves the migration streaming, with the
// reason in Status, to be cut over again.
func (m *Manager) Cutover(ctx context.Context, id string, opts CutoverOptions) error {
	r := m.get(id)
	if r == nil {
		return ErrNotRunning
	}
	mig, err := m.DB.GetMigration(ctx, id)
	if err != nil {
		return err
	}
	if mig.Status == adapters.StatusPaused {
		return ErrPaused
	}
	r.mu.Lock()
	if r.cutover != nil && r.cutover.Running {
		r.mu.Unlock()
		return ErrCutoverRunning
	}
	// A cutover interrupted by a restart is in phase cutover: start over.
	if mig.Phase != PhaseStreaming && mig.Phase != PhaseCutover {
		r.mu.Unlock()
		return ErrNotStreaming
	}
	r.cutover = newCutoverStatus()
	cctx, cancel := context.WithCancel(context.Background())
	r.cutoverCancel = cancel
	r.mu.Unlock()
	if err := m.DB.SetMigrationPhase(ctx, id, PhaseCutover); err != nil {
		cancel()
		return err
	}
	go m.runCutover(cctx, r, opts)
	return nil
}

// step sets a cutover step's state and detail.
func (r *run) step(name, state, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cutover == nil {
		return
	}
	for i := range r.cutover.Steps {
		if r.cutover.Steps[i].Name == name {
			r.cutover.Steps[i].State = state
			if detail != "" || state == "running" {
				r.cutover.Steps[i].Detail = detail
			}
		}
	}
}

func (m *Manager) runCutover(ctx context.Context, r *run, opts CutoverOptions) {
	log := m.log().With("migration_id", r.id)
	log.Info("live.cutover_started", "ignore_writes", opts.IgnoreWrites)
	fail := func(step string, err error) {
		r.step(step, "failed", err.Error())
		now := time.Now().UTC()
		r.mu.Lock()
		r.cutover.Running, r.cutover.Error, r.cutover.FinishedAt = false, err.Error(), &now
		r.mu.Unlock()
		if ctx.Err() == nil {
			// The stream is untouched: it goes on, for another try.
			if err := m.DB.SetMigrationPhase(context.Background(), r.id, PhaseStreaming); err != nil {
				log.Warn("live.cutover_phase", "error", err)
			}
		}
		log.Warn("live.cutover_failed", "step", step, "error", err)
	}

	src, err := registry.NewChangeSource(ctx, r.project.SourceConfig, r.id)
	if err != nil {
		fail(StepDrain, err)
		return
	}
	defer src.Close()

	// 1. Drain to the source's position now.
	r.step(StepDrain, "running", "")
	seq, pos, err := m.drain(ctx, r, src)
	if err != nil {
		fail(StepDrain, err)
		return
	}
	if stream, err := m.DB.GetCDCStream(ctx, r.id); err == nil && stream != nil {
		stream.CutoverPos = src.FormatPosition(pos)
		if err := m.DB.SaveCDCStream(ctx, stream); err != nil {
			log.Warn("live.cutover_pos", "error", err)
		}
	}
	r.step(StepDrain, "done", fmt.Sprintf("applied every change up to %s", src.FormatPosition(pos)))

	// 2. Nothing more may arrive for the migrated tables.
	r.step(StepQuiet, "running", fmt.Sprintf("waiting %s for writes", m.quietWait()))
	select {
	case <-ctx.Done():
		fail(StepQuiet, ctx.Err())
		return
	case <-time.After(m.quietWait()):
	}
	// A write made while waiting may not have reached capture yet (a
	// sharded MongoDB cluster's mongos only passes a change on once every
	// shard is past it): count only once capture has confirmed the source's
	// position after the wait.
	r.step(StepQuiet, "running", "making sure every write made while waiting has been captured")
	if err := m.awaitCaptured(ctx, r, src); err != nil {
		fail(StepQuiet, err)
		return
	}
	if after := m.queuedSince(r, seq); after > 0 {
		if !opts.IgnoreWrites {
			r.mu.Lock()
			r.cutover.WritesAfter = after
			r.mu.Unlock()
			fail(StepQuiet, fmt.Errorf("writes are still happening on the source: %d change(s) to the migrated tables arrived after the cutover point. Stop writing to the source and cut over again, or cut over anyway and leave later writes behind", after))
			return
		}
		seq2, pos2, err := m.drain(ctx, r, src)
		if err != nil {
			fail(StepQuiet, err)
			return
		}
		_ = seq2
		r.step(StepQuiet, "done", fmt.Sprintf("%d change(s) arrived after the cutover point; drained again up to %s as asked, and anything written later is not migrated", after, src.FormatPosition(pos2)))
	} else {
		r.step(StepQuiet, "done", "no changes to the migrated tables since the cutover point")
	}

	// 3. Sequences: logical replication doesn't carry them.
	r.step(StepSequences, "running", "")
	n, err := m.syncSequences(ctx, r, src)
	switch {
	case err != nil:
		fail(StepSequences, err)
		return
	case n < 0:
		r.step(StepSequences, "skipped", fmt.Sprintf("a %s target has no sequences to set", r.project.TargetConfig.Type))
	default:
		r.step(StepSequences, "done", fmt.Sprintf("%d sequence(s) set", n))
	}

	// 4. What replay could have broken, now that it is done.
	r.step(StepConstraints, "running", "")
	tables, err := m.tables(r)
	if err == nil {
		err = engine.DeferredSchema(ctx, r.project, m.DB, tables, engine.StageCutover)
	}
	if err != nil {
		fail(StepConstraints, err)
		return
	}
	r.step(StepConstraints, "done", "")

	// 5. The stream ends: workers stop, the slot and publication go.
	r.step(StepFinish, "running", "")
	project := r.project
	// Supervision stops first, or it would restart the workers teardown
	// stops.
	m.forget(r)
	if err := m.teardown(ctx, r.id, project); err != nil {
		// The data is all there; only the cleanup failed. Supervise again,
		// so the cutover can be retried and the cleanup tried once more.
		m.Supervise(r.id, project)
		if nr := m.get(r.id); nr != nil {
			nr.mu.Lock()
			nr.cutover = r.cutover
			nr.mu.Unlock()
		}
		fail(StepFinish, err)
		return
	}
	bg := context.Background()
	if err := m.DB.SetMigrationStatus(bg, r.id, adapters.StatusCompleted); err != nil {
		log.Error("live.complete", "error", err)
	}
	if err := m.DB.SetMigrationPhase(bg, r.id, PhaseCompleted); err != nil {
		log.Error("live.complete", "error", err)
	}
	r.step(StepFinish, "done", "")
	now := time.Now().UTC()
	r.mu.Lock()
	r.cutover.Running, r.cutover.FinishedAt = false, &now
	r.mu.Unlock()
	m.remember(r)
	log.Info("live.cutover_done")
	if m.OnCompleted != nil {
		m.OnCompleted(r.id, project)
	}
}

// drain records the source's position now and waits until every change
// up to it is applied: capture must confirm the position, which fixes the
// queue seq apply must reach. It returns that seq and the position.
func (m *Manager) drain(ctx context.Context, r *run, src cdc.ChangeSource) (uint64, cdc.Position, error) {
	pos, err := src.CurrentPosition(ctx)
	if err != nil {
		return 0, nil, err
	}
	var seq uint64
	reached := false
	tick := time.NewTicker(m.interval() / 2)
	defer tick.Stop()
	for {
		r.mu.Lock()
		st := r.status
		r.mu.Unlock()
		if !reached && st.Capture != nil && len(st.Capture.Confirmed) > 0 && src.Behind(st.Capture.Confirmed, pos) == 0 {
			seq, reached = st.Capture.QueuedSeq, true
		}
		if reached && st.Apply != nil && st.Apply.AppliedSeq >= seq {
			return seq, pos, nil
		}
		// Apply won't pass a schema change before it is accepted.
		sc := st.SchemaChange
		if sc == nil && st.Apply != nil {
			sc = st.Apply.Blocked
		}
		if sc != nil {
			return 0, nil, fmt.Errorf("a schema change on the source stopped the stream at %s (%s): apply it to the target, resume the migration, then cut over again",
				sc.Table, strings.Join(sc.Summary, "; "))
		}
		detail := fmt.Sprintf("capturing up to %s", src.FormatPosition(pos))
		if reached {
			detail = fmt.Sprintf("applying: %d change(s) to go", st.LagChanges)
		}
		if st.CaptureError != "" || st.ApplyError != "" {
			detail += " (a worker is restarting: " + st.CaptureError + st.ApplyError + ")"
		}
		r.step(StepDrain, "running", detail)
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-tick.C:
		}
	}
}

// awaitCaptured waits until capture has confirmed the source's position
// now, so every change made before this call is queued.
func (m *Manager) awaitCaptured(ctx context.Context, r *run, src cdc.ChangeSource) error {
	pos, err := src.CurrentPosition(ctx)
	if err != nil {
		return err
	}
	tick := time.NewTicker(m.interval() / 2)
	defer tick.Stop()
	for {
		r.mu.Lock()
		st := r.status
		r.mu.Unlock()
		if st.Capture != nil && len(st.Capture.Confirmed) > 0 && src.Behind(st.Capture.Confirmed, pos) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// queuedSince is how many changes capture queued after seq, by its
// latest report.
func (m *Manager) queuedSince(r *run, seq uint64) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status.Capture == nil || r.status.Capture.QueuedSeq <= seq {
		return 0
	}
	return r.status.Capture.QueuedSeq - seq
}

// syncSequences sets the target's sequences to the source's values, and
// returns how many it set, or -1 when the target keeps none.
func (m *Manager) syncSequences(ctx context.Context, r *run, src cdc.ChangeSource) (int, error) {
	tgt, err := registry.NewTarget(r.project.TargetConfig.Type)
	if err != nil {
		return 0, err
	}
	syncer, ok := tgt.(adapters.SequenceSyncer)
	if !ok {
		return -1, nil
	}
	values, err := src.Sequences(ctx)
	if err != nil {
		return 0, err
	}
	if err := tgt.Connect(ctx, r.project.TargetConfig); err != nil {
		return 0, fmt.Errorf("connect to target: %w", err)
	}
	defer tgt.Close()
	return syncer.SetSequences(ctx, values)
}

// remember keeps a finished cutover's status, for the API to show after
// the run itself is gone.
func (m *Manager) remember(r *run) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		m.done = map[string]Status{}
	}
	r.mu.Lock()
	st := r.status
	st.Phase = PhaseCompleted
	c := *r.cutover
	st.Cutover = &c
	r.mu.Unlock()
	m.done[r.id] = st
}
