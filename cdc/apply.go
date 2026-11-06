package cdc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// HighWaterTable is the reserved table name under which apply records a
// stream's high-water mark: the last seq of a batch every table of which has
// committed. Apply restarts reading after it, and the queue below it can go.
// Per-table seqs (each committed with that table's part of a batch) say
// which changes after it a table already has.
const HighWaterTable = "*"

// ApplyOptions tunes the apply loop.
type ApplyOptions struct {
	// A batch closes at MaxBatch changes or MaxWait after its first one.
	MaxBatch int
	MaxWait  time.Duration
	// Workers is how many tables apply in parallel.
	Workers int
	// RemoveApplied deletes queue segments below the high-water mark.
	RemoveApplied bool
}

func (o *ApplyOptions) defaults() {
	if o.MaxBatch <= 0 {
		o.MaxBatch = 5000
	}
	if o.MaxWait <= 0 {
		o.MaxWait = 500 * time.Millisecond
	}
	if o.Workers <= 0 {
		o.Workers = 4
	}
}

// ApplyStatus is what the apply loop reports while it runs.
type ApplyStatus struct {
	AppliedSeq     uint64    `json:"applied_seq"`
	AppliedCommit  time.Time `json:"applied_commit_at,omitempty"`
	ChangesApplied int64     `json:"changes_applied"`
	Batches        int64     `json:"batches"`
	LastBatchAt    time.Time `json:"last_batch_at,omitempty"`
	// Tables is what apply has done to each table (activity.go).
	Tables map[string]TableActivity `json:"tables,omitempty"`
	// Blocked is the schema change apply has stopped at, waiting for it to
	// be accepted: every change before it is applied, none after it.
	Blocked *SchemaChange `json:"blocked,omitempty"`
}

// SchemaChange is a schema change (DDL) on the source that apply stopped at.
type SchemaChange struct {
	Seq        uint64    `json:"seq"`
	Table      string    `json:"table"`
	Columns    []Column  `json:"columns"`
	OldColumns []Column  `json:"old_columns,omitempty"`
	CommitAt   time.Time `json:"commit_at,omitempty"`
	// Summary says what changed, one line per difference.
	Summary []string `json:"summary"`
}

// NewSchemaChange describes an OpSchema change.
func NewSchemaChange(c *Change) *SchemaChange {
	return &SchemaChange{
		Seq: c.Seq, Table: c.Table, Columns: c.Columns, OldColumns: c.OldColumns,
		CommitAt: c.CommitAt, Summary: DescribeColumns(c.OldColumns, c.Columns),
	}
}

// DescribeColumns lists how a table's columns went from old to new. A
// renamed column reads as one dropped and one added; nil old is a table
// the stream didn't know by its name, which is how a renamed table looks.
func DescribeColumns(old, new []Column) []string {
	if old == nil {
		return []string{"a table this migration doesn't know by that name: was a table renamed?"}
	}
	was := map[string]string{}
	for _, c := range old {
		was[c.Name] = c.Type
	}
	is := map[string]bool{}
	var out []string
	for _, c := range new {
		is[c.Name] = true
		t, ok := was[c.Name]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("column %s added (%s)", c.Name, c.Type))
		case t != c.Type:
			out = append(out, fmt.Sprintf("column %s changed type: %s → %s", c.Name, t, c.Type))
		}
	}
	for _, c := range old {
		if !is[c.Name] {
			out = append(out, fmt.Sprintf("column %s dropped", c.Name))
		}
	}
	if len(out) == 0 {
		out = append(out, "columns reordered")
	}
	return out
}

// Applier replays a queue into a target.
type Applier struct {
	Stream string
	Dir    string
	Target adapters.ChangeApplier
	Opts   ApplyOptions
	// Refetch reads a row's current state from the source by key. It is
	// needed for an update that changed the key without sending every
	// column, when the target has the row under neither key (see
	// moveRow); without it such a change fails the apply.
	Refetch func(ctx context.Context, table string, key map[string]interface{}) (row map[string]interface{}, found bool, err error)
	// SchemaAccepted returns the seq up to which schema changes have been
	// accepted, to be applied past. Apply stops at any later one until it
	// is; without SchemaAccepted it stops at every one.
	SchemaAccepted func() uint64
	// ActivityFile, when set, keeps the per-table counts across restarts.
	ActivityFile string

	// held is a change read ahead of a batch: a schema change, which goes
	// in a batch of its own.
	held *Change

	mu       sync.Mutex
	status   ApplyStatus
	activity map[string]TableActivity
	total    atomic.Int64
}

// Status returns a snapshot of the loop's progress.
func (a *Applier) Status() ApplyStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.status
	s.ChangesApplied = a.total.Load()
	if len(a.activity) > 0 {
		s.Tables = make(map[string]TableActivity, len(a.activity))
		for t, act := range a.activity {
			s.Tables[t] = act
		}
	}
	return s
}

// Run applies changes until ctx is done or applying fails.
func (a *Applier) Run(ctx context.Context) error {
	a.Opts.defaults()
	if err := a.Target.PrepareChangeState(ctx); err != nil {
		return err
	}
	if err := a.loadActivity(); err != nil {
		return err
	}
	applied, err := a.Target.AppliedSeqs(ctx, a.Stream)
	if err != nil {
		return err
	}
	hw := applied[HighWaterTable]
	a.mu.Lock()
	a.status.AppliedSeq = hw
	a.mu.Unlock()

	r, err := OpenReader(a.Dir, hw)
	if err != nil {
		return err
	}
	defer r.Close()
	for {
		batch, err := a.readBatch(ctx, r)
		if err != nil {
			return err
		}
		if batch[0].Op == OpSchema {
			if err := a.awaitSchema(ctx, &batch[0]); err != nil {
				return err
			}
		}
		if err := a.applyBatch(ctx, batch, applied); err != nil {
			return err
		}
	}
}

// awaitSchema waits until the schema change c is accepted, reporting it as
// Blocked meanwhile.
func (a *Applier) awaitSchema(ctx context.Context, c *Change) error {
	accepted := func() bool { return a.SchemaAccepted != nil && a.SchemaAccepted() >= c.Seq }
	if accepted() {
		return nil
	}
	a.mu.Lock()
	a.status.Blocked = NewSchemaChange(c)
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.status.Blocked = nil
		a.mu.Unlock()
	}()
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for !accepted() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

// readBatch waits for a first change, then takes what else arrives within
// MaxWait, up to MaxBatch. A schema change is a batch on its own: the
// changes before it are applied first, and those after it only once it is
// accepted.
func (a *Applier) readBatch(ctx context.Context, r *Reader) ([]Change, error) {
	var first Change
	if a.held != nil {
		first, a.held = *a.held, nil
	} else {
		var err error
		if first, err = r.Next(ctx); err != nil {
			return nil, err
		}
	}
	batch := []Change{first}
	if first.Op == OpSchema {
		return batch, nil
	}
	deadline := time.Now().Add(a.Opts.MaxWait)
	for len(batch) < a.Opts.MaxBatch {
		c, ok, err := r.TryNext()
		if err != nil {
			return nil, err
		}
		if ok && c.Op == OpSchema {
			a.held = &c
			break
		}
		if ok {
			batch = append(batch, c)
			continue
		}
		if time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return batch, nil
}

// applyBatch applies each table's changes in seq order, tables in parallel,
// then records the batch's end as the high-water mark.
func (a *Applier) applyBatch(ctx context.Context, batch []Change, applied map[string]uint64) error {
	byTable := map[string][]Change{}
	var tables []string
	for _, c := range batch {
		if c.Op == OpSchema {
			continue // accepted: nothing to write but the high-water mark
		}
		if c.Seq <= applied[c.Table] {
			continue // committed before a restart
		}
		if _, ok := byTable[c.Table]; !ok {
			tables = append(tables, c.Table)
		}
		byTable[c.Table] = append(byTable[c.Table], c)
	}
	sort.Strings(tables)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		errs    []error
		sem     = make(chan struct{}, a.Opts.Workers)
		results = map[string]uint64{}
	)
	for _, t := range tables {
		wg.Add(1)
		sem <- struct{}{}
		go func(table string, changes []Change) {
			defer wg.Done()
			defer func() { <-sem }()
			err := a.applyTable(ctx, table, changes)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			results[table] = changes[len(changes)-1].Seq
		}(t, byTable[t])
	}
	wg.Wait()
	for t, seq := range results {
		applied[t] = seq
	}
	if len(errs) > 0 {
		// Joined, not flattened to text: a stop mid-write must still read
		// as context.Canceled.
		return fmt.Errorf("apply: %w", errors.Join(errs...))
	}

	last := batch[len(batch)-1]
	tx, err := a.Target.BeginChanges(ctx)
	if err != nil {
		return err
	}
	if err := tx.SetApplied(ctx, a.Stream, HighWaterTable, last.Seq); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	applied[HighWaterTable] = last.Seq
	if a.Opts.RemoveApplied {
		if err := RemoveThrough(a.Dir, last.Seq); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.status.AppliedSeq = last.Seq
	if !last.CommitAt.IsZero() {
		a.status.AppliedCommit = last.CommitAt
	}
	a.status.Batches++
	a.status.LastBatchAt = time.Now()
	a.mu.Unlock()
	return a.saveActivity()
}

// applyTable applies one table's changes, in order, in one transaction with
// its new applied seq. Consecutive whole-row writes go out as one upsert and
// consecutive deletes as one delete.
func (a *Applier) applyTable(ctx context.Context, table string, changes []Change) error {
	tx, err := a.Target.BeginChanges(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var upserts []map[string]interface{}
	// Keys in upserts: a statement may touch a row only once, and a key
	// that comes back starts a new statement rather than replacing its
	// earlier version. Merged, two rows swapping a unique value in three
	// steps become two rows each taking the other's value while it still
	// holds it, which PostgreSQL and MySQL refuse: they check a unique
	// index row by row within a statement. In source order every step is a
	// state the source itself had.
	upsertAt := map[string]bool{}
	var deletes []map[string]interface{}
	flush := func() error {
		if len(upserts) > 0 {
			if err := tx.Upsert(ctx, table, upserts); err != nil {
				return err
			}
			upserts, upsertAt = nil, map[string]bool{}
		}
		if len(deletes) > 0 {
			if err := tx.Delete(ctx, table, deletes); err != nil {
				return err
			}
			deletes = nil
		}
		return nil
	}
	upsert := func(row, key map[string]interface{}) error {
		if len(deletes) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		k := keyString(key)
		if upsertAt[k] {
			if err := flush(); err != nil {
				return err
			}
		}
		upsertAt[k] = true
		upserts = append(upserts, row)
		return nil
	}
	del := func(key map[string]interface{}) error {
		if len(upserts) > 0 {
			if err := flush(); err != nil {
				return err
			}
		}
		deletes = append(deletes, key)
		return nil
	}

	for i := range changes {
		c := &changes[i]
		var err error
		switch {
		case c.Op == OpTruncate:
			if err = flush(); err == nil {
				err = tx.Truncate(ctx, table)
			}
		case c.Op == OpDelete:
			err = del(c.Key)
		case c.Op == OpInsert, c.Op == OpUpdate && !c.Partial():
			if c.OldKey != nil {
				if err = del(c.OldKey); err != nil {
					break
				}
			}
			err = upsert(c.After, c.Key)
		case c.Op == OpUpdate && c.OldKey != nil:
			if err = flush(); err == nil {
				err = a.moveRow(ctx, tx, table, c)
			}
		case c.Op == OpUpdate:
			// Some columns weren't sent: update the others in place. A
			// row that isn't there was deleted later in the stream, or the
			// snapshot didn't copy it because it was already gone.
			if err = flush(); err == nil {
				_, err = tx.Update(ctx, table, []adapters.RowUpdate{{Key: c.Key, Set: c.After}})
			}
		default:
			err = fmt.Errorf("unknown change op %q", byte(c.Op))
		}
		if err != nil {
			return fmt.Errorf("%s seq %d (%s): %w", table, c.Seq, c.Op, err)
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf("%s: %w", table, err)
	}
	if err := tx.SetApplied(ctx, a.Stream, table, changes[len(changes)-1].Seq); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: commit: %w", table, err)
	}
	a.total.Add(int64(len(changes)))
	a.noteActivity(table, changes)
	return nil
}

// moveRow applies an update that changed the key but left some columns
// out. Depending on when the snapshot read it, the target holds the row
// under the new key (read after the change: its state is newer, so keep it
// and drop the old key's row), under the old key only (move it), or under
// neither (the copy passed the new key before the change and reached the
// old one after it). In the last case the unsent columns are nowhere in the
// stream, so the row is read again from the source; that state is newer
// than this change, which later changes in the stream build on as usual.
func (a *Applier) moveRow(ctx context.Context, tx adapters.ChangeTx, table string, c *Change) error {
	atNew, err := tx.Exists(ctx, table, c.Key)
	if err != nil {
		return err
	}
	if atNew {
		if err := tx.Delete(ctx, table, []map[string]interface{}{c.OldKey}); err != nil {
			return err
		}
		_, err := tx.Update(ctx, table, []adapters.RowUpdate{{Key: c.Key, Set: c.After}})
		return err
	}
	n, err := tx.Update(ctx, table, []adapters.RowUpdate{{Key: c.OldKey, Set: c.After}})
	if err != nil || n > 0 {
		return err
	}
	if a.Refetch == nil {
		return fmt.Errorf("row moved to a new key is on the target under neither key, and there is no source to read it from")
	}
	row, found, err := a.Refetch(ctx, table, c.Key)
	if err != nil {
		return fmt.Errorf("read moved row from the source: %w", err)
	}
	if !found {
		return nil // deleted since; the delete is later in the stream
	}
	return tx.Upsert(ctx, table, []map[string]interface{}{row})
}

// keyString renders a key so equal keys compare equal.
func keyString(key map[string]interface{}) string {
	cols := make([]string, 0, len(key))
	for k := range key {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	var b strings.Builder
	for _, c := range cols {
		fmt.Fprintf(&b, "%s=%T:%v;", c, key[c], key[c])
	}
	return b.String()
}
