package cdc

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// TableActivity is what apply has done to one table since the stream
// began: changes applied by kind, and when the last one happened.
type TableActivity struct {
	Inserts   int64 `json:"inserts"`
	Updates   int64 `json:"updates"`
	Deletes   int64 `json:"deletes"`
	Truncates int64 `json:"truncates,omitempty"`
	// LastCommitAt is the source's commit time of the last change applied,
	// LastAppliedAt when apply wrote it to the target.
	LastCommitAt  time.Time `json:"last_commit_at,omitempty"`
	LastAppliedAt time.Time `json:"last_applied_at,omitempty"`
}

// Total is the number of changes applied to the table.
func (t TableActivity) Total() int64 { return t.Inserts + t.Updates + t.Deletes + t.Truncates }

// noteActivity counts a table's changes once they are committed.
func (a *Applier) noteActivity(table string, changes []Change) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activity == nil {
		a.activity = map[string]TableActivity{}
	}
	t := a.activity[table]
	for _, c := range changes {
		switch c.Op {
		case OpInsert:
			t.Inserts++
		case OpUpdate:
			t.Updates++
		case OpDelete:
			t.Deletes++
		case OpTruncate:
			t.Truncates++
		}
		if !c.CommitAt.IsZero() {
			t.LastCommitAt = c.CommitAt
		}
	}
	t.LastAppliedAt = time.Now()
	a.activity[table] = t
}

// loadActivity reads the counts an earlier apply process saved, so they
// (and ChangesApplied) carry on across restarts.
func (a *Applier) loadActivity() error {
	if a.ActivityFile == "" {
		return nil
	}
	data, err := os.ReadFile(a.ActivityFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	act := map[string]TableActivity{}
	if err := json.Unmarshal(data, &act); err != nil {
		return nil // only counts: start over rather than fail
	}
	var total int64
	for _, t := range act {
		total += t.Total()
	}
	a.mu.Lock()
	a.activity = act
	a.mu.Unlock()
	a.total.Store(total)
	return nil
}

// saveActivity writes the counts, atomically. They are informational: a
// crash between a commit and this save can lose a batch's counts, never a
// change.
func (a *Applier) saveActivity() error {
	if a.ActivityFile == "" {
		return nil
	}
	a.mu.Lock()
	data, err := json.Marshal(a.activity)
	a.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := a.ActivityFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Clean(a.ActivityFile))
}
