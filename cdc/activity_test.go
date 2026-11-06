package cdc_test

import (
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// Apply counts what it applies, per table and kind, and a restarted apply
// picks the counts (and ChangesApplied) up where the last one left them.
func TestApplyCountsActivity(t *testing.T) {
	tgt, _ := sqliteTarget(t)
	dir := t.TempDir()
	w, err := cdc.OpenWriter(dir, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	commit := time.Date(2026, 11, 5, 21, 27, 58, 0, time.UTC)
	ins := func(table string, id int64) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(id), After: row(id, "v", "b"), CommitAt: commit}
	}
	appendTxn(t, w, ins("a", 1), ins("a", 2), ins("b", 1))
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(1), After: row(1, "v2", "b"), CommitAt: commit},
		cdc.Change{Table: "a", Op: cdc.OpDelete, Key: key(2), CommitAt: commit})

	file := filepath.Join(t.TempDir(), "activity.json")
	newApplier := func() *cdc.Applier {
		return &cdc.Applier{Stream: "s1", Dir: dir, Target: tgt.(adapters.ChangeApplier), ActivityFile: file,
			Opts: cdc.ApplyOptions{MaxBatch: 10, MaxWait: 10 * time.Millisecond}}
	}
	a := newApplier()
	last, _ := w.Last()
	runUntil(t, a, last)
	st := a.Status()
	want := map[string]cdc.TableActivity{
		"a": {Inserts: 2, Updates: 1, Deletes: 1},
		"b": {Inserts: 1},
	}
	for table, w := range want {
		got := st.Tables[table]
		if got.Inserts != w.Inserts || got.Updates != w.Updates || got.Deletes != w.Deletes ||
			!got.LastCommitAt.Equal(commit) || got.LastAppliedAt.IsZero() {
			t.Errorf("table %s: %+v, want %+v", table, got, w)
		}
	}
	if st.ChangesApplied != 5 {
		t.Fatalf("changes applied = %d, want 5", st.ChangesApplied)
	}

	// A new apply process: the counts carry on.
	appendTxn(t, w, ins("b", 2))
	a = newApplier()
	last, _ = w.Last()
	runUntil(t, a, last)
	st = a.Status()
	if st.Tables["b"].Inserts != 2 || st.Tables["a"].Total() != 4 || st.ChangesApplied != 6 {
		t.Fatalf("after a restart: %+v, %d applied", st.Tables, st.ChangesApplied)
	}
}
