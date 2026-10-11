package stats

import (
	"errors"
	"sort"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/engine"
)

// A caller reordering a snapshot's TableDetails (as the stats handler does,
// sorting by name) must not reorder the collector's own rows: tableIndex maps
// each table to its row by position, so later events would land on the wrong
// table (#125).
func TestSnapshotIsolatedFromCaller(t *testing.T) {
	c := NewCollector("m1", "p1", nil, MigrationConfig{}, nil)
	c.apply(engine.ProgressEvent{
		Kind:       engine.EventMigrationStart,
		TableNames: []string{"compat_reviews", "compat_inventory", "compat_events"},
	})

	snap := c.Snapshot()
	sort.Slice(snap.TableDetails, func(i, j int) bool {
		return snap.TableDetails[i].Name < snap.TableDetails[j].Name
	})
	snap.TableDetails[0].Status = "scribbled"
	snap.Errors = append(snap.Errors, "scribbled")

	c.apply(engine.ProgressEvent{Kind: engine.EventTableDone, TableName: "compat_reviews", RowsTransferred: 10})
	c.apply(engine.ProgressEvent{Kind: engine.EventTableFailed, TableName: "compat_inventory", Err: errors.New("boom")})

	want := map[string]string{
		"compat_reviews":   "done",
		"compat_inventory": "failed",
		"compat_events":    "pending",
	}
	got := c.Snapshot()
	for _, td := range got.TableDetails {
		if td.Status != want[td.Name] {
			t.Errorf("%s: status %q, want %q", td.Name, td.Status, want[td.Name])
		}
	}
	if len(got.Errors) != 0 {
		t.Errorf("Errors = %v, want none", got.Errors)
	}
}
