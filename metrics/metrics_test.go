package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

// run wires a collector to m the way the migrations handler does and returns
// its event channel. Closing the channel ends the run.
func run(t *testing.T, m *Metrics, migrationID string) (chan engine.ProgressEvent, *stats.Collector) {
	t.Helper()
	events := make(chan engine.ProgressEvent)
	col := stats.NewCollector(migrationID, "p1", events, stats.MigrationConfig{}, nil)
	col.SetObserver(m.Track(migrationID, "proj", col))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	col.Start(ctx)
	return events, col
}

// gauge returns the value of the named metric whose labels include want, and
// whether it was found.
func gauge(t *testing.T, m *Metrics, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metric:
		for _, mt := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range mt.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue metric
				}
			}
			return mt.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Events carry running totals; the counter must add only increases, so a
// resumed table's checkpointed rows, a dropped batch and out-of-order segment
// events are each counted exactly once.
func TestRowsCountedFromRunningTotals(t *testing.T) {
	m := New()
	events, _ := run(t, m, "m1")
	for _, ev := range []engine.ProgressEvent{
		{Kind: engine.EventTableStart, TableName: "t", RowsTransferred: 100, RowsTotal: 300}, // resumed
		{Kind: engine.EventBatch, TableName: "t", RowsTransferred: 150, ReadDuration: time.Millisecond, WriteDuration: 2 * time.Millisecond},
		{Kind: engine.EventBatch, TableName: "t", RowsTransferred: 120}, // older segment's total, arriving late
		// The batch taking it to 250 was dropped.
		{Kind: engine.EventTableDone, TableName: "t", RowsTransferred: 300},
		{Kind: engine.EventTableFailed, TableName: "u"},
		{Kind: engine.EventError},
	} {
		events <- ev
	}
	close(events)
	waitFor(t, "run to end", func() bool {
		return testutil.ToFloat64(m.migrationsFinished.WithLabelValues("proj", "failed")) == 1
	})

	if got := testutil.ToFloat64(m.rowsTransferred.WithLabelValues("proj", "t")); got != 200 {
		t.Errorf("rows transferred = %v, want 200", got)
	}
	if got := testutil.ToFloat64(m.tablesFinished.WithLabelValues("proj", "completed")); got != 1 {
		t.Errorf("tables completed = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.tablesFinished.WithLabelValues("proj", "failed")); got != 1 {
		t.Errorf("tables failed = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.batchWrite); got != 1 {
		t.Errorf("batch write histogram series = %d, want 1", got)
	}
}

// Per-migration gauges exist while the migration runs and go when it ends.
func TestGaugesFollowActiveMigration(t *testing.T) {
	m := New()
	events, col := run(t, m, "m1")
	events <- engine.ProgressEvent{Kind: engine.EventMigrationStart, TableNames: []string{"t", "u"}, TableCounts: map[string]int64{"t": 100, "u": 100}}
	events <- engine.ProgressEvent{Kind: engine.EventTableStart, TableName: "t", RowsTotal: 100}
	events <- engine.ProgressEvent{Kind: engine.EventBatch, TableName: "t", RowsTransferred: 50, RowsTotal: 100}
	waitFor(t, "batch applied", func() bool { return col.Snapshot().Rows.Transferred == 50 })

	ids := map[string]string{"project": "proj", "migration": "m1"}
	if v, ok := gauge(t, m, "xferdb_migration_progress_ratio", ids); !ok || v != 0.25 {
		t.Errorf("progress ratio = %v (found %v), want 0.25", v, ok)
	}
	if v, _ := gauge(t, m, "xferdb_migration_phase", map[string]string{"migration": "m1", "phase": "in_progress"}); v != 1 {
		t.Errorf("phase in_progress = %v, want 1", v)
	}
	if v, _ := gauge(t, m, "xferdb_migrations_active", map[string]string{"phase": "in_progress"}); v != 1 {
		t.Errorf("active in_progress = %v, want 1", v)
	}
	if v, _ := gauge(t, m, "xferdb_migration_tables", map[string]string{"migration": "m1", "state": "in_progress"}); v != 1 {
		t.Errorf("tables in_progress = %v, want 1", v)
	}

	events <- engine.ProgressEvent{Kind: engine.EventComplete}
	close(events)
	waitFor(t, "run untracked", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.runs) == 0
	})
	if _, ok := gauge(t, m, "xferdb_migration_progress_ratio", ids); ok {
		t.Error("progress ratio still exported after the migration ended")
	}
	if v, ok := gauge(t, m, "xferdb_migrations_active", map[string]string{"phase": "in_progress"}); !ok || v != 0 {
		t.Errorf("active in_progress = %v (found %v), want 0", v, ok)
	}
}
