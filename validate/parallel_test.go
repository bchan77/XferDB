package validate

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Tables validated several at once give exactly the results they give one
// at a time, every table is reported once, and report is never called
// concurrently (#200).
func TestRunWorkersMatchSequential(t *testing.T) {
	p, tgt := migrated(t)
	// One failing table, so the comparison covers a failure's detail too.
	if _, err := tgt.Exec(`UPDATE items SET name = 'APPLE' WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	run := func(workers int) map[string]state.ValidationTable {
		got := map[string]state.ValidationTable{}
		var inReport, overlaps atomic.Int32
		err := Run(context.Background(), Request{
			Project: p, Tables: allTables, Level: LevelL3, BatchSize: 2, Workers: workers,
		}, func(r state.ValidationTable) {
			if inReport.Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(5 * time.Millisecond) // widen the window for an overlap
			if _, dup := got[r.TableName]; dup {
				t.Errorf("workers %d: %s reported twice", workers, r.TableName)
			}
			got[r.TableName] = r
			inReport.Add(-1)
		})
		if err != nil {
			t.Fatalf("workers %d: %v", workers, err)
		}
		if overlaps.Load() > 0 {
			t.Errorf("workers %d: report called concurrently", workers)
		}
		if len(got) != len(allTables) {
			t.Errorf("workers %d: %d tables reported, want %d", workers, len(got), len(allTables))
		}
		return got
	}
	want := run(1)
	if want["items"].Result != state.ValidationFail {
		t.Fatalf("items: %s, want the planted failure", want["items"].Result)
	}
	for _, workers := range []int{2, 4, 10} {
		got := run(workers)
		for table, w := range want {
			g := got[table]
			if g.Result != w.Result || g.Detail != w.Detail || g.SourceHash != w.SourceHash ||
				g.TargetHash != w.TargetHash || g.RowsChecked != w.RowsChecked {
				t.Errorf("workers %d, %s: got %+v, want %+v", workers, table, g, w)
			}
		}
	}
}

func TestRunWorkersCancelled(t *testing.T) {
	p, _ := migrated(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Run(ctx, Request{Project: p, Tables: allTables, Level: LevelL2, Workers: 3},
		func(state.ValidationTable) { t.Error("a table was reported after cancel") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want context.Canceled", err)
	}
}

// both keeps the error that came first and drops the one its cancel caused,
// which lib/pq doesn't report as context.Canceled.
func TestBothKeepsTheFirstError(t *testing.T) {
	boom := errors.New("boom")
	stopped := errors.New("pq: canceling statement due to user request")
	failNow := func(context.Context) error { return boom }
	waitCancel := func(ctx context.Context) error { <-ctx.Done(); return stopped }
	if a, b := both(context.Background(), failNow, waitCancel); a != boom || b != nil {
		t.Errorf("a fails first: got %v, %v", a, b)
	}
	if a, b := both(context.Background(), waitCancel, failNow); a != nil || b != boom {
		t.Errorf("b fails first: got %v, %v", a, b)
	}
	ok := func(context.Context) error { return nil }
	if a, b := both(context.Background(), ok, ok); a != nil || b != nil {
		t.Errorf("neither fails: got %v, %v", a, b)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if a, b := both(ctx, waitCancel, waitCancel); a == nil || b == nil {
		t.Errorf("ctx cancelled: got %v, %v, want both errors", a, b)
	}
}
