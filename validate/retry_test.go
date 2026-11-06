package validate

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/lib/pq"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// flakyCount fails its first row counts with the error YugabyteDB gives a
// connection whose cached schema is older than the table's (just after its
// indexes went in), then counts as usual.
type flakyCount struct {
	adapters.SourceAdapter
	failures int
}

func (f *flakyCount) GetRowCount(ctx context.Context, table string) (int64, error) {
	if f.failures > 0 {
		f.failures--
		return 0, &pq.Error{Code: "40001", Message: "schema version mismatch for table 0000400000003000800000000000403: expected 2, got 1"}
	}
	return f.SourceAdapter.GetRowCount(ctx, table)
}

func TestValidateRetriesTransientErrors(t *testing.T) {
	old := retryDelays
	retryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	defer func() { retryDelays = old }()

	dir := t.TempDir()
	ctx := context.Background()
	open := func(name string) adapters.SourceAdapter {
		path := filepath.Join(dir, name+".db")
		db := mustOpen(t, "sqlite3", path)
		mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`)
		mustExec(t, db, `INSERT INTO t VALUES (1, 'a'), (2, 'b')`)
		db.Close()
		a, err := registry.NewSource("sqlite")
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: path}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { a.Close() })
		return a
	}
	src := open("src")

	tgt := &flakyCount{SourceAdapter: open("tgt"), failures: 2}
	if r := validateTable(ctx, src, tgt, "t", LevelL2, 10, 0, nil, false, "yugabyte", newTracker(nil, "t", 0)); r.Result != state.ValidationPass {
		t.Fatalf("after two transient errors: %s (%s)", r.Result, r.Detail)
	}

	// Past the retries it fails, with the error.
	tgt = &flakyCount{SourceAdapter: open("tgt2"), failures: 10}
	if r := validateTable(ctx, src, tgt, "t", LevelL2, 10, 0, nil, false, "yugabyte", newTracker(nil, "t", 0)); r.Result != state.ValidationFail || tgt.failures != 6 {
		t.Fatalf("persistent transient errors: %s (%s), %d failures left", r.Result, r.Detail, tgt.failures)
	}
}

func TestIsTransient(t *testing.T) {
	for err, want := range map[error]bool{
		&pq.Error{Code: "40001"}: true,
		&pq.Error{Code: "40P01"}: true,
		&pq.Error{Code: "42P01"}: false,
		errors.New("count(compat_events): pq: schema version mismatch for table x: expected 2, got 1 (40001)"): true,
		errors.New("connection refused"): false,
	} {
		if got := isTransient(err); got != want {
			t.Errorf("isTransient(%v) = %v, want %v", err, got, want)
		}
	}
}
