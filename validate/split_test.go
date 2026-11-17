package validate

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// smallSplits lowers the split thresholds so a test table of a few thousand
// rows is read in parts.
func smallSplits(t *testing.T, minRows, partRows int64) {
	t.Helper()
	oldMin, oldPart := splitMinRows, splitPartRows
	splitMinRows, splitPartRows = minRows, partRows
	t.Cleanup(func() { splitMinRows, splitPartRows = oldMin, oldPart })
}

// migratedBig migrates a SQLite table of n integer-keyed rows (keys 1..n,
// with a gap) plus two small tables, and returns the project and the target.
func migratedBig(t *testing.T, n int) (*adapters.Project, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	srcPath, tgtPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "tgt.db")
	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	stmts := []string{
		`CREATE TABLE big (id INTEGER PRIMARY KEY, name TEXT, v REAL)`,
		`CREATE TABLE small1 (id INTEGER PRIMARY KEY, s TEXT)`,
		`INSERT INTO small1 VALUES (1, 'a'), (2, 'b')`,
		`CREATE TABLE small2 (k TEXT PRIMARY KEY, s TEXT)`,
		`INSERT INTO small2 VALUES ('x', 'y')`,
	}
	for _, stmt := range stmts {
		if _, err := src.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	tx, _ := src.Begin()
	for i := 1; i <= n; i++ {
		id := i
		if i > n/2 {
			id = i + 1000 // a gap in the keys: parts by key range are uneven
		}
		if _, err := tx.Exec(`INSERT INTO big VALUES (?, ?, ?)`, id, fmt.Sprintf("row %d", id), float64(id)/3); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	meta, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { meta.Close() })
	p := &adapters.Project{ID: "p1", Name: "split",
		SourceConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: srcPath},
		TargetConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: tgtPath}, CreatedAt: time.Now()}
	ctx := context.Background()
	if err := meta.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	mig := &adapters.Migration{ID: "m1", ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
	if err := meta.CreateMigration(ctx, mig); err != nil {
		t.Fatal(err)
	}
	if err := engine.New(mig.ID, p, meta).Run(ctx); err != nil {
		t.Fatalf("migration: %v", err)
	}
	tgt, err := sql.Open("sqlite3", tgtPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tgt.Close() })
	return p, tgt
}

var bigTables = []string{"big", "small1", "small2"}

func validateBig(t *testing.T, p *adapters.Project, workers int, level string) map[string]state.ValidationTable {
	t.Helper()
	got := map[string]state.ValidationTable{}
	err := Run(context.Background(), Request{Project: p, Tables: bigTables, Level: level, BatchSize: 100, Workers: workers},
		func(r state.ValidationTable) { got[r.TableName] = r })
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// A table read in parts by several workers has exactly the fingerprint of
// one pass, and passes.
func TestSplitTableMatchesOnePass(t *testing.T) {
	p, _ := migratedBig(t, 3000)
	onePass := validateBig(t, p, 1, LevelL2)["big"] // one worker: never split
	smallSplits(t, 100, 250)                        // 12 parts
	split := validateBig(t, p, 3, LevelL2)["big"]
	if split.Result != state.ValidationPass {
		t.Fatalf("split: %s (%s)", split.Result, split.Detail)
	}
	if split.SourceHash != onePass.SourceHash || split.TargetHash != onePass.TargetHash || split.RowsChecked != 3000 {
		t.Errorf("split %s/%s %d rows, one pass %s/%s", split.SourceHash, split.TargetHash, split.RowsChecked,
			onePass.SourceHash, onePass.TargetHash)
	}
}

// Defects inside one part, and a target row keyed beyond every source key
// (only the open-ended last part reads it), still fail the table.
func TestSplitTableCatchesDefects(t *testing.T) {
	cases := []struct{ name, plant, detail string }{
		{"changed value in a middle part", `UPDATE big SET name = 'X' WHERE id = 3000`, "contents differ"},
		{"row moved past the last key", `UPDATE big SET id = 999999 WHERE id = 10`, "contents differ"},
		{"row moved below the first key", `UPDATE big SET id = -5 WHERE id = 2600`, "contents differ"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, tgt := migratedBig(t, 3000)
			smallSplits(t, 100, 250)
			if res, err := tgt.Exec(c.plant); err != nil {
				t.Fatal(err)
			} else if n, _ := res.RowsAffected(); n != 1 {
				t.Fatalf("plant changed %d rows, want 1", n)
			}
			got := validateBig(t, p, 3, LevelL2)
			r := got["big"]
			if r.Result != state.ValidationFail || !strings.Contains(r.Detail, c.detail) {
				t.Fatalf("result %s (%s), want a fail saying %q", r.Result, r.Detail, c.detail)
			}
			for _, other := range []string{"small1", "small2"} {
				if got[other].Result != state.ValidationPass {
					t.Errorf("%s: %s", other, got[other].Result)
				}
			}
		})
	}
}

// The pool hands out whole tables before parts: a worker other than the
// table's own takes a part only once no whole table is left, and every part
// runs exactly once.
func TestPoolTablesBeforeParts(t *testing.T) {
	p := newPool([]string{"big", "a", "b", "c", "d"})
	var mu sync.Mutex
	var owner *Session
	ran := map[int]int{}
	helped := 0
	ctx := context.Background()
	table := func(s *Session) func(string) bool {
		return func(name string) bool {
			if name != "big" {
				time.Sleep(30 * time.Millisecond)
				return true
			}
			mu.Lock()
			owner = s
			mu.Unlock()
			parts := make([]part, 12)
			for i := range parts {
				i := i
				parts[i] = func(w *Session) error {
					p.mu.Lock()
					left := len(p.tables)
					p.mu.Unlock()
					mu.Lock()
					ran[i]++
					if w != owner {
						helped++
						if left > 0 {
							t.Errorf("part %d taken by a helper while %d whole tables were waiting", i, left)
						}
					}
					mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					return nil
				}
			}
			if err := p.split(ctx, s, parts); err != nil {
				t.Error(err)
			}
			return true
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(s *Session) { defer wg.Done(); p.work(ctx, s, table(s)) }(&Session{})
	}
	wg.Wait()
	for i := 0; i < 12; i++ {
		if ran[i] != 1 {
			t.Errorf("part %d ran %d times", i, ran[i])
		}
	}
	if helped == 0 {
		t.Error("no idle worker helped with the big table")
	}
}

// A failing part fails its table with the side it failed on, and the
// table's other parts stop being handed out.
func TestPoolPartFailure(t *testing.T) {
	p := newPool(nil)
	boom := sideError{err: fmt.Errorf("boom")}
	var mu sync.Mutex
	ran := 0
	parts := make([]part, 20)
	for i := range parts {
		i := i
		parts[i] = func(*Session) error {
			mu.Lock()
			ran++
			mu.Unlock()
			if i == 1 {
				return boom
			}
			return nil
		}
	}
	err := p.split(context.Background(), &Session{}, parts)
	if se, ok := err.(sideError); !ok || se.source {
		t.Fatalf("err %v, want the target-side error", err)
	}
	if ran >= 20 {
		t.Errorf("%d parts ran after one failed", ran)
	}
}
