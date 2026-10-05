package validate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// migrated runs a real SQLite -> SQLite migration through the engine and
// returns the project plus an open handle on the target for planting defects.
func migrated(t *testing.T) (*adapters.Project, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	tgtPath := filepath.Join(dir, "tgt.db")

	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, stmt := range []string{
		`CREATE TABLE items (
			id INTEGER PRIMARY KEY, name TEXT, price NUMERIC(10,2), active BOOLEAN,
			created TIMESTAMP, payload JSON, raw BLOB, ratio REAL)`,
		`INSERT INTO items VALUES
			(1, 'apple', 1.50, 1, '2024-03-01 10:30:00.123456+00:00', '{"b":1,"a":[1,2]}', x'dead', 0.1),
			(2, 'pear',  2.00, 0, '2024-03-02 11:00:00+00:00', '{}', x'', 1.25),
			(3, NULL,    NULL, NULL, NULL, NULL, NULL, NULL)`,
		// No primary key, with a duplicated row: compared as a multiset.
		`CREATE TABLE notes (body TEXT, n INTEGER)`,
		`INSERT INTO notes VALUES ('same', 1), ('same', 1), ('other', 2)`,
	} {
		if _, err := src.Exec(stmt); err != nil {
			t.Fatalf("seed source: %v\n%s", err, stmt)
		}
	}

	meta, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { meta.Close() })
	p := &adapters.Project{
		ID:           "p1",
		Name:         "sqlite-to-sqlite",
		SourceConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: srcPath},
		TargetConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: tgtPath},
		CreatedAt:    time.Now(),
	}
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

func runValidation(t *testing.T, p *adapters.Project, level string) map[string]state.ValidationTable {
	t.Helper()
	got := map[string]state.ValidationTable{}
	err := Run(context.Background(), Request{
		Project: p, Tables: []string{"items", "notes"}, Level: level, BatchSize: 2,
	}, func(r state.ValidationTable) { got[r.TableName] = r })
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	return got
}

func TestValidateSQLitePassesAfterMigration(t *testing.T) {
	p, _ := migrated(t)
	for _, level := range []string{LevelL1, LevelL2} {
		for table, r := range runValidation(t, p, level) {
			if r.Result != state.ValidationPass {
				t.Errorf("%s %s: result %s (%s), want pass", level, table, r.Result, r.Detail)
			}
		}
	}
}

func TestValidateSQLiteCatchesPlantedDefects(t *testing.T) {
	cases := []struct {
		name   string
		plant  string
		table  string
		level  string
		detail string // substring the failure must explain itself with
	}{
		{"changed value", `UPDATE items SET name = 'APPLE' WHERE id = 1`, "items", LevelL2, "contents differ"},
		{"changed json", `UPDATE items SET payload = '{"a":[1,2],"b":2}' WHERE id = 1`, "items", LevelL2, "contents differ"},
		{"value set to null", `UPDATE items SET ratio = NULL WHERE id = 2`, "items", LevelL2, "contents differ"},
		{"deleted row", `DELETE FROM items WHERE id = 2`, "items", LevelL1, "row counts differ"},
		{"extra row", `INSERT INTO items (id, name) VALUES (99, 'ghost')`, "items", LevelL1, "row counts differ"},
		{"duplicate swapped", `UPDATE notes SET body = 'other', n = 2 WHERE rowid = 1`, "notes", LevelL2, "contents differ"},
		{"extra column", `ALTER TABLE notes ADD COLUMN surprise TEXT`, "notes", LevelL1, "columns only in target: surprise"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, tgt := migrated(t)
			if _, err := tgt.Exec(c.plant); err != nil {
				t.Fatalf("plant: %v", err)
			}
			got := runValidation(t, p, c.level)
			r := got[c.table]
			if r.Result != state.ValidationFail {
				t.Fatalf("%s: result %s, want fail", c.table, r.Result)
			}
			if !strings.Contains(r.Detail, c.detail) {
				t.Errorf("detail %q does not mention %q", r.Detail, c.detail)
			}
			for other, o := range got {
				if other != c.table && o.Result != state.ValidationPass {
					t.Errorf("untouched table %s: result %s (%s)", other, o.Result, o.Detail)
				}
			}
		})
	}
}

func TestValidateMissingTargetTable(t *testing.T) {
	p, tgt := migrated(t)
	if _, err := tgt.Exec(`DROP TABLE notes`); err != nil {
		t.Fatal(err)
	}
	r := runValidation(t, p, LevelL2)["notes"]
	if r.Result != state.ValidationFail || !strings.Contains(r.Detail, "target") {
		t.Fatalf("result %s (%s), want a fail naming the target", r.Result, r.Detail)
	}
}

func TestCheckLevelAndMode(t *testing.T) {
	for _, ok := range []string{"", LevelL1, LevelL2} {
		if err := CheckLevel(ok); err != nil {
			t.Errorf("CheckLevel(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{LevelL3, "l2", "full"} {
		if err := CheckLevel(bad); err == nil {
			t.Errorf("CheckLevel(%q) accepted", bad)
		}
	}
	for _, ok := range []string{"", ModeAfter, ModeOff} {
		if err := CheckMode(ok); err != nil {
			t.Errorf("CheckMode(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{ModePerTable, "never"} {
		if err := CheckMode(bad); err == nil {
			t.Errorf("CheckMode(%q) accepted", bad)
		}
	}
}
