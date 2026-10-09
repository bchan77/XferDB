package handlers

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// migratedProject migrates three SQLite tables with validation off and
// returns the metadb, project and migration ID.
func migratedProject(t *testing.T) (*state.MetaDB, *adapters.Project, string) {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)`, `INSERT INTO a VALUES (1, 'x'), (2, 'y')`,
		`CREATE TABLE b (id INTEGER PRIMARY KEY, v TEXT)`, `INSERT INTO b VALUES (1, 'x')`,
		`CREATE TABLE c (id INTEGER PRIMARY KEY, v TEXT)`, `INSERT INTO c VALUES (1, 'x')`,
	} {
		if _, err := src.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	p := &adapters.Project{
		ID: "p1", Name: "pt", CreatedAt: time.Now(),
		SourceConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: srcPath},
		TargetConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: filepath.Join(dir, "tgt.db")},
	}
	if err := db.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	mig := &adapters.Migration{ID: "m1", ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
	if err := db.CreateMigration(ctx, mig); err != nil {
		t.Fatal(err)
	}
	if err := engine.New(mig.ID, p, db).Run(ctx); err != nil {
		t.Fatal(err)
	}
	return db, p, mig.ID
}

func tableResults(t *testing.T, db *state.MetaDB, vid string) map[string]state.ValidationTable {
	t.Helper()
	ts, err := db.GetValidationTables(context.Background(), vid)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]state.ValidationTable{}
	for _, x := range ts {
		out[x.TableName] = x
	}
	return out
}

// TestPerTableValidatesBeforeTheMigrationEnds hands the runner tables one at
// a time, as the engine does, and checks each is validated while the
// migration would still be running, before finish.
func TestPerTableValidatesBeforeTheMigrationEnds(t *testing.T) {
	db, p, migID := migratedProject(t)
	h := NewValidationsHandler(db, slog.New(slog.DiscardHandler))
	ctx := context.Background()

	r := h.beginPerTable(migID, p, "L2", 0, 2, slog.New(slog.DiscardHandler))
	r.tableDone("a")
	r.tableDone("a") // a repeat is ignored

	var vid string
	deadline := time.Now().Add(10 * time.Second)
	for vid == "" || tableResults(t, db, vid)["a"].Result != state.ValidationPass {
		if time.Now().After(deadline) {
			t.Fatal("table a was not validated before the migration finished")
		}
		if vs, _ := db.ListValidations(ctx, migID); len(vs) > 0 {
			vid = vs[0].ID
		}
		time.Sleep(20 * time.Millisecond)
	}
	if v, _ := db.GetValidation(ctx, vid); v.Status != adapters.StatusInProgress || v.Options.Mode != "per_table" {
		t.Fatalf("mid-migration validation is %s (mode %q), want in_progress per_table", v.Status, v.Options.Mode)
	}

	// The migration failed on c: b completed but its callback never came;
	// finish picks it up from migration_tables, and records c as skipped.
	if err := db.UpsertTableProgress(ctx, migID, adapters.TableProgress{TableName: "c", Status: adapters.StatusFailed}); err != nil {
		t.Fatal(err)
	}
	r.finish()

	got := tableResults(t, db, vid)
	if got["a"].Result != state.ValidationPass || got["b"].Result != state.ValidationPass {
		t.Errorf("a = %s, b = %s; want both pass", got["a"].Result, got["b"].Result)
	}
	if got["c"].Result != state.ValidationSkipped || got["c"].Detail == "" {
		t.Errorf("c = %s (%q); want skipped with a reason", got["c"].Result, got["c"].Detail)
	}
	if v, _ := db.GetValidation(ctx, vid); v.Status != adapters.StatusCompleted {
		t.Errorf("validation status %s after finish, want completed", v.Status)
	}
	if vs, _ := db.ListValidations(ctx, migID); len(vs) != 1 {
		t.Errorf("%d validations, want one", len(vs))
	}
	h.mu.Lock()
	busy := len(h.byProject) + len(h.cancels)
	h.mu.Unlock()
	if busy != 0 {
		t.Errorf("finish left the project marked busy")
	}
}

// TestPerTableNothingCompleted leaves no validation behind when no table
// completed, as mode after does for a failed migration.
func TestPerTableNothingCompleted(t *testing.T) {
	db, p, _ := migratedProject(t)
	h := NewValidationsHandler(db, slog.New(slog.DiscardHandler))
	ctx := context.Background()
	mig := &adapters.Migration{ID: "m2", ProjectID: p.ID, Status: adapters.StatusFailed, CreatedAt: time.Now()}
	if err := db.CreateMigration(ctx, mig); err != nil {
		t.Fatal(err)
	}
	r := h.beginPerTable(mig.ID, p, "L2", 0, 1, slog.New(slog.DiscardHandler))
	r.finish()
	if vs, _ := db.ListValidations(ctx, mig.ID); len(vs) != 0 {
		t.Errorf("%d validations for a migration that completed nothing, want none", len(vs))
	}
}
