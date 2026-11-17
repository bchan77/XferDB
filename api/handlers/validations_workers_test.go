package handlers

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// An on-demand validation uses the project's table_workers, else as many as
// the migration ran with, else the default (#200): a project created without
// table_workers and migrated with a config default of 5 validated one table
// at a time.
func TestOnDemandWorkers(t *testing.T) {
	db, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	p := &adapters.Project{ID: "p1", Name: "w", CreatedAt: time.Now(),
		SourceConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: "src.db"},
		TargetConfig: adapters.ConnectionConfig{Type: "sqlite", DSN: "tgt.db"}}
	if err := db.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"with-run-config", "without"} {
		if err := db.CreateMigration(ctx, &adapters.Migration{ID: id, ProjectID: p.ID,
			Status: adapters.StatusCompleted, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SetRunConfig(ctx, "with-run-config", adapters.TransferConfig{TableWorkers: 5}); err != nil {
		t.Fatal(err)
	}
	h := NewValidationsHandler(db, slog.Default())

	cases := []struct {
		name      string
		project   int
		migration string
		want      int
	}{
		{"project setting wins", 3, "with-run-config", 3},
		{"no project setting: the run's", 0, "with-run-config", 5},
		{"neither: the default", 0, "without", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pc := *p
			pc.TransferConfig.TableWorkers = c.project
			if got := h.onDemandWorkers(ctx, c.migration, &pc); got != c.want {
				t.Errorf("got %d workers, want %d", got, c.want)
			}
		})
	}
}
