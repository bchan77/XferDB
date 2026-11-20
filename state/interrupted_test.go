package state

import (
	"context"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// A restart ends every snapshot migration that was running, paused ones
// included (their engine is gone, so nothing could resume or cancel them);
// live and finished ones keep their status.
func TestMarkInterruptedMigrations(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	proj := seedProject(t, db)
	now := time.Now()
	want := map[string]adapters.MigrationStatus{}
	for _, c := range []struct {
		id     string
		status adapters.MigrationStatus
		mode   string
		after  adapters.MigrationStatus
	}{
		{"pending", adapters.StatusPending, "", adapters.StatusFailed},
		{"running", adapters.StatusInProgress, "", adapters.StatusFailed},
		{"paused", adapters.StatusPaused, "", adapters.StatusFailed},
		{"done", adapters.StatusCompleted, "", adapters.StatusCompleted},
		{"cancelled", adapters.StatusCancelled, "", adapters.StatusCancelled},
		{"live-paused", adapters.StatusPaused, adapters.MigrationModeLive, adapters.StatusPaused},
		{"live-running", adapters.StatusInProgress, adapters.MigrationModeLive, adapters.StatusInProgress},
	} {
		if err := db.CreateMigration(ctx, &adapters.Migration{ID: c.id, ProjectID: proj, Status: c.status, CreatedAt: now, Mode: c.mode}); err != nil {
			t.Fatal(err)
		}
		want[c.id] = c.after
	}
	n, err := db.MarkInterruptedMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("marked %d, want 3", n)
	}
	for id, status := range want {
		m, err := db.GetMigration(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if m.Status != status {
			t.Errorf("%s: %s, want %s", id, m.Status, status)
		}
		if status == adapters.StatusFailed && m.Error != "interrupted by server restart" {
			t.Errorf("%s: error %q", id, m.Error)
		}
	}
	// Failed by the restart, a paused one can be resumed.
	if ok, err := db.ReopenMigration(ctx, "paused"); err != nil || !ok {
		t.Errorf("reopen the formerly paused migration: %v, %v", ok, err)
	}
}
