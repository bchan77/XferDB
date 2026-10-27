package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestCDCStreamAndMode(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := &adapters.Project{ID: "p1", Name: "p1"}
	if err := db.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, mg := range []*adapters.Migration{
		{ID: "m1", ProjectID: "p1", Status: adapters.StatusPending, CreatedAt: time.Now()},
		{ID: "m2", ProjectID: "p1", Status: adapters.StatusPending, CreatedAt: time.Now(), Mode: adapters.MigrationModeLive, Phase: "setup"},
	} {
		if err := db.CreateMigration(ctx, mg); err != nil {
			t.Fatal(err)
		}
	}
	m1, _ := db.GetMigration(ctx, "m1")
	m2, _ := db.GetMigration(ctx, "m2")
	if m1.Mode != adapters.MigrationModeSnapshot || m1.Phase != "" || m2.Mode != adapters.MigrationModeLive || m2.Phase != "setup" {
		t.Fatalf("modes: %+v / %+v", m1, m2)
	}
	if err := db.SetMigrationPhase(ctx, "m2", "snapshot"); err != nil {
		t.Fatal(err)
	}
	if m2, _ = db.GetMigration(ctx, "m2"); m2.Phase != "snapshot" {
		t.Fatalf("phase = %q", m2.Phase)
	}

	if s, err := db.GetCDCStream(ctx, "m2"); err != nil || s != nil {
		t.Fatalf("GetCDCStream before save = %v, %v", s, err)
	}
	st := &CDCStream{MigrationID: "m2", SlotName: "xferdb_m2", Publication: "xferdb_m2", StartPos: "0/16B3748"}
	if err := db.SaveCDCStream(ctx, st); err != nil {
		t.Fatal(err)
	}
	st.CatchupPos = "0/17000000"
	if err := db.SaveCDCStream(ctx, st); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetCDCStream(ctx, "m2")
	if err != nil || got.StartPos != "0/16B3748" || got.CatchupPos != "0/17000000" || got.SlotName != "xferdb_m2" {
		t.Fatalf("GetCDCStream = %+v, %v", got, err)
	}
	if err := db.DeleteMigration(ctx, "m2"); err != nil {
		t.Fatal(err)
	}
	if s, _ := db.GetCDCStream(ctx, "m2"); s != nil {
		t.Fatal("DeleteMigration left the stream record")
	}
}
