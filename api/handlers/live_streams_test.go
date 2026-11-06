package handlers

import (
	"context"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
)

// fakeStreams is a source that lists the streams it is given.
type fakeStreams struct {
	adapters.SourceAdapter
	streams []adapters.LiveStream
}

func (f *fakeStreams) LiveStreams(context.Context) ([]adapters.LiveStream, error) {
	return f.streams, nil
}
func (f *fakeStreams) DropLiveStream(context.Context, string) error { return nil }

// A stream is a leftover unless a migration still running on this server
// owns it: one whose migration ended, or that no migration here owns.
func TestClassifyStreams(t *testing.T) {
	_, db, cleanup := newTestHandler(t)
	defer cleanup()
	ctx := context.Background()
	p := &adapters.Project{ID: "p", Name: "proj"}
	if err := db.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	for _, m := range []adapters.Migration{
		{ID: "11111111-aaaa", ProjectID: "p", Status: adapters.StatusInProgress, Mode: adapters.MigrationModeLive},
		{ID: "22222222-bbbb", ProjectID: "p", Status: adapters.StatusCancelled, Mode: adapters.MigrationModeLive},
	} {
		m.CreatedAt = time.Now()
		if err := db.CreateMigration(ctx, &m); err != nil {
			t.Fatal(err)
		}
		if m.Status != adapters.StatusPending {
			if err := db.SetMigrationStatus(ctx, m.ID, m.Status); err != nil {
				t.Fatal(err)
			}
		}
	}
	running, ended, unknown := postgres.SlotName("11111111-aaaa"), postgres.SlotName("22222222-bbbb"), "xferdb_0123456789"
	src := &fakeStreams{streams: []adapters.LiveStream{
		{Name: running, Slot: true, Publication: true, Active: true},
		{Name: ended, Slot: true, Publication: true},
		{Name: unknown, Publication: true},
	}}
	streams, ok, err := classifyStreams(ctx, db, src)
	if !ok || err != nil || len(streams) != 3 {
		t.Fatalf("classifyStreams = %+v, %v, %v", streams, ok, err)
	}
	byName := map[string]liveStreamInfo{}
	for _, s := range streams {
		byName[s.Name] = s
	}
	if s := byName[running]; s.Leftover || s.MigrationID != "11111111-aaaa" || s.ProjectName != "proj" {
		t.Errorf("running migration's stream: %+v", s)
	}
	if s := byName[ended]; !s.Leftover || s.MigrationStatus != "cancelled" {
		t.Errorf("cancelled migration's stream: %+v", s)
	}
	if s := byName[unknown]; !s.Leftover || s.MigrationID != "" {
		t.Errorf("unknown stream: %+v", s)
	}
	c, any := leftoverCheck(streams)
	if !any || !c.Warning || !c.OK || c.Detail != "2 on the source: "+ended+", "+unknown {
		t.Errorf("leftover check = %+v, %v", c, any)
	}
	if _, any := leftoverCheck(streams[:0]); any {
		t.Error("a leftover check with no streams")
	}
}
