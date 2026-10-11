package engine

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// With a subscriber, a full channel must not drop table or terminal events,
// only batch events (#137).
func TestEmitDeliversLifecycleEventsWhenFull(t *testing.T) {
	e := New("m1", &adapters.Project{}, nil)
	events := e.Events()
	for i := 0; i < cap(e.events); i++ {
		e.emit(ProgressEvent{Kind: EventBatch})
	}
	e.emit(ProgressEvent{Kind: EventBatch, TableName: "dropped"}) // full: dropped, must not block

	sent := make(chan struct{})
	go func() {
		e.emit(ProgressEvent{Kind: EventTableFailed, TableName: "t"})
		e.emit(ProgressEvent{Kind: EventError})
		close(sent)
	}()
	// Nothing has been read yet, so a delivering emit is still waiting.
	select {
	case <-sent:
		t.Fatal("emit returned while the channel was full: lifecycle event dropped")
	case <-time.After(100 * time.Millisecond):
	}

	var got []ProgressEvent
	timeout := time.After(5 * time.Second)
	for len(got) < cap(e.events)+2 {
		select {
		case ev := <-events:
			got = append(got, ev)
		case <-timeout:
			t.Fatalf("received %d events, want %d", len(got), cap(e.events)+2)
		}
	}
	<-sent
	for _, ev := range got {
		if ev.TableName == "dropped" {
			t.Error("batch event sent to a full channel was delivered, want dropped")
		}
	}
	if tail := got[len(got)-2:]; tail[0].Kind != EventTableFailed || tail[1].Kind != EventError {
		t.Errorf("last events = %s, %s; want %s, %s", tail[0].Kind, tail[1].Kind, EventTableFailed, EventError)
	}
}

// With no subscriber, nothing drains the channel, so emit must never block.
func TestEmitWithoutSubscriberNeverBlocks(t *testing.T) {
	e := New("m1", &adapters.Project{}, nil)
	done := make(chan struct{})
	go func() {
		for i := 0; i < cap(e.events)+10; i++ {
			e.emit(ProgressEvent{Kind: EventTableDone})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emit blocked with no subscriber")
	}
}

// A table copied by the segment-parallel path whose completed status can't be
// saved must fail, not finish unrecorded where validation would skip it (#138).
func TestParallelCompletedWriteFailureFailsTable(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	statePath := filepath.Join(dir, "state.db")

	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, stmt := range []string{
		`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')`,
		`CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)`,
		`INSERT INTO tags VALUES (1, 'x'), (2, 'y')`,
	} {
		if _, err := src.Exec(stmt); err != nil {
			t.Fatalf("seed source: %v\n%s", err, stmt)
		}
	}

	meta, err := state.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { meta.Close() })

	// Reject only the completed write for items, as a lock or I/O error would.
	raw, err := sql.Open("sqlite3", statePath)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"INSERT", "UPDATE"} {
		if _, err := raw.Exec(`CREATE TRIGGER reject_items_completed_` + op + ` BEFORE ` + op + ` ON migration_tables
			WHEN NEW.table_name = 'items' AND NEW.status = 'completed'
			BEGIN SELECT RAISE(ABORT, 'injected write failure'); END`); err != nil {
			t.Fatal(err)
		}
	}
	raw.Close()

	p := &adapters.Project{
		ID:             "p1",
		Name:           "parallel",
		SourceConfig:   adapters.ConnectionConfig{Type: "sqlite", DSN: srcPath},
		TargetConfig:   adapters.ConnectionConfig{Type: "sqlite", DSN: filepath.Join(dir, "tgt.db")},
		TransferConfig: adapters.TransferConfig{SegmentWorkers: 2},
		CreatedAt:      time.Now(),
	}
	ctx := context.Background()
	if err := meta.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	mig := &adapters.Migration{ID: "m1", ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
	if err := meta.CreateMigration(ctx, mig); err != nil {
		t.Fatal(err)
	}

	err = New(mig.ID, p, meta).Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "items") || !strings.Contains(err.Error(), "injected write failure") {
		t.Fatalf("Run error = %v, want items failing with the injected write failure", err)
	}

	progress, err := meta.GetTableProgress(ctx, mig.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]adapters.MigrationStatus{"items": adapters.StatusFailed, "tags": adapters.StatusCompleted}
	for _, tp := range progress {
		if w, ok := want[tp.TableName]; ok && tp.Status != w {
			t.Errorf("%s: status %s, want %s", tp.TableName, tp.Status, w)
		}
		delete(want, tp.TableName)
	}
	for name := range want {
		t.Errorf("%s: no migration_tables row", name)
	}
}
