package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

func patch(t *testing.T, url string, body any, wantStatus int) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPatch, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != wantStatus {
		t.Fatalf("PATCH %s: status %d (%v), want %d", url, resp.StatusCode, out["error"], wantStatus)
	}
	return out
}

// A migration whose post-schema fails after the copy is resumed: the copied
// table isn't dropped (recreate_schema) or copied again, the failed index is
// created, and the migration completes.
func TestResumeFailedMigrationAfterCopy(t *testing.T) {
	dir := t.TempDir()
	srcPath, tgtPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "tgt.db")
	exec := func(path string, stmts ...string) {
		t.Helper()
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	exec(srcPath,
		`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
		`CREATE INDEX items_name_ix ON items (name)`,
		`INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	// A table on the target named like the index makes creating it fail.
	exec(tgtPath, `CREATE TABLE items_name_ix (x INTEGER)`)

	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv := httptest.NewServer(NewServer(db, slog.New(slog.DiscardHandler)).Handler())
	defer srv.Close()
	var p struct {
		ID string `json:"id"`
	}
	post(t, srv.URL+"/api/v1/projects", map[string]any{
		"name":          "resume",
		"source_config": map[string]any{"type": "sqlite", "dsn": srcPath},
		"target_config": map[string]any{"type": "sqlite", "dsn": tgtPath},
	}, http.StatusCreated, &p)

	var m struct {
		ID string `json:"id"`
	}
	post(t, srv.URL+"/api/v1/projects/"+p.ID+"/migrations",
		map[string]any{"recreate_schema": true, "validation": "off"}, http.StatusCreated, &m)
	migURL := srv.URL + "/api/v1/migrations/" + m.ID
	status := func() string {
		var mig struct {
			Status string `json:"status"`
		}
		get(t, migURL, &mig)
		return mig.Status
	}
	waitFor(t, "migration to fail", func() bool { return status() == "failed" })

	// A completed migration can't be resumed; a live one is covered by its
	// own check. The table was copied; mark a row so a second copy (or a
	// drop) would show.
	exec(tgtPath, `UPDATE items SET name = 'kept' WHERE id = 1`, `DROP TABLE items_name_ix`)

	patch(t, migURL, map[string]any{"action": "resume"}, http.StatusAccepted)
	waitFor(t, "resumed migration to complete", func() bool { return status() == "completed" })
	patch(t, migURL, map[string]any{"action": "resume"}, http.StatusConflict)

	tgt, err := sql.Open("sqlite3", tgtPath)
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	var name string
	var n int
	if err := tgt.QueryRow(`SELECT name FROM items WHERE id = 1`).Scan(&name); err != nil || name != "kept" {
		t.Errorf("row 1 name %q (%v): the completed table was dropped or copied again", name, err)
	}
	tgt.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'items_name_ix'`).Scan(&n)
	if n != 1 {
		t.Errorf("index items_name_ix not created by the resumed run")
	}

	var steps []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	get(t, migURL+"/schema-steps", &steps)
	if len(steps) != 2 || steps[0].Status != "failed" || steps[1].Status != "done" {
		t.Errorf("schema steps %+v, want the failed attempt then the done one", steps)
	}
}

// A migration paused when the server stopped has no engine after a restart.
// Left paused, nothing could resume it (409) or cancel it (404); the restart
// (Recover) now fails it as interrupted, and Resume runs it to completion.
func TestResumeMigrationPausedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	srcPath, tgtPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "tgt.db")
	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`, `INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')`} {
		if _, err := src.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	src.Close()

	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)

	// The server before the restart: it paused this migration, then died.
	before := httptest.NewServer(NewServer(db, log).Handler())
	var p struct {
		ID string `json:"id"`
	}
	post(t, before.URL+"/api/v1/projects", map[string]any{
		"name":          "paused-restart",
		"source_config": map[string]any{"type": "sqlite", "dsn": srcPath},
		"target_config": map[string]any{"type": "sqlite", "dsn": tgtPath},
	}, http.StatusCreated, &p)
	const id = "paused-across-restart"
	if err := db.CreateMigration(ctx, &adapters.Migration{ID: id, ProjectID: p.ID, Status: adapters.StatusPaused, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetRunConfig(ctx, id, adapters.TransferConfig{Validation: "off"}); err != nil {
		t.Fatal(err)
	}
	// Paused with no engine: neither resume nor cancel can reach it.
	patch(t, before.URL+"/api/v1/migrations/"+id, map[string]any{"action": "resume"}, http.StatusConflict)
	patch(t, before.URL+"/api/v1/migrations/"+id, map[string]any{"action": "cancel"}, http.StatusNotFound)
	before.Close()

	// The restart.
	s := NewServer(db, log)
	s.Recover(ctx)
	after := httptest.NewServer(s.Handler())
	defer after.Close()
	migURL := after.URL + "/api/v1/migrations/" + id
	var mig struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	get(t, migURL, &mig)
	if mig.Status != "failed" || mig.Error != "interrupted by server restart" {
		t.Fatalf("after the restart: %s (%q), want failed, interrupted by server restart", mig.Status, mig.Error)
	}

	patch(t, migURL, map[string]any{"action": "resume"}, http.StatusAccepted)
	waitFor(t, "resumed migration to complete", func() bool {
		get(t, migURL, &mig)
		return mig.Status == "completed"
	})
	tgt, err := sql.Open("sqlite3", tgtPath)
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	var n int
	if err := tgt.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&n); err != nil || n != 3 {
		t.Errorf("target items: %d rows (%v), want 3", n, err)
	}
}
