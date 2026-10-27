package handlers

import (
	"database/sql"

	"context"
	"encoding/json"
	_ "github.com/lib/pq"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func runPreflight(t *testing.T, h *ProjectsHandler, id, body string) (int, map[string]json.RawMessage) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/"+id+"/preflight", strings.NewReader(body))
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	h.Preflight(rec, req)
	var out map[string]json.RawMessage
	json.Unmarshal(rec.Body.Bytes(), &out) //nolint:errcheck
	return rec.Code, out
}

// The live checks run only when asked for, and a source live migration
// can't stream from yet fails them with the reason.
func TestPreflightLiveMode(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	dir := t.TempDir()
	p := &adapters.Project{ID: "p", Name: "p",
		SourceConfig: adapters.ConnectionConfig{Type: "sqlite", Database: filepath.Join(dir, "src.db")},
		TargetConfig: adapters.ConnectionConfig{Type: "sqlite", Database: filepath.Join(dir, "dst.db")},
	}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatal(err)
	}

	code, out := runPreflight(t, h, "p", "")
	if code != http.StatusOK || out["live"] != nil || string(out["status"]) != `"ready"` {
		t.Fatalf("plain preflight: %d %s", code, out)
	}

	code, out = runPreflight(t, h, "p", `{"mode":"live"}`)
	var live liveResult
	if err := json.Unmarshal(out["live"], &live); code != http.StatusOK || err != nil {
		t.Fatalf("live preflight: %d %v %s", code, err, out)
	}
	if live.Ready || string(out["status"]) != `"failed"` {
		t.Fatalf("sqlite source should not be ready for live: %+v", live)
	}
	var sawSource, sawTarget bool
	for _, c := range live.Checks {
		if strings.Contains(c.Fix, "PostgreSQL sources only") && !c.OK {
			sawSource = true
		}
		if c.Name == "a target that can apply changes" && c.OK {
			sawTarget = true
		}
	}
	if !sawSource || !sawTarget {
		t.Fatalf("checks = %+v", live.Checks)
	}

	if code, _ := runPreflight(t, h, "p", `{"mode":"bogus"}`); code != http.StatusBadRequest {
		t.Fatalf("bad mode: %d", code)
	}
}

// Against a real logical-replication server every check passes, and a
// keyless table fails just its check.
func TestPreflightLivePostgres(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set")
	}
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	dir := t.TempDir()
	p := &adapters.Project{ID: "p", Name: "p",
		SourceConfig:   adapters.ConnectionConfig{Type: "postgres", DSN: dsn},
		TargetConfig:   adapters.ConnectionConfig{Type: "sqlite", Database: filepath.Join(dir, "dst.db")},
		TransferConfig: adapters.TransferConfig{Tables: []string{"xferdb_preflight_k", "xferdb_preflight_nok"}},
	}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	src, _ := sqlOpen(dsn)
	defer src.Close()
	src.Exec(`CREATE TABLE IF NOT EXISTS xferdb_preflight_k (id int PRIMARY KEY); CREATE TABLE IF NOT EXISTS xferdb_preflight_nok (id int)`)
	defer src.Exec(`DROP TABLE IF EXISTS xferdb_preflight_k, xferdb_preflight_nok`)

	_, out := runPreflight(t, h, "p", `{"mode":"live"}`)
	var live liveResult
	if err := json.Unmarshal(out["live"], &live); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	for _, c := range live.Checks {
		wantOK := c.Name != "a primary key on every table"
		if c.OK != wantOK && !c.Warning {
			t.Errorf("check %q ok=%v: %s", c.Name, c.OK, c.Detail)
		}
		if !wantOK && !strings.Contains(c.Detail, "xferdb_preflight_nok") {
			t.Errorf("key check detail %q doesn't name the table", c.Detail)
		}
	}
	if live.Ready {
		t.Fatal("ready despite a keyless table")
	}
}

func sqlOpen(dsn string) (*sql.DB, error) { return sql.Open("postgres", dsn) }
