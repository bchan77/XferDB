package api

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/logring"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// A support bundle explains a failed table on its own: the table's error is
// saved and shows in migrations.json and timeline.json, next to the schema
// override saved before the run, the settings the run used, and the server's
// log lines.
func TestSupportBundleTimeline(t *testing.T) {
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
		`CREATE TABLE good (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO good VALUES (1, 'a')`,
		`CREATE TABLE bad (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO bad VALUES (1, 'a')`)
	// The target's bad table has a NOT NULL column the source doesn't fill.
	exec(tgtPath, `CREATE TABLE bad (id INTEGER PRIMARY KEY, v TEXT, must TEXT NOT NULL)`)

	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ring := logring.New(1000)
	log := slog.New(logring.NewHandler(slog.DiscardHandler, ring, slog.LevelInfo, logring.SkipOKRequests))
	s := NewServer(db, log)
	s.SetLogRing(ring, "")
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	var p struct {
		ID string `json:"id"`
	}
	post(t, srv.URL+"/api/v1/projects", map[string]any{
		"name":          "bundle",
		"source_config": map[string]any{"type": "sqlite", "dsn": srcPath},
		"target_config": map[string]any{"type": "sqlite", "dsn": tgtPath},
	}, http.StatusCreated, &p)
	// Another project's log lines stay out of this project's bundle.
	log.Info("unrelated", "project_id", "someone-else")
	log.Info("http.request", "path", "/api/v1/projects/someone-else/analyze", "status", 500)

	body := `[{"collection":"good","field_name":"v","pg_type":"TEXT","strategy":"direct","is_pk":false,"nullable":true}]`
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/api/v1/projects/"+p.ID+"/schema-plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode/100 != 2 {
		t.Fatalf("override: %v %v", resp.StatusCode, err)
	}

	var m struct {
		ID string `json:"id"`
	}
	post(t, srv.URL+"/api/v1/projects/"+p.ID+"/migrations",
		map[string]any{"validation": "off"}, http.StatusCreated, &m)
	waitFor(t, "migration to end", func() bool {
		var mig struct {
			Status string `json:"status"`
		}
		get(t, srv.URL+"/api/v1/migrations/"+m.ID, &mig)
		return mig.Status == "failed" || mig.Status == "completed"
	})

	resp, err := http.Get(srv.URL + "/api/v1/projects/" + p.ID + "/support-bundle")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	files := map[string][]byte{}
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		files[hdr.Name], _ = io.ReadAll(tr)
	}
	for _, f := range []string{"README.txt", "timeline.json", "logs.jsonl", "migrations.json", "migration_stats.json", "validations.json", "schema_plan.json"} {
		if _, ok := files[f]; !ok {
			t.Errorf("bundle has no %s", f)
		}
	}

	var migs []struct {
		ID        string          `json:"id"`
		RunConfig json.RawMessage `json:"run_config"`
		Tables    []struct {
			TableName string `json:"table_name"`
			Status    string `json:"status"`
			Error     string `json:"error"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(files["migrations.json"], &migs); err != nil || len(migs) != 1 {
		t.Fatalf("migrations.json %s: %v", files["migrations.json"], err)
	}
	var rc struct {
		Validation string `json:"validation"`
	}
	if json.Unmarshal(migs[0].RunConfig, &rc); rc.Validation != "off" {
		t.Errorf("run_config %s, want the run's settings", migs[0].RunConfig)
	}
	var badErr string
	for _, tb := range migs[0].Tables {
		if tb.TableName == "bad" {
			badErr = tb.Error
			if tb.Status != "failed" {
				t.Errorf("bad: status %s", tb.Status)
			}
		} else if tb.Error != "" {
			t.Errorf("%s: error %q on a table that didn't fail", tb.TableName, tb.Error)
		}
	}
	if !strings.Contains(badErr, "NOT NULL") {
		t.Errorf("bad table error %q, want the NOT NULL failure", badErr)
	}

	var timeline []struct {
		Event  string `json:"event"`
		Table  string `json:"table"`
		Status string `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(files["timeline.json"], &timeline); err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, e := range timeline {
		key := e.Event
		if e.Table != "" {
			key += ":" + e.Table
		}
		if _, ok := pos[key]; !ok {
			pos[key] = i
		}
		if e.Event == "table.ended" && e.Table == "bad" && !strings.Contains(e.Detail, "NOT NULL") {
			t.Errorf("timeline table.ended bad: %q, want its error", e.Detail)
		}
	}
	for _, k := range []string{"schema.override:good", "migration.created", "table.ended:bad", "migration.ended"} {
		if _, ok := pos[k]; !ok {
			t.Errorf("timeline has no %s: %s", k, files["timeline.json"])
		}
	}
	if pos["schema.override:good"] > pos["migration.created"] || pos["table.ended:bad"] > pos["migration.ended"] {
		t.Errorf("timeline out of order: %s", files["timeline.json"])
	}

	logs := string(files["logs.jsonl"])
	if !strings.Contains(logs, m.ID) || strings.Contains(logs, "someone-else") {
		t.Errorf("logs.jsonl: want this migration's lines and not another project's:\n%s", logs)
	}
}
