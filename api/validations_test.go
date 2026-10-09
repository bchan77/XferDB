package api

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// newSQLiteProject starts an API server and creates a SQLite -> SQLite
// project whose source holds one small table.
func newSQLiteProject(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)`,
		`INSERT INTO items VALUES (1, 'a', 1), (2, 'b', 0), (3, NULL, NULL)`,
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
	srv := httptest.NewServer(NewServer(db, slog.New(slog.DiscardHandler)).Handler())
	t.Cleanup(srv.Close)

	var p struct {
		ID string `json:"id"`
	}
	post(t, srv.URL+"/api/v1/projects", map[string]any{
		"name":          "s2s",
		"source_config": map[string]any{"type": "sqlite", "dsn": srcPath},
		"target_config": map[string]any{"type": "sqlite", "dsn": filepath.Join(dir, "tgt.db")},
	}, http.StatusCreated, &p)
	return srv, p.ID
}

func post(t *testing.T, url string, body any, wantStatus int, out any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		t.Fatalf("POST %s: status %d (%v), want %d", url, resp.StatusCode, e["error"], wantStatus)
	}
	if out != nil {
		json.NewDecoder(resp.Body).Decode(out)
	}
}

func get(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(out)
}

type validationResult struct {
	ID     string `json:"id"`
	Level  string `json:"level"`
	Status string `json:"status"`
	Tables []struct {
		TableName string `json:"table_name"`
		Result    string `json:"result"`
		Stale     bool   `json:"stale"`
	} `json:"tables"`
}

// waitFor polls until cond returns true or fails the test after 10s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func listValidations(t *testing.T, srv *httptest.Server, migrationID string) []validationResult {
	var vs []validationResult
	get(t, srv.URL+"/api/v1/migrations/"+migrationID+"/validations", &vs)
	return vs
}

func awaitValidation(t *testing.T, srv *httptest.Server, id string) validationResult {
	t.Helper()
	var v validationResult
	waitFor(t, "validation to finish", func() bool {
		get(t, srv.URL+"/api/v1/validations/"+id, &v)
		return v.Status == "completed" || v.Status == "failed" || v.Status == "cancelled"
	})
	return v
}

func startMigration(t *testing.T, srv *httptest.Server, projectID string, body map[string]any) (string, string) {
	t.Helper()
	var m struct {
		ID         string `json:"id"`
		Validation string `json:"validation"`
	}
	post(t, srv.URL+"/api/v1/projects/"+projectID+"/migrations", body, http.StatusCreated, &m)
	waitFor(t, "migration to complete", func() bool {
		var mig struct {
			Status string `json:"status"`
		}
		get(t, srv.URL+"/api/v1/migrations/"+m.ID, &mig)
		return mig.Status == "completed"
	})
	return m.ID, m.Validation
}

func TestMigrationIsValidatedByDefault(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	migID, mode := startMigration(t, srv, projectID, map[string]any{})
	if mode != "after" {
		t.Fatalf("effective validation mode %q, want after", mode)
	}

	var vs []validationResult
	waitFor(t, "auto validation to appear", func() bool {
		vs = listValidations(t, srv, migID)
		return len(vs) > 0
	})
	v := awaitValidation(t, srv, vs[0].ID)
	if v.Status != "completed" || v.Level != "L2" {
		t.Fatalf("validation %s at %s, want completed at L2", v.Status, v.Level)
	}
	if len(v.Tables) != 1 || v.Tables[0].Result != "pass" {
		t.Fatalf("tables = %+v, want items: pass", v.Tables)
	}
}

func TestSkipValidationPerRun(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	migID, mode := startMigration(t, srv, projectID, map[string]any{"validation": "off"})
	if mode != "off" {
		t.Fatalf("effective validation mode %q, want off", mode)
	}
	// Give a would-be auto validation time to show up.
	time.Sleep(300 * time.Millisecond)
	if vs := listValidations(t, srv, migID); len(vs) != 0 {
		t.Fatalf("validation ran despite validation=off: %+v", vs)
	}

	// A manual run still works when the mode is off.
	var v validationResult
	post(t, srv.URL+"/api/v1/migrations/"+migID+"/validations", map[string]any{"level": "L1"}, http.StatusCreated, &v)
	if got := awaitValidation(t, srv, v.ID); got.Status != "completed" || got.Tables[0].Result != "pass" {
		t.Fatalf("manual validation = %+v", got)
	}
}

func TestValidationRejectsUnavailableOptions(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	post(t, srv.URL+"/api/v1/projects/"+projectID+"/migrations", map[string]any{"validation": "per-table"}, http.StatusBadRequest, nil)
	post(t, srv.URL+"/api/v1/projects/"+projectID+"/migrations", map[string]any{"validation_level": "L4"}, http.StatusBadRequest, nil)
	post(t, srv.URL+"/api/v1/projects/"+projectID+"/migrations", map[string]any{"validation_sample": 101}, http.StatusBadRequest, nil)

	migID, _ := startMigration(t, srv, projectID, map[string]any{"validation": "off"})
	url := srv.URL + "/api/v1/migrations/" + migID + "/validations"
	post(t, url, map[string]any{"level": "L4"}, http.StatusBadRequest, nil)
	post(t, url, map[string]any{"level": "L3", "sample": -1}, http.StatusBadRequest, nil)
	post(t, url, map[string]any{"level": "L2", "sample": 10}, http.StatusBadRequest, nil)
}

func TestL3ValidationRecordsDiffs(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	migID, _ := startMigration(t, srv, projectID, map[string]any{"validation": "off"})

	var p struct {
		TargetConfig struct {
			DSN string `json:"dsn"`
		} `json:"target_config"`
	}
	get(t, srv.URL+"/api/v1/projects/"+projectID, &p)
	tgt, err := sql.Open("sqlite3", p.TargetConfig.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	if _, err := tgt.Exec(`UPDATE items SET name = 'changed' WHERE id = 2`); err != nil {
		t.Fatal(err)
	}

	var started struct {
		ID      string `json:"id"`
		Level   string `json:"level"`
		Options struct {
			Sample int `json:"sample"`
		} `json:"options"`
	}
	post(t, srv.URL+"/api/v1/migrations/"+migID+"/validations", map[string]any{"level": "L3", "sample": 100}, http.StatusCreated, &started)
	if started.Level != "L3" || started.Options.Sample != 0 {
		t.Fatalf("started at %s with sample %d, want L3 with sample 0 (100%% is every row)", started.Level, started.Options.Sample)
	}
	awaitValidation(t, srv, started.ID)

	var v struct {
		Tables []struct {
			TableName    string `json:"table_name"`
			Result       string `json:"result"`
			LevelRun     string `json:"level_run"`
			DiffMismatch int64  `json:"diff_mismatch"`
		} `json:"tables"`
	}
	get(t, srv.URL+"/api/v1/validations/"+started.ID, &v)
	if len(v.Tables) != 1 || v.Tables[0].Result != "fail" || v.Tables[0].LevelRun != "L3" || v.Tables[0].DiffMismatch != 1 {
		t.Fatalf("tables = %+v, want items failing at L3 with 1 mismatch", v.Tables)
	}

	var diffs []struct {
		Kind    string             `json:"kind"`
		Key     map[string]*string `json:"key"`
		Columns []string           `json:"columns"`
		Source  map[string]*string `json:"source"`
		Target  map[string]*string `json:"target"`
	}
	get(t, srv.URL+"/api/v1/validations/"+started.ID+"/tables/items/diffs", &diffs)
	if len(diffs) != 1 {
		t.Fatalf("diffs = %+v, want one", diffs)
	}
	d := diffs[0]
	if d.Kind != "value_mismatch" || *d.Key["id"] != "2" || len(d.Columns) != 1 || d.Columns[0] != "name" ||
		*d.Source["name"] != "b" || *d.Target["name"] != "changed" {
		t.Errorf("diff = %+v", d)
	}
}

func TestValidationMarksStaleAfterRemigration(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	firstID, _ := startMigration(t, srv, projectID, map[string]any{"validation": "off"})
	var v validationResult
	post(t, srv.URL+"/api/v1/migrations/"+firstID+"/validations", map[string]any{}, http.StatusCreated, &v)
	if got := awaitValidation(t, srv, v.ID); got.Tables[0].Stale {
		t.Fatal("fresh result reported stale")
	}

	startMigration(t, srv, projectID, map[string]any{"validation": "off", "truncate": true})
	if got := awaitValidation(t, srv, v.ID); !got.Tables[0].Stale {
		t.Fatal("result not marked stale after the table was migrated again")
	}
}

func TestValidationSkipsTablesNotCompleted(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	migID, _ := startMigration(t, srv, projectID, map[string]any{"validation": "off"})
	var v validationResult
	post(t, srv.URL+"/api/v1/migrations/"+migID+"/validations",
		map[string]any{"tables": []string{"items", "nope"}}, http.StatusCreated, &v)
	got := awaitValidation(t, srv, v.ID)
	results := map[string]string{}
	for _, tb := range got.Tables {
		results[tb.TableName] = tb.Result
	}
	if results["items"] != "pass" || results["nope"] != "skipped" {
		t.Fatalf("results = %v, want items pass and nope skipped", results)
	}
}

func put(t *testing.T, url string, body any, wantStatus int) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		var e map[string]any
		json.NewDecoder(resp.Body).Decode(&e)
		t.Fatalf("PUT %s: status %d (%v), want %d", url, resp.StatusCode, e["error"], wantStatus)
	}
}

type validationStatus struct {
	Active *struct {
		ID string `json:"id"`
	} `json:"active"`
	Tables []struct {
		TableName   string `json:"table_name"`
		Result      string `json:"result"`
		LevelRun    string `json:"level_run"`
		MigrationID string `json:"migration_id"`
		Stale       bool   `json:"stale"`
	} `json:"tables"`
}

func TestValidationStatusReportsLatestPerTable(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	statusURL := srv.URL + "/api/v1/projects/" + projectID + "/validation-status"

	var st validationStatus
	get(t, statusURL, &st)
	if st.Active != nil || len(st.Tables) != 0 {
		t.Fatalf("fresh project status = %+v, want no tables and nothing active", st)
	}

	firstID, _ := startMigration(t, srv, projectID, map[string]any{"validation_level": "L1"})
	waitFor(t, "auto validation to finish", func() bool {
		get(t, statusURL, &st)
		return st.Active == nil && len(st.Tables) == 1
	})
	if tb := st.Tables[0]; tb.TableName != "items" || tb.Result != "pass" || tb.LevelRun != "L1" || tb.MigrationID != firstID || tb.Stale {
		t.Fatalf("after first migration: %+v", tb)
	}

	// A later migration that skips validation leaves the earlier result in
	// place, now stale.
	startMigration(t, srv, projectID, map[string]any{"validation": "off", "truncate": true})
	get(t, statusURL, &st)
	if len(st.Tables) != 1 || !st.Tables[0].Stale || st.Tables[0].MigrationID != firstID {
		t.Fatalf("after unvalidated re-migration: %+v, want the first result marked stale", st.Tables)
	}

	var missing map[string]any
	resp, err := http.Get(srv.URL + "/api/v1/projects/nope/validation-status")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&missing)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown project: status %d, want 404", resp.StatusCode)
	}
}

func TestUpdateProjectSetsValidationDefaults(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	projURL := srv.URL + "/api/v1/projects/" + projectID

	var p map[string]any
	get(t, projURL, &p)
	tc, _ := p["transfer_config"].(map[string]any)
	if tc == nil {
		tc = map[string]any{}
	}
	tc["validation"] = "off"
	tc["validation_level"] = "L1"
	body := map[string]any{
		"name": p["name"], "description": p["description"],
		"source_config": p["source_config"], "target_config": p["target_config"],
		"transfer_config": tc,
	}
	put(t, projURL, body, http.StatusOK)

	var after struct {
		TransferConfig struct {
			Validation      string `json:"validation"`
			ValidationLevel string `json:"validation_level"`
		} `json:"transfer_config"`
	}
	get(t, projURL, &after)
	if after.TransferConfig.Validation != "off" || after.TransferConfig.ValidationLevel != "L1" {
		t.Fatalf("transfer_config after update = %+v", after.TransferConfig)
	}

	// The project default now applies to a run that doesn't override it.
	if _, mode := startMigration(t, srv, projectID, map[string]any{}); mode != "off" {
		t.Fatalf("effective mode %q, want the project's off", mode)
	}

	// An update without transfer_config leaves it alone.
	delete(body, "transfer_config")
	put(t, projURL, body, http.StatusOK)
	get(t, projURL, &after)
	if after.TransferConfig.Validation != "off" {
		t.Fatalf("update without transfer_config changed it: %+v", after.TransferConfig)
	}

	// Values the server can't run are rejected.
	body["transfer_config"] = map[string]any{"validation": "sometimes"}
	put(t, projURL, body, http.StatusBadRequest)
}

func TestPerTableValidation(t *testing.T) {
	srv, projectID := newSQLiteProject(t)
	migID, mode := startMigration(t, srv, projectID, map[string]any{"validation": "per_table", "table_workers": 2})
	if mode != "per_table" {
		t.Fatalf("effective validation mode %q, want per_table", mode)
	}

	var vs []validationResult
	waitFor(t, "per-table validation to appear", func() bool {
		vs = listValidations(t, srv, migID)
		return len(vs) > 0
	})
	if len(vs) != 1 {
		t.Fatalf("%d validations, want exactly one per migration", len(vs))
	}
	v := awaitValidation(t, srv, vs[0].ID)
	if v.Status != "completed" || len(v.Tables) != 1 || v.Tables[0].Result != "pass" {
		t.Fatalf("validation = %+v, want completed with items: pass", v)
	}
	var full struct {
		Options struct {
			Mode string `json:"mode"`
		} `json:"options"`
	}
	get(t, srv.URL+"/api/v1/validations/"+v.ID, &full)
	if full.Options.Mode != "per_table" {
		t.Errorf("options.mode = %q, want per_table", full.Options.Mode)
	}
}

