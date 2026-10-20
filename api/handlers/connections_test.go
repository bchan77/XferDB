package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

func postListDatabases(t *testing.T, h *ProjectsHandler, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections/databases", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	h.ListDatabases(rec, req)
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func TestListDatabasesRejectsBadRequests(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	cases := map[string]listDatabasesRequest{
		"sqlite":  {Config: adapters.ConnectionConfig{Type: "sqlite", DSN: "/tmp/x.db"}},
		"mongodb": {Config: adapters.ConnectionConfig{Type: "mongodb", DSN: "mongodb://h/db"}},
		"no host": {Config: adapters.ConnectionConfig{Type: "postgres"}},
	}
	for name, body := range cases {
		rec, _ := postListDatabases(t, h, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, rec.Code)
		}
	}
	rec, _ := postListDatabases(t, h, listDatabasesRequest{
		Config:    adapters.ConnectionConfig{Type: "postgres", Host: "h"},
		ProjectID: "no-such-project",
	})
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown project: status %d, want 404", rec.Code)
	}
}

// A server that isn't listening is a 422 carrying the driver's error and the
// hint, for the form to show under the fields.
func TestListDatabasesReportsConnectionError(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	for _, typ := range []string{"postgres", "mysql"} {
		rec, out := postListDatabases(t, h, listDatabasesRequest{
			Config: adapters.ConnectionConfig{Type: typ, Host: "127.0.0.1", Port: port, Username: "u", Password: "p"},
		})
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status %d, want 422 (body %s)", typ, rec.Code, rec.Body)
		}
		msg, _ := out["error"].(string)
		if !strings.Contains(msg, "connection refused") || !strings.Contains(msg, "check that the host and port") {
			t.Errorf("%s: error %q lacks the driver error or the hint", typ, msg)
		}
	}
}

// The stored password is reused only while the form still points at the
// stored server and login.
func TestFillStoredPassword(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	p := &adapters.Project{
		ID: "p1", Name: "p1",
		SourceConfig: adapters.ConnectionConfig{Type: "postgres", Host: "src", Username: "alice", Password: "s3cret"},
		TargetConfig: adapters.ConnectionConfig{Type: "mysql", Host: "tgt", Port: 3306, Username: "bob", Password: "t4rget"},
	}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		side string
		cfg  adapters.ConnectionConfig
		want string
	}{
		{"source unchanged", "source", adapters.ConnectionConfig{Type: "postgres", Host: "src", Port: 5432, Username: "alice"}, "s3cret"},
		{"target default port", "target", adapters.ConnectionConfig{Type: "mysql", Host: "tgt", Username: "bob"}, "t4rget"},
		{"host changed", "source", adapters.ConnectionConfig{Type: "postgres", Host: "evil", Username: "alice"}, ""},
		{"user changed", "target", adapters.ConnectionConfig{Type: "mysql", Host: "tgt", Username: "eve"}, ""},
		{"port changed", "source", adapters.ConnectionConfig{Type: "postgres", Host: "src", Port: 6543, Username: "alice"}, ""},
	}
	for _, c := range cases {
		cfg := c.cfg
		if err := h.fillStoredPassword(context.Background(), &cfg, "p1", c.side); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if cfg.Password != c.want {
			t.Errorf("%s: password %q, want %q", c.name, cfg.Password, c.want)
		}
	}
}

// TestListDatabasesLive lists the databases of the server named by
// XFERDB_TEST_TARGET_DSN (postgres:// or mysql://) with the database name
// left blank, as the form sends it, and expects the DSN's own database among
// them.
func TestListDatabasesLive(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set")
	}
	cfg, err := registry.ParseConnectionString(dsn)
	if err != nil {
		t.Fatal(err)
	}
	want := cfg.Database
	cfg.Database, cfg.DSN = "", ""
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	rec, out := postListDatabases(t, h, listDatabasesRequest{Config: cfg})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	dbs, _ := out["databases"].([]any)
	for _, d := range dbs {
		if d == want {
			return
		}
	}
	t.Errorf("databases %v lack %q", dbs, want)
}

func TestCreateTargetDatabaseRejectsBadRequests(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	for id, tgt := range map[string]adapters.ConnectionConfig{
		"sqlite": {Type: "sqlite", DSN: "/tmp/x.db"},
		"nodb":   {Type: "postgres", Host: "h"},
	} {
		if err := db.CreateProject(context.Background(), &adapters.Project{ID: id, Name: id, TargetConfig: tgt}); err != nil {
			t.Fatal(err)
		}
	}
	for id, want := range map[string]int{"sqlite": 400, "nodb": 400, "missing": 404} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/"+id+"/target-database", nil)
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		h.CreateTargetDatabase(rec, req)
		if rec.Code != want {
			t.Errorf("%s: status %d, want %d (%s)", id, rec.Code, want, rec.Body)
		}
	}
}

// TestCreateTargetDatabaseLive points a project's target at a database that
// doesn't exist on the XFERDB_TEST_TARGET_DSN server: preflight must offer to
// create it, creating it twice must succeed, and preflight must then pass
// the connection.
func TestCreateTargetDatabaseLive(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set")
	}
	cfg, err := registry.ParseConnectionString(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin := cfg
	cfg.DSN = ""
	cfg.Database = fmt.Sprintf("xferdb_create_test_%d", time.Now().UnixNano())
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	p := &adapters.Project{ID: "p", Name: "p", SourceConfig: cfg, TargetConfig: cfg}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropTestDatabase(t, admin, cfg.Database) })

	preflight := func() map[string]any {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/p/preflight", nil)
		req.SetPathValue("id", "p")
		rec := httptest.NewRecorder()
		h.Preflight(rec, req)
		var out map[string]any
		json.Unmarshal(rec.Body.Bytes(), &out)
		return out
	}
	if got := preflight()["target_missing_database"]; got != cfg.Database {
		t.Fatalf("target_missing_database = %v, want %q", got, cfg.Database)
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/projects/p/target-database", nil)
		req.SetPathValue("id", "p")
		rec := httptest.NewRecorder()
		h.CreateTargetDatabase(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create #%d: status %d: %s", i+1, rec.Code, rec.Body)
		}
	}
	out := preflight()
	if out["target_missing_database"] != nil {
		t.Errorf("still missing after create: %v", out)
	}
	if tgt, _ := out["target"].(map[string]any); tgt != nil && tgt["errors"] != nil {
		t.Errorf("target errors after create: %v", tgt["errors"])
	}
}

func dropTestDatabase(t *testing.T, admin adapters.ConnectionConfig, name string) {
	driver, dsn, stmt := "postgres", admin.DSN, `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`
	if admin.Type == "mysql" {
		port := admin.Port
		if port == 0 {
			port = 3306
		}
		driver = "mysql"
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/", admin.Username, admin.Password, admin.Host, port)
		stmt = "DROP DATABASE IF EXISTS `" + name + "`"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Logf("drop %s: %v", name, err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(stmt); err != nil {
		t.Logf("drop %s: %v", name, err)
	}
}
