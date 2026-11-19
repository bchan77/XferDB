package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

func postListSchemas(t *testing.T, h *ProjectsHandler, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections/schemas", bytes.NewReader(b))
	rec := httptest.NewRecorder()
	h.ListSchemas(rec, req)
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec, out
}

func TestListSchemasRejectsBadRequests(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	cases := map[string]struct {
		body listDatabasesRequest
		want string
	}{
		"mysql":       {listDatabasesRequest{Config: adapters.ConnectionConfig{Type: "mysql", Host: "h", Database: "d"}}, "supported for"},
		"mongodb":     {listDatabasesRequest{Config: adapters.ConnectionConfig{Type: "mongodb", DSN: "mongodb://h/db"}}, "supported for"},
		"no host":     {listDatabasesRequest{Config: adapters.ConnectionConfig{Type: "postgres", Database: "d"}}, "host"},
		"no database": {listDatabasesRequest{Config: adapters.ConnectionConfig{Type: "postgres", Host: "h"}}, "enter the database first"},
		"no service":  {listDatabasesRequest{Config: adapters.ConnectionConfig{Type: "oracle", Host: "h"}}, "enter the service name first"},
	}
	for name, c := range cases {
		rec, out := postListSchemas(t, h, c.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, rec.Code)
		}
		if msg, _ := out["error"].(string); !strings.Contains(msg, c.want) {
			t.Errorf("%s: error %q lacks %q", name, msg, c.want)
		}
	}
	rec, _ := postListSchemas(t, h, listDatabasesRequest{
		Config:    adapters.ConnectionConfig{Type: "oracle", Host: "h", Database: "FREEPDB1"},
		ProjectID: "no-such-project",
	})
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown project: status %d, want 404", rec.Code)
	}
}

func TestListSchemasReportsConnectionError(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	for _, typ := range []string{"postgres", "oracle"} {
		rec, out := postListSchemas(t, h, listDatabasesRequest{
			Config: adapters.ConnectionConfig{Type: typ, Host: "127.0.0.1", Port: port, Database: "d", Username: "u", Password: "p"},
		})
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status %d, want 422 (body %s)", typ, rec.Code, rec.Body)
		}
		if msg, _ := out["error"].(string); !strings.Contains(msg, "refused") {
			t.Errorf("%s: error %q lacks the driver error", typ, msg)
		}
	}
}

// The settings form leaves a stored MongoDB URI blank; listing databases
// fills it from the project.
func TestFillStoredMongoURI(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()
	p := &adapters.Project{
		ID: "m1", Name: "m1",
		SourceConfig: adapters.ConnectionConfig{Type: "mongodb", DSN: "mongodb://u:pw@h:27017/shop"},
		TargetConfig: adapters.ConnectionConfig{Type: "postgres", Host: "tgt", Username: "bob", Password: "t4rget"},
	}
	if err := db.CreateProject(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	body := listDatabasesRequest{ProjectID: "m1", Side: "source"}
	cfg := adapters.ConnectionConfig{Type: "mongodb"}
	if err := h.fillStored(context.Background(), &cfg, body); err != nil {
		t.Fatal(err)
	}
	if cfg.DSN != p.SourceConfig.DSN {
		t.Errorf("DSN %q, want the stored one", cfg.DSN)
	}
	cfg = adapters.ConnectionConfig{Type: "mongodb", DSN: "mongodb://other/x"}
	h.fillStored(context.Background(), &cfg, body)
	if cfg.DSN != "mongodb://other/x" {
		t.Errorf("a typed URI was replaced by %q", cfg.DSN)
	}
}

// liveList lists through the handler what the DSN in env connects to,
// after prep adjusts the parsed config, and returns the names under key.
func liveList(t *testing.T, env string, list func(*testing.T, *ProjectsHandler, any) (*httptest.ResponseRecorder, map[string]any), key string, prep func(*adapters.ConnectionConfig)) []string {
	t.Helper()
	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skip(env + " not set")
	}
	cfg, err := registry.ParseConnectionString(dsn)
	if err != nil {
		t.Fatal(err)
	}
	prep(&cfg)
	h, _, cleanup := newTestHandler(t)
	defer cleanup()
	rec, out := list(t, h, listDatabasesRequest{Config: cfg})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	raw, ok := out[key].([]any)
	if !ok {
		t.Fatalf("no %s list in %s", key, rec.Body)
	}
	names := make([]string, len(raw))
	for i, n := range raw {
		names[i], _ = n.(string)
	}
	return names
}

func checkNames(t *testing.T, names []string, want string, never ...string) {
	t.Helper()
	found := want == ""
	for _, n := range names {
		found = found || n == want
		for _, bad := range never {
			if n == bad {
				t.Errorf("%v includes %q", names, bad)
			}
		}
	}
	if !found {
		t.Errorf("%v lacks %q", names, want)
	}
}

// PostgreSQL (XFERDB_TEST_TARGET_DSN, a postgres:// one): public is listed,
// the system schemas aren't.
func TestListSchemasPostgresLive(t *testing.T) {
	if !strings.HasPrefix(os.Getenv("XFERDB_TEST_TARGET_DSN"), "postgres") {
		t.Skip("XFERDB_TEST_TARGET_DSN is not a postgres DSN")
	}
	names := liveList(t, "XFERDB_TEST_TARGET_DSN", postListSchemas, "schemas", func(c *adapters.ConnectionConfig) { c.DSN = "" })
	checkNames(t, names, "public", "pg_catalog", "information_schema", "pg_toast")
}

// Oracle (XFERDB_TEST_ORACLE): a schema the DSN names that doesn't exist
// doesn't stop the listing, and Oracle's own schemas are left out.
func TestListSchemasOracleLive(t *testing.T) {
	names := liveList(t, "XFERDB_TEST_ORACLE", postListSchemas, "schemas", func(c *adapters.ConnectionConfig) { c.Schema = "NO_SUCH_SCHEMA" })
	checkNames(t, names, "", "SYS", "SYSTEM", "XDB", "MDSYS", "AUDSYS")
}

// MongoDB (XFERDB_TEST_MONGO_RS): the URI's own database (if it names one and
// it holds data) is listed with no database in the config; admin, config and
// local never are.
func TestListDatabasesMongoLive(t *testing.T) {
	names := liveList(t, "XFERDB_TEST_MONGO_RS", postListDatabases, "databases", func(c *adapters.ConnectionConfig) { c.Database = "" })
	checkNames(t, names, "", "admin", "config", "local")
}
