package api

import (
	"bytes"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// liveTestServer starts a server with live migrations on a SQLite target
// and a live migration of tables, waiting until it streams.
func liveTestServer(t *testing.T, srcDSN string, src *sql.DB, tables ...string) (*httptest.Server, string, *sql.DB) {
	t.Helper()
	dir := t.TempDir()
	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	launcher := &live.InProcessLauncher{}
	t.Cleanup(launcher.StopAll)
	s := NewServer(db, slog.New(slog.DiscardHandler))
	s.EnableLive(filepath.Join(dir, "cdc"), launcher)
	s.live.Interval = 200 * time.Millisecond
	s.live.QuietWait = 300 * time.Millisecond
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(func() { s.live.Shutdown(); hs.Close() })

	tgtPath := filepath.Join(dir, "target.db")
	var p struct {
		ID string `json:"id"`
	}
	post(t, hs.URL+"/api/v1/projects", map[string]any{
		"name":          "ddl",
		"source_config": map[string]any{"type": "postgres", "dsn": srcDSN},
		"target_config": map[string]any{"type": "sqlite", "dsn": tgtPath},
	}, http.StatusCreated, &p)
	var m struct {
		ID string `json:"id"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/migrations", map[string]any{
		"mode": "live", "tables": tables, "validation": "off",
	}, http.StatusCreated, &m)
	t.Cleanup(func() { dropStream(src, m.ID) })
	waitLive(t, hs, m.ID, "streaming", func(l liveMig) bool { return l.Phase == "streaming" })
	tgt, err := sql.Open("sqlite3", tgtPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tgt.Close() })
	return hs, m.ID, tgt
}

func patchMigration(t *testing.T, hs *httptest.Server, id, action string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPatch, hs.URL+"/api/v1/migrations/"+id, bytes.NewReader([]byte(`{"action":"`+action+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s: %s", action, resp.Status)
	}
}

func count(db *sql.DB, q string) int {
	var n int
	db.QueryRow(q).Scan(&n)
	return n
}

// TestLiveMigrationSchemaChange: a column added on the source pauses a
// streaming live migration where the stream reaches it, with the change in
// its status, and nothing after it is applied (other tables included).
// Cutover is refused meanwhile. Once the column is on the target, resuming
// accepts the change: the rows written with it arrive, and the migration
// can be cut over. Needs XFERDB_TEST_CDC_PG.
func TestLiveMigrationSchemaChange(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live schema change test")
	}
	src, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	items, other := "ddl_items_"+suffix, "ddl_other_"+suffix
	mustExecDB(t, src, `CREATE TABLE `+items+` (id int PRIMARY KEY, name text)`)
	mustExecDB(t, src, `CREATE TABLE `+other+` (id int PRIMARY KEY)`)
	t.Cleanup(func() { src.Exec(`DROP TABLE IF EXISTS ` + items + `, ` + other) })
	mustExecDB(t, src, `INSERT INTO `+items+` SELECT i, 'n' || i FROM generate_series(1, 100) i`)

	hs, id, tgt := liveTestServer(t, dsn, src, items, other)

	mustExecDB(t, src, `INSERT INTO `+items+` VALUES (101, 'before')`)
	mustExecDB(t, src, `ALTER TABLE `+items+` ADD COLUMN note text`)
	mustExecDB(t, src, `INSERT INTO `+items+` VALUES (102, 'after', 'hello')`)
	mustExecDB(t, src, `INSERT INTO `+other+` VALUES (1)`)

	var paused struct {
		Status string `json:"status"`
		Live   struct {
			SchemaChange *struct {
				Table   string   `json:"table"`
				Summary []string `json:"summary"`
			} `json:"schema_change"`
		} `json:"live"`
	}
	waitLive(t, hs, id, "the pause at the schema change", func(l liveMig) bool {
		paused.Live.SchemaChange = nil
		get(t, hs.URL+"/api/v1/migrations/"+id, &paused)
		return l.Status == "paused" && paused.Live.SchemaChange != nil
	})
	sc := paused.Live.SchemaChange
	if sc.Table != items || strings.Join(sc.Summary, ";") != "column note added (text)" {
		t.Fatalf("schema change = %+v", sc)
	}
	time.Sleep(500 * time.Millisecond)
	if n := count(tgt, `SELECT count(*) FROM `+items+` WHERE id = 101`); n != 1 {
		t.Fatal("the row written before the schema change wasn't applied")
	}
	if n := count(tgt, `SELECT count(*) FROM `+items+` WHERE id = 102`) + count(tgt, `SELECT count(*) FROM `+other); n != 0 {
		t.Fatal("changes after the schema change were applied before it was accepted")
	}
	post(t, hs.URL+"/api/v1/migrations/"+id+"/cutover", map[string]any{}, http.StatusConflict, nil)

	// The change goes on the target by hand, then resume accepts it.
	if _, err := tgt.Exec(`ALTER TABLE ` + items + ` ADD COLUMN note TEXT`); err != nil {
		t.Fatal(err)
	}
	patchMigration(t, hs, id, "resume")
	waitLive(t, hs, id, "the rows after the schema change", func(l liveMig) bool {
		return l.Status == "in_progress" && l.Live.LagChanges == 0 &&
			count(tgt, `SELECT count(*) FROM `+items+` WHERE id = 102 AND note = 'hello'`) == 1 &&
			count(tgt, `SELECT count(*) FROM `+other) == 1
	})
	paused.Live.SchemaChange = nil
	get(t, hs.URL+"/api/v1/migrations/"+id, &paused)
	if paused.Live.SchemaChange != nil {
		t.Fatalf("schema change still reported after resuming: %+v", paused.Live.SchemaChange)
	}

	post(t, hs.URL+"/api/v1/migrations/"+id+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	waitMigration(t, hs, id, "completed")
}

// TestLiveMigrationSlotLost: a replication slot that disappears from the
// source (dropped by hand here; invalidated past max_slot_wal_keep_size in
// real life) can't be streamed from again, so the migration fails with the
// reason rather than restarting capture forever. Needs XFERDB_TEST_CDC_PG.
func TestLiveMigrationSlotLost(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live slot test")
	}
	src, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	table := fmt.Sprint("lost_", time.Now().UnixNano())
	mustExecDB(t, src, `CREATE TABLE `+table+` (id int PRIMARY KEY)`)
	t.Cleanup(func() { src.Exec(`DROP TABLE IF EXISTS ` + table) })

	hs, id, _ := liveTestServer(t, dsn, src, table)
	var ret struct {
		Live struct {
			Retention *struct {
				RetainedBytes int64  `json:"retained_bytes"`
				State         string `json:"state"`
			} `json:"retention"`
		} `json:"live"`
	}
	get(t, hs.URL+"/api/v1/migrations/"+id, &ret)
	if ret.Live.Retention == nil {
		t.Fatal("no retention reported for a streaming migration")
	}

	slot := postgres.SlotName(id)
	deadline := time.Now().Add(20 * time.Second)
	for {
		src.Exec(`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL`, slot)
		if _, err := src.Exec(`SELECT pg_drop_replication_slot($1)`, slot); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("couldn't drop the slot")
		}
		time.Sleep(50 * time.Millisecond)
	}

	end := time.Now().Add(30 * time.Second)
	var l liveMig
	for time.Now().Before(end) {
		l = liveMig{}
		get(t, hs.URL+"/api/v1/migrations/"+id, &l)
		if l.Status == "failed" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if l.Status != "failed" || !strings.Contains(l.Error, "is gone from the source") {
		t.Fatalf("after the slot was dropped: %s %q", l.Status, l.Error)
	}
	var pubs int
	src.QueryRow(`SELECT count(*) FROM pg_publication WHERE pubname = $1`, slot).Scan(&pubs)
	if pubs != 0 {
		t.Error("the publication was left behind")
	}
}
