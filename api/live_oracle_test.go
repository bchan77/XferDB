package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestLiveMigrationOracle runs a live migration from Oracle through the
// API (#195), with a writer inserting, updating, deleting, changing keys
// and rolling back throughout: snapshot, a server restart with apply
// stopped, streaming, a cutover refused while the writer runs, then the
// writer stopped, a marker row streamed, the cutover, and validation.
//
// XFERDB_TEST_ORACLE_LIVE is an oracle:// DSN with the LogMiner privileges
// LiveChecks lists, on a database in ARCHIVELOG mode with primary key
// supplemental logging (on 21c, also archive_lag_target, 30s will do). The
// targets are a SQLite file, and a new database on XFERDB_TEST_ORACLE_PG
// (postgres://), XFERDB_TEST_ORACLE_MYSQL (mysql://, a login that can
// create databases) and XFERDB_TEST_YB when they are set.
func TestLiveMigrationOracle(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_ORACLE_LIVE")
	if dsn == "" {
		t.Skip("XFERDB_TEST_ORACLE_LIVE not set; skipping Oracle live migration test")
	}
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "target.db")
		runLiveOracle(t, dsn, map[string]any{"type": "sqlite", "dsn": path}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
	for _, tc := range []struct{ name, env, typ string }{
		{"postgres", "XFERDB_TEST_ORACLE_PG", "postgres"},
		{"yugabyte", "XFERDB_TEST_YB", "yugabyte"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := os.Getenv(tc.env)
			if base == "" {
				t.Skip(tc.env + " not set")
			}
			admin, err := sql.Open("postgres", sqlDSNOf(base))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { admin.Close() })
			name := fmt.Sprintf("xferdb_olive_%d", time.Now().UnixNano())
			mustExecDB(t, admin, `CREATE DATABASE `+name)
			t.Cleanup(func() { admin.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`) })
			tgt := withDB(base, name)
			runLiveOracle(t, dsn, map[string]any{"type": tc.typ, "dsn": tgt}, func(t *testing.T) *sql.DB {
				db, err := sql.Open("postgres", sqlDSNOf(tgt))
				if err != nil {
					t.Fatal(err)
				}
				return db
			})
		})
	}
	t.Run("mysql", func(t *testing.T) {
		base := os.Getenv("XFERDB_TEST_ORACLE_MYSQL")
		if base == "" {
			t.Skip("XFERDB_TEST_ORACLE_MYSQL not set")
		}
		native := func(db string) string {
			u, err := url.Parse(base)
			if err != nil {
				t.Fatal(err)
			}
			pw, _ := u.User.Password()
			return fmt.Sprintf("%s:%s@tcp(%s)/%s", u.User.Username(), pw, u.Host, db)
		}
		admin, err := sql.Open("mysql", native(""))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { admin.Close() })
		name := fmt.Sprintf("xferdb_olive_%d", time.Now().UnixNano())
		mustExecDB(t, admin, `CREATE DATABASE `+name)
		t.Cleanup(func() { admin.Exec(`DROP DATABASE IF EXISTS ` + name) })
		runLiveOracle(t, dsn, map[string]any{"type": "mysql", "dsn": withDB(base, name)}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("mysql", native(name))
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
}

func runLiveOracle(t *testing.T, dsn string, target map[string]any, openTarget func(*testing.T) *sql.DB) {
	ctx := context.Background()
	src, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	for _, tbl := range []string{"LV_LINES", "LV_ITEMS"} {
		src.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE") //nolint:errcheck // may not exist
	}
	for _, stmt := range []string{
		`CREATE TABLE lv_items (id NUMBER(10) PRIMARY KEY, name VARCHAR2(40) NOT NULL, qty NUMBER(8),
			price NUMBER(10,2), updated TIMESTAMP(6), note CLOB)`,
		`CREATE TABLE lv_lines (item_id NUMBER(10), line_no NUMBER(3), sku VARCHAR2(20),
			CONSTRAINT lv_lines_pk PRIMARY KEY (item_id, line_no))`,
		`INSERT INTO lv_items SELECT LEVEL, 'item ' || LEVEL, MOD(LEVEL, 50), LEVEL / 4,
			TIMESTAMP '2025-01-01 00:00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND'),
			CASE WHEN MOD(LEVEL, 100) = 0 THEN TO_CLOB(RPAD('n', 4000, 'n')) || TO_CLOB(RPAD('m', 1000, 'm')) END
			FROM dual CONNECT BY LEVEL <= 5000`,
		`INSERT INTO lv_lines SELECT i.id, l.n, 'SKU-' || i.id || '-' || l.n FROM lv_items i, (SELECT LEVEL n FROM dual CONNECT BY LEVEL <= 2) l`,
		`COMMIT`,
	} {
		if _, err := src.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%v\n%s", err, stmt)
		}
	}
	t.Cleanup(func() {
		for _, tbl := range []string{"LV_LINES", "LV_ITEMS"} {
			src.ExecContext(context.Background(), "DROP TABLE "+tbl+" PURGE") //nolint:errcheck
		}
	})

	dir := t.TempDir()
	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	launcher := &live.InProcessLauncher{}
	defer launcher.StopAll()
	newServer := func() (*Server, *httptest.Server) {
		s := NewServer(db, slog.New(slog.DiscardHandler))
		s.EnableLive(filepath.Join(dir, "cdc"), launcher)
		s.live.Interval = 200 * time.Millisecond
		s.live.QuietWait = 500 * time.Millisecond
		return s, httptest.NewServer(s.Handler())
	}
	srv, hs := newServer()

	var p struct {
		ID string `json:"id"`
	}
	post(t, hs.URL+"/api/v1/projects", map[string]any{
		"name":          fmt.Sprintf("olive-%d", time.Now().UnixNano()),
		"source_config": map[string]any{"type": "oracle", "dsn": dsn},
		"target_config": target,
		// Readiness checks these, not every table in the schema.
		"transfer_config": map[string]any{"tables": []string{"lv_items", "lv_lines"}},
	}, http.StatusCreated, &p)
	began := time.Now()
	step := func(what string) { t.Logf("%6.1fs %s", time.Since(began).Seconds(), what) }

	var ready struct {
		Live struct {
			Ready  bool `json:"ready"`
			Checks []struct {
				Name, Detail string
				OK, Warning  bool
			} `json:"checks"`
		} `json:"live"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/preflight", map[string]any{"mode": "live"}, http.StatusOK, &ready)
	if !ready.Live.Ready {
		t.Fatalf("live readiness: %+v", ready.Live.Checks)
	}

	stopWriter, writerDone, ops := oracleWriter(t, src)
	var stopOnce sync.Once
	halt := func() { stopOnce.Do(func() { close(stopWriter); <-writerDone }) }
	defer halt() // before src closes, if the test stops early
	var m struct {
		ID   string `json:"id"`
		Mode string `json:"mode"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/migrations", map[string]any{
		"mode": "live", "validation_level": "L3", "tables": []string{"lv_items", "lv_lines"}, "batch_size": 200, "table_workers": 2,
	}, http.StatusCreated, &m)
	if m.Mode != "live" {
		t.Fatalf("mode = %q", m.Mode)
	}
	waitLive(t, hs, m.ID, "catch-up", func(l liveMig) bool { return l.Phase == "catch_up" || l.Phase == "streaming" })
	step(fmt.Sprintf("snapshot done after %d writer operations", ops.Load()))

	// Apply stopped and the server restarted: the stream picks up again.
	wdir := filepath.Join(dir, "cdc", m.ID)
	if err := launcher.Stop(ctx, wdir, worker.RoleApply); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	srv.live.Shutdown()
	hs.Close()
	srv, hs = newServer()
	defer func() {
		srv.live.Shutdown()
		hs.Close()
	}()
	srv.ResumeLive(ctx)
	waitLive(t, hs, m.ID, "streaming", func(l liveMig) bool { return l.Phase == "streaming" })
	step("streaming")
	time.Sleep(time.Second)

	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	cut := waitLive(t, hs, m.ID, "the busy cutover to stop", func(l liveMig) bool {
		return l.Live.Cutover != nil && !l.Live.Cutover.Running
	})
	if !strings.Contains(cut.Live.Cutover.Error, "writes are still happening") || cut.Phase != "streaming" {
		t.Fatalf("cutover under writes: phase %s, error %q", cut.Phase, cut.Live.Cutover.Error)
	}
	halt()
	step(fmt.Sprintf("busy cutover refused; writer stopped after %d operations", ops.Load()))
	if _, err := src.ExecContext(ctx, `INSERT INTO lv_items (id, name) VALUES (9999999, 'marker')`); err != nil {
		t.Fatal(err)
	}
	tgt := openTarget(t)
	defer tgt.Close()
	defer func() {
		if t.Failed() {
			var raw map[string]any
			get(t, hs.URL+"/api/v1/migrations/"+m.ID, &raw)
			t.Logf("migration at failure: %v", raw["live"])
		}
	}()
	streamed := waitLive(t, hs, m.ID, "the marker to apply", func(l liveMig) bool {
		return count(tgt, `SELECT count(*) FROM lv_items WHERE name = 'marker'`) == 1 && l.Live.LagChanges == 0
	})
	if a := streamed.Live.Apply; a == nil || a.Tables["lv_items"].Updates == 0 || a.Tables["lv_items"].Deletes == 0 {
		t.Errorf("stream activity = %+v", streamed.Live.Apply)
	}
	for _, tbl := range []string{"lv_items", "lv_lines"} {
		var n int
		if err := src.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if got := count(tgt, `SELECT count(*) FROM `+tbl); got != n {
			t.Errorf("%s: source has %d rows, target %d", tbl, n, got)
		}
	}

	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	waitMigration(t, hs, m.ID, "completed")
	step("cut over")
	var vs []validationResult
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if vs = listValidations(t, hs, m.ID); len(vs) == 1 && vs[0].Status == "completed" {
			break
		}
	}
	if len(vs) != 1 || vs[0].Status != "completed" {
		t.Fatalf("validation after cutover: %+v", vs)
	}
	checkValidationPassed(t, hs, vs[0].ID, "lv_items", "lv_lines")
	if t.Failed() {
		var raw any
		get(t, hs.URL+"/api/v1/validations/"+vs[0].ID, &raw)
		t.Logf("validation: %v", raw)
		for _, tbl := range []string{"lv_items", "lv_lines"} {
			var diffs any
			get(t, hs.URL+"/api/v1/validations/"+vs[0].ID+"/tables/"+tbl+"/diffs", &diffs)
			d := fmt.Sprint(diffs)
			if len(d) > 3000 {
				d = d[:3000]
			}
			t.Logf("%s differences: %s", tbl, d)
		}
	}
	step("validated")
}

// oracleWriter changes lv_items and lv_lines until stopped, one small
// transaction at a time: inserts, updates (the CLOB too), deletes, key
// changes, and now and then a transaction rolled back.
func oracleWriter(t *testing.T, db *sql.DB) (chan struct{}, chan struct{}, *atomic.Int64) {
	stop, done := make(chan struct{}), make(chan struct{})
	var ops atomic.Int64
	go func() {
		defer close(done)
		ctx := context.Background()
		rng := rand.New(rand.NewSource(1))
		next := int64(100000)
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := rng.Int63n(5000) + 1
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Errorf("writer: %v", err)
				return
			}
			var stmts []string
			switch rng.Intn(10) {
			case 0, 1, 2:
				next++
				stmts = []string{
					fmt.Sprintf(`INSERT INTO lv_items (id, name, qty, updated) VALUES (%d, 'new %d', 1, SYSTIMESTAMP)`, next, next),
					fmt.Sprintf(`INSERT INTO lv_lines VALUES (%d, 1, 'SKU-new')`, next),
				}
			case 3, 4, 5:
				stmts = []string{fmt.Sprintf(`UPDATE lv_items SET qty = NVL(qty, 0) + 1, price = %d.25, updated = SYSTIMESTAMP WHERE id = %d`, rng.Intn(100), id)}
			case 6:
				stmts = []string{fmt.Sprintf(`UPDATE lv_items SET note = TO_CLOB(RPAD('w', 3000, 'w')) || TO_CLOB('%d') WHERE id = %d`, rng.Intn(1000), id)}
			case 7:
				stmts = []string{
					fmt.Sprintf(`DELETE FROM lv_lines WHERE item_id = %d`, id),
					fmt.Sprintf(`DELETE FROM lv_items WHERE id = %d`, id),
				}
			case 8:
				next++
				// A key change.
				stmts = []string{fmt.Sprintf(`UPDATE lv_lines SET line_no = line_no + 10 WHERE item_id = %d AND line_no < 10`, id)}
			case 9:
				stmts = []string{fmt.Sprintf(`UPDATE lv_items SET name = 'rolled back' WHERE id = %d`, id), "ROLLBACK"}
			}
			rollback := false
			for _, s := range stmts {
				if s == "ROLLBACK" {
					rollback = true
					break
				}
				if _, err := tx.ExecContext(ctx, s); err != nil {
					t.Errorf("writer: %v\n%s", err, s)
					tx.Rollback() //nolint:errcheck
					return
				}
			}
			if rollback {
				tx.Rollback() //nolint:errcheck
			} else if err := tx.Commit(); err != nil {
				t.Errorf("writer commit: %v", err)
				return
			}
			ops.Add(1)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return stop, done, &ops
}
