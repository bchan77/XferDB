package api

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
	"gitea.homelab.local/nextdevops/XferDB/state"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// TestLiveMigration runs a live migration through the API while a writer
// keeps changing the source: setup, snapshot, catch-up, streaming. Partway
// it kills the apply worker and restarts the server, and at the end the
// target must hold exactly the source's rows, with the unique index that
// catch-up adds; deleting the migration must drop the source's slot. Needs
// XFERDB_TEST_CDC_PG (wal_level=logical); the targets are a SQLite file and
// a new database on the same PostgreSQL server.
func TestLiveMigration(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live migration API test")
	}
	src, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Closed after runLive's cleanups, which drop its tables and stream
	// with it (a defer would close it before them).
	t.Cleanup(func() { src.Close() })

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "target.db")
		runLive(t, "postgres", dsn, src, map[string]any{"type": "sqlite", "dsn": path}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}, func(db *sql.DB, index string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, index).Scan(&n)
			return n == 1
		}, nil, nil)
	})
	t.Run("postgres", func(t *testing.T) {
		name := fmt.Sprintf("xferdb_live_tgt_%d", time.Now().UnixNano())
		mustExecDB(t, src, `CREATE DATABASE `+name)
		t.Cleanup(func() { src.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`) })
		tgtDSN := withDatabase(t, dsn, name)
		runLive(t, "postgres", dsn, src, map[string]any{"type": "postgres", "dsn": tgtDSN}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("postgres", tgtDSN)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}, func(db *sql.DB, index string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM pg_indexes WHERE indexname = $1`, index).Scan(&n)
			return n == 1
		}, func(db *sql.DB, table string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND conrelid = to_regclass($1)`, table).Scan(&n)
			return n == 1
		}, func(db *sql.DB, table string) int64 {
			var v int64
			if err := db.QueryRow(`SELECT nextval(pg_get_serial_sequence($1, 'id'))`, table).Scan(&v); err != nil {
				t.Fatalf("nextval on the target: %v", err)
			}
			return v
		})
	})
}

// TestLiveMigrationYugabyte is TestLiveMigration from a YugabyteDB source
// (#158): catch-up and cutover end by hybrid-time positions and heartbeats.
// The targets are a SQLite file and a new database on the same YugabyteDB
// cluster, whose schema pg_dump copies. Needs XFERDB_TEST_YB, a YugabyteDB
// DSN (yugabyte:// or postgres://).
func TestLiveMigrationYugabyte(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_YB")
	if dsn == "" {
		t.Skip("XFERDB_TEST_YB not set; skipping YugabyteDB live migration API test")
	}
	pgDSN := "postgres://" + strings.TrimPrefix(dsn, "yugabyte://")
	src, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "target.db")
		runLive(t, "yugabyte", dsn, src, map[string]any{"type": "sqlite", "dsn": path}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}, func(db *sql.DB, index string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, index).Scan(&n)
			return n == 1
		}, nil, nil)
	})
	t.Run("yugabyte", func(t *testing.T) {
		name := fmt.Sprintf("xferdb_live_tgt_%d", time.Now().UnixNano())
		mustExecDB(t, src, `CREATE DATABASE `+name)
		t.Cleanup(func() { src.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`) })
		tgtDSN := withDatabase(t, pgDSN, name)
		runLive(t, "yugabyte", dsn, src, map[string]any{"type": "yugabyte", "dsn": tgtDSN}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("postgres", tgtDSN)
			if err != nil {
				t.Fatal(err)
			}
			return db
		}, func(db *sql.DB, index string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM pg_indexes WHERE indexname = $1`, index).Scan(&n)
			return n == 1
		}, func(db *sql.DB, table string) bool {
			var n int
			db.QueryRow(`SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND conrelid = to_regclass($1)`, table).Scan(&n)
			return n == 1
		}, func(db *sql.DB, table string) int64 {
			var v int64
			if err := db.QueryRow(`SELECT nextval(pg_get_serial_sequence($1, 'id'))`, table).Scan(&v); err != nil {
				t.Fatalf("nextval on the target: %v", err)
			}
			return v
		})
	})
}

// hasFK, when set, reports whether a table has a foreign key on the
// target; nextval, when set, draws the next value of a table's id sequence
// there. A SQLite target has neither: its foreign keys are declared, not
// enforced, from the start, and it keeps no sequences.
func runLive(t *testing.T, srcType, srcDSN string, src *sql.DB, target map[string]any, openTarget func(*testing.T) *sql.DB,
	hasIndex func(*sql.DB, string) bool, hasFK func(*sql.DB, string) bool, nextval func(*sql.DB, string) int64) {
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	items, notes := "live_items_"+suffix, "live_notes_"+suffix
	mustExecDB(t, src, `CREATE TABLE `+items+` (grp text, id int, name text, big text, email text NOT NULL, PRIMARY KEY (grp, id))`)
	mustExecDB(t, src, `CREATE UNIQUE INDEX `+items+`_email ON `+items+` (email)`)
	mustExecDB(t, src, `CREATE INDEX `+items+`_name ON `+items+` (name)`)
	mustExecDB(t, src, `CREATE TABLE `+notes+` (id bigint PRIMARY KEY, body text)`)
	// kids has a foreign key, held back until cutover, and a serial id,
	// whose sequence cutover sets on a PostgreSQL target.
	kids := "live_kids_" + suffix
	mustExecDB(t, src, `CREATE TABLE `+kids+` (id serial PRIMARY KEY, note_id bigint REFERENCES `+notes+` (id) ON DELETE CASCADE, label text)`)
	t.Cleanup(func() { src.Exec(`DROP TABLE IF EXISTS ` + kids + `, ` + items + `, ` + notes) })
	// PostgreSQL's default already; YugabyteDB's, CHANGE, can't be streamed.
	for _, table := range []string{items, notes, kids} {
		mustExecDB(t, src, `ALTER TABLE `+table+` REPLICA IDENTITY DEFAULT`)
	}
	mustExecDB(t, src, `INSERT INTO `+items+` SELECT (ARRAY['a','b','c'])[i % 3 + 1], i, 'row' || i, NULL, 'e' || i FROM generate_series(0, 2999) i`)
	mustExecDB(t, src, `INSERT INTO `+notes+` SELECT i, 'note' || i FROM generate_series(0, 2999) i`)
	mustExecDB(t, src, `INSERT INTO `+kids+` (note_id, label) SELECT i, 'kid' || i FROM generate_series(0, 2999, 3) i`)

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
		"name":          "live-" + suffix,
		"source_config": map[string]any{"type": srcType, "dsn": srcDSN},
		"target_config": target,
	}, http.StatusCreated, &p)

	stopWriter, writerDone, txns := liveWriter(t, src, items, notes, kids)
	var m struct {
		ID   string `json:"id"`
		Mode string `json:"mode"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/migrations", map[string]any{
		"mode": "live", "tables": []string{items, notes, kids}, "batch_size": 50, "table_workers": 2,
	}, http.StatusCreated, &m)
	if m.Mode != "live" {
		t.Fatalf("mode = %q", m.Mode)
	}
	// Whatever happens, drop the slot and publication: a failed run must
	// not leave the source keeping WAL.
	t.Cleanup(func() { dropStream(src, m.ID) })

	waitLive(t, hs, m.ID, "catch-up", func(l liveMig) bool {
		return l.Phase == "catch_up" || l.Phase == "streaming"
	})
	t.Logf("snapshot done after %d writer transactions", txns.Load())
	// Apply crashes: supervision restarts it.
	wdir := filepath.Join(dir, "cdc", m.ID)
	if err := launcher.Stop(ctx, wdir, worker.RoleApply); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	// The server restarts: the workers keep running, and the new server
	// supervises them again.
	srv.live.Shutdown()
	hs.Close()
	srv, hs = newServer()
	defer func() { srv.live.Shutdown(); hs.Close() }()
	srv.ResumeLive(ctx)

	waitLive(t, hs, m.ID, "streaming", func(l liveMig) bool { return l.Phase == "streaming" })
	time.Sleep(time.Second) // keep writing while streaming

	// Cutting over while the source is still written to stops at the
	// quiet check, and the migration goes on streaming.
	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	cut := waitLive(t, hs, m.ID, "the busy cutover to stop", func(l liveMig) bool {
		return l.Live.Cutover != nil && !l.Live.Cutover.Running
	})
	if !strings.Contains(cut.Live.Cutover.Error, "writes are still happening") || cut.Live.Cutover.WritesAfter == 0 || cut.Phase != "streaming" {
		t.Fatalf("cutover under writes: phase %s, %d writes after, error %q", cut.Phase, cut.Live.Cutover.WritesAfter, cut.Live.Cutover.Error)
	}

	close(stopWriter)
	<-writerDone
	t.Logf("writer stopped after %d transactions", txns.Load())
	mustExecDB(t, src, `INSERT INTO `+items+` VALUES ('zz', -1, 'done', NULL, 'marker')`)
	tgt := openTarget(t)
	defer tgt.Close()
	waitLive(t, hs, m.ID, "the last change to apply", func(l liveMig) bool {
		var n int
		tgt.QueryRow(`SELECT count(*) FROM ` + items + ` WHERE email = 'marker'`).Scan(&n)
		return n == 1 && l.Live.LagChanges == 0
	})

	compare := func(table, q string) {
		t.Helper()
		want, got := liveRows(t, src, q, false), liveRows(t, tgt, q, false)
		if strings.Join(want, ",") != strings.Join(got, ",") {
			t.Fatalf("%s: source has %d rows, target %d, or they differ", table, len(want), len(got))
		}
	}
	for _, q := range []struct{ src, tgt string }{
		{`SELECT grp || '|' || id || '|' || name || '|' || COALESCE(md5(big), '-') || '|' || email FROM ` + items,
			`SELECT grp || '|' || id || '|' || name || '|' || COALESCE(big, '-') || '|' || email FROM ` + items},
		{`SELECT id || '|' || body FROM ` + notes, `SELECT id || '|' || body FROM ` + notes},
	} {
		want, got := liveRows(t, src, q.src, false), liveRows(t, tgt, q.tgt, true)
		if len(want) != len(got) {
			t.Fatalf("source has %d rows, target %d (%s)", len(want), len(got), q.src)
		}
		for i := range want {
			if want[i] != got[i] {
				t.Fatalf("row %d differs:\n source %s\n target %s", i, want[i], got[i])
			}
		}
	}
	if !hasIndex(tgt, items+"_email") {
		t.Errorf("the unique index wasn't created after catch-up")
	}
	if !hasIndex(tgt, items+"_name") {
		t.Errorf("the plain index wasn't created after the snapshot")
	}
	if hasFK != nil && hasFK(tgt, kids) {
		t.Errorf("the foreign key was created before cutover")
	}

	// Cutover: drain, quiet check, sequences, foreign keys, slot dropped,
	// completed, validated.
	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	waitMigration(t, hs, m.ID, "completed")
	compare(kids, `SELECT id || '|' || note_id || '|' || label FROM `+kids)
	if hasFK != nil && !hasFK(tgt, kids) {
		t.Errorf("the foreign key wasn't created at cutover")
	}
	if nextval != nil {
		var maxID int64
		src.QueryRow(`SELECT max(id) FROM ` + kids).Scan(&maxID)
		if v := nextval(tgt, kids); v <= maxID {
			t.Errorf("target sequence gives %d after cutover; the source has handed out up to %d", v, maxID)
		}
	}
	var slots int
	src.QueryRow(`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, postgres.SlotName(m.ID)).Scan(&slots)
	if slots != 0 {
		t.Errorf("the replication slot is still there after cutover")
	}
	if _, err := os.Stat(wdir); !os.IsNotExist(err) {
		t.Errorf("worker directory still there after cutover: %v", err)
	}
	var vs []validationResult
	waitFor(t, "the validation after cutover", func() bool {
		vs = listValidations(t, hs, m.ID)
		return len(vs) == 1 && vs[0].Status == "completed"
	})
	for _, vt := range vs[0].Tables {
		if vt.Result != "pass" {
			t.Errorf("validation after cutover: %s %s", vt.TableName, vt.Result)
		}
	}

}

type liveMig struct {
	Status string `json:"status"`
	Phase  string `json:"phase"`
	Error  string `json:"error"`
	Live   struct {
		LagChanges   uint64 `json:"lag_changes"`
		CaptureError string `json:"capture_error"`
		ApplyError   string `json:"apply_error"`
		Cutover      *struct {
			Running     bool   `json:"running"`
			Error       string `json:"error"`
			WritesAfter uint64 `json:"writes_after"`
		} `json:"cutover"`
	} `json:"live"`
}

func waitLive(t *testing.T, hs *httptest.Server, id, what string, cond func(liveMig) bool) liveMig {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var l liveMig
	for time.Now().Before(deadline) {
		l = liveMig{}
		get(t, hs.URL+"/api/v1/migrations/"+id, &l)
		if l.Status == "failed" || l.Status == "cancelled" {
			t.Fatalf("migration %s: %s", l.Status, l.Error)
		}
		if cond(l) {
			return l
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %+v", what, l)
	return l
}

// waitMigration waits for a migration to reach status.
func waitMigration(t *testing.T, hs *httptest.Server, id, status string) {
	t.Helper()
	// Cutover drops the slot, which YugabyteDB refuses for up to a minute
	// after the stream stops.
	deadline := time.Now().Add(180 * time.Second)
	var l liveMig
	for time.Now().Before(deadline) {
		l = liveMig{}
		get(t, hs.URL+"/api/v1/migrations/"+id, &l)
		if l.Status == status {
			return
		}
		if l.Status == "failed" || l.Status == "cancelled" {
			t.Fatalf("migration %s: %s", l.Status, l.Error)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if c := l.Live.Cutover; c != nil {
		t.Fatalf("timed out waiting for %s: %+v; cutover %+v", status, l, *c)
	}
	t.Fatalf("timed out waiting for %s: %+v", status, l)
}

func liveGrp(i int) string { return []string{"a", "b", "c"}[i%3] }

// liveWriter changes the source until stop is closed: inserts, updates of
// plain and TOASTed columns, key changes, deletes, and unique values moved
// between rows, several per transaction.
func liveWriter(t *testing.T, db *sql.DB, items, notes, kids string) (chan struct{}, chan struct{}, *atomic.Int64) {
	stop, done := make(chan struct{}), make(chan struct{})
	var txns atomic.Int64
	rnd := mrand.New(mrand.NewSource(7))
	next := 10000
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			tx, err := db.Begin()
			if err != nil {
				t.Error(err)
				return
			}
			for j := 0; j < 1+rnd.Intn(4); j++ {
				i := rnd.Intn(3200)
				next++
				var q string
				var args []interface{}
				switch rnd.Intn(9) {
				case 0:
					q, args = `INSERT INTO `+items+` VALUES ($1, $2, 'new', NULL, $3) ON CONFLICT DO NOTHING`, []interface{}{liveGrp(next), next, fmt.Sprint("n", next)}
				case 1:
					q, args = `UPDATE `+items+` SET name = 'upd' || $3 WHERE grp = $1 AND id = $2`, []interface{}{liveGrp(i), i, next}
				case 2:
					raw := make([]byte, 4000)
					rand.Read(raw)
					q, args = `UPDATE `+items+` SET big = $3 WHERE grp = $1 AND id = $2`, []interface{}{liveGrp(i), i, hex.EncodeToString(raw)}
				case 3:
					q, args = `UPDATE `+items+` SET id = $3 WHERE grp = $1 AND id = $2`, []interface{}{liveGrp(i), i, next}
				case 4:
					q, args = `DELETE FROM `+items+` WHERE grp = $1 AND id = $2`, []interface{}{liveGrp(i), i}
				case 5:
					// A row takes the email another row just gave up: in
					// replay order the target briefly holds both.
					q = `WITH a AS (SELECT grp, id, email FROM ` + items + ` WHERE email NOT LIKE 'x%' ORDER BY random() LIMIT 1),
					      freed AS (UPDATE ` + items + ` t SET email = 'x' || $1::text FROM a WHERE t.grp = a.grp AND t.id = a.id RETURNING a.email)
					      INSERT INTO ` + items + ` SELECT 'c', $1::int, 'took', NULL, email FROM freed`
					args = []interface{}{next}
				case 6:
					q, args = `INSERT INTO `+notes+` VALUES ($1, 'new') ON CONFLICT (id) DO UPDATE SET body = 'again'`, []interface{}{i}
				case 7:
					q, args = `DELETE FROM `+notes+` WHERE id = $1`, []interface{}{i}
				case 8:
					q, args = `INSERT INTO `+kids+` (note_id, label) SELECT id, 'k' || $1::text FROM `+notes+` ORDER BY random() LIMIT 1`, []interface{}{next}
				}
				if _, err := tx.Exec(q, args...); err != nil {
					tx.Rollback()
					t.Error(q, err)
					return
				}
			}
			if err := tx.Commit(); err != nil {
				t.Error(err)
				return
			}
			txns.Add(1)
		}
	}()
	return stop, done, &txns
}

func liveRows(t *testing.T, db *sql.DB, q string, hashBig bool) []string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		if hashBig {
			// The target's big column is compared by md5, as the source's.
			if f := strings.Split(s, "|"); len(f) == 5 && f[3] != "-" {
				f[3] = md5Hex(f[3])
				s = strings.Join(f, "|")
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func mustExecDB(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// withDatabase returns dsn, a postgres:// URL, pointed at database name.
func withDatabase(t *testing.T, dsn, name string) string {
	t.Helper()
	i := strings.LastIndex(dsn, "/")
	if !strings.HasPrefix(dsn, "postgres") || i < 0 {
		t.Skip("XFERDB_TEST_CDC_PG must be a postgres:// URL for the postgres target")
	}
	rest := dsn[i+1:]
	query := ""
	if q := strings.Index(rest, "?"); q >= 0 {
		query = rest[q:]
	}
	return dsn[:i+1] + name + query
}

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestLiveMigrationResumesSnapshot starts a server on a live migration a
// restart interrupted before its snapshot finished, as ResumeLive finds it
// at startup: the snapshot runs again (its setup keeping the stream if it
// exists) and the migration goes on to stream, the target ending up equal
// to the source.
func TestLiveMigrationResumesSnapshot(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live migration resume test")
	}
	ctx := context.Background()
	src, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() }) // after the cleanups that use it
	table := fmt.Sprintf("live_resume_%d", time.Now().UnixNano())
	mustExecDB(t, src, `CREATE TABLE `+table+` (id int PRIMARY KEY, body text)`)
	t.Cleanup(func() { src.Exec(`DROP TABLE IF EXISTS ` + table) })
	mustExecDB(t, src, `INSERT INTO `+table+` SELECT i, 'b' || i FROM generate_series(1, 2000) i`)

	dir := t.TempDir()
	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	launcher := &live.InProcessLauncher{}
	defer launcher.StopAll()
	s := NewServer(db, slog.New(slog.DiscardHandler))
	s.EnableLive(filepath.Join(dir, "cdc"), launcher)
	s.live.Interval = 200 * time.Millisecond
	hs := httptest.NewServer(s.Handler())
	defer hs.Close()
	defer s.live.Shutdown()

	tgtPath := filepath.Join(dir, "target.db")
	var p struct {
		ID string `json:"id"`
	}
	post(t, hs.URL+"/api/v1/projects", map[string]any{
		"name":          "resume",
		"source_config": map[string]any{"type": "postgres", "dsn": dsn},
		"target_config": map[string]any{"type": "sqlite", "dsn": tgtPath},
	}, http.StatusCreated, &p)
	mig := &adapters.Migration{ID: fmt.Sprint("resume-", time.Now().UnixNano()), ProjectID: p.ID,
		Status: adapters.StatusInProgress, CreatedAt: time.Now(), Mode: adapters.MigrationModeLive, Phase: live.PhaseSnapshot}
	if err := db.CreateMigration(ctx, mig); err != nil {
		t.Fatal(err)
	}
	if err := db.SetRunConfig(ctx, mig.ID, adapters.TransferConfig{Tables: []string{table}, BatchSize: 100}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropStream(src, mig.ID) })

	s.ResumeLive(ctx)
	mustExecDB(t, src, `UPDATE `+table+` SET body = 'changed' WHERE id % 7 = 0`)
	mustExecDB(t, src, `DELETE FROM `+table+` WHERE id % 11 = 0`)
	mustExecDB(t, src, `INSERT INTO `+table+` VALUES (-1, 'marker')`)
	tgt, err := sql.Open("sqlite3", tgtPath)
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	waitLive(t, hs, mig.ID, "the resumed migration to stream the marker", func(l liveMig) bool {
		var n int
		tgt.QueryRow(`SELECT count(*) FROM ` + table + ` WHERE id = -1`).Scan(&n)
		return l.Phase == "streaming" && n == 1 && l.Live.LagChanges == 0
	})
	q := `SELECT id || '|' || body FROM ` + table
	want, got := liveRows(t, src, q, false), liveRows(t, tgt, q, false)
	if strings.Join(want, ",") != strings.Join(got, ",") {
		t.Fatalf("source has %d rows, target %d, or they differ", len(want), len(got))
	}
	req, _ := http.NewRequest(http.MethodDelete, hs.URL+"/api/v1/migrations/"+mig.ID, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var slots int
	src.QueryRow(`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, postgres.SlotName(mig.ID)).Scan(&slots)
	if slots != 0 {
		t.Errorf("the replication slot is still there after delete")
	}
}

// dropStream drops a test's replication slot and publication, should the
// test have failed before deleting its migration. The slot stays active
// for a moment after its worker stops, and an active slot can't be dropped.
func dropStream(src *sql.DB, id string) {
	name := postgres.SlotName(id)
	for i := 0; i < 800; i++ { // YugabyteDB holds a slot active for 60s
		var active sql.NullBool
		if err := src.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, name).Scan(&active); err == sql.ErrNoRows {
			break
		}
		if !active.Bool {
			src.Exec(`SELECT pg_drop_replication_slot($1)`, name)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	src.Exec(`DROP PUBLICATION IF EXISTS ` + name)
	// A YugabyteDB source's heartbeat row, and the table once it's empty.
	src.Exec(`DELETE FROM xferdb_heartbeat WHERE stream = $1`, name)
	src.Exec(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM xferdb_heartbeat) THEN DROP TABLE xferdb_heartbeat; END IF; END $$`)
}
