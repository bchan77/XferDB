package worker

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// TestLiveMigrationConverges runs the whole pipeline the way a live
// migration will: the slot is created, a writer keeps changing the source
// while the snapshot is copied and while capture and apply run (both are
// stopped and restarted partway), and once the writer stops the target must
// hold exactly the source's rows. Needs XFERDB_TEST_CDC_PG (wal_level=logical);
// the target is a SQLite file.
func TestLiveMigrationConverges(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live migration test")
	}
	ctx := context.Background()
	srcCfg := adapters.ConnectionConfig{Type: "postgres", DSN: dsn}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id := fmt.Sprintf("live%d", time.Now().UnixNano())
	table := "xferdb_live_" + id
	exec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(`CREATE TABLE ` + table + ` (grp text, id int, name text, big text, n bigint, PRIMARY KEY (grp, id))`)
	defer exec(`DROP TABLE IF EXISTS ` + table)
	for i := 0; i < 500; i++ {
		exec(`INSERT INTO `+table+` VALUES ($1, $2, $3, NULL, $4)`, grpOf(i), i, fmt.Sprint("row", i), i)
	}

	// Setup: the slot exists before the copy starts.
	cs, err := registry.NewChangeSource(ctx, srcCfg, id)
	if err != nil {
		t.Fatal(err)
	}
	start, err := cs.Prepare(ctx, []string{table})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cs.Teardown(context.Background()) //nolint:errcheck
		cs.Close()
	}()

	stopWriter, writerDone := startWriter(t, db, table)

	// Snapshot copy, while the writer runs.
	tgtPath := filepath.Join(t.TempDir(), "target.db")
	tgtCfg := adapters.ConnectionConfig{Type: "sqlite", Database: tgtPath}
	copyTable(t, srcCfg, tgtCfg, table)

	dir := t.TempDir()
	if err := WriteSpec(dir, &Spec{
		MigrationID: id, Source: srcCfg, Target: tgtCfg, Tables: []string{table},
		StartPos: start, SegmentBytes: 64 << 10,
	}); err != nil {
		t.Fatal(err)
	}
	run := func(role string) (stop func()) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Run(ctx, dir, role) }()
		return func() {
			cancel()
			if err := <-done; err != nil && err != context.Canceled {
				t.Errorf("%s: %v", role, err)
			}
		}
	}
	stopCapture, stopApply := run(RoleCapture), run(RoleApply)
	time.Sleep(1500 * time.Millisecond)
	// Crash and restart both mid-stream.
	stopApply()
	stopCapture()
	time.Sleep(300 * time.Millisecond)
	stopCapture, stopApply = run(RoleCapture), run(RoleApply)
	defer func() { stopApply(); stopCapture() }()
	time.Sleep(1500 * time.Millisecond)

	close(stopWriter)
	<-writerDone
	// A last marker change: once it's applied, everything before it is.
	exec(`INSERT INTO ` + table + ` VALUES ('zz', -1, 'done', NULL, 0)`)
	waitConverged(t, dir, tgtPath, table)

	want, got := sourceRows(t, db, table), targetRows(t, tgtPath, table)
	if len(want) != len(got) {
		t.Fatalf("source has %d rows, target %d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("row %d differs:\n source %s\n target %s", i, want[i], got[i])
		}
	}
	st, err := ReadStatus(ctx, dir, RoleApply)
	if err != nil || st.Apply.ChangesApplied == 0 {
		t.Fatalf("apply status = %+v, %v", st, err)
	}
	t.Logf("%d rows match; apply status %+v", len(want), *st.Apply)
}

func grpOf(i int) string { return []string{"a", "b", "c"}[i%3] }

// startWriter changes random rows until stop is closed: inserts, updates,
// TOASTed updates, key changes and deletes, several per transaction.
func startWriter(t *testing.T, db *sql.DB, table string) (chan struct{}, chan struct{}) {
	stop, done := make(chan struct{}), make(chan struct{})
	rnd := mrand.New(mrand.NewSource(1))
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
			for j := 0; j < 1+rnd.Intn(5); j++ {
				i := rnd.Intn(600)
				var q string
				var args []interface{}
				switch rnd.Intn(6) {
				case 0:
					next++
					q, args = `INSERT INTO `+table+` VALUES ($1, $2, 'new', NULL, 0) ON CONFLICT DO NOTHING`, []interface{}{grpOf(next), next}
				case 1:
					q, args = `UPDATE `+table+` SET n = n + 1, name = 'upd' WHERE grp = $1 AND id = $2`, []interface{}{grpOf(i), i}
				case 2:
					raw := make([]byte, 5000)
					rand.Read(raw)
					q, args = `UPDATE `+table+` SET big = $3 WHERE grp = $1 AND id = $2`, []interface{}{grpOf(i), i, hex.EncodeToString(raw)}
				case 3:
					// Only name changes: big, if TOASTed, is sent unchanged.
					q, args = `UPDATE `+table+` SET name = name || '+' WHERE grp = $1 AND id = $2`, []interface{}{grpOf(i), i}
				case 4:
					next++
					q, args = `UPDATE `+table+` SET id = $3 WHERE grp = $1 AND id = $2`, []interface{}{grpOf(i), i, next}
				case 5:
					q, args = `DELETE FROM `+table+` WHERE grp = $1 AND id = $2`, []interface{}{grpOf(i), i}
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
		}
	}()
	return stop, done
}

// copyTable is the snapshot: create the target table and copy every row,
// paging by key up to the last key at the start. A live snapshot must page by key, never by OFFSET: while
// the source is being written, OFFSET paging skips or repeats rows, and a
// row the copy skips and no later change rewrites in full is never fixed.
func copyTable(t *testing.T, srcCfg, tgtCfg adapters.ConnectionConfig, table string) {
	t.Helper()
	ctx := context.Background()
	src, _ := registry.NewSource("postgres")
	if err := src.Connect(ctx, srcCfg); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	db, err := sql.Open("postgres", srcCfg.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tgt, _ := registry.NewTarget("sqlite")
	if err := tgt.Connect(ctx, tgtCfg); err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	schema, err := src.GetSchema(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	if err := tgt.CreateTable(ctx, schema); err != nil {
		t.Fatal(err)
	}
	// Stop at the last key there was when the copy began: rows inserted
	// after it come through the stream, and chasing them never ends.
	var maxGrp string
	var maxID int64
	if err := db.QueryRow(`SELECT grp, id FROM `+table+` ORDER BY grp DESC, id DESC LIMIT 1`).Scan(&maxGrp, &maxID); err != nil {
		t.Fatal(err)
	}
	lastGrp, lastID := "", int64(-1<<31)
	for {
		rows, err := db.Query(`SELECT * FROM `+table+` WHERE (grp, id) > ($1, $2) AND (grp, id) <= ($3, $4) ORDER BY grp, id LIMIT 100`, lastGrp, lastID, maxGrp, maxID)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := adapters.ScanRecords(rows)
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			return
		}
		if err := tgt.WriteBatch(ctx, table, &adapters.Batch{Records: recs, Size: len(recs)}); err != nil {
			t.Fatal(err)
		}
		lastGrp, lastID = recs[len(recs)-1]["grp"].(string), recs[len(recs)-1]["id"].(int64)
		time.Sleep(20 * time.Millisecond) // let the writer interleave
	}
}

func waitConverged(t *testing.T, dir, tgtPath, table string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		db, _ := sql.Open("sqlite3", tgtPath)
		var n int
		db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %q WHERE grp = 'zz' AND id = -1`, table)).Scan(&n)
		db.Close()
		if n == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	cap, cerr := ReadStatus(context.Background(), dir, RoleCapture)
	app, aerr := ReadStatus(context.Background(), dir, RoleApply)
	t.Fatalf("marker not applied within 60s; capture %+v %v, apply %+v %v", cap, cerr, app, aerr)
}

func sourceRows(t *testing.T, db *sql.DB, table string) []string {
	return collect(t, db, `SELECT grp, id::text, name, COALESCE(md5(big), 'NULL'), n::text FROM `+table)
}

func targetRows(t *testing.T, path, table string) []string {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := collect(t, db, fmt.Sprintf(`SELECT grp, CAST(id AS TEXT), name, COALESCE(big, 'NULL'), CAST(n AS TEXT) FROM %q`, table))
	// SQLite has no md5(): hash the big column here to match.
	for i, r := range rows {
		f := strings.Split(r, "|")
		if f[3] != "NULL" {
			f[3] = md5hex(f[3])
		}
		rows[i] = strings.Join(f, "|")
	}
	sort.Strings(rows)
	return rows
}

func collect(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var f [5]string
		if err := rows.Scan(&f[0], &f[1], &f[2], &f[3], &f[4]); err != nil {
			t.Fatal(err)
		}
		out = append(out, strings.Join(f[:], "|"))
	}
	sort.Strings(out)
	return out
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}
