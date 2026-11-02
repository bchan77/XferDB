package cdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/postgres"
	"gitea.homelab.local/nextdevops/XferDB/adapters/sqlite"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	_ "github.com/mattn/go-sqlite3"
)

func sqliteTarget(t *testing.T) (adapters.TargetAdapter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "target.db")
	tgt := sqlite.NewTarget()
	if err := tgt.Connect(context.Background(), adapters.ConnectionConfig{Type: "sqlite", Database: path}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tgt.Close() })
	for _, name := range []string{"a", "b"} {
		err := tgt.CreateTable(context.Background(), &adapters.TableSchema{Name: name, Columns: []adapters.ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "v", Type: "text", Nullable: true},
			{Name: "big", Type: "text", Nullable: true},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	return tgt, path
}

func dump(t *testing.T, path, table string) string {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(fmt.Sprintf(`SELECT id, COALESCE(v, 'NULL'), COALESCE(big, 'NULL') FROM %q ORDER BY id`, table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int64
		var v, big string
		rows.Scan(&id, &v, &big)
		out = append(out, fmt.Sprintf("%d:%s:%s", id, v, big))
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

func row(id int64, v, big string) map[string]interface{} {
	return map[string]interface{}{"id": id, "v": v, "big": big}
}

func key(id int64) map[string]interface{} { return map[string]interface{}{"id": id} }

func appendTxn(t *testing.T, w *cdc.Writer, changes ...cdc.Change) {
	t.Helper()
	ptrs := make([]*cdc.Change, len(changes))
	for i := range changes {
		ptrs[i] = &changes[i]
	}
	ptrs[len(ptrs)-1].Pos = cdc.Position{byte(len(changes))}
	if err := w.Append(ptrs...); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

// runUntil applies until the target's high-water mark reaches seq.
func runUntil(t *testing.T, a *cdc.Applier, seq uint64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	deadline := time.After(10 * time.Second)
	for a.Status().AppliedSeq < seq {
		select {
		case err := <-done:
			t.Fatalf("apply stopped: %v", err)
		case <-deadline:
			t.Fatalf("applied %d of %d", a.Status().AppliedSeq, seq)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("Run: %v", err)
	}
}

func TestApplyReplaysEveryKindOfChange(t *testing.T) {
	tgt, path := sqliteTarget(t)
	dir := t.TempDir()
	w, err := cdc.OpenWriter(dir, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	ins := func(table string, id int64, v string) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(id), After: row(id, v, "B"+v)}
	}
	appendTxn(t, w, ins("a", 1, "x"), ins("a", 2, "y"), ins("b", 1, "p"))
	// The same key twice in one batch, then a delete and re-insert.
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(1), After: row(1, "x2", "Bx")},
		cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(1), After: row(1, "x3", "Bx")})
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpDelete, Key: key(2)}, ins("a", 2, "y2"))
	// A partial update keeps the column it didn't send.
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(1),
		After: map[string]interface{}{"id": int64(1), "v": "x4"}, Unchanged: []string{"big"}})
	// A key change, whole and partial.
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(3), OldKey: key(2), After: row(3, "y3", "By3")})
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(4), OldKey: key(1),
		After: map[string]interface{}{"id": int64(4), "v": "x5"}, Unchanged: []string{"big"}})
	// A partial update of a row that isn't there is skipped.
	appendTxn(t, w, cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(99),
		After: map[string]interface{}{"id": int64(99), "v": "ghost"}, Unchanged: []string{"big"}})
	appendTxn(t, w, cdc.Change{Table: "b", Op: cdc.OpTruncate}, ins("b", 7, "q"))

	a := &cdc.Applier{Stream: "s1", Dir: dir, Target: tgt.(adapters.ChangeApplier),
		Opts: cdc.ApplyOptions{MaxBatch: 4, MaxWait: 10 * time.Millisecond, RemoveApplied: true}}
	lastSeq, _ := w.Last()
	runUntil(t, a, lastSeq)

	if got, want := dump(t, path, "a"), "3:y3:By3 4:x5:Bx"; got != want {
		t.Fatalf("table a = %q, want %q", got, want)
	}
	if got, want := dump(t, path, "b"), "7:q:Bq"; got != want {
		t.Fatalf("table b = %q, want %q", got, want)
	}
	if st, _ := cdc.QueueStats(dir); st.Segments > 2 {
		t.Fatalf("applied segments not removed: %+v", st)
	}
}

// TestApplySwapsUniqueValuesInOrder replays two rows swapping a unique
// value through a third, as one transaction does on the source. Merging
// each key's updates into one statement used to give each row the other's
// value while it still held it, which a PostgreSQL target refuses (SQLite's
// INSERT OR REPLACE happens to cope). The PostgreSQL run needs
// XFERDB_TEST_CDC_PG.
func TestApplySwapsUniqueValuesInOrder(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		tgt, path := sqliteTarget(t)
		applySwap(t, tgt, "a")
		if got, want := dump(t, path, "a"), "1:blue: 2:red:"; got != want {
			t.Fatalf("table a = %q, want %q", got, want)
		}
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("XFERDB_TEST_CDC_PG")
		if dsn == "" {
			t.Skip("XFERDB_TEST_CDC_PG not set")
		}
		tgt := postgres.NewTarget()
		if err := tgt.Connect(context.Background(), adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
			t.Fatal(err)
		}
		defer tgt.Close()
		table := fmt.Sprintf("xferdb_swap_%d", time.Now().UnixNano())
		if err := tgt.CreateTable(context.Background(), &adapters.TableSchema{Name: table, Columns: []adapters.ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "v", Type: "text", Nullable: true},
			{Name: "big", Type: "text", Nullable: true},
		}}); err != nil {
			t.Fatal(err)
		}
		defer tgt.DropTable(context.Background(), table) //nolint:errcheck
		applySwap(t, tgt, table)
		db, _ := sql.Open("postgres", dsn)
		defer db.Close()
		var got string
		db.QueryRow(`SELECT string_agg(id || ':' || v, ' ' ORDER BY id) FROM ` + table).Scan(&got)
		if want := "1:blue 2:red"; got != want {
			t.Fatalf("table = %q, want %q", got, want)
		}
	})
}

func applySwap(t *testing.T, tgt adapters.TargetAdapter, table string) {
	t.Helper()
	if err := tgt.CreateIndexes(context.Background(), table, []adapters.IndexDef{{Name: table + "_v", Columns: []string{"v"}, Unique: true}}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	w, err := cdc.OpenWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	upd := func(id int64, v string) cdc.Change {
		return cdc.Change{Table: table, Op: cdc.OpUpdate, Key: key(id), After: row(id, v, "")}
	}
	ap, ok := tgt.(adapters.ChangeApplier)
	if !ok {
		t.Fatal("target can't apply changes")
	}
	if err := ap.PrepareChangeState(context.Background()); err != nil {
		t.Fatal(err)
	}
	stream := fmt.Sprintf("swap-%d", time.Now().UnixNano())
	defer ap.ClearChangeState(context.Background(), stream) //nolint:errcheck
	a := &cdc.Applier{Stream: stream, Dir: dir, Target: ap,
		Opts: cdc.ApplyOptions{MaxBatch: 100, MaxWait: 50 * time.Millisecond}}
	// The rows are on the target before the swap arrives, as they would be
	// after the snapshot.
	appendTxn(t, w, cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(1), After: row(1, "red", "")},
		cdc.Change{Table: table, Op: cdc.OpInsert, Key: key(2), After: row(2, "blue", "")})
	seq, _ := w.Last()
	runUntil(t, a, seq)
	appendTxn(t, w, upd(1, "swap"), upd(2, "red"), upd(1, "blue"))
	seq, _ = w.Last()
	runUntil(t, a, seq)
}

func TestApplyRestartsWhereTheTargetIs(t *testing.T) {
	tgt, path := sqliteTarget(t)
	ap := tgt.(adapters.ChangeApplier)
	dir := t.TempDir()
	w, _ := cdc.OpenWriter(dir, 0)
	defer w.Close()
	appendTxn(t, w,
		cdc.Change{Table: "a", Op: cdc.OpInsert, Key: key(1), After: row(1, "one", "")},
		cdc.Change{Table: "b", Op: cdc.OpInsert, Key: key(1), After: row(1, "uno", "")},
	)
	appendTxn(t, w,
		cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(1), After: row(1, "two", "")},
		cdc.Change{Table: "b", Op: cdc.OpUpdate, Key: key(1), After: row(1, "dos", "")},
	)
	// A crash after table a committed the whole batch but before b did and
	// before the high-water mark: a's row and seq are on the target.
	ctx := context.Background()
	ap.PrepareChangeState(ctx)
	tx, _ := ap.BeginChanges(ctx)
	tx.Upsert(ctx, "a", []map[string]interface{}{row(1, "two", "")})
	tx.SetApplied(ctx, "s1", "a", 3)
	tx.Commit()
	// Meanwhile someone wrote to a on the target directly: a replay of seq
	// 1 or 3 would undo it, so it shows whether they are skipped.
	db, _ := sql.Open("sqlite3", path)
	db.Exec(`UPDATE a SET big = 'kept' WHERE id = 1`)
	db.Close()

	a := &cdc.Applier{Stream: "s1", Dir: dir, Target: ap, Opts: cdc.ApplyOptions{MaxWait: 10 * time.Millisecond}}
	runUntil(t, a, 4)
	if got := dump(t, path, "a"); got != "1:two:kept" {
		t.Fatalf("a = %q: a change it already had was applied again", got)
	}
	if got := dump(t, path, "b"); got != "1:dos:" {
		t.Fatalf("b = %q", got)
	}
	seqs, _ := ap.AppliedSeqs(ctx, "s1")
	if seqs[cdc.HighWaterTable] != 4 || seqs["b"] != 4 || seqs["a"] != 3 {
		t.Fatalf("applied = %v", seqs)
	}
}

// An update that changes the key without sending every column, against each
// state the snapshot can leave the row in.
func TestApplyMovedRowWithUnsentColumns(t *testing.T) {
	move := func(from, to int64, v string) cdc.Change {
		return cdc.Change{Table: "a", Op: cdc.OpUpdate, Key: key(to), OldKey: key(from),
			After: map[string]interface{}{"id": to, "v": v}, Unchanged: []string{"big"}}
	}
	tgt, path := sqliteTarget(t)
	ap := tgt.(adapters.ChangeApplier)
	ctx := context.Background()
	ap.PrepareChangeState(ctx)
	tx, _ := ap.BeginChanges(ctx)
	tx.Upsert(ctx, "a", []map[string]interface{}{
		row(1, "old-only", "B1"), // copied before the move: only the old key
		row(2, "stale", "B2"),    // copied at both keys:
		row(3, "newer", "B3"),    //   the new key's row is the newer state
	})
	tx.Commit()
	dir := t.TempDir()
	w, _ := cdc.OpenWriter(dir, 0)
	defer w.Close()
	appendTxn(t, w,
		move(1, 11, "moved"),
		move(2, 3, "moved2"),
		move(4, 14, "refetched"), // at neither key: read from the source
		move(5, 15, "gone"),      // at neither, and since deleted on the source
	)
	var asked []int64
	a := &cdc.Applier{Stream: "s", Dir: dir, Target: ap, Opts: cdc.ApplyOptions{MaxWait: 10 * time.Millisecond},
		Refetch: func(_ context.Context, table string, k map[string]interface{}) (map[string]interface{}, bool, error) {
			id := k["id"].(int64)
			asked = append(asked, id)
			if id == 14 {
				return row(14, "from-source", "B14"), true, nil
			}
			return nil, false, nil
		}}
	runUntil(t, a, 4)
	if got, want := dump(t, path, "a"), "11:moved:B1 14:from-source:B14 3:moved2:B3"; got != want {
		t.Fatalf("a = %q, want %q", got, want)
	}
	if fmt.Sprint(asked) != "[14 15]" {
		t.Fatalf("refetched %v, want [14 15]", asked)
	}

	// Without a source to read from, that case fails instead of losing
	// the columns.
	appendTxn(t, w, move(6, 16, "x"))
	a2 := &cdc.Applier{Stream: "s", Dir: dir, Target: ap, Opts: cdc.ApplyOptions{MaxWait: 10 * time.Millisecond}}
	if err := a2.Run(ctx); err == nil || !strings.Contains(err.Error(), "neither key") {
		t.Fatalf("Run without Refetch: %v", err)
	}
}
