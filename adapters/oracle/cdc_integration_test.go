package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

// TestChangeStream streams a set of transactions through LogMiner and
// checks the changes they become (#195): inserts and updates read back as
// of their commit (two updates of one row arrive with their own values),
// deletes by key, a key change as a delete and an insert, a rolled-back
// transaction not at all, a LOB write, a composite key with a DATE, a
// quoted mixed-case table, and a column added with ALTER TABLE as a schema
// change. XFERDB_TEST_ORACLE_LIVE is an oracle:// DSN of a login with the
// LogMiner privileges LiveChecks lists, on a database in ARCHIVELOG mode
// with primary key supplemental logging. On 21c, which mines archived logs
// only, set archive_lag_target (30s will do) or this waits for a log switch.
func TestChangeStream(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_ORACLE_LIVE")
	if dsn == "" {
		t.Skip("XFERDB_TEST_ORACLE_LIVE not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exec := func(stmts ...string) {
		t.Helper()
		for _, s := range stmts {
			if _, err := db.ExecContext(ctx, s); err != nil {
				t.Fatalf("%v\n%s", err, s)
			}
		}
	}
	for _, tbl := range []string{"XL_ITEMS", "XL_COMP", `"XlMixed"`} {
		db.ExecContext(ctx, "DROP TABLE "+tbl+" PURGE") //nolint:errcheck // may not exist
	}
	exec(
		`CREATE TABLE xl_items (id NUMBER(10) PRIMARY KEY, name VARCHAR2(40) UNIQUE, amt NUMBER(12,2), note CLOB)`,
		`CREATE TABLE xl_comp (k1 VARCHAR2(10), k2 DATE, v NUMBER, PRIMARY KEY (k1, k2))`,
		`CREATE TABLE "XlMixed" ("Id" NUMBER(5) PRIMARY KEY, "Label" VARCHAR2(20))`,
		`INSERT INTO xl_items VALUES (1, 'one', 1.5, NULL)`,
		`COMMIT`,
	)
	// A flashback query can't read a table as of moments after its CREATE
	// (ORA-01466); the stream would fall back to the current rows, and the
	// two updates below would arrive with the same values.
	time.Sleep(15 * time.Second)

	cs, err := NewChangeSource(ctx, adapters.ConnectionConfig{Type: "oracle", DSN: dsn}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	cs.poll = 200 * time.Millisecond
	tables := []string{"xl_items", "xl_comp", "XlMixed"}
	if err := cs.SetTables(tables); err != nil {
		t.Fatal(err)
	}
	start, err := cs.Prepare(ctx, tables)
	if err != nil {
		t.Fatal(err)
	}

	out := make(chan cdc.Change, 100)
	errc := make(chan error, 1)
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { errc <- cs.Stream(sctx, start, out) }()
	var got []cdc.Change
	var transactions int
	// until reads changes until one matches.
	until := func(what string, done func(cdc.Change) bool) {
		t.Helper()
		for {
			select {
			case c := <-out:
				got = append(got, c)
				if c.Pos != nil {
					transactions++
				}
				if done(c) {
					return
				}
			case err := <-errc:
				t.Fatalf("stream ended waiting for %s: %v; so far:\n%s", what, err, describe(got))
			case <-ctx.Done():
				t.Fatalf("timed out waiting for %s; so far:\n%s", what, describe(got))
			}
		}
	}

	tx := func(stmts ...string) {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range stmts {
			if _, err := tx.ExecContext(ctx, s); err != nil {
				t.Fatalf("%v\n%s", err, s)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	tx(`INSERT INTO xl_items VALUES (2, 'two', 2.5, NULL)`)
	tx(`UPDATE xl_items SET name = 'one-b' WHERE id = 1`)
	tx(`UPDATE xl_items SET name = 'one-c', amt = 10 WHERE id = 1`)
	tx(`DELETE FROM xl_items WHERE id = 2`)
	rb, _ := db.BeginTx(ctx, nil)
	rb.ExecContext(ctx, `INSERT INTO xl_items VALUES (3, 'rolled back', 0, NULL)`) //nolint:errcheck
	rb.Rollback()                                                                  //nolint:errcheck
	tx(`UPDATE xl_items SET id = 10 WHERE id = 1`)
	tx(`UPDATE xl_items SET note = TO_CLOB(RPAD('n', 4000, 'n')) || TO_CLOB(RPAD('m', 2000, 'm')) WHERE id = 10`)
	// Replacing a LOB stored out of line logs no UPDATE, only LOB
	// operations without a ROWID.
	tx(`UPDATE xl_items SET note = TO_CLOB(RPAD('w', 4000, 'w')) || TO_CLOB(RPAD('v', 500, 'v')) WHERE id = 10`)
	// Two rows swap a unique value through a third: each row read back has
	// the other's old value, so both are deleted before they're written.
	tx(`INSERT INTO xl_items VALUES (20, 'twenty', 0, NULL)`)
	tx(`UPDATE xl_items SET name = 'swap' WHERE id = 10`,
		`UPDATE xl_items SET name = 'one-c' WHERE id = 20`,
		`UPDATE xl_items SET name = 'twenty' WHERE id = 10`)
	tx(`INSERT INTO xl_comp VALUES ('a', DATE '2024-02-03' + 0.5, 7)`,
		`INSERT INTO "XlMixed" VALUES (1, 'x')`)
	// Mined before the ALTER TABLE: the online dictionary can't read a
	// table's redo from before its definition changed.
	until("the inserts", func(c cdc.Change) bool { return c.Table == "XlMixed" && c.Pos != nil })
	exec(`ALTER TABLE xl_items ADD (extra VARCHAR2(10))`)
	tx(`UPDATE xl_items SET extra = 'e' WHERE id = 10`)
	until("the new column", func(c cdc.Change) bool { return c.After != nil && c.After["extra"] == "e" })
	stop()
	<-errc
	t.Logf("%d changes in %d transactions:\n%s", len(got), transactions, describe(got))

	want := []string{
		"schema XlMixed", "schema xl_comp", "schema xl_items", // the session's baseline, any order
		"upsert xl_items 2 name=two",
		"upsert xl_items 1 name=one-b",
		"upsert xl_items 1 name=one-c",
		"delete xl_items 2",
		"delete xl_items 1", "upsert xl_items 10 name=one-c",
		"upsert xl_items 10 note=6000",
		"upsert xl_items 10 note=4500",
		"upsert xl_items 20 name=twenty",
		"delete xl_items 10", "delete xl_items 20", "upsert xl_items 10 note=4500", "upsert xl_items 20 name=one-c",
		"upsert xl_comp a/2024-02-03 12:00:00", "upsert XlMixed 1",
		"schema xl_items +extra",
		"upsert xl_items 10 extra=e",
	}
	var have []string
	for _, c := range got {
		have = append(have, summary(c))
	}
	// The baseline schema changes come in map order.
	baseline := map[string]bool{}
	for _, h := range have[:3] {
		baseline[h] = true
	}
	for _, w := range want[:3] {
		if !baseline[w] {
			t.Errorf("baseline: missing %q in %v", w, have[:3])
		}
	}
	if strings.Join(have[3:], "\n") != strings.Join(want[3:], "\n") {
		t.Errorf("changes:\n%s\nwant:\n%s", strings.Join(have[3:], "\n"), strings.Join(want[3:], "\n"))
	}
	if transactions != 11 { // the ALTER TABLE carries no Pos
		t.Errorf("%d transactions with a Pos, want 11", transactions)
	}

	// Acked up to the last change, the stream confirms past it once idle.
	var last cdc.Position
	for _, c := range got {
		if c.Pos != nil {
			last = c.Pos
		}
	}
	if err := cs.Ack(ctx, last); err != nil {
		t.Fatal(err)
	}
	cur, err := cs.CurrentPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cs.Behind(last, cur) == 0 {
		t.Errorf("Behind(%s, %s) = 0", last, cur)
	}
	// Restarting from the last position sends nothing again.
	restart := func(from cdc.Position, wait time.Duration) ([]cdc.Change, error) {
		cs2, err := NewChangeSource(ctx, adapters.ConnectionConfig{Type: "oracle", DSN: dsn}, "test")
		if err != nil {
			t.Fatal(err)
		}
		defer cs2.Close()
		cs2.poll = 200 * time.Millisecond
		cs2.SetTables(tables) //nolint:errcheck
		ch := make(chan cdc.Change, 100)
		rctx, rstop := context.WithTimeout(ctx, wait)
		defer rstop()
		err = cs2.Stream(rctx, from, ch)
		close(ch)
		var data []cdc.Change
		for c := range ch {
			if c.Op != cdc.OpSchema {
				data = append(data, c)
			}
		}
		return data, err
	}
	if again, _ := restart(last, 5*time.Second); len(again) > 0 {
		t.Errorf("resent after restart:\n%s", describe(again))
	}

	// A stream behind across a schema change can't read what came before
	// it, and says so rather than skipping it.
	tx(`UPDATE xl_items SET name = 'lost' WHERE id = 10`)
	exec(`ALTER TABLE xl_items ADD (extra2 NUMBER)`)
	if data, err := restart(last, 3*time.Minute); !errors.Is(err, errUndecodable) {
		t.Errorf("stream behind a schema change: %v, want errUndecodable; it sent:\n%s", err, describe(data))
	}
}

func summary(c cdc.Change) string {
	switch c.Op {
	case cdc.OpSchema:
		s := "schema " + c.Table
		for _, col := range c.Columns {
			if col.Name == "extra" {
				s += " +extra"
			}
		}
		return s
	case cdc.OpDelete:
		return "delete " + c.Table + " " + keyOf(c.Key)
	}
	s := "upsert " + c.Table + " " + keyOf(c.Key)
	switch {
	case c.After["extra"] != nil:
		s += fmt.Sprintf(" extra=%v", c.After["extra"])
	case c.After["note"] != nil:
		s += fmt.Sprintf(" note=%d", len(fmt.Sprint(c.After["note"])))
	case c.After["name"] != nil:
		s += fmt.Sprintf(" name=%v", c.After["name"])
	}
	return s
}

func keyOf(k map[string]interface{}) string {
	if id, ok := k["id"]; ok {
		return fmt.Sprint(id)
	}
	if id, ok := k["Id"]; ok {
		return fmt.Sprint(id)
	}
	if t, ok := k["k2"].(time.Time); ok {
		return fmt.Sprintf("%v/%s", k["k1"], t.Format("2006-01-02 15:04:05"))
	}
	return fmt.Sprint(k)
}

func describe(cs []cdc.Change) string {
	var b strings.Builder
	for _, c := range cs {
		fmt.Fprintf(&b, "  %s pos=%s\n", summary(c), c.Pos)
	}
	return b.String()
}
