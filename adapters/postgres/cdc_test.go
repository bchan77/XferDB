package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

var _ cdc.ChangeSource = (*ChangeSource)(nil)

// cdcSource connects to XFERDB_TEST_CDC_PG, a DSN for a server with
// wal_level=logical and a login allowed to replicate, or skips the test.
func cdcSource(t *testing.T) (adapters.ConnectionConfig, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("XFERDB_TEST_CDC_PG")
	if dsn == "" {
		t.Skip("XFERDB_TEST_CDC_PG not set; skipping live logical replication test")
	}
	cfg := adapters.ConnectionConfig{Type: "postgres", DSN: dsn}
	db, err := sql.Open("postgres", buildDSN(cfg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return cfg, db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...interface{}) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// startStream prepares a slot for tables and streams it into a channel.
func startStream(t *testing.T, cfg adapters.ConnectionConfig, stream string, tables []string, from cdc.Position) (*ChangeSource, cdc.Position, chan cdc.Change, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	src, err := NewChangeSource(ctx, cfg, stream)
	if err != nil {
		t.Fatal(err)
	}
	start, err := src.Prepare(ctx, tables)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if from == nil {
		from = start
	}
	out := make(chan cdc.Change, 1000)
	done := make(chan error, 1)
	go func() { done <- src.Stream(ctx, from, out) }()
	stop := func() {
		cancel()
		if err := <-done; err != nil && err != context.Canceled {
			t.Errorf("Stream: %v", err)
		}
	}
	return src, start, out, stop
}

// next returns the next row change, skipping the stream's descriptions of
// tables (OpSchema), which nextChange returns too.
func next(t *testing.T, out chan cdc.Change) cdc.Change {
	t.Helper()
	for {
		if c := nextChange(t, out); c.Op != cdc.OpSchema {
			return c
		}
	}
}

func nextChange(t *testing.T, out chan cdc.Change) cdc.Change {
	t.Helper()
	select {
	case c := <-out:
		return c
	case <-time.After(15 * time.Second):
		t.Fatal("no change within 15s")
	}
	return cdc.Change{}
}

// TestStreamDescribesTables: the stream describes a table's columns exactly
// as Columns does (else every new session would look like a schema change),
// generated columns left out, and again after a column is added or retyped.
// Retention reports the slot, and a slot that is gone as lost.
func TestStreamDescribesTables(t *testing.T) {
	cfg, db := cdcSource(t)
	stream := fmt.Sprintf("ddl%d", time.Now().UnixNano())
	table := "xferdb_cdc_ddl_" + stream
	mustExec(t, db, `DROP DOMAIN IF EXISTS xferdb_cdc_code CASCADE; CREATE DOMAIN xferdb_cdc_code AS varchar(8)`)
	mustExec(t, db, `CREATE TABLE `+table+` (id int PRIMARY KEY, vc varchar(40), num numeric(10,2),
		code xferdb_cdc_code, tags text[], at timestamptz(3), twice int GENERATED ALWAYS AS (id * 2) STORED)`)
	defer mustExec(t, db, `DROP TABLE IF EXISTS `+table)

	src, _, out, stopStream := startStream(t, cfg, stream, []string{table}, nil)
	defer src.Teardown(context.Background()) //nolint:errcheck
	var once sync.Once
	stop := func() { once.Do(stopStream) }
	defer stop()
	ctx := context.Background()
	columns := func() []cdc.Column {
		t.Helper()
		m, err := src.Columns(ctx, []string{table})
		if err != nil {
			t.Fatal(err)
		}
		return m[table]
	}
	described := func() cdc.Change {
		t.Helper()
		c := nextChange(t, out)
		if c.Op != cdc.OpSchema || c.Table != table || c.Pos != nil {
			t.Fatalf("want the table described first, got %v %s", c.Op, c.Table)
		}
		if row := next(t, out); row.Op != cdc.OpInsert {
			t.Fatalf("then the insert, got %v", row.Op)
		}
		return c
	}

	before := columns()
	want := []cdc.Column{{Name: "id", Type: "integer"}, {Name: "vc", Type: "character varying(40)"}, {Name: "num", Type: "numeric(10,2)"},
		{Name: "code", Type: "xferdb_cdc_code"}, {Name: "tags", Type: "text[]"}, {Name: "at", Type: "timestamp(3) with time zone"}}
	if !reflect.DeepEqual(before, want) {
		t.Fatalf("Columns = %v, want %v", before, want)
	}
	mustExec(t, db, `INSERT INTO `+table+` (id) VALUES (1)`)
	if c := described(); !cdc.SameColumns(c.Columns, before) {
		t.Fatalf("stream described %v, Columns %v", c.Columns, before)
	}

	mustExec(t, db, `ALTER TABLE `+table+` ADD COLUMN note text, ALTER COLUMN vc TYPE varchar(80)`)
	mustExec(t, db, `INSERT INTO `+table+` (id) VALUES (2)`)
	c := described()
	if !cdc.SameColumns(c.Columns, columns()) {
		t.Fatalf("after DDL the stream described %v, Columns %v", c.Columns, columns())
	}
	got := cdc.DescribeColumns(before, c.Columns)
	if !reflect.DeepEqual(got, []string{"column vc changed type: character varying(40) → character varying(80)", "column note added (text)"}) {
		t.Fatalf("described the change as %q", got)
	}

	ret, err := src.Retention(ctx)
	if err != nil || ret.Lost || ret.RetainedBytes < 0 {
		t.Fatalf("Retention = %+v, %v", ret, err)
	}
	stop()
	if err := src.Teardown(ctx); err != nil {
		t.Fatal(err)
	}
	if ret, err := src.Retention(ctx); err != nil || !ret.Lost || ret.State != "missing" {
		t.Fatalf("Retention of a dropped slot = %+v, %v", ret, err)
	}
}

// TestDecodeMatchesSnapshot is the rule a live migration rests on: for every
// column type, a value decoded from the change stream is the same Go value
// the snapshot read (ReadBatch, through lib/pq) returns for that row.
func TestDecodeMatchesSnapshot(t *testing.T) {
	cfg, db := cdcSource(t)
	stream := fmt.Sprintf("types%d", time.Now().UnixNano())
	table := "xferdb_cdc_types_" + stream
	mustExec(t, db, `DROP TYPE IF EXISTS xferdb_cdc_mood CASCADE; CREATE TYPE xferdb_cdc_mood AS ENUM ('sad', 'happy')`)
	mustExec(t, db, `DROP DOMAIN IF EXISTS xferdb_cdc_posint CASCADE; CREATE DOMAIN xferdb_cdc_posint AS integer CHECK (VALUE > 0)`)
	mustExec(t, db, `CREATE TABLE `+table+` (
		id int PRIMARY KEY, i2 smallint, i8 bigint, num numeric(12,3), r real, d double precision,
		b boolean, t text, vc varchar(20), ch char(3), ba bytea, dt date, tm time, ttz timetz,
		ts timestamp(6), tstz timestamptz, u uuid, j json, jb jsonb, arr int[], mood xferdb_cdc_mood,
		dom xferdb_cdc_posint, iv interval, ip inet, money_ numeric)`)
	defer mustExec(t, db, `DROP TABLE IF EXISTS `+table)

	src, _, out, stop := startStream(t, cfg, stream, []string{table}, nil)
	defer src.Teardown(context.Background()) //nolint:errcheck
	defer stop()

	mustExec(t, db, `INSERT INTO `+table+` VALUES
		(1, -32768, 9007199254740993, -12345.678, 3.14159, 2.718281828459045, true, 'héllo
world', 'vc', 'ab', '\x00ff10', '2026-10-27', '23:59:59.999999', '10:30:00+05:30',
		'2026-10-27 10:30:00.123456', '2026-10-27 10:30:00.123456-04', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		'{"a": [1, 2]}', '{"b": {"c": null}}', '{1,NULL,3}', 'happy', 42, '1 day 02:03:04', '10.0.0.1/8', 1e30),
		(2, NULL, NULL, NULL, 'NaN', '-Infinity', false, '', NULL, NULL, '\x', '-infinity', '24:00:00', NULL,
		'infinity', '1999-12-31 23:59:59+14', NULL, NULL, NULL, '{}', NULL, NULL, NULL, NULL, NULL)`)

	got := map[int64]map[string]interface{}{}
	for len(got) < 2 {
		c := next(t, out)
		if c.Op != cdc.OpInsert {
			t.Fatalf("unexpected %v", c.Op)
		}
		got[c.After["id"].(int64)] = c.After
	}

	s := &Source{}
	if err := s.Connect(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, err := s.ReadBatch(context.Background(), table, adapters.BatchOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range b.Records {
		id := want["id"].(int64)
		for col, wv := range want {
			gv := got[id][col]
			if wt, ok := wv.(time.Time); ok {
				gt, ok := gv.(time.Time)
				if !ok || !gt.Equal(wt) {
					t.Errorf("row %d %s: stream %#v, snapshot %#v", id, col, gv, wv)
				}
				continue
			}
			if f, ok := wv.(float64); ok && f != f { // NaN
				if g, ok := gv.(float64); !ok || g == g {
					t.Errorf("row %d %s: stream %#v, snapshot NaN", id, col, gv)
				}
				continue
			}
			if !reflect.DeepEqual(gv, wv) {
				t.Errorf("row %d %s: stream %#v (%T), snapshot %#v (%T)", id, col, gv, gv, wv, wv)
			}
		}
	}
}

func TestStreamChanges(t *testing.T) {
	cfg, db := cdcSource(t)
	stream := fmt.Sprintf("stream%d", time.Now().UnixNano())
	table := "xferdb_cdc_rows_" + stream
	other := "xferdb_cdc_other_" + stream
	mustExec(t, db, `CREATE TABLE `+table+` (id int, tenant text, name text, big text, PRIMARY KEY (tenant, id))`)
	mustExec(t, db, `CREATE TABLE `+other+` (id int PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE `+table+`_nokey (id int)`)
	defer mustExec(t, db, `DROP TABLE IF EXISTS `+table+`, `+other+`, `+table+`_nokey`)

	src, err := NewChangeSource(context.Background(), cfg, stream+"x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Prepare(context.Background(), []string{table, table + "_nokey"}); err == nil || !strings.Contains(err.Error(), "_nokey") {
		t.Fatalf("Prepare with a keyless table: %v", err)
	}
	src.Close()

	src, start, out, stop := startStream(t, cfg, stream, []string{table}, nil)
	defer src.Teardown(context.Background()) //nolint:errcheck
	// Prepare again returns the slot that's already there.
	if again, err := src.Prepare(context.Background(), []string{table}); err != nil || string(again) != string(start) {
		t.Fatalf("second Prepare = %v, %v; want %v", again, err, start)
	}

	raw := make([]byte, 8000)
	rand.Read(raw)
	big := hex.EncodeToString(raw)                      // incompressible, so it's stored TOASTed
	mustExec(t, db, `INSERT INTO `+other+` VALUES (1)`) // not published
	tx, _ := db.Begin()
	tx.Exec(`INSERT INTO `+table+` VALUES (1, 'a', 'one', $1)`, big)
	tx.Exec(`INSERT INTO ` + table + ` VALUES (2, 'a', 'two', NULL)`)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	c1, c2 := next(t, out), next(t, out)
	if c1.Op != cdc.OpInsert || c1.Pos != nil || c2.Pos == nil {
		t.Fatalf("a transaction's Pos belongs on its last change only: %+v / %+v", c1.Pos, c2.Pos)
	}
	if c1.After["big"] != big || c1.Key["id"] != int64(1) || c1.Key["tenant"] != "a" || len(c1.Key) != 2 {
		t.Fatalf("insert decoded as %+v", c1.Key)
	}

	mustExec(t, db, `UPDATE `+table+` SET name = 'uno' WHERE id = 1`)
	u := next(t, out)
	if u.Op != cdc.OpUpdate || u.After["name"] != "uno" || !u.Partial() || u.Unchanged[0] != "big" || u.OldKey != nil {
		t.Fatalf("update leaving a TOASTed column: %+v", u)
	}
	if _, ok := u.After["big"]; ok {
		t.Fatal("unchanged TOASTed column should be left out of After")
	}

	mustExec(t, db, `UPDATE `+table+` SET id = 3 WHERE id = 2`)
	k := next(t, out)
	if k.OldKey["id"] != int64(2) || k.Key["id"] != int64(3) || k.OldKey["tenant"] != "a" {
		t.Fatalf("key change: key %v old %v", k.Key, k.OldKey)
	}

	mustExec(t, db, `DELETE FROM `+table+` WHERE id = 3`)
	d := next(t, out)
	if d.Op != cdc.OpDelete || d.Key["id"] != int64(3) || d.Key["tenant"] != "a" {
		t.Fatalf("delete: %+v", d)
	}
	if err := src.Ack(context.Background(), d.Pos); err != nil {
		t.Fatal(err)
	}

	mustExec(t, db, `TRUNCATE `+table)
	tr := next(t, out)
	if tr.Op != cdc.OpTruncate || tr.Table != table || tr.Pos == nil {
		t.Fatalf("truncate: %+v", tr)
	}

	// The ack reaches the server, and a restart from the acked position
	// resends only what came after it.
	acked, _ := decodeLSN(d.Pos)
	waitFor(t, func() bool {
		var flushed string
		db.QueryRow(`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, SlotName(stream)).Scan(&flushed)
		lsn, err := pglogrepl.ParseLSN(flushed)
		return err == nil && lsn >= acked
	})
	stop()
	src.Close()
	src2, _, out2, stop2 := startStream(t, cfg, stream, []string{table}, d.Pos)
	if c := next(t, out2); c.Op != cdc.OpTruncate {
		t.Fatalf("after restart got %v first, want the truncate", c.Op)
	}
	stop2()

	if err := src2.Teardown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	db.QueryRow(`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`, SlotName(stream)).Scan(&n)
	var p int
	db.QueryRow(`SELECT count(*) FROM pg_publication WHERE pubname = $1`, SlotName(stream)).Scan(&p)
	if n != 0 || p != 0 {
		t.Fatalf("Teardown left %d slots, %d publications", n, p)
	}
	src2.Close()
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 20s")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
