package oracle

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/cdc"
)

func TestPositions(t *testing.T) {
	p := position{Restart: 90, Commit: 100, XID: "0A00", At: 1700000000000000}
	got, err := decodePosition(p.encode())
	if err != nil || got != p {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	if _, err := decodePosition(cdc.Position("1/2")); err == nil {
		t.Error("a short position decoded")
	}
	// Ordered by commit, then XID.
	if !(position{Commit: 5, XID: "B"}).less(position{Commit: 6, XID: "A"}) || !(position{Commit: 5, XID: "A"}).less(position{Commit: 5, XID: "B"}) {
		t.Error("position order")
	}
	// An idle mark at an SCN is past every transaction committed there.
	if !(position{Commit: 5, XID: "FFFF"}).less(position{Commit: 5, XID: "~"}) {
		t.Error("idle mark should follow real XIDs")
	}
	cs := &ChangeSource{}
	behind := cs.Behind(position{Commit: 5, At: 1_000_000}.encode(), position{Commit: 9, At: 3_000_000}.encode())
	if behind != 2_000_000 {
		t.Errorf("Behind = %d, want 2s", behind)
	}
	if cs.Behind(position{Commit: 9, At: 3}.encode(), position{Commit: 9, At: 5}.encode()) != 0 {
		t.Error("a position at the target is not behind")
	}
}

func TestKeyTexts(t *testing.T) {
	num := column{name: "id", dataType: "NUMBER", conv: convInt}
	dec := column{name: "d", dataType: "NUMBER"}
	chr := column{name: "c", dataType: "CHAR", charLen: 4}
	raw := column{name: "r", dataType: "RAW"}
	ts := column{name: "t", dataType: "TIMESTAMP(6)"}
	// The same key, as MINE_VALUE and TO_CHAR may write it.
	for _, pair := range [][2]string{{".5", "0.5"}, {"12.50", "12.5"}, {"1E+3", "1000"}} {
		if normKey(dec, pair[0]) != normKey(dec, pair[1]) {
			t.Errorf("%s and %s differ: %s, %s", pair[0], pair[1], normKey(dec, pair[0]), normKey(dec, pair[1]))
		}
	}
	if normKey(raw, "deadbeef") != normKey(raw, "DEADBEEF") || normKey(chr, "ab") != normKey(chr, "ab  ") {
		t.Error("RAW case or CHAR padding changes the key")
	}
	if v, err := keyValue(num, "42"); err != nil || v != int64(42) {
		t.Errorf("NUMBER(10) key = %v (%T), %v", v, v, err)
	}
	if v, _ := keyValue(chr, "ab"); v != "ab  " {
		t.Errorf("CHAR(4) key = %q, want it padded as ReadBatch reads it", v)
	}
	if v, _ := keyValue(ts, "2024-05-06 07:08:09.123456000"); !v.(time.Time).Equal(time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)) {
		t.Errorf("TIMESTAMP key = %v", v)
	}
	if v, _ := keyValue(raw, "deadbeef"); string(v.([]byte)) != "\xde\xad\xbe\xef" {
		t.Errorf("RAW key = %x", v)
	}
	for dt, want := range map[string]keyKind{"NUMBER": keyNumber, "VARCHAR2": keyString, "DATE": keyDate,
		"TIMESTAMP(6) WITH TIME ZONE": keyUnsupported, "CLOB": keyUnsupported, "RAW": keyRaw} {
		if got := kindOfKey(dt); got != want {
			t.Errorf("kindOfKey(%s) = %v, want %v", dt, got, want)
		}
	}
}

func TestKeyCompare(t *testing.T) {
	key := []column{{oracleName: "A", dataType: "NUMBER"}, {oracleName: "B", dataType: "DATE"}}
	args := []interface{}{"x"}
	got := keyCompare(key, ">", []string{"5", "2024-01-02 00:00:00"}, &args)
	want := `(("A" > TO_NUMBER(:2)) OR ("A" = TO_NUMBER(:3) AND "B" > TO_DATE(:4, 'YYYY-MM-DD HH24:MI:SS')))`
	if got != want {
		t.Errorf("keyCompare >:\n got %s\nwant %s", got, want)
	}
	// Each placeholder has its own value: go-ora binds by position.
	if len(args) != 4 || args[1] != "5" || args[2] != "5" || args[3] != "2024-01-02 00:00:00" {
		t.Errorf("args = %v", args)
	}
	args = nil
	if got := keyCompare(key[:1], "<=", []string{"7"}, &args); got != `(("A" <= TO_NUMBER(:1)))` || len(args) != 1 {
		t.Errorf("keyCompare <= = %s, %v", got, args)
	}
}

func TestCollect(t *testing.T) {
	info := &liveInfo{name: "items", otable: "ITEMS", key: []column{{name: "id", oracleName: "ID", dataType: "NUMBER"}}}
	cs := &ChangeSource{tables: map[string]*liveInfo{"ITEMS": info}, objects: map[string]string{"7001": "items"}}
	k := func(s string) []sql.NullString { return []sql.NullString{{String: s, Valid: s != ""}} }
	open := map[string]*txn{}
	committed, err := cs.collect([]minedRow{
		{scn: 10, op: opInsert, xid: "T1", table: "ITEMS", redo: k("1")},
		{scn: 11, op: opInsert, xid: "T2", table: "ITEMS", redo: k("2")},
		{scn: 12, op: opUpdate, xid: "T3", table: "ITEMS", redo: k(""), undo: k("3")}, // key only in the undo
		{scn: 13, op: opInsert, xid: "RB", table: "ITEMS", redo: k("")},               // keyless, but rolled back
		{scn: 14, op: opRollback, xid: "RB"},
		{scn: 15, op: opCommit, xid: "T2"},
		{scn: 16, op: opCommit, xid: "T1"},
		{scn: 17, op: opInsert, xid: "OTHER", table: "NOT_OURS"},
		{scn: 18, op: opCommit, xid: "OTHER"},
	}, open)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 2 || committed[0].xid != "T2" || committed[1].xid != "T1" || committed[1].commit != 16 {
		t.Errorf("committed = %+v", committed)
	}
	if len(open) != 1 || open["T3"] == nil || len(open["T3"].keys["ITEMS"]) != 1 {
		t.Errorf("open = %+v", open)
	}
	// Problems surface at commit.
	_, err = cs.collect([]minedRow{
		{scn: 20, op: opUpdate, xid: "T4", table: "ITEMS", info: "Dictionary Mismatch"},
		{scn: 21, op: opCommit, xid: "T4"},
	}, open)
	if !errors.Is(err, errUndecodable) {
		t.Errorf("mismatch: %v", err)
	}
	_, err = cs.collect([]minedRow{{scn: 22, op: opInsert, xid: "T5", table: "OBJ# 7001"}}, open)
	if !errors.Is(err, errUndecodable) {
		t.Errorf("undecodable object: %v", err)
	}
	_, err = cs.collect([]minedRow{
		{scn: 23, op: opInsert, xid: "T6", table: "ITEMS", redo: k("")},
		{scn: 24, op: opCommit, xid: "T6"},
	}, open)
	if err == nil || !strings.Contains(err.Error(), "supplemental logging") {
		t.Errorf("keyless committed change: %v", err)
	}
}

func TestFlushed(t *testing.T) {
	cs := &ChangeSource{}
	t0 := time.Now()
	if cs.flushed(t0) != 0 {
		t.Error("nothing sampled yet")
	}
	cs.sample(t0, 100)
	cs.sample(t0.Add(2*time.Second), 200)
	cs.sample(t0.Add(6*time.Second), 300)
	if got := cs.flushed(t0.Add(6 * time.Second)); got != 100 {
		t.Errorf("flushed at 6s = %d, want 100 (sampled 6s before)", got)
	}
	if got := cs.flushed(t0.Add(7 * time.Second)); got != 200 {
		t.Errorf("flushed at 7s = %d, want 200", got)
	}
	cs.sample(t0.Add(20*time.Second), 400)
	if len(cs.samples) > 2 {
		t.Errorf("old samples kept: %v", cs.samples)
	}
}

func TestLobLocatorKey(t *testing.T) {
	info := &liveInfo{name: "lobprobe", otable: "LOBPROBE", key: []column{
		{oracleName: "Num", dataType: "NUMBER"}, {oracleName: "C", dataType: "CHAR"},
		{oracleName: "D", dataType: "DATE"}, {oracleName: "TS", dataType: "TIMESTAMP(6)"},
		{oracleName: "R", dataType: "RAW"},
	}}
	// As 23ai writes it, on a fixedNLS session.
	redo := `DECLARE
 loc_c CLOB;
 buf_c VARCHAR2(6162);
BEGIN
 select "NOTE" into loc_c from "XFERDB"."LOBPROBE" where "Num" = '-12.5' and "C" = 'ab   ' and "D" = TO_DATE('2025-02-03 04:05:06', 'YYYY-MM-DD HH24:MI:SS') and "TS" = TO_TIMESTAMP('2025-02-03 04:05:06.123456000') and "R" = HEXTORAW('0aff') for update;`
	got, err := lobLocatorKey("XFERDB", info, redo)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-12.5", "ab   ", "2025-02-03 04:05:06", "2025-02-03 04:05:06.123456000", "0aff"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	if keyID(info.key, got) != keyID(info.key, []string{"-12.50", "ab", "2025-02-03 04:05:06", "2025-02-03 04:05:06.123456000", "0AFF"}) {
		t.Error("the key's id differs from MINE_VALUE's spelling")
	}

	// Quotes in names and values, key columns in another order, a column
	// that isn't part of the key.
	info2 := &liveInfo{name: "t", otable: `My"T`, key: []column{{oracleName: "K2", dataType: "VARCHAR2"}, {oracleName: "ID", dataType: "NUMBER"}}}
	got, err = lobLocatorKey("XFERDB", info2, `select "NOTE" into loc_c from "XFERDB"."My"T" where "ID" = '2100' and "X" = 'y' and "K2" = 'a''b and "C" = ''z' for update;`)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{`a'b and "C" = 'z`, "2100"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	for _, bad := range []string{
		`select "NOTE" into loc_c from "XFERDB"."OTHER" where "ID" = '1' for update;`,
		`select "NOTE" into loc_c from "XFERDB"."My"T" where "ID" = '1' for update;`,
		`select "NOTE" into loc_c from "XFERDB"."My"T" where "ID" = '1' and "K2" = 'x`,
		`select "NOTE" into loc_c from "XFERDB"."My"T" where "ID" = SYSDATE and "K2" = 'x' for update;`,
		"",
	} {
		if _, err := lobLocatorKey("XFERDB", info2, bad); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}
