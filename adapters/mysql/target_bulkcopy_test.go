package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestWriteBatchLoadData_RoundTripsSpecialValues is a regression/coverage test
// for writeBatchLoadData's hand-rolled LOAD DATA escaping. Unlike postgres's
// COPY protocol (see adapters/postgres's analogous test), MySQL's LOAD DATA
// has no separate binary encoding to worry about — every value becomes
// escaped text — but a hand-rolled escaper is exactly the kind of code that
// silently corrupts data on backslashes, tabs, newlines, or NULLs if it's
// wrong, so this exercises all of those plus a []byte value simulating what
// go-sql-driver/mysql hands back for a source DECIMAL column.
//
// Requires a live, reachable MySQL instance with local_infile enabled; set
// XFERDB_TEST_TARGET_DSN (e.g. "root@tcp(127.0.0.1:3306)/xferdb_bulktest")
// to run it. Skipped otherwise.
func TestWriteBatchLoadData_RoundTripsSpecialValues(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live MySQL integration test")
	}

	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_bulkcopy_%d", time.Now().UnixNano())

	tgt := &Target{schemas: make(map[string]*adapters.TableSchema)}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "mysql", DSN: dsn}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()
	defer tgt.DropTable(ctx, table) //nolint:errcheck

	schema := &adapters.TableSchema{
		Name: table,
		Columns: []adapters.ColumnDef{
			{Name: "id", Type: "int", PrimaryKey: true, Nullable: false},
			{Name: "amount", Type: "decimal(10,2)", Nullable: true},
			{Name: "notes", Type: "text", Nullable: true},
			{Name: "raw", Type: "varbinary(32)", Nullable: true},
		},
	}
	if err := tgt.CreateTable(ctx, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tgt.EnableCopy()

	rawBytes := []byte{0x00, 0x01, 0xFF, 0x10, 0x7F}
	batch := &adapters.Batch{
		Records: []map[string]interface{}{
			{
				"id":     1,
				"amount": []byte("454.30"), // simulates go-sql-driver/mysql's own DECIMAL scan result
				"notes":  "tab\there\nnewline\\backslash",
				"raw":    rawBytes,
			},
			{
				"id":     2,
				"amount": nil,
				"notes":  nil,
				"raw":    nil,
			},
			{
				"id":     3,
				"amount": []byte("0.00"),
				"notes":  "",
				"raw":    []byte{},
			},
		},
		Size: 3,
	}
	if err := tgt.WriteBatch(ctx, table, batch); err != nil {
		t.Fatalf("write batch (load data): %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verification connection: %v", err)
	}
	defer db.Close()

	type row struct {
		amount sql.NullString
		notes  sql.NullString
		raw    []byte
	}
	read := func(id int) row {
		t.Helper()
		var r row
		if err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT CAST(amount AS CHAR), notes, raw FROM %s WHERE id = ?", quote(table)), id,
		).Scan(&r.amount, &r.notes, &r.raw); err != nil {
			t.Fatalf("read back row %d: %v", id, err)
		}
		return r
	}

	r1 := read(1)
	if r1.amount.String != "454.30" {
		t.Errorf("row 1 amount corrupted: got %q, want %q", r1.amount.String, "454.30")
	}
	if r1.notes.String != "tab\there\nnewline\\backslash" {
		t.Errorf("row 1 notes corrupted: got %q", r1.notes.String)
	}
	if string(r1.raw) != string(rawBytes) {
		t.Errorf("row 1 raw corrupted: got %x, want %x", r1.raw, rawBytes)
	}

	r2 := read(2)
	if r2.amount.Valid || r2.notes.Valid || r2.raw != nil {
		t.Errorf("row 2 expected all-NULL, got amount=%+v notes=%+v raw=%x", r2.amount, r2.notes, r2.raw)
	}

	r3 := read(3)
	if !r3.notes.Valid || r3.notes.String != "" {
		t.Errorf("row 3 notes expected empty string (not NULL), got %+v", r3.notes)
	}
}

// TestWriteBatchLoadData_ReplacesOnDuplicateKey confirms writeBatchLoadData's
// use of REPLACE INTO TABLE (rather than a bare LOAD DATA) means a second
// write hitting an existing primary key overwrites the row instead of
// aborting the batch — the defense-in-depth EnableCopy's doc comment claims
// over postgres's raw-COPY equivalent, which has no such fallback.
func TestWriteBatchLoadData_ReplacesOnDuplicateKey(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live MySQL integration test")
	}

	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_bulkcopy_replace_%d", time.Now().UnixNano())

	tgt := &Target{schemas: make(map[string]*adapters.TableSchema)}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "mysql", DSN: dsn}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()
	defer tgt.DropTable(ctx, table) //nolint:errcheck

	schema := &adapters.TableSchema{
		Name: table,
		Columns: []adapters.ColumnDef{
			{Name: "id", Type: "int", PrimaryKey: true, Nullable: false},
			{Name: "val", Type: "varchar(40)", Nullable: true},
		},
	}
	if err := tgt.CreateTable(ctx, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tgt.EnableCopy()

	first := &adapters.Batch{Records: []map[string]interface{}{{"id": 1, "val": "first"}}, Size: 1}
	if err := tgt.WriteBatch(ctx, table, first); err != nil {
		t.Fatalf("first write: %v", err)
	}
	second := &adapters.Batch{Records: []map[string]interface{}{{"id": 1, "val": "second"}}, Size: 1}
	if err := tgt.WriteBatch(ctx, table, second); err != nil {
		t.Fatalf("second write (should REPLACE, not error): %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open verification connection: %v", err)
	}
	defer db.Close()

	var count int
	var val string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quote(table))).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after REPLACE, got %d", count)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id = 1", quote(table))).Scan(&val); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if val != "second" {
		t.Errorf("expected REPLACE to overwrite with %q, got %q", "second", val)
	}
}
