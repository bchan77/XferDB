package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestWriteBatchFast_ChunksAndRoundTripsValues exercises the bulk-copy path
// (EnableCopy -> writeBatchFast): multi-row INSERT OR REPLACE statements
// chunked to stay under SQLite's bound-parameter limit. Writes enough rows to
// force multiple chunks and checks every row lands with the right values in
// the right place — an off-by-one in the chunk/placeholder bookkeeping would
// scramble or drop rows silently rather than error.
//
// Needs no external server (SQLite is just a file), so this runs
// unconditionally as part of `go test ./...`.
func TestWriteBatchFast_ChunksAndRoundTripsValues(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "bulkcopy_test.db")

	tgt := &Target{}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: dbPath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()

	table := "bulk_rows"
	schema := &adapters.TableSchema{
		Name: table,
		Columns: []adapters.ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "label", Type: "TEXT", Nullable: true},
			{Name: "blob_val", Type: "BLOB", Nullable: true},
		},
	}
	if err := tgt.CreateTable(ctx, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tgt.EnableCopy()

	const rowCount = 2500 // several times over the ~300-row chunk size for a 3-column table
	records := make([]map[string]interface{}, rowCount)
	for i := 0; i < rowCount; i++ {
		var blob interface{}
		if i%7 == 0 {
			blob = nil
		} else {
			blob = []byte(fmt.Sprintf("blob-%d", i))
		}
		records[i] = map[string]interface{}{
			"id":       i,
			"label":    fmt.Sprintf("row-%d", i),
			"blob_val": blob,
		}
	}
	if err := tgt.WriteBatch(ctx, table, &adapters.Batch{Records: records, Size: rowCount}); err != nil {
		t.Fatalf("write batch (fast path): %v", err)
	}

	var count int
	if err := tgt.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quote(table))).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != rowCount {
		t.Fatalf("expected %d rows, got %d", rowCount, count)
	}

	// Spot-check rows at chunk boundaries and edges, where an off-by-one
	// would most likely surface.
	for _, id := range []int{0, 1, 299, 300, 301, 899, 900, rowCount - 1} {
		var label string
		var blob []byte
		err := tgt.db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT label, blob_val FROM %s WHERE id = ?", quote(table)), id,
		).Scan(&label, &blob)
		if err != nil {
			t.Fatalf("read back id=%d: %v", id, err)
		}
		wantLabel := fmt.Sprintf("row-%d", id)
		if label != wantLabel {
			t.Errorf("id=%d: label = %q, want %q", id, label, wantLabel)
		}
		if id%7 == 0 {
			if blob != nil {
				t.Errorf("id=%d: expected NULL blob, got %x", id, blob)
			}
		} else {
			wantBlob := fmt.Sprintf("blob-%d", id)
			if string(blob) != wantBlob {
				t.Errorf("id=%d: blob = %q, want %q", id, blob, wantBlob)
			}
		}
	}
}

// TestWriteBatchFast_ReplacesOnDuplicateKey confirms the bulk-copy path's use
// of INSERT OR REPLACE means a second write hitting an existing primary key
// overwrites the row instead of erroring, matching the regular path's
// behavior (EnableCopy only changes throughput/durability tradeoffs here, not
// conflict semantics).
func TestWriteBatchFast_ReplacesOnDuplicateKey(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "bulkcopy_replace_test.db")

	tgt := &Target{}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: dbPath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()

	table := "bulk_replace"
	schema := &adapters.TableSchema{
		Name: table,
		Columns: []adapters.ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "val", Type: "TEXT", Nullable: true},
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

	var count int
	var val string
	if err := tgt.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quote(table))).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row after REPLACE, got %d", count)
	}
	if err := tgt.db.QueryRowContext(ctx, fmt.Sprintf("SELECT val FROM %s WHERE id = 1", quote(table))).Scan(&val); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if val != "second" {
		t.Errorf("expected REPLACE to overwrite with %q, got %q", "second", val)
	}
}
