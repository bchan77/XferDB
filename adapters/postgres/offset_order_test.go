package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestReadBatchOffsetPagesInKeyOrder pages a table whose rows are stored out
// of key order through ReadBatch's LIMIT/OFFSET path and expects every row
// exactly once, in primary-key order. Without an ORDER BY, PostgreSQL returns
// heap order, and on a large table synchronized scans start each page's scan
// where the last one stopped: a 2M-row copy duplicated some rows and lost
// about 760k others. Opt-in: XFERDB_TEST_TARGET_DSN.
func TestReadBatchOffsetPagesInKeyOrder(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_offset_%d", time.Now().UnixNano())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS ` + quote(table)) }) //nolint:errcheck
	// Insert keys descending, then rewrite every third row so its new tuple
	// lands at the end of the heap: physical order is far from key order.
	for _, stmt := range []string{
		`CREATE TABLE ` + quote(table) + ` (id integer PRIMARY KEY, v text)`,
		`INSERT INTO ` + quote(table) + ` SELECT g, 'x' FROM generate_series(1000, 1, -1) g`,
		`UPDATE ` + quote(table) + ` SET v = 'y' WHERE id % 3 = 0`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	src := NewSource()
	if err := src.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var ids []int64
	for offset := 0; ; {
		b, err := src.ReadBatch(ctx, table, adapters.BatchOptions{Limit: 37, Offset: offset})
		if err != nil {
			t.Fatal(err)
		}
		if b.Size == 0 {
			break
		}
		for _, r := range b.Records {
			ids = append(ids, r["id"].(int64))
		}
		offset += b.Size
	}
	if len(ids) != 1000 {
		t.Fatalf("read %d rows, want 1000", len(ids))
	}
	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("row %d has id %d, want %d: pages are not in key order", i, id, i+1)
		}
	}
}
