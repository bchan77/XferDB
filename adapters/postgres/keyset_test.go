package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

var _ adapters.KeysetSource = (*Source)(nil)

// TestKeysetPaging pages a table with a composite key of several types and
// checks every row comes back once, in key order, with the same values an
// OFFSET read returns, and that the bound leaves out larger keys.
func TestKeysetPaging(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		dsn = os.Getenv("XFERDB_TEST_CDC_PG")
	}
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN / XFERDB_TEST_CDC_PG not set; skipping keyset paging test")
	}
	ctx := context.Background()
	cfg := adapters.ConnectionConfig{Type: "postgres", DSN: dsn}
	db, err := sql.Open("postgres", buildDSN(cfg))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table := fmt.Sprintf("xferdb_keyset_%d", time.Now().UnixNano())
	mustExec(t, db, `CREATE TABLE `+table+` (name text, at timestamptz, amount numeric, note text, PRIMARY KEY (name, at, amount))`)
	defer mustExec(t, db, `DROP TABLE `+table)
	// Names that sort differently bytewise and by collation, and several
	// rows per name so the later key columns decide the order.
	for i, name := range []string{"b", "B", "a", "ä", "z", "a b", "ab", "10", "9"} {
		for j := 0; j < 3; j++ {
			mustExec(t, db, `INSERT INTO `+table+` VALUES ($1, $2, $3, $4)`,
				name, time.Date(2026, 1, 1, j, 0, 0, 123456000, time.UTC), fmt.Sprintf("%d.%d5", i, j), fmt.Sprint("n", i, j))
		}
	}

	src := &Source{}
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	upTo, err := src.MaxKey(ctx, table)
	if err != nil || upTo == nil {
		t.Fatalf("MaxKey = %v, %v", upTo, err)
	}
	// Rows added past the bound must not be read.
	mustExec(t, db, `INSERT INTO `+table+` VALUES ('zz', now(), 1, 'late')`)

	var got []map[string]interface{}
	var after []string
	for {
		b, err := src.ReadBatch(ctx, table, adapters.BatchOptions{Keyset: true, KeyAfter: after, KeyUpTo: upTo, Limit: 4})
		if err != nil {
			t.Fatal(err)
		}
		if b.Size == 0 {
			break
		}
		got = append(got, b.Records...)
		if after, err = adapters.DecodeKey(b.LastKey); err != nil {
			t.Fatal(err)
		}
	}
	want, err := src.ReadBatch(ctx, table, adapters.BatchOptions{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	// The OFFSET read orders by the same key; drop the late row.
	want.Records = want.Records[:len(want.Records)-1]
	if len(got) != len(want.Records) {
		t.Fatalf("keyset read %d rows, want %d", len(got), len(want.Records))
	}
	for i := range got {
		if !reflect.DeepEqual(got[i], want.Records[i]) {
			t.Fatalf("row %d:\n keyset %v\n offset %v", i, got[i], want.Records[i])
		}
	}

	ints := table + "_ints"
	mustExec(t, db, `CREATE TABLE `+ints+` (id int PRIMARY KEY)`)
	defer mustExec(t, db, `DROP TABLE `+ints)
	if k, err := src.MaxKey(ctx, ints); err != nil || k != nil {
		t.Fatalf("MaxKey of an empty table = %v, %v", k, err)
	}
	// The largest key by the column's type, not by its text ("9" > "100").
	mustExec(t, db, `INSERT INTO `+ints+` VALUES (9), (100), (20)`)
	if k, err := src.MaxKey(ctx, ints); err != nil || !reflect.DeepEqual(k, []string{"100"}) {
		t.Fatalf("MaxKey = %v, %v; want [100]", k, err)
	}
}
