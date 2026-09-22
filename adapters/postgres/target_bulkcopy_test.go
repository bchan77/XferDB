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

// TestWriteBatchCopy_NonByteaByteSlicePassesThroughAsText is a regression test
// for a bug where writeBatchCopy handed every []byte value straight to
// pq.CopyIn, which always hex-encodes []byte as a bytea literal regardless of
// the target column's real type. Source drivers commonly hand back non-binary
// columns (e.g. MySQL DECIMAL/NUMERIC) as []byte, so a value like "454.30"
// was corrupted into the literal bytes of the bytea encoding of that string
// instead of being copied in as plain numeric text.
//
// Requires a live, reachable postgres/YugabyteDB instance; set
// XFERDB_TEST_TARGET_DSN (e.g. "host=... port=... dbname=... user=... password=... sslmode=...")
// to run it. Skipped otherwise.
func TestWriteBatchCopy_NonByteaByteSlicePassesThroughAsText(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}

	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_bulkcopy_%d", time.Now().UnixNano())

	tgt := &Target{schemas: make(map[string]*adapters.TableSchema)}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()
	defer tgt.DropTable(ctx, table) //nolint:errcheck

	schema := &adapters.TableSchema{
		Name: table,
		Columns: []adapters.ColumnDef{
			{Name: "id", Type: "integer", PrimaryKey: true, Nullable: false},
			{Name: "amount", Type: "numeric(10,2)", Nullable: true},
			{Name: "raw", Type: "bytea", Nullable: true},
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
				"amount": []byte("454.30"), // simulates go-sql-driver/mysql's DECIMAL scan result
				"raw":    rawBytes,         // simulates a genuine bytea/blob column
			},
		},
		Size: 1,
	}
	if err := tgt.WriteBatch(ctx, table, batch); err != nil {
		t.Fatalf("write batch (bulk copy): %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open verification connection: %v", err)
	}
	defer db.Close()

	var amount string
	var raw []byte
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT amount::text, raw FROM %s WHERE id = 1`, quote(table)),
	).Scan(&amount, &raw); err != nil {
		t.Fatalf("read back row: %v", err)
	}

	if amount != "454.30" {
		t.Errorf("numeric column corrupted: got %q, want %q", amount, "454.30")
	}
	if string(raw) != string(rawBytes) {
		t.Errorf("bytea column corrupted: got %x, want %x", raw, rawBytes)
	}
}
