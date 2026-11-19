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

// TestXMLColumnWithoutLibxml writes an xml column on whatever server
// XFERDB_TEST_TARGET_DSN names. One built without libxml (YugabyteDB 2025.2)
// refuses every xml value, so the column must be created as text there; one
// with it keeps xml. Either way the document comes back as written.
func TestXMLColumnWithoutLibxml(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_xml_%d", time.Now().UnixNano())

	tgt := &Target{schemas: make(map[string]*adapters.TableSchema)}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()
	defer tgt.DropTable(ctx, table) //nolint:errcheck

	schema := &adapters.TableSchema{Name: table, Columns: []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "doc", Type: "xml", Nullable: true},
	}}
	if err := tgt.CreateTable(ctx, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}
	const doc = `<line no="1"><sku>SKU-1</sku><qty>4</qty></line>`
	batch := &adapters.Batch{Records: []map[string]interface{}{{"id": 1, "doc": doc}, {"id": 2, "doc": nil}}, Size: 2}
	if err := tgt.WriteBatch(ctx, table, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var typ, got string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT pg_typeof(doc)::text, doc::text FROM %s WHERE id = 1`, quote(table))).Scan(&typ, &got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	want := "xml"
	if !tgt.xmlOK {
		want = "text"
	}
	t.Logf("server xml support: %v, column type %s", tgt.xmlOK, typ)
	if typ != want || got != doc {
		t.Errorf("got %s %q, want %s %q", typ, got, want, doc)
	}
}
