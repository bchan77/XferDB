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

// TestNonPublicSchemaRoundTrip writes to and reads from a schema other than
// public on the XFERDB_TEST_TARGET_DSN server: the target creates the
// missing schema, upserts and COPYs into it, and a source with the same
// Schema lists, introspects and counts those tables while a source without
// it (public) doesn't see them. Before, every catalog query was pinned to
// public, so a database whose tables live elsewhere looked empty.
func TestNonPublicSchemaRoundTrip(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	ctx := context.Background()
	schemaName := fmt.Sprintf("xferdb_schema_%d", time.Now().UnixNano())
	cfg := adapters.ConnectionConfig{Type: "postgres", DSN: dsn, Schema: schemaName}
	t.Cleanup(func() {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			db.Exec(`DROP SCHEMA IF EXISTS ` + quote(schemaName) + ` CASCADE`) //nolint:errcheck
			db.Close()
		}
	})

	tgt := NewTarget()
	if err := tgt.Connect(ctx, cfg); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer tgt.Close()
	cols := []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "name", Type: "text", Nullable: true},
	}
	rows := &adapters.Batch{Records: []map[string]interface{}{{"id": 1, "name": "a"}, {"id": 2, "name": "b"}}}
	for _, table := range []string{"upserted", "copied"} {
		if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: table, Columns: cols}); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	for i := 0; i < 2; i++ { // the second write must upsert, not duplicate
		if err := tgt.WriteBatch(ctx, "upserted", rows); err != nil {
			t.Fatalf("upsert #%d: %v", i+1, err)
		}
	}
	copyTgt := NewTarget()
	if err := copyTgt.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer copyTgt.Close()
	copyTgt.(adapters.BulkCopyWriter).EnableCopy()
	if err := copyTgt.WriteBatch(ctx, "copied", rows); err != nil {
		t.Fatalf("copy: %v", err)
	}

	src := NewSource()
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	tables, err := src.ListTables(ctx)
	if err != nil || len(tables) != 2 {
		t.Fatalf("ListTables = %v, %v; want copied and upserted", tables, err)
	}
	for _, ts := range tables {
		if len(ts.Columns) != 2 || !ts.Columns[0].PrimaryKey {
			t.Errorf("%s: columns %+v, want id as primary key", ts.Name, ts.Columns)
		}
		if n, err := src.GetRowCount(ctx, ts.Name); err != nil || n != 2 {
			t.Errorf("%s: %d rows, %v; want 2", ts.Name, n, err)
		}
	}
	if info, _ := src.GetInfo(ctx); info == nil || info.Schema != schemaName || info.Tables != 2 {
		t.Errorf("GetInfo = %+v, want schema %s with 2 tables", info, schemaName)
	}

	// Schema "public" doesn't see them; no Schema (all schemas) sees them
	// qualified.
	public := NewSource()
	if err := public.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn, Schema: "public"}); err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	pubTables, _ := public.ListTables(ctx)
	for _, ts := range pubTables {
		if ts.Name == "upserted" || ts.Name == "copied" {
			t.Errorf("public sees %s from %s", ts.Name, schemaName)
		}
	}
	all := NewSource()
	if err := all.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer all.Close()
	allTables, _ := all.ListTables(ctx)
	seen := map[string]bool{}
	for _, ts := range allTables {
		seen[ts.Name] = true
	}
	if !seen[schemaName+".upserted"] || !seen[schemaName+".copied"] {
		t.Errorf("all-schemas ListTables lacks %s.upserted/.copied: %v", schemaName, seen)
	}
	// A schema with no tables points at the ones that have some.
	empty := NewSource()
	if err := empty.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn, Schema: schemaName + "_none"}); err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if info, _ := empty.GetInfo(ctx); info == nil || info.Tables != 0 || !contains(info.OtherSchemas, schemaName) {
		t.Errorf("empty schema GetInfo = %+v, want 0 tables and %s among other_schemas", info, schemaName)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// TestAllSchemasRoundTrip is the default mode, no Schema: tables outside the
// connection's own schema are named "schema.table", and a target given such
// names creates the schema and writes there, a foreign key across schemas
// included. Opt-in like the test above.
func TestAllSchemasRoundTrip(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	ctx := context.Background()
	stamp := time.Now().UnixNano()
	sa, sb := fmt.Sprintf("xferdb_a_%d", stamp), fmt.Sprintf("xferdb_b_%d", stamp)
	cfg := adapters.ConnectionConfig{Type: "postgres", DSN: dsn}
	t.Cleanup(func() {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			db.Exec(`DROP SCHEMA IF EXISTS ` + quote(sa) + `, ` + quote(sb) + ` CASCADE`) //nolint:errcheck
			db.Close()
		}
	})

	parent, child := sa+".customers", sb+".orders"
	tgt := NewTarget()
	if err := tgt.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: parent, Columns: []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true}, {Name: "name", Type: "text", Nullable: true}}}); err != nil {
		t.Fatalf("create %s: %v", parent, err)
	}
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: child, Columns: []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true}, {Name: "customer_id", Type: "integer", Nullable: true}}}); err != nil {
		t.Fatalf("create %s: %v", child, err)
	}
	if err := tgt.CreateIndexes(ctx, child, []adapters.IndexDef{{Name: "orders_customer_idx", Columns: []string{"customer_id"}}}); err != nil {
		t.Fatalf("index: %v", err)
	}
	if err := tgt.WriteBatch(ctx, parent, &adapters.Batch{Records: []map[string]interface{}{{"id": 1, "name": "a"}, {"id": 2, "name": "b"}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	copyTgt := NewTarget()
	if err := copyTgt.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer copyTgt.Close()
	copyTgt.(adapters.BulkCopyWriter).EnableCopy()
	if err := copyTgt.WriteBatch(ctx, child, &adapters.Batch{Records: []map[string]interface{}{{"id": 10, "customer_id": 1}, {"id": 11, "customer_id": 2}}}); err != nil {
		t.Fatalf("copy: %v", err)
	}
	fk := adapters.ForeignKey{Name: "orders_customer_fk", Columns: []string{"customer_id"}, RefTable: parent, RefColumns: []string{"id"}}
	for i := 0; i < 2; i++ { // the second call must find it and skip
		if err := tgt.CreateConstraints(ctx, child, []adapters.ForeignKey{fk}, nil); err != nil {
			t.Fatalf("fk #%d: %v", i+1, err)
		}
	}

	src := NewSource()
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	tables, err := src.ListTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]adapters.TableSchema{}
	for _, ts := range tables {
		byName[ts.Name] = ts
	}
	got, ok := byName[child]
	if !ok {
		t.Fatalf("ListTables lacks %s", child)
	}
	if len(got.ForeignKeys) != 1 || got.ForeignKeys[0].RefTable != parent {
		t.Errorf("%s foreign keys = %+v, want one referencing %s", child, got.ForeignKeys, parent)
	}
	if len(got.Indexes) != 1 || !got.Columns[0].PrimaryKey {
		t.Errorf("%s: indexes %+v, columns %+v", child, got.Indexes, got.Columns)
	}
	if n, err := src.GetRowCount(ctx, child); err != nil || n != 2 {
		t.Errorf("count %s = %d, %v", child, n, err)
	}
	b, err := src.ReadBatch(ctx, parent, adapters.BatchOptions{Limit: 10})
	if err != nil || len(b.Records) != 2 {
		t.Errorf("read %s: %v, %v", parent, b, err)
	}
	rows, err := src.(adapters.RowLookup).LookupRows(ctx, parent, []string{"id"}, [][]interface{}{{2}})
	if err != nil || len(rows) != 1 {
		t.Errorf("lookup %s: %v, %v", parent, rows, err)
	}
	if info, _ := src.GetInfo(ctx); info == nil || !contains(info.Schemas, sa) || !contains(info.Schemas, sb) || info.Schema != "" {
		t.Errorf("GetInfo = %+v, want schemas incl. %s and %s", info, sa, sb)
	}
	if err := tgt.TruncateTable(ctx, child); err != nil {
		t.Errorf("truncate: %v", err)
	}
	if err := tgt.DropTable(ctx, child); err != nil {
		t.Errorf("drop: %v", err)
	}
}
