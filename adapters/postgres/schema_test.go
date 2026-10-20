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

	public := NewSource()
	if err := public.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer public.Close()
	// A schema with no tables points at the ones that have some.
	empty := NewSource()
	if err := empty.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn, Schema: schemaName + "_none"}); err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	if info, _ := empty.GetInfo(ctx); info == nil || info.Tables != 0 || !contains(info.OtherSchemas, schemaName) {
		t.Errorf("empty schema GetInfo = %+v, want 0 tables and %s among other_schemas", info, schemaName)
	}
	pubTables, _ := public.ListTables(ctx)
	for _, ts := range pubTables {
		if ts.Name == "upserted" || ts.Name == "copied" {
			t.Errorf("public sees %s from %s", ts.Name, schemaName)
		}
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
