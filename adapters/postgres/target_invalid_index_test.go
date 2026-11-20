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

// TestCreateIndexReplacesInvalidIndex: a failed CREATE INDEX CONCURRENTLY (or
// a YugabyteDB online build that failed or was cancelled) leaves an invalid
// index. CreateIndexes must drop and rebuild it rather than take the name as
// already done, or a resumed migration keeps a unique index that enforces
// nothing. Needs XFERDB_TEST_TARGET_DSN.
func TestCreateIndexReplacesInvalidIndex(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	ctx := context.Background()
	table := fmt.Sprintf("xferdb_test_invalid_ix_%d", time.Now().UnixNano())
	index := table + "_uk"

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	defer db.Exec(`DROP TABLE IF EXISTS ` + quote(table)) //nolint:errcheck
	for _, q := range []string{
		`CREATE TABLE ` + quote(table) + ` (id int PRIMARY KEY, slug text)`,
		`INSERT INTO ` + quote(table) + ` VALUES (1, 'a'), (2, 'a')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// The duplicate makes the build fail, leaving the index invalid.
	if _, err := db.Exec(`CREATE UNIQUE INDEX CONCURRENTLY ` + quote(index) + ` ON ` + quote(table) + ` (slug)`); err == nil {
		t.Fatal("concurrent build over a duplicate succeeded")
	}
	valid := func() (exists, ok bool) {
		err := db.QueryRow(`SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE c.relname = $1`, index).Scan(&ok)
		return err == nil, ok
	}
	if exists, ok := valid(); !exists || ok {
		t.Fatalf("setup: index exists=%v valid=%v, want an invalid one", exists, ok)
	}
	if _, err := db.Exec(`UPDATE ` + quote(table) + ` SET slug = 'b' WHERE id = 2`); err != nil {
		t.Fatal(err)
	}

	tgt := &Target{schemas: make(map[string]*adapters.TableSchema)}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer tgt.Close()
	ix := []adapters.IndexDef{{Name: index, Columns: []string{"slug"}, Unique: true}}
	if err := tgt.CreateIndexes(ctx, table, ix); err != nil {
		t.Fatalf("create indexes: %v", err)
	}
	if exists, ok := valid(); !exists || !ok {
		t.Fatalf("after CreateIndexes: exists=%v valid=%v, want a valid index", exists, ok)
	}
	if _, err := db.Exec(`INSERT INTO ` + quote(table) + ` VALUES (3, 'a')`); err == nil {
		t.Error("duplicate slug accepted: the unique index enforces nothing")
	}
	// A valid index is left alone.
	if err := tgt.CreateIndexes(ctx, table, ix); err != nil {
		t.Fatalf("create indexes again: %v", err)
	}
}
