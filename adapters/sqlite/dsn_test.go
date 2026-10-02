package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestConnect_AcceptsSQLiteURL covers the DSN a config-file project hands the
// adapter unparsed. Before the fix, sqlite:///abs/path.db failed with "unable
// to open database file", so a SQLite project could only be created through
// the CLI's --source/--target flags.
func TestConnect_AcceptsSQLiteURL(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "url.db")

	tgt := &Target{}
	if err := tgt.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: "sqlite://" + dbPath}); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{
		Name:    "t",
		Columns: []adapters.ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tgt.Close()

	// The file must land at the path, not under a literal "sqlite:" name.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected database at %s: %v", dbPath, err)
	}

	src := &Source{}
	if err := src.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: "sqlite://" + dbPath}); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close()
}
