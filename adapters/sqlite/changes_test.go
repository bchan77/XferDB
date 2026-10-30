package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/changetest"
)

var _ adapters.ChangeApplier = (*Target)(nil)

func TestChangeApplier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target.db")
	tgt := NewTarget()
	if err := tgt.Connect(context.Background(), adapters.ConnectionConfig{Type: "sqlite", Database: path}); err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	changetest.Run(t, tgt, func(table string) map[string]string {
		return readRows(t, tgt.(*Target).db, table)
	})
}

func readRows(t *testing.T, db *sql.DB, table string) map[string]string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT CAST(id AS TEXT), CAST(tenant AS TEXT), name FROM %s`, quote(table)))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, tenant, name string
		if err := rows.Scan(&id, &tenant, &name); err != nil {
			t.Fatal(err)
		}
		out[id+"/"+tenant] = name
	}
	return out
}
