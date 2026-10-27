package postgres

import (
	"context"
	"os"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/changetest"
)

var _ adapters.ChangeApplier = (*Target)(nil)

// Needs XFERDB_TEST_TARGET_DSN, as the bulk-copy test does.
func TestChangeApplier(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_TARGET_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_TARGET_DSN not set; skipping live postgres integration test")
	}
	tgt := NewTarget()
	if err := tgt.Connect(context.Background(), adapters.ConnectionConfig{Type: "postgres", DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer tgt.Close()
	changetest.Run(t, tgt, func(table string) map[string]string {
		rows, err := tgt.(*Target).db.Query(`SELECT id::text, tenant, name FROM ` + qualify(table))
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
	})
}
