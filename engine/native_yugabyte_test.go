package engine

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// pg_dump and psql reach a yugabyte source or target like a postgres one:
// its URL scheme rewritten, and its own default port (#158).
func TestPgConnStrYugabyte(t *testing.T) {
	for _, c := range []struct {
		cfg  adapters.ConnectionConfig
		want string
	}{
		{adapters.ConnectionConfig{Type: "yugabyte", DSN: "yugabyte://u:p@h:5433/db"}, "postgres://u:p@h:5433/db"},
		{adapters.ConnectionConfig{Type: "postgres", DSN: "postgres://u@h/db"}, "postgres://u@h/db"},
		{adapters.ConnectionConfig{Type: "yugabyte", Host: "h", Username: "u", Database: "db"}, "postgres://u@h:5433/db"},
		{adapters.ConnectionConfig{Type: "postgres", Host: "h", Username: "u", Database: "db"}, "postgres://u@h/db"},
	} {
		if got := pgConnStr(c.cfg); got != c.want {
			t.Errorf("pgConnStr(%+v) = %q, want %q", c.cfg, got, c.want)
		}
	}
}

// pg_dump never copies a YugabyteDB source's heartbeat table.
func TestPgDumpSchemaArgsYugabyte(t *testing.T) {
	src := adapters.ConnectionConfig{Type: "yugabyte"}
	args, ok := pgDumpSchemaArgs(src, adapters.ConnectionConfig{Type: "yugabyte"})
	if !ok || len(args) != 1 || args[0] != "--exclude-table=*.xferdb_heartbeat" {
		t.Fatalf("args = %v, %v", args, ok)
	}
	src.Schema = "s"
	args, ok = pgDumpSchemaArgs(src, adapters.ConnectionConfig{Type: "postgres", Schema: "s"})
	if !ok || len(args) != 2 || args[0] != `--schema="s"` {
		t.Fatalf("args = %v, %v", args, ok)
	}
}
