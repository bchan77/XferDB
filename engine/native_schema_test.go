package engine

import (
	"reflect"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestPgDumpSchemaArgs(t *testing.T) {
	pg := func(schema string) adapters.ConnectionConfig {
		return adapters.ConnectionConfig{Type: "postgres", Schema: schema}
	}
	for _, c := range []struct {
		src, tgt string
		args     []string
		ok       bool
	}{
		{"", "", nil, true},       // all schemas into the same layout
		{"", "public", nil, true}, // public is the default own schema
		{"loadtest", "loadtest", []string{`--schema="loadtest"`}, true},
		{`we"ird`, `we"ird`, []string{`--schema="we""ird"`}, true},
		{"loadtest", "", nil, false}, // bare names would land in public
		{"", "staging", nil, false},  // public tables would land in public, not staging
	} {
		args, ok := pgDumpSchemaArgs(pg(c.src), pg(c.tgt))
		if ok != c.ok || !reflect.DeepEqual(args, c.args) {
			t.Errorf("src %q tgt %q: %v %v, want %v %v", c.src, c.tgt, args, ok, c.args, c.ok)
		}
	}
}
