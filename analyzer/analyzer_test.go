package analyzer

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// A table the target doesn't have yet is "new", not "incompatible": the
// migration creates it, and every table of a first migration is one.
func TestTableStatus(t *testing.T) {
	src := &adapters.TableSchema{Name: "t", Columns: []adapters.ColumnDef{
		{Name: "id", Type: "bigint"}, {Name: "amount", Type: "numeric(10,2)"},
	}}
	cases := []struct {
		name string
		tgt  *adapters.TableSchema
		want string
	}{
		{"missing table", nil, "new"},
		{"same", &adapters.TableSchema{Name: "t", Columns: src.Columns}, "compatible"},
		{"type mismatch is a warning", &adapters.TableSchema{Name: "t", Columns: []adapters.ColumnDef{
			{Name: "id", Type: "bigint"}, {Name: "amount", Type: "double precision"}}}, "compatible"},
		{"missing column", &adapters.TableSchema{Name: "t", Columns: []adapters.ColumnDef{
			{Name: "id", Type: "bigint"}}}, "incompatible"},
	}
	for _, c := range cases {
		issues := Compare(src, c.tgt)
		if got := tableStatus(issues); got != c.want {
			t.Errorf("%s: status %q, want %q (issues %+v)", c.name, got, c.want, issues)
		}
	}
	if issues := Compare(src, nil); issues[0].Severity == "error" {
		t.Errorf("a missing table is an error: %+v", issues)
	}
}
