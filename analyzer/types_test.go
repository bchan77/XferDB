package analyzer

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestSameType(t *testing.T) {
	same := [][2]string{
		{"varchar(10)", "character varying(10)"}, // Oracle source vs PostgreSQL format_type
		{"numeric(19)", "numeric(19,0)"},
		{"NUMERIC(9, 0)", "numeric(9)"},
		{"char(2)", "character(2)"},
		{"char", "character(1)"},
		{"timestamp(6)", "timestamp without time zone"},
		{"timestamp(6) with time zone", "timestamp with time zone"},
		{"timestamptz", "timestamp(6) with time zone"},
		{"int", "integer"},
		{"int8", "bigint"},
		{"float8", "double precision"},
		{"bool", "boolean"},
		{"decimal(12,2)", "numeric(12,2)"},
		{"varchar", "character varying"},
	}
	for _, p := range same {
		if !SameType(p[0], p[1]) {
			t.Errorf("SameType(%q, %q) = false, want true (%q vs %q)", p[0], p[1], canonicalType(p[0]), canonicalType(p[1]))
		}
	}
	different := [][2]string{
		{"varchar(10)", "character varying(20)"},
		{"numeric(19)", "bigint"},
		{"numeric(12,2)", "numeric(12,3)"},
		{"timestamp(0)", "timestamp without time zone"},
		{"timestamp", "timestamptz"},
		{"char(2)", "varchar(2)"},
		{"jsonb", "json"},
	}
	for _, p := range different {
		if SameType(p[0], p[1]) {
			t.Errorf("SameType(%q, %q) = true, want false", p[0], p[1])
		}
	}
}

// The badges from the report: every one was the same type spelled two ways.
func TestCompareIgnoresSpelling(t *testing.T) {
	src := &adapters.TableSchema{Name: "accounts", Columns: []adapters.ColumnDef{
		{Name: "id", Type: "bigint"}, {Name: "code", Type: "varchar(10)"},
		{Name: "ref", Type: "numeric(19)"}, {Name: "at", Type: "timestamp(6)"}, {Name: "amt", Type: "numeric(12,2)"},
	}}
	tgt := &adapters.TableSchema{Name: "accounts", Columns: []adapters.ColumnDef{
		{Name: "id", Type: "bigint"}, {Name: "code", Type: "character varying(10)"},
		{Name: "ref", Type: "numeric(19,0)"}, {Name: "at", Type: "timestamp without time zone"}, {Name: "amt", Type: "numeric(12,4)"},
	}}
	issues := Compare(src, tgt)
	if len(issues) != 1 || issues[0].Column != "amt" {
		t.Errorf("issues %+v, want only amt's real difference", issues)
	}
}
