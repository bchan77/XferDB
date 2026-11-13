package sqlite

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestWithoutRowid(t *testing.T) {
	for ddl, want := range map[string]bool{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT)`:                         false,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, n TEXT) WITHOUT ROWID`:           true,
		`CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT, without   rowid`:         true,
		`CREATE TABLE "without rowid" (id INTEGER PRIMARY KEY, n TEXT)`:           false,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, c TEXT DEFAULT 'WITHOUT ROWID')`: false,
	} {
		if got := withoutRowid(ddl); got != want {
			t.Errorf("withoutRowid(%s) = %v", ddl, got)
		}
	}
}

func TestRowidAlias(t *testing.T) {
	col := func(name, typ string, pk bool) adapters.ColumnDef {
		return adapters.ColumnDef{Name: name, Type: typ, PrimaryKey: pk}
	}
	if i := rowidAlias([]adapters.ColumnDef{col("n", "TEXT", false), col("id", "integer", true)}); i != 1 {
		t.Errorf("lone INTEGER key: %d", i)
	}
	if i := rowidAlias([]adapters.ColumnDef{col("id", "BIGINT", true)}); i != -1 {
		t.Errorf("BIGINT key is no rowid alias: %d", i)
	}
	if i := rowidAlias([]adapters.ColumnDef{col("a", "INTEGER", true), col("b", "INTEGER", true)}); i != -1 {
		t.Errorf("composite key: %d", i)
	}
}
