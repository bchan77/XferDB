package mysql

import (
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestAutoIncrementColumn(t *testing.T) {
	cols := func(cs ...adapters.ColumnDef) []adapters.ColumnDef { return cs }
	if got := autoIncrementColumn(cols(
		adapters.ColumnDef{Name: "id", PrimaryKey: true, Identity: true},
		adapters.ColumnDef{Name: "n"},
	)); got != "id" {
		t.Errorf("identity primary key: %q", got)
	}
	// Not the key's first column: MySQL refuses AUTO_INCREMENT there.
	if got := autoIncrementColumn(cols(
		adapters.ColumnDef{Name: "tenant", PrimaryKey: true},
		adapters.ColumnDef{Name: "id", PrimaryKey: true, Identity: true},
	)); got != "" {
		t.Errorf("second key column: %q", got)
	}
	if got := autoIncrementColumn(cols(
		adapters.ColumnDef{Name: "id", PrimaryKey: true},
		adapters.ColumnDef{Name: "seq", Identity: true},
	)); got != "" {
		t.Errorf("identity outside the key: %q", got)
	}
}

func TestIsIntegerType(t *testing.T) {
	for _, ok := range []string{"int", "bigint", "INT(11)", "bigint unsigned", "tinyint(4) unsigned zerofill", "mediumint"} {
		if !isIntegerType(ok) {
			t.Errorf("%s should be an integer type", ok)
		}
	}
	for _, no := range []string{"decimal(10,0)", "varchar(20)", "double", "bit(1)"} {
		if isIntegerType(no) {
			t.Errorf("%s isn't an integer type", no)
		}
	}
}

func TestTargetDSN(t *testing.T) {
	got := targetDSN("root:p@ss@tcp(h:3306)/db?parseTime=true")
	cfg, err := driver.ParseDSN(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cfg.Params["sql_mode"], "NO_AUTO_VALUE_ON_ZERO") || cfg.Passwd != "p@ss" || cfg.DBName != "db" || !cfg.ParseTime {
		t.Errorf("targetDSN = %q", got)
	}
	// A DSN that sets sql_mode keeps its own.
	own := "u:p@tcp(h:3306)/db?sql_mode=%27ANSI%27"
	if got := targetDSN(own); got != own {
		t.Errorf("targetDSN changed a DSN with its own sql_mode: %q", got)
	}
}
