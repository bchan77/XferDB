package validate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestLossyRules pins each conversion to what PostgreSQL 16 and MySQL stored
// in real XferDB migrations with the same overrides.
func TestLossyRules(t *testing.T) {
	cases := []struct {
		name             string
		from, to, target string
		in               interface{}
		want             string
	}{
		// numeric(10,4) -> numeric(10,2): half away from zero, both targets.
		{"scale pg", "numeric(10,4)", "numeric(10,2)", "postgres", []byte("1.2345"), "1.23"},
		{"scale pg half", "numeric(10,4)", "numeric(10,2)", "postgres", []byte("1.2355"), "1.24"},
		{"scale pg negative half", "numeric(10,4)", "numeric(10,2)", "postgres", "-1.2350", "-1.24"},
		{"scale pg carry", "numeric(10,4)", "numeric(10,2)", "postgres", "9.9999", "10"},
		{"scale mysql", "decimal(10,4)", "decimal(10,2)", "mysql", []byte("1.2355"), "1.24"},
		// A SQLite NUMERIC arrives as float64; its shortest form is rounded.
		{"scale from float64", "numeric(10,4)", "numeric(10,2)", "mysql", 1.2355, "1.24"},
		{"double to numeric", "double precision", "numeric(10,1)", "postgres", 1.25, "1.3"},
		// double -> integer on MySQL: a bound double rounds half to even,
		// decimal text half away from zero.
		{"int from double", "double", "int", "mysql", 2.5, "2"},
		{"int from double odd", "double", "int", "mysql", 1.5, "2"},
		{"int from double negative", "double", "int", "mysql", -2.5, "-2"},
		{"int from double below half", "double", "int", "mysql", 1.49, "1"},
		{"int from decimal text", "decimal(5,1)", "int", "mysql", "2.5", "3"},
		{"int from decimal bytes", "decimal(5,1)", "int", "mysql", []byte("-2.5"), "-3"},
		// timestamp -> date on MySQL: whole seconds first.
		{"date mysql", "timestamp", "date", "mysql", time.Date(2024, 3, 1, 23, 59, 59, 999999000, time.UTC), "2024-03-02"},
		{"date mysql half second", "datetime(6)", "date", "mysql", "2024-03-01 23:59:59.5", "2024-03-02"},
		{"date mysql below half", "datetime(6)", "date", "mysql", "2024-03-01 23:59:59.499999", "2024-03-01"},
	}
	for _, c := range cases {
		rule := lossyRule(c.from, c.to, c.target)
		if rule == nil {
			t.Errorf("%s: no rule for %s -> %s on %s", c.name, c.from, c.to, c.target)
			continue
		}
		got, ok := rule(c.in)
		if !ok || got != c.want {
			t.Errorf("%s: rule(%#v) = %q, %v; want %q", c.name, c.in, got, ok, c.want)
		}
	}
}

// TestNoLossyRule covers overrides that need no modelling: nothing is lost,
// the target refuses the value instead (the migration fails), or the target
// stores values unchanged.
func TestNoLossyRule(t *testing.T) {
	cases := []struct{ from, to, target string }{
		{"numeric(10,2)", "numeric(10,4)", "postgres"}, // widening
		{"numeric(10,2)", "numeric(12,2)", "mysql"},
		{"integer", "numeric(10,2)", "postgres"},
		{"numeric(10,4)", "numeric", "postgres"},     // no scale given: nothing to round to
		{"double precision", "integer", "postgres"},  // PostgreSQL refuses 1.5
		{"timestamp", "date", "postgres"},            // truncation is what canonDate does
		{"numeric(10,4)", "numeric(10,2)", "sqlite"}, // SQLite stores as given
		{"varchar(50)", "varchar(5)", "mysql"},       // strict mode refuses
		{"double precision", "double precision", "mysql"},
	}
	for _, c := range cases {
		if lossyRule(c.from, c.to, c.target) != nil {
			t.Errorf("%s -> %s on %s: unexpected rule", c.from, c.to, c.target)
		}
	}
}

func TestIntended(t *testing.T) {
	c := column{kind: kindDecimal, lossy: lossyRule("numeric(10,4)", "numeric(10,2)", "postgres")}
	if v, changed := intended("1.2345", c); v != "1.23" || !changed {
		t.Errorf("intended(1.2345) = %q, %v", v, changed)
	}
	if v, changed := intended("1.2300", c); v != "1.23" || changed {
		t.Errorf("intended(1.2300) = %q, %v; a value the plan does not change is not lossy", v, changed)
	}
	if v, changed := intended(nil, c); v != nullToken || changed {
		t.Errorf("intended(nil) = %q, %v", v, changed)
	}
}

// TestExpectedLossyResult runs the L2 and L3 paths on SQLite files laid out
// as a PostgreSQL target would hold them after a numeric(10,4) ->
// numeric(10,2) override: no server needed. The live test covers the real
// databases.
func TestExpectedLossyResult(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	setup := func(name string, stmts ...string) adapters.SourceAdapter {
		path := filepath.Join(dir, name+".db")
		db := mustOpen(t, "sqlite3", path)
		for _, s := range stmts {
			mustExec(t, db, s)
		}
		db.Close()
		a, err := registry.NewSource("sqlite")
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Connect(ctx, adapters.ConnectionConfig{Type: "sqlite", DSN: path}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { a.Close() })
		return a
	}
	src := setup("src", `CREATE TABLE t (id INTEGER PRIMARY KEY, d NUMERIC(10,4), n TEXT)`,
		`INSERT INTO t VALUES (1, '1.2345', 'a'), (2, '1.2300', 'b'), (3, '9.9999', 'c')`)
	tgt := setup("tgt", `CREATE TABLE t (id INTEGER PRIMARY KEY, d NUMERIC(10,2), n TEXT)`,
		`INSERT INTO t VALUES (1, '1.23', 'a'), (2, '1.23', 'b'), (3, '10.00', 'c')`)
	plan := []state.SchemaPlanRow{{Collection: "t", FieldName: "d", PgType: "numeric(10,2)", Nullable: true}}

	for _, level := range []string{LevelL2, LevelL3} {
		r := validateTable(ctx, src, tgt, "t", level, 2, 0, plan, false, "postgres")
		if r.Result != state.ValidationExpectedLossy || r.LossyRows != 2 {
			t.Fatalf("%s: result %s with %d lossy rows (%s), want expected_lossy with 2", level, r.Result, r.LossyRows, r.Detail)
		}
		if !strings.Contains(r.Detail, "(d)") {
			t.Errorf("%s: detail %q should name the column", level, r.Detail)
		}
		if len(r.Diffs) != 2 || r.Diffs[0].Kind != state.DiffLossyByPlan || *r.Diffs[0].Source["d"] != "1.2345" || *r.Diffs[0].Target["d"] != "1.23" {
			t.Errorf("%s: examples %+v", level, r.Diffs)
		}
	}

	// Without the override the same target is a plain failure.
	if r := validateTable(ctx, src, tgt, "t", LevelL2, 2, 0, nil, false, "postgres"); r.Result != state.ValidationFail {
		t.Errorf("no plan: result %s, want fail", r.Result)
	}

	// A real defect next to the plan's conversion fails, and L3 says which.
	bad := setup("bad", `CREATE TABLE t (id INTEGER PRIMARY KEY, d NUMERIC(10,2), n TEXT)`,
		`INSERT INTO t VALUES (1, '1.23', 'a'), (2, '1.23', 'X'), (3, '10.00', 'c')`)
	r := validateTable(ctx, src, bad, "t", LevelL3, 2, 0, plan, false, "postgres")
	if r.Result != state.ValidationFail || r.DiffMismatch != 1 || r.LossyRows != 2 {
		t.Fatalf("defect: result %s, %d mismatches, %d lossy (%s); want fail, 1, 2", r.Result, r.DiffMismatch, r.LossyRows, r.Detail)
	}
}
