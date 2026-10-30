package adapters

import "testing"

// TestTranslateDefault checks each kind of default from each source
// dialect on each other target, as the targets' CreateTable use it.
func TestTranslateDefault(t *testing.T) {
	for _, c := range []struct {
		from, expr, to, colType string
		want                    string // "" means no default
	}{
		// PostgreSQL to SQLite: the reported bug, now() in CREATE TABLE.
		{DialectPostgres, "now()", DialectSQLite, "TEXT", "CURRENT_TIMESTAMP"},
		{DialectPostgres, "CURRENT_TIMESTAMP", DialectSQLite, "TEXT", "CURRENT_TIMESTAMP"},
		{DialectPostgres, "'new'::text", DialectSQLite, "TEXT", "'new'"},
		{DialectPostgres, "'it''s'::character varying", DialectSQLite, "TEXT", "'it''s'"},
		{DialectPostgres, "0", DialectSQLite, "INTEGER", "0"},
		{DialectPostgres, "'-1'::integer", DialectSQLite, "INTEGER", "'-1'"},
		{DialectPostgres, "(-1)", DialectSQLite, "INTEGER", "-1"},
		{DialectPostgres, "9.99", DialectSQLite, "REAL", "9.99"},
		{DialectPostgres, "true", DialectSQLite, "INTEGER", "1"},
		{DialectPostgres, "false", DialectSQLite, "INTEGER", "0"},
		{DialectPostgres, "CURRENT_DATE", DialectSQLite, "TEXT", "CURRENT_DATE"},
		{DialectPostgres, "gen_random_uuid()", DialectSQLite, "TEXT", ""},
		{DialectPostgres, "nextval('orders_id_seq'::regclass)", DialectSQLite, "INTEGER", ""},
		{DialectPostgres, "NULL::character varying", DialectSQLite, "TEXT", ""},
		{DialectPostgres, "lower('X'::text)", DialectSQLite, "TEXT", ""},
		{DialectPostgres, "'{}'::jsonb", DialectSQLite, "TEXT", "'{}'"},

		// PostgreSQL to MySQL.
		{DialectPostgres, "now()", DialectMySQL, "datetime(6)", "CURRENT_TIMESTAMP(6)"},
		{DialectPostgres, "now()", DialectMySQL, "datetime", "CURRENT_TIMESTAMP"},
		{DialectPostgres, "now()", DialectMySQL, "text", ""},
		{DialectPostgres, "CURRENT_DATE", DialectMySQL, "date", "(CURRENT_DATE)"},
		{DialectPostgres, "'new'::text", DialectMySQL, "text", "('new')"},
		{DialectPostgres, "'new'::character varying", DialectMySQL, "varchar(20)", "'new'"},
		{DialectPostgres, `'a\b'::text`, DialectMySQL, "varchar(20)", `'a\\b'`},
		{DialectPostgres, "true", DialectMySQL, "tinyint(1)", "1"},
		{DialectPostgres, "gen_random_uuid()", DialectMySQL, "char(36)", "(uuid())"},
		{DialectPostgres, "uuid_generate_v4()", DialectMySQL, "char(36)", "(uuid())"},
		{DialectPostgres, "nextval('t_id_seq'::regclass)", DialectMySQL, "int", ""},

		// MySQL to PostgreSQL and SQLite (as mysqlDefaultSQL reports them).
		{DialectMySQL, "'new'", DialectPostgres, "character varying(20)", "'new'"},
		{DialectMySQL, `'it''s \\ ok'`, DialectPostgres, "text", `'it''s \ ok'`},
		{DialectMySQL, "1", DialectPostgres, "boolean", "true"},
		{DialectMySQL, "0", DialectPostgres, "boolean", "false"},
		{DialectMySQL, "5", DialectPostgres, "boolean", ""},
		{DialectMySQL, "0", DialectPostgres, "integer", "0"},
		{DialectMySQL, "CURRENT_TIMESTAMP", DialectPostgres, "timestamp", "CURRENT_TIMESTAMP"},
		{DialectMySQL, "CURRENT_TIMESTAMP(6)", DialectPostgres, "timestamp(6)", "CURRENT_TIMESTAMP"},
		{DialectMySQL, "(uuid())", DialectPostgres, "char(36)", "gen_random_uuid()"},
		{DialectMySQL, "(curdate())", DialectPostgres, "date", "CURRENT_DATE"},
		{DialectMySQL, "(now())", DialectSQLite, "TEXT", "CURRENT_TIMESTAMP"},
		{DialectMySQL, "b'1'", DialectPostgres, "bit(1)", ""},
		{DialectMySQL, `(_utf8mb4'it\'s \\ x')`, DialectPostgres, "text", `'it''s \ x'`},
		{DialectMySQL, `(concat(_utf8mb4'a',_utf8mb4'b'))`, DialectPostgres, "text", ""},

		// SQLite to PostgreSQL and MySQL.
		{DialectSQLite, "(datetime('now'))", DialectPostgres, "timestamp", "CURRENT_TIMESTAMP"},
		{DialectSQLite, "CURRENT_TIMESTAMP", DialectMySQL, "datetime", "CURRENT_TIMESTAMP"},
		{DialectSQLite, "'pending'", DialectMySQL, "text", "('pending')"},
		{DialectSQLite, "TRUE", DialectPostgres, "boolean", "true"},

		// The same dialect keeps what the source wrote, however exotic.
		{DialectPostgres, "nextval('t_id_seq'::regclass)", DialectPostgres, "integer", "nextval('t_id_seq'::regclass)"},
		{DialectMySQL, "(uuid())", DialectMySQL, "char(36)", "(uuid())"},
		// So does a default whose dialect wasn't recorded.
		{"", "now()", DialectSQLite, "TEXT", "now()"},
	} {
		expr := c.expr
		col := ColumnDef{Name: "c", DefaultValue: &expr, DefaultDialect: c.from}
		got, ok := TranslateDefault(col, c.to, c.colType)
		if !ok {
			got = ""
		}
		if got != c.want {
			t.Errorf("%s %q -> %s %s: got %q, want %q", c.from, c.expr, c.to, c.colType, got, c.want)
		}
	}
	if _, ok := TranslateDefault(ColumnDef{Name: "c"}, DialectSQLite, "TEXT"); ok {
		t.Errorf("a column without a default got one")
	}
}
