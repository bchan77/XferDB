package mysql

import "testing"

// A Postgres source reports its own type names (pg_catalog.format_type), which
// went into MySQL DDL verbatim and failed CREATE TABLE before a row was
// written -- "Error 1064 ... near 'without time zone NOT NULL'".
func TestMapTypeToMySQL_PostgresTypes(t *testing.T) {
	cases := map[string]string{
		"integer":                     "int",
		"bigint":                      "bigint",
		"smallint":                    "smallint",
		"boolean":                     "tinyint(1)",
		"real":                        "float",
		"double precision":            "double",
		"character varying(200)":      "varchar(200)",
		"character(10)":               "char(10)",
		"numeric(12,2)":               "decimal(12,2)",
		"timestamp without time zone": "datetime(6)",
		"timestamp with time zone":    "datetime(6)",
		"timestamptz":                 "datetime(6)",
		"time without time zone":      "time(6)",
		"date":                        "date",
		"bytea":                       "longblob",
		"jsonb":                       "json",
		"uuid":                        "char(36)",
		"text":                        "text",
	}
	for src, want := range cases {
		if got := mapTypeToMySQL(src); got != want {
			t.Errorf("mapTypeToMySQL(%q) = %q, want %q", src, got, want)
		}
	}
}

// An unqualified DECIMAL is DECIMAL(10,0) in MySQL: every fractional digit
// would be dropped silently. The analyzer emits a bare "numeric" for a
// MongoDB Decimal128, so this is a live path, not a hypothetical one.
func TestMapTypeToMySQL_UnqualifiedNumericKeepsItsScale(t *testing.T) {
	for _, src := range []string{"numeric", "decimal"} {
		got := mapTypeToMySQL(src)
		if got == "decimal" || got == "numeric" {
			t.Errorf("mapTypeToMySQL(%q) = %q, which MySQL reads as DECIMAL(10,0) "+
				"and truncates every fraction", src, got)
		}
		if got != "decimal(65,30)" {
			t.Errorf("mapTypeToMySQL(%q) = %q, want decimal(65,30)", src, got)
		}
	}
}

func TestMapTypeToMySQL_CaseAndSpacingInsensitive(t *testing.T) {
	for _, src := range []string{"INTEGER", "  integer  ", "Integer"} {
		if got := mapTypeToMySQL(src); got != "int" {
			t.Errorf("mapTypeToMySQL(%q) = %q, want int", src, got)
		}
	}
}

// Unknown types pass through so MySQL rejects them loudly, rather than this
// guessing at something that silently stores the wrong thing.
func TestMapTypeToMySQL_UnknownPassesThrough(t *testing.T) {
	for _, src := range []string{"vector(1536)", "integer[]", "geometry"} {
		if got := mapTypeToMySQL(src); got != src {
			t.Errorf("mapTypeToMySQL(%q) = %q, want it unchanged", src, got)
		}
	}
}

// MySQL cannot index TEXT/BLOB/JSON without a prefix length, so a primary key
// of that type fails at CREATE TABLE. A MongoDB _id maps to text.
func TestMySQLKeyableType(t *testing.T) {
	for _, src := range []string{"text", "longtext", "longblob", "json"} {
		if got := mysqlKeyableType(src); got != "varchar(255)" {
			t.Errorf("mysqlKeyableType(%q) = %q, want varchar(255)", src, got)
		}
	}
	// Types MySQL can key are left alone.
	for _, src := range []string{"int", "bigint", "varchar(64)", "char(36)", "datetime(6)"} {
		if got := mysqlKeyableType(src); got != src {
			t.Errorf("mysqlKeyableType(%q) = %q, want it unchanged", src, got)
		}
	}
}
