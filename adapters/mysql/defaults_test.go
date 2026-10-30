package mysql

import "testing"

// TestMySQLDefaultSQL feeds mysqlDefaultSQL what information_schema reports
// (as seen on MySQL 26.7) and checks the result is SQL that can follow
// DEFAULT.
func TestMySQLDefaultSQL(t *testing.T) {
	for _, c := range []struct {
		def, extra, colType string
		want                string // "" means no default
	}{
		{"new", "", "varchar(20)", "'new'"},
		{`it's a \ b`, "", "varchar(20)", `'it''s a \\ b'`},
		{"0", "", "int", "0"},
		{"-1", "", "int", "-1"},
		{"9.99", "", "decimal(10,2)", "9.99"},
		{"1", "", "tinyint(1)", "1"},
		{"10", "", "varchar(5)", "'10'"}, // a string column's digits stay a string
		{"b'1'", "", "bit(1)", "b'1'"},
		{"b", "", "enum('a','b')", "'b'"},
		{"NULL", "", "varchar(5)", ""},
		{"CURRENT_TIMESTAMP(6)", "DEFAULT_GENERATED", "datetime(6)", "CURRENT_TIMESTAMP(6)"},
		{"CURRENT_TIMESTAMP", "DEFAULT_GENERATED on update CURRENT_TIMESTAMP", "timestamp", "CURRENT_TIMESTAMP"},
		{"CURRENT_TIMESTAMP", "", "timestamp", "CURRENT_TIMESTAMP"}, // MySQL 5.7
		{"curdate()", "DEFAULT_GENERATED", "date", "(curdate())"},
		{"uuid()", "DEFAULT_GENERATED", "char(36)", "(uuid())"},
		{`_utf8mb4\'it\\\'s \\\\ x\'`, "DEFAULT_GENERATED", "text", `(_utf8mb4'it\'s \\ x')`},
		{`concat(_utf8mb4\'a\',_utf8mb4\'b\')`, "DEFAULT_GENERATED", "text", `(concat(_utf8mb4'a',_utf8mb4'b'))`},
		{"'quoted'", "", "varchar(20)", "'quoted'"}, // MariaDB quotes literals itself
	} {
		got, ok := mysqlDefaultSQL(c.def, c.extra, c.colType)
		if !ok {
			got = ""
		}
		if got != c.want {
			t.Errorf("mysqlDefaultSQL(%q, %q, %q) = %q, want %q", c.def, c.extra, c.colType, got, c.want)
		}
	}
}
