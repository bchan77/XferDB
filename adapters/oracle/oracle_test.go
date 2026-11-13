package oracle

import (
	"net/url"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestMapType(t *testing.T) {
	cases := []struct {
		dataType          string
		char, prec, scale int
		want              string
		conv              convKind
	}{
		{"NUMBER", 0, 10, 0, "bigint", convInt},
		{"NUMBER", 0, 4, 0, "smallint", convInt},
		{"NUMBER", 0, 9, 0, "integer", convInt},
		{"NUMBER", 0, 18, 0, "bigint", convInt},
		{"NUMBER", 0, 19, 0, "numeric(19)", convNone},
		{"NUMBER", 0, 12, 2, "numeric(12,2)", convNone},
		{"NUMBER", 0, -1, -1, "numeric", convNone},
		{"NUMBER", 0, -1, 0, "numeric(38)", convNone}, // INTEGER
		{"NUMBER", 0, 5, -2, "numeric", convNone},
		{"FLOAT", 0, 53, -1, "double precision", convFloat},
		{"BINARY_FLOAT", 0, -1, -1, "real", convNone},
		{"BINARY_DOUBLE", 0, -1, -1, "double precision", convNone},
		{"VARCHAR2", 20, -1, -1, "varchar(20)", convNone},
		{"NVARCHAR2", 20, -1, -1, "varchar(20)", convNone},
		{"CHAR", 5, -1, -1, "char(5)", convNone},
		{"NCHAR", 0, -1, -1, "char(1)", convNone},
		{"CLOB", 0, -1, -1, "text", convNone},
		{"NCLOB", 0, -1, -1, "text", convNone},
		{"LONG", 0, -1, -1, "text", convNone},
		{"BLOB", 0, -1, -1, "bytea", convNone},
		{"RAW", 0, -1, -1, "bytea", convNone},
		{"LONG RAW", 0, -1, -1, "bytea", convNone},
		{"DATE", 0, -1, -1, "timestamp(0)", convNone},
		{"TIMESTAMP(6)", 0, -1, 6, "timestamp(6)", convNone},
		{"TIMESTAMP(3)", 0, -1, 3, "timestamp(3)", convNone},
		{"TIMESTAMP(6) WITH TIME ZONE", 0, -1, 6, "timestamp(6) with time zone", convNone},
		{"TIMESTAMP(6) WITH LOCAL TIME ZONE", 0, -1, 6, "timestamp(6) with time zone", convNone},
		{"INTERVAL YEAR(2) TO MONTH", 0, 2, 0, "interval", convInterval},
		{"INTERVAL DAY(3) TO SECOND(6)", 0, 3, 6, "interval", convInterval},
		{"JSON", 0, -1, -1, "jsonb", convJSON},
		{"XMLTYPE", 0, -1, -1, "xml", convNone},
		{"BOOLEAN", 0, -1, -1, "boolean", convBool},
		{"ROWID", 0, -1, -1, "varchar(18)", convNone},
	}
	for _, c := range cases {
		got, conv := mapType(c.dataType, c.char, c.prec, c.scale)
		if got != c.want || conv != c.conv {
			t.Errorf("mapType(%s, %d, %d, %d) = %q, %v; want %q, %v", c.dataType, c.char, c.prec, c.scale, got, conv, c.want, c.conv)
		}
	}
	for _, unsupported := range []string{"SDO_GEOMETRY", "BFILE", "MY_OBJECT_T", "ANYDATA"} {
		if got, _ := mapType(unsupported, 0, -1, -1); !strings.HasPrefix(got, "unsupported Oracle type") {
			t.Errorf("mapType(%s) = %q, want it reported as unsupported", unsupported, got)
		}
	}
}

func TestFoldNames(t *testing.T) {
	got := foldNames([]string{"CUSTOMERS", "ORDER_ITEMS", "MixedCase", "lower", "A$B#C", "DUP", "dup"})
	want := map[string]string{
		"CUSTOMERS": "customers", "ORDER_ITEMS": "order_items", "A$B#C": "a$b#c",
		"MixedCase": "MixedCase", "lower": "lower",
		// Folding DUP would collide with the quoted "dup": both keep their case.
		"DUP": "DUP", "dup": "dup",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("foldNames: %s -> %q, want %q", k, got[k], v)
		}
	}
}

func TestISOInterval(t *testing.T) {
	cases := map[string]string{
		"+01-02":              "P1Y2M",
		"-03-00":              "-P3Y0M",
		"+03 04:05:06.789000": "P3DT4H5M6.789S",
		"-00 00:00:07.000000": "-P0DT0H0M7S",
		"+10 23:59:00":        "P10DT23H59M0S",
	}
	for in, want := range cases {
		got, err := isoInterval(in)
		if err != nil || got != want {
			t.Errorf("isoInterval(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := isoInterval("garbage"); err == nil {
		t.Error("isoInterval(garbage) succeeded")
	}
}

func TestConvert(t *testing.T) {
	cases := []struct {
		in   interface{}
		k    convKind
		want interface{}
	}{
		{"123456789012345678", convInt, int64(123456789012345678)},
		{"1.5", convFloat, 1.5},
		{[]byte(`{"a":1}`), convJSON, `{"a":1}`},
		{"1", convBool, true},
		{"0", convBool, false},
		{"+01-02", convInterval, "P1Y2M"},
		{"1234567890.12", convNone, "1234567890.12"}, // numeric stays exact text
		{nil, convInt, nil},
	}
	for _, c := range cases {
		got, err := convert(c.in, c.k)
		if err != nil || got != c.want {
			t.Errorf("convert(%v, %v) = %v (%T), %v; want %v", c.in, c.k, got, got, err, c.want)
		}
	}
	if _, err := convert("12.5", convInt); err == nil {
		t.Error("convert(12.5, convInt) succeeded")
	}
}

func TestBuildURL(t *testing.T) {
	got := buildURL(adapters.ConnectionConfig{Host: "db.example", Database: "FREEPDB1", Username: "app", Password: "p@ss/w:rd"})
	u, err := url.Parse(got)
	if err != nil || u.Host != "db.example:1521" || u.Path != "/FREEPDB1" || u.User.Username() != "app" {
		t.Fatalf("buildURL = %q (%v)", got, err)
	}
	if pw, _ := u.User.Password(); pw != "p@ss/w:rd" {
		t.Errorf("password read back as %q", pw)
	}
	got = buildURL(adapters.ConnectionConfig{Host: "db.example", Port: 2484, Database: "ORCL", Username: "u", Password: "p", SSLMode: "require"})
	if !strings.Contains(got, ":2484/ORCL") || !strings.Contains(got, "SSL=true") || !strings.Contains(got, "SSL+VERIFY=false") {
		t.Errorf("buildURL with require = %q", got)
	}
	// A DSN on its own: fields come from it, schema is XferDB's, other
	// options pass through to go-ora.
	cfg := withDSN(adapters.ConnectionConfig{DSN: "oracle://u:p%40w@h:1600/SVC?schema=hr&TRACE+FILE=x"})
	if cfg.Host != "h" || cfg.Port != 1600 || cfg.Database != "SVC" || cfg.Username != "u" || cfg.Password != "p@w" || cfg.Schema != "hr" {
		t.Errorf("withDSN = %+v", cfg)
	}
	got = buildURL(cfg)
	if strings.Contains(got, "schema") || !strings.Contains(got, "TRACE+FILE=x") || !strings.Contains(got, "h:1600/SVC") {
		t.Errorf("buildURL(DSN) = %q", got)
	}
	// Fields typed in the UI win over a DSN's.
	if c := withDSN(adapters.ConnectionConfig{DSN: "oracle://u:p@h/SVC?schema=hr", Schema: "SALES"}); c.Schema != "SALES" {
		t.Errorf("withDSN overrode the schema field: %q", c.Schema)
	}
}

func TestDefaultSQL(t *testing.T) {
	if _, ok := defaultSQL("\"XFERDB\".\"ISEQ$$_7\".nextval", true); ok {
		t.Error("an identity column's default was kept")
	}
	if got, ok := defaultSQL("SYSDATE \n", false); !ok || got != "SYSDATE" {
		t.Errorf("defaultSQL(SYSDATE) = %q, %v", got, ok)
	}
	if _, ok := defaultSQL("NULL ", false); ok {
		t.Error("a NULL default was kept")
	}
	// Oracle defaults translate like any other dialect's.
	for def, want := range map[string]string{"SYSDATE": "CURRENT_TIMESTAMP", "'open'": "'open'", "0": "0"} {
		d := def
		got, ok := adapters.TranslateDefault(adapters.ColumnDef{Type: "text", DefaultValue: &d, DefaultDialect: adapters.DialectOracle}, adapters.DialectPostgres, "text")
		if !ok || !strings.EqualFold(got, want) {
			t.Errorf("TranslateDefault(oracle %s) = %q, %v; want %q", def, got, ok, want)
		}
	}
}

func TestIdentityType(t *testing.T) {
	cases := []struct {
		dataType    string
		prec, scale int
		want        string
	}{
		{"NUMBER", -1, -1, "bigint"}, // NUMBER GENERATED ... AS IDENTITY
		{"NUMBER", -1, 0, "bigint"},  // INTEGER
		{"NUMBER", 22, 0, "bigint"},
		{"NUMBER", 9, 0, "integer"}, // already an integer type
		{"NUMBER", 10, 2, "numeric(10,2)"},
	}
	for _, c := range cases {
		typ, conv := mapType(c.dataType, 0, c.prec, c.scale)
		if got, _ := identityType(typ, conv, c.scale); got != c.want {
			t.Errorf("identity %s(%d,%d) = %s, want %s", c.dataType, c.prec, c.scale, got, c.want)
		}
	}
}
