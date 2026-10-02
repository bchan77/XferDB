package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestMapTypeToSQLite_SourceTypes(t *testing.T) {
	cases := map[string]string{
		// Postgres, as pg_catalog.format_type spells them.
		"integer":                     "INTEGER",
		"bigint":                      "INTEGER",
		"smallint":                    "INTEGER",
		"boolean":                     "BOOLEAN",
		"real":                        "REAL",
		"double precision":            "REAL",
		"numeric(12,2)":               "NUMERIC",
		"numeric(15,0)":               "NUMERIC",
		"numeric(16,2)":               "TEXT",
		"numeric(38,10)":              "TEXT",
		"numeric":                     "TEXT",
		"text":                        "TEXT",
		"character varying(200)":      "TEXT",
		"character varying":           "TEXT",
		"character(10)":               "TEXT",
		"uuid":                        "TEXT",
		"json":                        "TEXT",
		"jsonb":                       "TEXT",
		"interval":                    "TEXT",
		"point":                       "TEXT",
		"money":                       "TEXT",
		"integer[]":                   "TEXT",
		"bytea":                       "BLOB",
		"date":                        "DATE",
		"timestamp without time zone": "TIMESTAMP",
		"timestamp(3) with time zone": "TIMESTAMP",
		"timestamptz":                 "TIMESTAMP",
		"time without time zone":      "TEXT",
		"time(6) with time zone":      "TEXT",
		// MySQL, as information_schema.columns.column_type spells them.
		"int":                             "INTEGER",
		"int unsigned":                    "INTEGER",
		"bigint(20) unsigned":             "INTEGER",
		"tinyint(1)":                      "BOOLEAN",
		"tinyint(4)":                      "INTEGER",
		"year":                            "INTEGER",
		"double":                          "REAL",
		"decimal(10,2)":                   "NUMERIC",
		"decimal(10,2) unsigned zerofill": "NUMERIC",
		"decimal(65,30)":                  "TEXT",
		"varchar(255)":                    "TEXT",
		"longtext":                        "TEXT",
		"enum('a','b')":                   "TEXT",
		"longblob":                        "BLOB",
		"varbinary(16)":                   "BLOB",
		"datetime":                        "DATETIME",
		"datetime(6)":                     "DATETIME",
		"timestamp":                       "TIMESTAMP",
	}
	for in, want := range cases {
		if got := mapTypeToSQLite(in); got != want {
			t.Errorf("mapTypeToSQLite(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMapTypeToSQLite_CaseAndSpacingInsensitive(t *testing.T) {
	for _, in := range []string{"TIMESTAMP WITHOUT TIME ZONE", "  timestamp   without time zone "} {
		if got := mapTypeToSQLite(in); got != "TIMESTAMP" {
			t.Errorf("mapTypeToSQLite(%q) = %q, want TIMESTAMP", in, got)
		}
	}
}

// SQLite's own declared types map to themselves, except bare NUMERIC: the
// adapter can't tell a SQLite source's NUMERIC from Postgres's unbounded
// numeric, and rounding the latter is the worse failure.
func TestMapTypeToSQLite_SQLiteTypes(t *testing.T) {
	cases := map[string]string{
		"INTEGER": "INTEGER", "TEXT": "TEXT", "REAL": "REAL", "BLOB": "BLOB",
		"BOOLEAN": "BOOLEAN", "DATETIME": "DATETIME", "DATE": "DATE",
		"TIMESTAMP": "TIMESTAMP", "NUMERIC": "TEXT",
	}
	for in, want := range cases {
		if got := mapTypeToSQLite(in); got != want {
			t.Errorf("mapTypeToSQLite(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMapTypeToSQLite_UnknownPassesThrough(t *testing.T) {
	for _, in := range []string{"", "tsvector", "Geometry"} {
		if got := mapTypeToSQLite(in); got != in {
			t.Errorf("mapTypeToSQLite(%q) = %q, want it unchanged", in, got)
		}
	}
}

func newTypesTarget(t *testing.T) *Target {
	t.Helper()
	tgt := &Target{}
	dbPath := filepath.Join(t.TempDir(), "types_test.db")
	if err := tgt.Connect(context.Background(), adapters.ConnectionConfig{Type: "sqlite", DSN: dbPath}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { tgt.Close() })
	return tgt
}

// TestWriteBatch_PostgresShapedValuesStoreFaithfully writes a Postgres schema
// and the values lib/pq produces for it, then checks what SQLite actually
// stored. Before #119 every row below was silently wrong: the wide numeric was
// rounded to 1.2345678901234567e+19, the []byte values were stored as BLOB,
// the jsonb "42" became an integer, and the timestamp read back as a string.
// Runs both write paths, since each binds values separately.
func TestWriteBatch_PostgresShapedValuesStoreFaithfully(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := map[bool]string{false: "upsert", true: "bulk-copy"}[bulk]
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tgt := newTypesTarget(t)
			if bulk {
				tgt.EnableCopy()
			}

			cols := []adapters.ColumnDef{
				{Name: "id", Type: "integer", PrimaryKey: true},
				{Name: "wide", Type: "numeric", Nullable: true},
				{Name: "money_ish", Type: "numeric(12,2)", Nullable: true},
				{Name: "u", Type: "uuid", Nullable: true},
				{Name: "doc", Type: "jsonb", Nullable: true},
				{Name: "scalar_doc", Type: "jsonb", Nullable: true},
				{Name: "raw", Type: "bytea", Nullable: true},
				{Name: "ts", Type: "timestamp without time zone", Nullable: true},
				{Name: "flag", Type: "boolean", Nullable: true},
			}
			if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: "t", Columns: cols}); err != nil {
				t.Fatalf("create table: %v", err)
			}
			ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
			rec := map[string]interface{}{
				"id":         int64(1),
				"wide":       []byte("12345678901234567890.123456789"),
				"money_ish":  []byte("12.50"),
				"u":          []byte("0b8f8e6c-3a4d-4b6e-9b1a-2f0e8d7c6b5a"),
				"doc":        []byte(`{"a":1}`),
				"scalar_doc": []byte("42"),
				"raw":        []byte{0x00, 0xff, 0x10},
				"ts":         ts,
				"flag":       true,
			}
			if err := tgt.WriteBatch(ctx, "t", &adapters.Batch{Records: []map[string]interface{}{rec}}); err != nil {
				t.Fatalf("write batch: %v", err)
			}

			want := map[string][2]string{ // column -> {typeof, stored text}
				"wide":       {"text", "12345678901234567890.123456789"},
				"money_ish":  {"real", "12.5"},
				"u":          {"text", "0b8f8e6c-3a4d-4b6e-9b1a-2f0e8d7c6b5a"},
				"doc":        {"text", `{"a":1}`},
				"scalar_doc": {"text", "42"},
				"raw":        {"blob", "\x00\xff\x10"},
				"flag":       {"integer", "1"},
			}
			for col, w := range want {
				var typ, val string
				q := `SELECT typeof(` + quote(col) + `), CAST(` + quote(col) + ` AS TEXT) FROM t`
				if err := tgt.db.QueryRowContext(ctx, q).Scan(&typ, &val); err != nil {
					t.Fatalf("read %s: %v", col, err)
				}
				if typ != w[0] || val != w[1] {
					t.Errorf("%s stored as %s %q, want %s %q", col, typ, val, w[0], w[1])
				}
			}

			// The uuid is findable by its text, which a BLOB never is.
			var n int
			if err := tgt.db.QueryRowContext(ctx, `SELECT count(*) FROM t WHERE u = ?`,
				"0b8f8e6c-3a4d-4b6e-9b1a-2f0e8d7c6b5a").Scan(&n); err != nil || n != 1 {
				t.Errorf("lookup by uuid text: n=%d err=%v, want 1 row", n, err)
			}

			// mattn/go-sqlite3 parses these back into Go types only for its
			// recognised decltypes.
			var gotTS, gotFlag interface{}
			if err := tgt.db.QueryRowContext(ctx, `SELECT ts, flag FROM t`).Scan(&gotTS, &gotFlag); err != nil {
				t.Fatalf("read ts/flag: %v", err)
			}
			if tv, ok := gotTS.(time.Time); !ok || !tv.Equal(ts) {
				t.Errorf("ts read back as %T %v, want time.Time %v", gotTS, gotTS, ts)
			}
			if bv, ok := gotFlag.(bool); !ok || !bv {
				t.Errorf("flag read back as %T %v, want bool true", gotFlag, gotFlag)
			}
		})
	}
}

// The MySQL source scans every column as []byte, so without the conversion a
// plain varchar landed as BLOB.
func TestWriteBatch_MySQLShapedTextIsNotBlob(t *testing.T) {
	ctx := context.Background()
	tgt := newTypesTarget(t)
	cols := []adapters.ColumnDef{
		{Name: "id", Type: "int unsigned", PrimaryKey: true},
		{Name: "name", Type: "varchar(255)", Nullable: true},
		{Name: "active", Type: "tinyint(1)", Nullable: true},
	}
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: "m", Columns: cols}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rec := map[string]interface{}{"id": []byte("7"), "name": []byte("ada"), "active": []byte("1")}
	if err := tgt.WriteBatch(ctx, "m", &adapters.Batch{Records: []map[string]interface{}{rec}}); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	var idType, nameType, activeType string
	if err := tgt.db.QueryRowContext(ctx, `SELECT typeof(id), typeof(name), typeof(active) FROM m`).
		Scan(&idType, &nameType, &activeType); err != nil {
		t.Fatalf("read: %v", err)
	}
	if idType != "integer" || nameType != "text" || activeType != "integer" {
		t.Errorf("stored as id=%s name=%s active=%s, want integer/text/integer", idType, nameType, activeType)
	}
}

// A column-level PRIMARY KEY on two columns is rejected outright ("table has
// more than one primary key").
func TestCreateTable_CompositePrimaryKey(t *testing.T) {
	ctx := context.Background()
	tgt := newTypesTarget(t)
	cols := []adapters.ColumnDef{
		{Name: "order_id", Type: "bigint", PrimaryKey: true},
		{Name: "line_no", Type: "integer", PrimaryKey: true},
		{Name: "sku", Type: "text", Nullable: true},
	}
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: "lines", Columns: cols}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	schema, err := tgt.GetSchema(ctx, "lines")
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	var pk []string
	for _, c := range schema.Columns {
		if c.PrimaryKey {
			pk = append(pk, c.Name)
		}
	}
	if len(pk) != 2 || pk[0] != "order_id" || pk[1] != "line_no" {
		t.Errorf("primary key = %v, want [order_id line_no]", pk)
	}

	// The composite key, not either column alone, is what upserts replace on.
	batch := &adapters.Batch{Records: []map[string]interface{}{
		{"order_id": int64(1), "line_no": int64(1), "sku": "a"},
		{"order_id": int64(1), "line_no": int64(2), "sku": "b"},
		{"order_id": int64(1), "line_no": int64(1), "sku": "a2"},
	}}
	if err := tgt.WriteBatch(ctx, "lines", batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	var n int
	if err := tgt.db.QueryRowContext(ctx, `SELECT count(*) FROM lines`).Scan(&n); err != nil || n != 2 {
		t.Errorf("row count = %d (err %v), want 2", n, err)
	}
}

// AlterTable's ADD COLUMN goes through the same mapping as CreateTable.
func TestAlterTable_AddColumnMapsType(t *testing.T) {
	ctx := context.Background()
	tgt := newTypesTarget(t)
	if err := tgt.CreateTable(ctx, &adapters.TableSchema{Name: "a", Columns: []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true}}}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	change := adapters.SchemaChange{Type: adapters.ChangeAddColumn,
		Column: adapters.ColumnDef{Name: "seen_at", Type: "timestamp with time zone", Nullable: true}}
	if err := tgt.AlterTable(ctx, "a", []adapters.SchemaChange{change}); err != nil {
		t.Fatalf("alter table: %v", err)
	}
	schema, err := tgt.GetSchema(ctx, "a")
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if got := schema.Columns[1].Type; got != "TIMESTAMP" {
		t.Errorf("added column declared %q, want TIMESTAMP", got)
	}
}
