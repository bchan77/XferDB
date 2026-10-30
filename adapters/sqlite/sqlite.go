package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/mattn/go-sqlite3"
)

// quote returns a safely double-quoted SQL identifier.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// maxExactREALDigits is the most significant decimal digits a REAL (IEEE
// double) round-trips exactly. SQLite's NUMERIC affinity converts decimal text
// to REAL, so a numeric wider than this is silently rounded.
const maxExactREALDigits = 15

// mapTypeToSQLite converts a source database type (PostgreSQL, MySQL, or one
// the MongoDB analyzer inferred) to a SQLite declared type. Unknown types are
// returned unchanged.
//
// SQLite never rejects a type name -- it derives a column affinity from
// substrings of it -- so passing source types through fails silently rather
// than loudly: `numeric` rounds to 15 digits, `point` and `interval` get
// INTEGER affinity because they contain "int", and `timestamp without time
// zone` reads back as a string because mattn/go-sqlite3 only parses the exact
// decltypes `timestamp`, `datetime` and `date` into time.Time (and `boolean`
// into bool). The names returned here are chosen for both the affinity and
// that readback.
func mapTypeToSQLite(srcType string) string {
	lower := strings.Join(strings.Fields(strings.ToLower(srcType)), " ")
	if lower == "" {
		return srcType
	}
	if strings.HasSuffix(lower, "[]") {
		// Postgres arrays arrive as their text literal, e.g. {1,2}.
		return "TEXT"
	}
	if lower == "tinyint(1)" {
		// MySQL's boolean.
		return "BOOLEAN"
	}

	// Base name without modifiers: "decimal(10,2) unsigned" -> "decimal",
	// "character varying(20)" -> "character varying".
	base := lower
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	base = strings.TrimSuffix(base, " zerofill")
	base = strings.TrimSuffix(base, " unsigned")

	switch base {
	case "integer", "int", "int2", "int4", "int8", "smallint", "bigint",
		"tinyint", "mediumint", "serial", "smallserial", "bigserial", "year":
		return "INTEGER"
	case "boolean", "bool":
		return "BOOLEAN"
	case "real", "float", "float4", "float8", "double", "double precision":
		return "REAL"
	case "bytea", "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return "BLOB"
	case "date":
		return "DATE"
	case "datetime":
		return "DATETIME"
	case "numeric", "decimal":
		if p, ok := numericPrecision(lower); ok && p <= maxExactREALDigits {
			return "NUMERIC"
		}
		// Unbounded, or wider than a REAL holds: keep the exact digits as
		// text rather than round them. Costs numeric ordering; never data.
		return "TEXT"
	case "text", "tinytext", "mediumtext", "longtext", "varchar", "char",
		"character varying", "character", "uuid", "json", "jsonb", "xml",
		"inet", "cidr", "macaddr", "money", "interval", "point", "enum", "set":
		return "TEXT"
	}
	// timestamp[(p)] [with|without time zone], timestamptz.
	if strings.HasPrefix(base, "timestamp") {
		return "TIMESTAMP"
	}
	// time[(p)] [with|without time zone], timetz. Must follow the timestamp check.
	if strings.HasPrefix(base, "time") {
		return "TEXT"
	}

	return srcType
}

// numericPrecision returns p from a lowercased "numeric(p[,s])".
func numericPrecision(lower string) (int, bool) {
	open := strings.IndexByte(lower, '(')
	if open < 0 {
		return 0, false
	}
	args := lower[open+1:]
	end := strings.IndexAny(args, ",)")
	if end < 0 {
		return 0, false
	}
	p, err := strconv.Atoi(strings.TrimSpace(args[:end]))
	return p, err == nil
}

// hasBlobAffinity reports whether SQLite gives a declared type BLOB affinity:
// it contains "BLOB", or no type was declared at all.
func hasBlobAffinity(declType string) bool {
	t := strings.TrimSpace(declType)
	return t == "" || strings.Contains(strings.ToUpper(t), "BLOB")
}

// openDB opens (or creates) a SQLite file using the DSN or Database field.
func openDB(config adapters.ConnectionConfig) (*sql.DB, error) {
	dsn := config.DSN
	if dsn == "" {
		dsn = config.Database
	}
	// A project defined in a config file keeps its DSN verbatim: config only
	// infers the adapter type from the scheme (registry.InferTypeFromDSN) and
	// never parses it the way registry.ParseConnectionString does for the CLI.
	// So sqlite:///path/to/file.db can arrive here as-is, and the driver would
	// treat the whole URL as a relative file name and fail to open it.
	dsn = strings.TrimPrefix(dsn, "sqlite://")
	if dsn == "" {
		return nil, fmt.Errorf("sqlite: no database path provided (set DSN or Database)")
	}
	return sql.Open("sqlite3", dsn)
}

// listTables queries sqlite_master for user tables.
func listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("sqlite list tables: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// getSchema introspects a table using PRAGMA table_info, index_list, and foreign_key_list.
// queryer is a *sql.DB or a *sql.Tx: a target's single connection may be
// held by an open transaction, so lookups made inside it must use the tx.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func getSchema(ctx context.Context, db queryer, table string) (*adapters.TableSchema, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quote(table)))
	if err != nil {
		return nil, fmt.Errorf("pragma table_info(%s): %w", table, err)
	}
	defer rows.Close()

	schema := &adapters.TableSchema{Name: table}
	for rows.Next() {
		var (
			cid     int
			name    string
			colType string
			notNull int
			dfltVal sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltVal, &pk); err != nil {
			return nil, err
		}
		col := adapters.ColumnDef{
			Name:       name,
			Type:       colType,
			Nullable:   notNull == 0,
			PrimaryKey: pk > 0,
		}
		if dfltVal.Valid {
			col.DefaultValue = &dfltVal.String
			col.DefaultDialect = adapters.DialectSQLite
		}
		schema.Columns = append(schema.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes, err := getIndexes(ctx, db, table)
	if err != nil {
		return nil, err
	}
	schema.Indexes = indexes

	fks, err := getForeignKeys(ctx, db, table)
	if err != nil {
		return nil, err
	}
	schema.ForeignKeys = fks

	return schema, nil
}

// getIndexes returns non-primary-key indexes for a table.
func getIndexes(ctx context.Context, db queryer, table string) ([]adapters.IndexDef, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA index_list(%s)`, quote(table)))
	if err != nil {
		return nil, fmt.Errorf("pragma index_list(%s): %w", table, err)
	}
	defer rows.Close()

	type idxMeta struct {
		name   string
		unique bool
	}
	var metas []idxMeta
	for rows.Next() {
		var seq, partial int
		var name, origin string
		var unique int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return nil, err
		}
		if origin == "pk" {
			continue // primary key — already captured in column defs
		}
		// Auto-generated UNIQUE constraint indexes have reserved names starting with
		// "sqlite_autoindex_". We can't recreate them with that name on the target,
		// but we still need to preserve the uniqueness — so we re-use the index with
		// a generated portable name (resolved when we fetch its columns below).
		metas = append(metas, idxMeta{name: name, unique: unique == 1})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []adapters.IndexDef
	for _, m := range metas {
		colRows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA index_info(%s)`, quote(m.name)))
		if err != nil {
			return nil, fmt.Errorf("pragma index_info(%s): %w", m.name, err)
		}
		var cols []string
		for colRows.Next() {
			var seqno, cid int
			var colName string
			if err := colRows.Scan(&seqno, &cid, &colName); err != nil {
				colRows.Close()
				return nil, err
			}
			cols = append(cols, colName)
		}
		colRows.Close()
		if err := colRows.Err(); err != nil {
			return nil, err
		}
		name := m.name
		if strings.HasPrefix(name, "sqlite_autoindex_") {
			// Rename to a portable name so it can be created on the target.
			name = "uq_" + table + "_" + strings.Join(cols, "_")
		}
		result = append(result, adapters.IndexDef{Name: name, Columns: cols, Unique: m.unique})
	}
	return result, nil
}

// humanSize formats bytes as a human-readable string.
func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.1f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// getForeignKeys returns foreign key constraints for a table.
func getForeignKeys(ctx context.Context, db queryer, table string) ([]adapters.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA foreign_key_list(%s)`, quote(table)))
	if err != nil {
		return nil, fmt.Errorf("pragma foreign_key_list(%s): %w", table, err)
	}
	defer rows.Close()

	type fkRow struct {
		id, seq                                int
		refTable, from, to, onUpdate, onDelete string
	}
	var fkRows []fkRow
	for rows.Next() {
		var r fkRow
		var match string
		if err := rows.Scan(&r.id, &r.seq, &r.refTable, &r.from, &r.to, &r.onUpdate, &r.onDelete, &match); err != nil {
			return nil, err
		}
		fkRows = append(fkRows, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Group multi-column FKs by id, preserving declaration order.
	var idOrder []int
	fkMap := make(map[int]*adapters.ForeignKey)
	for _, r := range fkRows {
		if _, ok := fkMap[r.id]; !ok {
			idOrder = append(idOrder, r.id)
			fkMap[r.id] = &adapters.ForeignKey{
				Name:     fmt.Sprintf("fk_%s_%d", strings.ReplaceAll(table, `"`, ""), r.id),
				RefTable: r.refTable,
				OnDelete: r.onDelete,
				OnUpdate: r.onUpdate,
			}
		}
		fkMap[r.id].Columns = append(fkMap[r.id].Columns, r.from)
		fkMap[r.id].RefColumns = append(fkMap[r.id].RefColumns, r.to)
	}

	result := make([]adapters.ForeignKey, 0, len(idOrder))
	for _, id := range idOrder {
		result = append(result, *fkMap[id])
	}
	return result, nil
}
