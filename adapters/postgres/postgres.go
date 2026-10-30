package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/lib/pq"
)

// buildDSN constructs a postgres connection string from config fields.
// If DSN is already set it is used directly.
func buildDSN(config adapters.ConnectionConfig) string {
	if config.DSN != "" {
		return withSearchPath(config.DSN, config.Schema)
	}
	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	port := config.Port
	if port == 0 {
		port = 5432
	}
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		dsnValue(config.Host), port, dsnValue(config.Database), dsnValue(config.Username),
		dsnValue(config.Password), dsnValue(sslMode),
	)
	return withSearchPath(dsn, config.Schema)
}

// withSearchPath adds schema to dsn (keyword or URL form) as the session's
// search_path; lib/pq passes unknown keys on as run-time parameters. Every
// unqualified table name the adapter uses (reads, upserts, COPY) then
// resolves in that schema, and the catalog queries filter on
// current_schema(). Empty schema leaves dsn as is: public, or whatever the
// DSN itself sets.
func withSearchPath(dsn, schema string) string {
	if schema == "" {
		return dsn
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return dsn + sep + "search_path=" + url.QueryEscape(quote(schema))
	}
	return dsn + " search_path=" + dsnValue(quote(schema))
}

// dsnValue quotes a keyword/value connection-string value. Unquoted, an empty
// value swallows the next keyword (`password= sslmode=disable` sets the
// password to "sslmode=disable"), and a space or quote splits it; libpq
// takes any value in single quotes with \ and \' escaped.
func dsnValue(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// quote returns a safely double-quoted PostgreSQL identifier.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Table names. A table in the connection's own schema (current_schema():
// ConnectionConfig.Schema via search_path, or public) is named bare, as it
// always has been, so existing projects, checkpoints and plans keep their
// names; a table in any other schema is named "schema.table". splitName and
// qualify turn such a name back into its parts and into SQL.

// splitName splits "schema.table" at the first dot; a bare name returns an
// empty schema, meaning the connection's own.
func splitName(name string) (schema, table string) {
	if i := strings.IndexByte(name, '.'); i > 0 {
		return name[:i], name[i+1:]
	}
	return "", name
}

// qualify returns name as a quoted SQL identifier, schema-qualified when it
// names one.
func qualify(name string) string {
	schema, table := splitName(name)
	if schema == "" {
		return quote(table)
	}
	return quote(schema) + "." + quote(table)
}

// schemaExpr is the SQL for the schema a name's catalog lookup filters on:
// the given one ($n), or the connection's own when it is empty.
func schemaExpr(n int) string {
	return fmt.Sprintf("COALESCE(NULLIF($%d::text, ''), current_schema())", n)
}

// listTables returns the base tables of the connection's own schema, or with
// all set, of every schema except the system ones, named as described above.
func listTables(ctx context.Context, db *sql.DB, all bool) ([]string, error) {
	filter := "table_schema = current_schema()"
	if all {
		// xferdb_cdc holds a live migration's applied positions
		// (changes.go), never data to migrate.
		filter = `table_schema NOT IN ('pg_catalog', 'information_schema', '` + changeStateSchema + `')
		  AND table_schema NOT LIKE 'pg\_%'`
	}
	rows, err := db.QueryContext(ctx, `
		SELECT CASE WHEN table_schema = current_schema() THEN table_name
		            ELSE table_schema || '.' || table_name END
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE' AND `+filter+`
		ORDER BY table_schema <> current_schema(), table_schema, table_name`)
	if err != nil {
		return nil, fmt.Errorf("postgres list tables: %w", err)
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

// schemasOf returns the distinct schemas of table names from listTables, a
// bare name counting as own, the connection's schema.
func schemasOf(names []string, own string) []string {
	var out []string
	seen := map[string]bool{}
	for _, n := range names {
		s, _ := splitName(n)
		if s == "" {
			s = own
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// schemasWithTables returns the schemas other than the connection's own that
// hold base tables, for pointing a user whose schema is empty at the right one.
func schemasWithTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT table_schema
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema NOT IN ('pg_catalog', 'information_schema', '`+changeStateSchema+`')
		  AND table_schema IS DISTINCT FROM current_schema()
		ORDER BY table_schema`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// getSchema introspects a table using information_schema and pg catalog tables.
func getSchema(ctx context.Context, db *sql.DB, name string) (*adapters.TableSchema, error) {
	schemaName, table := splitName(name)
	// Primary key columns.
	pkRows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.table_constraints tc
		    ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema    = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema    = `+schemaExpr(2)+`
		  AND tc.table_name      = $1`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("postgres primary keys(%s): %w", table, err)
	}
	defer pkRows.Close()

	pkCols := map[string]bool{}
	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err != nil {
			return nil, err
		}
		pkCols[col] = true
	}
	if err := pkRows.Err(); err != nil {
		return nil, err
	}

	// Columns — use pg_catalog directly so format_type() returns the exact type
	// string including dimensions and user-defined types (e.g. vector(1536),
	// character varying(255)). information_schema.columns returns 'USER-DEFINED'
	// for custom types like pgvector, which is not valid SQL.
	colRows, err := db.QueryContext(ctx, `
		SELECT
			a.attname,
			pg_catalog.format_type(a.atttypid, a.atttypmod),
			CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END,
			pg_catalog.pg_get_expr(d.adbin, d.adrelid)
		FROM pg_catalog.pg_attribute a
		LEFT JOIN pg_catalog.pg_attrdef d
			ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = format('%I.%I', `+schemaExpr(2)+`, $1::text)::regclass
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("postgres schema(%s): %w", table, err)
	}
	defer colRows.Close()

	schema := &adapters.TableSchema{Name: name}
	for colRows.Next() {
		var (
			name     string
			dataType string
			nullable string
			dfltVal  sql.NullString
		)
		if err := colRows.Scan(&name, &dataType, &nullable, &dfltVal); err != nil {
			return nil, err
		}
		col := adapters.ColumnDef{
			Name:       name,
			Type:       dataType,
			Nullable:   nullable == "YES",
			PrimaryKey: pkCols[name],
		}
		if dfltVal.Valid {
			col.DefaultValue = &dfltVal.String
			col.DefaultDialect = adapters.DialectPostgres
		}
		schema.Columns = append(schema.Columns, col)
	}
	if err := colRows.Err(); err != nil {
		return nil, err
	}

	indexes, err := getIndexes(ctx, db, schemaName, table)
	if err != nil {
		return nil, err
	}
	schema.Indexes = indexes

	fks, err := getForeignKeys(ctx, db, schemaName, table)
	if err != nil {
		return nil, err
	}
	schema.ForeignKeys = fks

	checks, err := getCheckConstraints(ctx, db, schemaName, table)
	if err != nil {
		return nil, err
	}
	schema.Checks = checks

	return schema, nil
}

// getIndexes returns non-primary-key indexes for a table using pg catalog tables.
func getIndexes(ctx context.Context, db *sql.DB, schemaName, table string) ([]adapters.IndexDef, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    i.relname AS index_name,
		    ix.indisunique AS is_unique,
		    string_agg(a.attname, ',' ORDER BY k.pos) AS columns
		FROM pg_class t
		JOIN pg_index ix ON ix.indrelid = t.oid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN generate_subscripts(ix.indkey, 1) AS k(pos) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ix.indkey[k.pos]
		WHERE n.nspname = `+schemaExpr(2)+` AND t.relname = $1 AND t.relkind = 'r'
		AND NOT ix.indisprimary
		AND ix.indkey[k.pos] != 0
		GROUP BY i.relname, ix.indisunique
		ORDER BY i.relname`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("postgres indexes(%s): %w", table, err)
	}
	defer rows.Close()

	var result []adapters.IndexDef
	for rows.Next() {
		var name, colCSV string
		var unique bool
		if err := rows.Scan(&name, &unique, &colCSV); err != nil {
			return nil, err
		}
		result = append(result, adapters.IndexDef{
			Name:    name,
			Columns: strings.Split(colCSV, ","),
			Unique:  unique,
		})
	}
	return result, rows.Err()
}

// getForeignKeys returns foreign key constraints for a table. A referenced
// table outside the connection's own schema is named "schema.table".
func getForeignKeys(ctx context.Context, db *sql.DB, schemaName, table string) ([]adapters.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    rc.constraint_name,
		    kcu.column_name,
		    CASE WHEN ccu.table_schema = current_schema() THEN ccu.table_name
		         ELSE ccu.table_schema || '.' || ccu.table_name END AS ref_table,
		    ccu.column_name AS ref_column,
		    rc.update_rule,
		    rc.delete_rule
		FROM information_schema.referential_constraints rc
		JOIN information_schema.key_column_usage kcu
		    ON kcu.constraint_name = rc.constraint_name
		   AND kcu.constraint_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu
		    ON ccu.constraint_name = rc.unique_constraint_name
		   AND ccu.constraint_schema = rc.unique_constraint_schema
		WHERE rc.constraint_schema = `+schemaExpr(2)+`
		AND kcu.table_name = $1
		ORDER BY rc.constraint_name, kcu.ordinal_position`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("postgres foreign keys(%s): %w", table, err)
	}
	defer rows.Close()

	var nameOrder []string
	fkMap := make(map[string]*adapters.ForeignKey)
	for rows.Next() {
		var name, col, refTable, refCol, updateRule, deleteRule string
		if err := rows.Scan(&name, &col, &refTable, &refCol, &updateRule, &deleteRule); err != nil {
			return nil, err
		}
		if _, ok := fkMap[name]; !ok {
			nameOrder = append(nameOrder, name)
			fkMap[name] = &adapters.ForeignKey{
				Name:     name,
				RefTable: refTable,
				OnUpdate: updateRule,
				OnDelete: deleteRule,
			}
		}
		fkMap[name].Columns = append(fkMap[name].Columns, col)
		fkMap[name].RefColumns = append(fkMap[name].RefColumns, refCol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]adapters.ForeignKey, 0, len(nameOrder))
	for _, n := range nameOrder {
		result = append(result, *fkMap[n])
	}
	return result, nil
}

// getCheckConstraints returns user-defined CHECK constraints for a table.
// Implicit NOT NULL checks generated by PostgreSQL are excluded.
func getCheckConstraints(ctx context.Context, db *sql.DB, schemaName, table string) ([]adapters.CheckConstraint, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cc.constraint_name, cc.check_clause
		FROM information_schema.check_constraints cc
		JOIN information_schema.table_constraints tc
		    ON tc.constraint_name = cc.constraint_name
		   AND tc.constraint_schema = cc.constraint_schema
		WHERE tc.table_schema = `+schemaExpr(2)+` AND tc.table_name = $1
		AND tc.constraint_type = 'CHECK'
		AND cc.check_clause NOT LIKE '% IS NOT NULL'
		ORDER BY cc.constraint_name`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("postgres check constraints(%s): %w", table, err)
	}
	defer rows.Close()

	var result []adapters.CheckConstraint
	for rows.Next() {
		var name, expr string
		if err := rows.Scan(&name, &expr); err != nil {
			return nil, err
		}
		result = append(result, adapters.CheckConstraint{Name: name, Expression: expr})
	}
	return result, rows.Err()
}

// pkColumns returns the primary key column names for a table.
func pkColumns(schema *adapters.TableSchema) []string {
	var pks []string
	for _, c := range schema.Columns {
		if c.PrimaryKey {
			pks = append(pks, c.Name)
		}
	}
	return pks
}

// mapTypeToPostgres converts a source database type (e.g. MySQL) to its
// PostgreSQL equivalent. If the type is already valid PostgreSQL or unknown,
// it is returned unchanged.
func mapTypeToPostgres(srcType string) string {
	// Normalize for comparison
	lower := strings.ToLower(strings.TrimSpace(srcType))

	// Direct mappings for MySQL types
	switch lower {
	case "datetime":
		return "timestamp"
	case "tinyint(1)":
		return "boolean"
	case "tinyint":
		return "smallint"
	case "mediumint":
		return "integer"
	case "int":
		return "integer"
	case "double":
		return "double precision"
	case "float":
		return "real"
	case "longtext", "mediumtext", "tinytext":
		return "text"
	case "longblob", "mediumblob", "tinyblob", "blob":
		return "bytea"
	case "json":
		return "jsonb"
	case "year":
		return "smallint"
	case "bit(1)":
		return "boolean"
	}

	// Pattern-based mappings
	// int(n) → integer (MySQL display width is not relevant in PG)
	if strings.HasPrefix(lower, "int(") {
		return "integer"
	}
	// bigint(n) → bigint
	if strings.HasPrefix(lower, "bigint(") {
		return "bigint"
	}
	// smallint(n) → smallint
	if strings.HasPrefix(lower, "smallint(") {
		return "smallint"
	}
	// datetime(p) → timestamp(p): fractional-second precision, which MySQL
	// reports on any DATETIME declared with it.
	if rest, ok := strings.CutPrefix(lower, "datetime("); ok {
		return "timestamp(" + rest
	}
	// tinyint(n) where n != 1 → smallint
	if strings.HasPrefix(lower, "tinyint(") {
		return "smallint"
	}
	// mediumint(n) → integer
	if strings.HasPrefix(lower, "mediumint(") {
		return "integer"
	}
	// double(m,d) → double precision
	if strings.HasPrefix(lower, "double(") {
		return "double precision"
	}
	// float(m,d) → real
	if strings.HasPrefix(lower, "float(") {
		return "real"
	}
	// bit(n) where n > 1 → bit(n) (same in PG)
	if strings.HasPrefix(lower, "bit(") {
		return srcType // PostgreSQL supports bit(n)
	}
	// enum(...) → text (PostgreSQL enums require explicit CREATE TYPE)
	if strings.HasPrefix(lower, "enum(") {
		return "text"
	}
	// set(...) → text (no direct equivalent)
	if strings.HasPrefix(lower, "set(") {
		return "text"
	}
	// varbinary(n) → bytea
	if strings.HasPrefix(lower, "varbinary(") {
		return "bytea"
	}
	// binary(n) → bytea
	if strings.HasPrefix(lower, "binary(") {
		return "bytea"
	}

	// Type is already PostgreSQL compatible or unknown — pass through
	return srcType
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
