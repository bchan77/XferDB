package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/go-sql-driver/mysql"
)

// mapTypeToMySQL converts a source database type (e.g. PostgreSQL) to its
// MySQL equivalent. If the type is already valid MySQL or unknown, it is
// returned unchanged so the server rejects it loudly rather than this guessing
// at something that silently stores the wrong thing.
//
// Without this, a Postgres source's own type names went straight into MySQL
// DDL -- `timestamp without time zone` and `character varying(200)` are not
// MySQL syntax, and CREATE TABLE failed before a single row was written.
func mapTypeToMySQL(srcType string) string {
	lower := strings.ToLower(strings.TrimSpace(srcType))

	switch lower {
	case "integer", "int4":
		return "int"
	case "int8":
		return "bigint"
	case "int2":
		return "smallint"
	case "boolean", "bool":
		return "tinyint(1)"
	case "real", "float4":
		return "float"
	case "double precision", "float8":
		return "double"
	case "bytea":
		return "longblob"
	case "jsonb":
		return "json"
	case "uuid":
		return "char(36)"
	case "money":
		return "decimal(19,4)"
	case "xml":
		return "text"
	case "inet", "cidr":
		return "varchar(43)"
	case "interval":
		// No MySQL interval type: keep the ISO 8601 / PostgreSQL text.
		return "varchar(64)"
	case "numeric", "decimal":
		// An unqualified DECIMAL is DECIMAL(10,0) in MySQL, which would drop
		// every fractional digit without complaining. Postgres numeric with no
		// modifier is arbitrary precision, so take MySQL's maximum instead.
		return "decimal(65,30)"
	case "character varying", "varchar":
		// Unbounded varchar has no MySQL counterpart; TEXT is the closest.
		return "text"
	}

	// timestamp[(p)] [with|without] time zone, and timestamptz. DATETIME rather
	// than TIMESTAMP: MySQL's TIMESTAMP only spans 1970-2038, so anything
	// outside that range would fail or clamp.
	if strings.HasPrefix(lower, "timestamp") {
		return "datetime(6)"
	}
	// time[(p)] [with|without] time zone. Must follow the timestamp check.
	if strings.HasPrefix(lower, "time") {
		return "time(6)"
	}
	if rest, ok := strings.CutPrefix(lower, "character varying("); ok {
		return "varchar(" + rest
	}
	if rest, ok := strings.CutPrefix(lower, "character("); ok {
		return "char(" + rest
	}
	if rest, ok := strings.CutPrefix(lower, "numeric("); ok {
		return "decimal(" + rest
	}

	// Already MySQL-compatible, or unknown -- pass through.
	return srcType
}

// mysqlKeyableType adapts a type that MySQL refuses to index. TEXT, BLOB and
// JSON columns cannot take a key without a prefix length, so a primary key of
// that type (a MongoDB _id maps to text) would fail at CREATE TABLE.
func mysqlKeyableType(mysqlType string) string {
	switch strings.ToLower(strings.TrimSpace(mysqlType)) {
	case "text", "tinytext", "mediumtext", "longtext",
		"blob", "tinyblob", "mediumblob", "longblob", "json":
		return "varchar(255)"
	}
	return mysqlType
}

// buildDSN constructs a MySQL DSN from config fields.
//
// The go-sql-driver/mysql expects its native DSN format:
//
//	user:password@tcp(host:port)/dbname?parseTime=true
//
// It does NOT understand URL-format connection strings like:
//
//	mysql://user:password@host:port/dbname
//
// The registry parser sets config.DSN to the original URL for convenience,
// but we must always rebuild the DSN in native format for MySQL.
func buildDSN(config adapters.ConnectionConfig) string {
	// If DSN is already in native format, use it directly.
	if config.DSN != "" && !strings.HasPrefix(config.DSN, "mysql://") {
		return config.DSN
	}

	// If DSN is URL format and config fields are empty, parse the URL.
	if strings.HasPrefix(config.DSN, "mysql://") && config.Host == "" {
		if parsed, err := parseURLDSN(config.DSN); err == nil {
			config = parsed
		}
	}

	port := config.Port
	if port == 0 {
		port = 3306
	}
	// Format: user:password@tcp(host:port)/dbname?parseTime=true
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.Username, config.Password, config.Host, port, config.Database)
}

// parseURLDSN extracts connection fields from a mysql:// URL.
func parseURLDSN(dsn string) (adapters.ConnectionConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return adapters.ConnectionConfig{}, err
	}
	cfg := adapters.ConnectionConfig{
		Type:     "mysql",
		Host:     u.Hostname(),
		Database: strings.TrimPrefix(u.Path, "/"),
		DSN:      dsn,
	}
	if u.User != nil {
		cfg.Username = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	if portStr := u.Port(); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			cfg.Port = port
		}
	}
	return cfg, nil
}

// quote returns a safely backtick-quoted MySQL identifier.
func quote(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// listTables returns all base tables in the current database.
func listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
		ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("mysql list tables: %w", err)
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

// getSchema introspects a table using information_schema.
func getSchema(ctx context.Context, db *sql.DB, table string) (*adapters.TableSchema, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, column_type, is_nullable, column_default, column_key, extra
		FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ?
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("mysql schema(%s): %w", table, err)
	}
	defer rows.Close()

	schema := &adapters.TableSchema{Name: table}
	for rows.Next() {
		var (
			name      string
			colType   string
			nullable  string
			dfltVal   sql.NullString
			columnKey string
			extra     string
		)
		if err := rows.Scan(&name, &colType, &nullable, &dfltVal, &columnKey, &extra); err != nil {
			return nil, err
		}
		col := adapters.ColumnDef{
			Name:       name,
			Type:       colType,
			Nullable:   nullable == "YES",
			PrimaryKey: columnKey == "PRI",
			Identity:   strings.Contains(strings.ToLower(extra), "auto_increment"),
		}
		if dfltVal.Valid {
			if expr, ok := mysqlDefaultSQL(dfltVal.String, extra, colType); ok {
				col.DefaultValue = &expr
				col.DefaultDialect = adapters.DialectMySQL
			}
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
func getIndexes(ctx context.Context, db *sql.DB, table string) ([]adapters.IndexDef, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT INDEX_NAME, NON_UNIQUE,
		    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',') AS COLUMNS
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		AND INDEX_NAME != 'PRIMARY'
		GROUP BY INDEX_NAME, NON_UNIQUE
		ORDER BY INDEX_NAME`, table)
	if err != nil {
		return nil, fmt.Errorf("mysql indexes(%s): %w", table, err)
	}
	defer rows.Close()

	var result []adapters.IndexDef
	for rows.Next() {
		var name, colCSV string
		var nonUnique int
		if err := rows.Scan(&name, &nonUnique, &colCSV); err != nil {
			return nil, err
		}
		result = append(result, adapters.IndexDef{
			Name:    name,
			Columns: strings.Split(colCSV, ","),
			Unique:  nonUnique == 0,
		})
	}
	return result, rows.Err()
}

// getForeignKeys returns foreign key constraints for a table.
func getForeignKeys(ctx context.Context, db *sql.DB, table string) ([]adapters.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    kcu.CONSTRAINT_NAME,
		    kcu.COLUMN_NAME,
		    kcu.REFERENCED_TABLE_NAME,
		    kcu.REFERENCED_COLUMN_NAME,
		    rc.UPDATE_RULE,
		    rc.DELETE_RULE
		FROM information_schema.KEY_COLUMN_USAGE kcu
		JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
		    ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		   AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
		WHERE kcu.TABLE_SCHEMA = DATABASE() AND kcu.TABLE_NAME = ?
		AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`, table)
	if err != nil {
		return nil, fmt.Errorf("mysql foreign keys(%s): %w", table, err)
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

// pkColumns returns the primary key column names for a schema.
func pkColumns(schema *adapters.TableSchema) []string {
	var pks []string
	for _, c := range schema.Columns {
		if c.PrimaryKey {
			pks = append(pks, c.Name)
		}
	}
	return pks
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

// mysqlDefaultSQL turns information_schema's column_default into SQL that
// can follow DEFAULT. MySQL reports a literal default unquoted (a string
// column defaulting to new reads `new`), and an expression default as its
// text with DEFAULT_GENERATED in extra; MariaDB quotes literals itself and
// reports a NULL default as the text NULL. Used raw, `DEFAULT new` failed
// CREATE TABLE even on a MySQL target.
func mysqlDefaultSQL(def, extra, colType string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(def))
	switch {
	case strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED"):
		if strings.HasPrefix(upper, "CURRENT_TIMESTAMP") {
			return def, true
		}
		// The expression's quotes and backslashes come escaped once more
		// than in SQL: ('it''s') reads _utf8mb4\'it\\\'s\'.
		return "(" + mysqlUnescapeExpr(def) + ")", true
	case upper == "NULL":
		return "", false
	case strings.HasPrefix(def, "'") && strings.HasSuffix(def, "'") && len(def) >= 2:
		return def, true // MariaDB: already a literal
	case strings.HasPrefix(upper, "CURRENT_TIMESTAMP"):
		return def, true // MySQL 5.7 doesn't mark it DEFAULT_GENERATED
	case mysqlNumericType(colType) && mysqlNumberRe.MatchString(def):
		return def, true
	}
	if strings.HasPrefix(strings.ToLower(colType), "bit") {
		return def, true // b'1', as MySQL reports it
	}
	return "'" + strings.ReplaceAll(strings.ReplaceAll(def, `\`, `\\`), "'", "''") + "'", true
}

var mysqlNumberRe = regexp.MustCompile(`^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$`)

func mysqlNumericType(colType string) bool {
	t := strings.ToLower(colType)
	for _, p := range []string{"tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "numeric", "float", "double", "real"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// mysqlUnescapeExpr undoes the extra escaping information_schema applies
// to an expression default's text: \' is a quote and \\ a backslash.
func mysqlUnescapeExpr(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && (s[i+1] == '\'' || s[i+1] == '\\') {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
