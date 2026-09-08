package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/go-sql-driver/mysql"
)

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
	// Ignore URL-format DSNs - they must be rebuilt in native format.
	// A native DSN never starts with a scheme like "mysql://".
	if config.DSN != "" && !strings.HasPrefix(config.DSN, "mysql://") {
		return config.DSN
	}
	port := config.Port
	if port == 0 {
		port = 3306
	}
	// Format: user:password@tcp(host:port)/dbname?parseTime=true
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		config.Username, config.Password, config.Host, port, config.Database)
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
		SELECT column_name, column_type, is_nullable, column_default, column_key
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
		)
		if err := rows.Scan(&name, &colType, &nullable, &dfltVal, &columnKey); err != nil {
			return nil, err
		}
		col := adapters.ColumnDef{
			Name:       name,
			Type:       colType,
			Nullable:   nullable == "YES",
			PrimaryKey: columnKey == "PRI",
		}
		if dfltVal.Valid {
			col.DefaultValue = &dfltVal.String
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
