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
// If DSN is already set it is used directly.
func buildDSN(config adapters.ConnectionConfig) string {
	if config.DSN != "" {
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
	return schema, rows.Err()
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
