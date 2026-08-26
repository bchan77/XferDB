package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/mattn/go-sqlite3"
)

// quote returns a safely double-quoted SQL identifier.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// openDB opens (or creates) a SQLite file using the DSN or Database field.
func openDB(config adapters.ConnectionConfig) (*sql.DB, error) {
	dsn := config.DSN
	if dsn == "" {
		dsn = config.Database
	}
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

// getSchema introspects a table using PRAGMA table_info.
func getSchema(ctx context.Context, db *sql.DB, table string) (*adapters.TableSchema, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, quote(table)))
	if err != nil {
		return nil, fmt.Errorf("pragma table_info(%s): %w", table, err)
	}
	defer rows.Close()

	schema := &adapters.TableSchema{Name: table}
	for rows.Next() {
		var (
			cid      int
			name     string
			colType  string
			notNull  int
			dfltVal  sql.NullString
			pk       int
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
		}
		schema.Columns = append(schema.Columns, col)
	}
	return schema, rows.Err()
}
