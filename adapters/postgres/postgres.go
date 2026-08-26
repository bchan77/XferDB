package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/lib/pq"
)

// buildDSN constructs a postgres connection string from config fields.
// If DSN is already set it is used directly.
func buildDSN(config adapters.ConnectionConfig) string {
	if config.DSN != "" {
		return config.DSN
	}
	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	port := config.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		config.Host, port, config.Database, config.Username, config.Password, sslMode,
	)
}

// quote returns a safely double-quoted PostgreSQL identifier.
func quote(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// listTables returns all base tables in the public schema.
func listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
		ORDER BY table_name`)
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

// getSchema introspects a table using information_schema.
func getSchema(ctx context.Context, db *sql.DB, table string) (*adapters.TableSchema, error) {
	// Fetch primary key columns first.
	pkRows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.table_constraints tc
		    ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema    = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema    = 'public'
		  AND tc.table_name      = $1`, table)
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

	// Fetch columns.
	colRows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("postgres schema(%s): %w", table, err)
	}
	defer colRows.Close()

	schema := &adapters.TableSchema{Name: table}
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
		}
		schema.Columns = append(schema.Columns, col)
	}
	return schema, colRows.Err()
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
