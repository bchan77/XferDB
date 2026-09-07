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

// getSchema introspects a table using information_schema and pg catalog tables.
func getSchema(ctx context.Context, db *sql.DB, table string) (*adapters.TableSchema, error) {
	// Primary key columns.
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
		WHERE a.attrelid = ('public.' || $1)::regclass
		AND a.attnum > 0
		AND NOT a.attisdropped
		ORDER BY a.attnum`, table)
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
	if err := colRows.Err(); err != nil {
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

	checks, err := getCheckConstraints(ctx, db, table)
	if err != nil {
		return nil, err
	}
	schema.Checks = checks

	return schema, nil
}

// getIndexes returns non-primary-key indexes for a table using pg catalog tables.
func getIndexes(ctx context.Context, db *sql.DB, table string) ([]adapters.IndexDef, error) {
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
		WHERE n.nspname = 'public' AND t.relname = $1 AND t.relkind = 'r'
		AND NOT ix.indisprimary
		AND ix.indkey[k.pos] != 0
		GROUP BY i.relname, ix.indisunique
		ORDER BY i.relname`, table)
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

// getForeignKeys returns foreign key constraints for a table.
func getForeignKeys(ctx context.Context, db *sql.DB, table string) ([]adapters.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    rc.constraint_name,
		    kcu.column_name,
		    ccu.table_name  AS ref_table,
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
		WHERE rc.constraint_schema = 'public'
		AND kcu.table_name = $1
		ORDER BY rc.constraint_name, kcu.ordinal_position`, table)
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
func getCheckConstraints(ctx context.Context, db *sql.DB, table string) ([]adapters.CheckConstraint, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT cc.constraint_name, cc.check_clause
		FROM information_schema.check_constraints cc
		JOIN information_schema.table_constraints tc
		    ON tc.constraint_name = cc.constraint_name
		   AND tc.constraint_schema = cc.constraint_schema
		WHERE tc.table_schema = 'public' AND tc.table_name = $1
		AND tc.constraint_type = 'CHECK'
		AND cc.check_clause NOT LIKE '% IS NOT NULL'
		ORDER BY cc.constraint_name`, table)
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
