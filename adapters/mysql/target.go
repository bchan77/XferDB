package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Target implements adapters.TargetAdapter for MySQL.
type Target struct {
	db      *sql.DB
	mu      sync.RWMutex
	schemas map[string]*adapters.TableSchema
}

func NewTarget() adapters.TargetAdapter {
	return &Target{schemas: make(map[string]*adapters.TableSchema)}
}

func (t *Target) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := sql.Open("mysql", buildDSN(config))
	if err != nil {
		return fmt.Errorf("mysql open: %w", err)
	}
	t.db = db
	return t.Ping(ctx)
}

func (t *Target) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

func (t *Target) Ping(ctx context.Context) error {
	return t.db.PingContext(ctx)
}

func (t *Target) ListTables(ctx context.Context) ([]adapters.TableSchema, error) {
	names, err := listTables(ctx, t.db)
	if err != nil {
		return nil, err
	}
	tables := make([]adapters.TableSchema, 0, len(names))
	for _, name := range names {
		schema, err := getSchema(ctx, t.db, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, *schema)
	}
	return tables, nil
}

func (t *Target) GetSchema(ctx context.Context, table string) (*adapters.TableSchema, error) {
	return getSchema(ctx, t.db, table)
}

func (t *Target) CreateTable(ctx context.Context, schema *adapters.TableSchema) error {
	defs := make([]string, 0, len(schema.Columns))
	var pks []string
	for _, c := range schema.Columns {
		def := fmt.Sprintf(`%s %s`, quote(c.Name), c.Type)
		if !c.Nullable {
			def += " NOT NULL"
		}
		if c.DefaultValue != nil {
			def += fmt.Sprintf(" DEFAULT %s", *c.DefaultValue)
		}
		defs = append(defs, def)
		if c.PrimaryKey {
			pks = append(pks, quote(c.Name))
		}
	}
	if len(pks) > 0 {
		defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks, ", ")))
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`,
		quote(schema.Name), strings.Join(defs, ", "))
	if _, err := t.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create table %s: %w", schema.Name, err)
	}

	t.mu.Lock()
	t.schemas[schema.Name] = schema
	t.mu.Unlock()
	return nil
}

func (t *Target) AlterTable(ctx context.Context, table string, changes []adapters.SchemaChange) error {
	for _, change := range changes {
		var query string
		switch change.Type {
		case adapters.ChangeAddColumn:
			def := fmt.Sprintf(`%s %s`, quote(change.Column.Name), change.Column.Type)
			if !change.Column.Nullable {
				def += " NOT NULL"
			}
			if change.Column.DefaultValue != nil {
				def += fmt.Sprintf(" DEFAULT %s", *change.Column.DefaultValue)
			}
			query = fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s`, quote(table), def)
		case adapters.ChangeDropColumn:
			query = fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`,
				quote(table), quote(change.Column.Name))
		case adapters.ChangeAlterColumn:
			def := fmt.Sprintf(`%s %s`, quote(change.Column.Name), change.Column.Type)
			if !change.Column.Nullable {
				def += " NOT NULL"
			}
			query = fmt.Sprintf(`ALTER TABLE %s MODIFY COLUMN %s`, quote(table), def)
		default:
			return fmt.Errorf("unsupported change type: %s", change.Type)
		}
		if _, err := t.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("alter table %s (%s %s): %w",
				table, change.Type, change.Column.Name, err)
		}
	}
	return nil
}

func (t *Target) WriteBatch(ctx context.Context, table string, batch *adapters.Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}

	schema, err := t.cachedSchema(ctx, table)
	if err != nil {
		return err
	}
	pks := pkColumns(schema)

	// Stable column order.
	cols := make([]string, 0, len(batch.Records[0]))
	for col := range batch.Records[0] {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	quotedCols := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quote(c)
		placeholders[i] = "?"
	}

	// Build ON DUPLICATE KEY UPDATE clause for non-PK columns.
	pkSet := map[string]bool{}
	for _, pk := range pks {
		pkSet[pk] = true
	}
	setClauses := make([]string, 0, len(cols))
	for _, col := range cols {
		if !pkSet[col] {
			setClauses = append(setClauses, fmt.Sprintf(`%s = VALUES(%s)`, quote(col), quote(col)))
		}
	}

	var upsertClause string
	if len(setClauses) > 0 {
		upsertClause = "ON DUPLICATE KEY UPDATE " + strings.Join(setClauses, ", ")
	} else {
		// All columns are PKs — nothing to update on conflict.
		upsertClause = "ON DUPLICATE KEY UPDATE " + quote(cols[0]) + " = " + quote(cols[0])
	}

	query := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) %s`,
		quote(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		upsertClause,
	)

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, rec := range batch.Records {
		vals := make([]interface{}, len(cols))
		for i, col := range cols {
			vals[i] = rec[col]
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return fmt.Errorf("insert record into %s: %w", table, err)
		}
	}

	return tx.Commit()
}

func (t *Target) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	check := &adapters.PermissionCheck{}

	if err := t.db.QueryRowContext(ctx,
		`SELECT 1 FROM information_schema.tables LIMIT 1`).Err(); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	_, err := t.db.ExecContext(ctx,
		`CREATE TEMPORARY TABLE IF NOT EXISTS _xferdb_probe (id INT PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	t.db.ExecContext(ctx, `DROP TEMPORARY TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
	check.CanWrite = true
	check.CanCreateTable = true

	return check, nil
}

// cachedSchema returns the schema for a table, fetching and caching it if needed.
func (t *Target) cachedSchema(ctx context.Context, table string) (*adapters.TableSchema, error) {
	t.mu.RLock()
	schema, ok := t.schemas[table]
	t.mu.RUnlock()
	if ok {
		return schema, nil
	}

	schema, err := getSchema(ctx, t.db, table)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.schemas[table] = schema
	t.mu.Unlock()
	return schema, nil
}
