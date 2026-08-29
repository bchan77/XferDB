package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Target implements adapters.TargetAdapter for SQLite.
type Target struct {
	db *sql.DB
}

func NewTarget() adapters.TargetAdapter {
	return &Target{}
}

func (t *Target) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := openDB(config)
	if err != nil {
		return err
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
	defs := make([]string, 0, len(schema.Columns)+len(schema.ForeignKeys)+len(schema.Checks))
	for _, c := range schema.Columns {
		def := fmt.Sprintf(`%s %s`, quote(c.Name), c.Type)
		if c.PrimaryKey {
			def += " PRIMARY KEY"
		}
		if !c.Nullable && !c.PrimaryKey {
			def += " NOT NULL"
		}
		if c.DefaultValue != nil {
			def += fmt.Sprintf(" DEFAULT %s", *c.DefaultValue)
		}
		defs = append(defs, def)
	}
	// SQLite FK constraints must be declared in CREATE TABLE (no ALTER TABLE ADD FK).
	for _, fk := range schema.ForeignKeys {
		fromCols := make([]string, len(fk.Columns))
		toCols := make([]string, len(fk.RefColumns))
		for i, c := range fk.Columns {
			fromCols[i] = quote(c)
		}
		for i, c := range fk.RefColumns {
			toCols[i] = quote(c)
		}
		fkDef := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s (%s)",
			strings.Join(fromCols, ", "), quote(fk.RefTable), strings.Join(toCols, ", "))
		if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
			fkDef += " ON DELETE " + fk.OnDelete
		}
		if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
			fkDef += " ON UPDATE " + fk.OnUpdate
		}
		defs = append(defs, fkDef)
	}
	for _, chk := range schema.Checks {
		defs = append(defs, fmt.Sprintf("CHECK (%s)", chk.Expression))
	}
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, quote(schema.Name), strings.Join(defs, ", "))
	if _, err := t.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create table %s: %w", schema.Name, err)
	}
	return nil
}

func (t *Target) CreateIndexes(ctx context.Context, table string, indexes []adapters.IndexDef) error {
	for _, idx := range indexes {
		cols := make([]string, len(idx.Columns))
		for i, c := range idx.Columns {
			cols[i] = quote(c)
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		query := fmt.Sprintf(`CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)`,
			unique, quote(idx.Name), quote(table), strings.Join(cols, ", "))
		if _, err := t.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create index %s on %s: %w", idx.Name, table, err)
		}
	}
	return nil
}

// CreateConstraints is a no-op for SQLite: FK and CHECK constraints are included
// inline in CreateTable since SQLite does not support ALTER TABLE ADD CONSTRAINT.
func (t *Target) CreateConstraints(_ context.Context, _ string, _ []adapters.ForeignKey, _ []adapters.CheckConstraint) error {
	return nil
}

func (t *Target) AlterTable(ctx context.Context, table string, changes []adapters.SchemaChange) error {
	for _, change := range changes {
		switch change.Type {
		case adapters.ChangeAddColumn:
			def := fmt.Sprintf(`%s %s`, quote(change.Column.Name), change.Column.Type)
			if change.Column.DefaultValue != nil {
				def += fmt.Sprintf(" DEFAULT %s", *change.Column.DefaultValue)
			}
			// SQLite ADD COLUMN cannot be NOT NULL without a default value.
			query := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s`, quote(table), def)
			if _, err := t.db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("add column %s.%s: %w", table, change.Column.Name, err)
			}
		default:
			return fmt.Errorf("sqlite does not support %s; rebuild the table manually", change.Type)
		}
	}
	return nil
}

func (t *Target) WriteBatch(ctx context.Context, table string, batch *adapters.Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}

	// Stable column order from first record.
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

	query := fmt.Sprintf(
		`INSERT OR REPLACE INTO %s (%s) VALUES (%s)`,
		quote(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
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

	if _, err := t.db.QueryContext(ctx, `SELECT name FROM sqlite_master LIMIT 1`); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	// Probe write access by creating and immediately dropping a sentinel table.
	_, err := t.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _xferdb_probe (id INTEGER PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	check.CanCreateTable = true

	_, err = t.db.ExecContext(ctx, `INSERT INTO _xferdb_probe DEFAULT VALUES`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("write check failed: %v", err))
	} else {
		check.CanWrite = true
	}

	t.db.ExecContext(ctx, `DROP TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
	return check, nil
}
