package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Target implements adapters.TargetAdapter for PostgreSQL.
type Target struct {
	db      *sql.DB
	mu      sync.RWMutex
	schemas map[string]*adapters.TableSchema // cached for upsert key resolution
}

func NewTarget() adapters.TargetAdapter {
	return &Target{schemas: make(map[string]*adapters.TableSchema)}
}

func (t *Target) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := sql.Open("postgres", buildDSN(config))
	if err != nil {
		return fmt.Errorf("postgres open: %w", err)
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

func (t *Target) CreateConstraints(ctx context.Context, table string, fks []adapters.ForeignKey, checks []adapters.CheckConstraint) error {
	for _, fk := range fks {
		var count int
		if err := t.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.table_constraints
			WHERE constraint_schema = 'public' AND table_name = $1 AND constraint_name = $2`,
			table, fk.Name).Scan(&count); err != nil {
			return fmt.Errorf("check fk existence %s: %w", fk.Name, err)
		}
		if count > 0 {
			continue // already exists — idempotent on resume
		}
		fromCols := make([]string, len(fk.Columns))
		toCols := make([]string, len(fk.RefColumns))
		for i, c := range fk.Columns {
			fromCols[i] = quote(c)
		}
		for i, c := range fk.RefColumns {
			toCols[i] = quote(c)
		}
		q := fmt.Sprintf(
			`ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)`,
			quote(table), quote(fk.Name),
			strings.Join(fromCols, ", "),
			quote(fk.RefTable),
			strings.Join(toCols, ", "),
		)
		if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
			q += " ON DELETE " + fk.OnDelete
		}
		if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
			q += " ON UPDATE " + fk.OnUpdate
		}
		if _, err := t.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("add foreign key %s.%s: %w", table, fk.Name, err)
		}
	}

	for _, chk := range checks {
		var count int
		if err := t.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.table_constraints
			WHERE constraint_schema = 'public' AND table_name = $1 AND constraint_name = $2`,
			table, chk.Name).Scan(&count); err != nil {
			return fmt.Errorf("check constraint existence %s: %w", chk.Name, err)
		}
		if count > 0 {
			continue
		}
		q := fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)`,
			quote(table), quote(chk.Name), chk.Expression)
		if _, err := t.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("add check constraint %s.%s: %w", table, chk.Name, err)
		}
	}
	return nil
}

func (t *Target) DropTable(ctx context.Context, table string) error {
	_, err := t.db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, quote(table)))
	return err
}

func (t *Target) TruncateTable(ctx context.Context, table string) error {
	_, err := t.db.ExecContext(ctx, fmt.Sprintf(`TRUNCATE TABLE %s`, quote(table)))
	return err
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
			query = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE %s`,
				quote(table), quote(change.Column.Name), change.Column.Type)
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

	// Build INSERT ... ON CONFLICT (...) DO UPDATE SET ...
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quote(c)
	}

	// PostgreSQL uses $1, $2, ... placeholders.
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	var upsertClause string
	if len(pks) > 0 {
		quotedPKs := make([]string, len(pks))
		for i, pk := range pks {
			quotedPKs[i] = quote(pk)
		}
		setClauses := make([]string, 0, len(cols))
		pkSet := map[string]bool{}
		for _, pk := range pks {
			pkSet[pk] = true
		}
		for _, col := range cols {
			if !pkSet[col] {
				setClauses = append(setClauses, fmt.Sprintf(`%s = EXCLUDED.%s`, quote(col), quote(col)))
			}
		}
		if len(setClauses) > 0 {
			upsertClause = fmt.Sprintf(`ON CONFLICT (%s) DO UPDATE SET %s`,
				strings.Join(quotedPKs, ", "), strings.Join(setClauses, ", "))
		} else {
			upsertClause = fmt.Sprintf(`ON CONFLICT (%s) DO NOTHING`, strings.Join(quotedPKs, ", "))
		}
	} else {
		upsertClause = "ON CONFLICT DO NOTHING"
	}

	// PostgreSQL allows at most 65535 bind parameters per query.
	// Chunk records so each INSERT stays under that limit.
	maxRows := 65535 / len(cols)
	if maxRows < 1 {
		maxRows = 1
	}

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	for start := 0; start < len(batch.Records); start += maxRows {
		end := start + maxRows
		if end > len(batch.Records) {
			end = len(batch.Records)
		}
		chunk := batch.Records[start:end]

		rowPlaceholders := make([]string, len(chunk))
		allVals := make([]interface{}, 0, len(chunk)*len(cols))
		for i, rec := range chunk {
			ph := make([]string, len(cols))
			for j := range cols {
				ph[j] = fmt.Sprintf("$%d", i*len(cols)+j+1)
			}
			rowPlaceholders[i] = "(" + strings.Join(ph, ", ") + ")"
			for _, col := range cols {
				allVals = append(allVals, rec[col])
			}
		}

		chunkQuery := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s %s`,
			quote(table),
			strings.Join(quotedCols, ", "),
			strings.Join(rowPlaceholders, ", "),
			upsertClause,
		)
		if _, err := tx.ExecContext(ctx, chunkQuery, allVals...); err != nil {
			return fmt.Errorf("insert chunk into %s: %w", table, err)
		}
	}

	return tx.Commit()
}

func (t *Target) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	check := &adapters.PermissionCheck{}

	// Probe: read
	if err := t.db.QueryRowContext(ctx, `SELECT 1 FROM information_schema.tables LIMIT 1`).Err(); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	// Probe: create table
	_, err := t.db.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS _xferdb_probe (id SERIAL PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	check.CanCreateTable = true

	// Probe: write
	_, err = t.db.ExecContext(ctx, `INSERT INTO _xferdb_probe DEFAULT VALUES`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("write check failed: %v", err))
	} else {
		check.CanWrite = true
	}

	t.db.ExecContext(ctx, `DROP TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
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
