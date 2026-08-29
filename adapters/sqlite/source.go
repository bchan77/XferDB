package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Source implements adapters.SourceAdapter for SQLite.
type Source struct {
	db *sql.DB
}

func NewSource() adapters.SourceAdapter {
	return &Source{}
}

func (s *Source) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := openDB(config)
	if err != nil {
		return err
	}
	s.db = db
	return s.Ping(ctx)
}

func (s *Source) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Source) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Source) ListTables(ctx context.Context) ([]adapters.TableSchema, error) {
	names, err := listTables(ctx, s.db)
	if err != nil {
		return nil, err
	}
	tables := make([]adapters.TableSchema, 0, len(names))
	for _, name := range names {
		schema, err := getSchema(ctx, s.db, name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, *schema)
	}
	return tables, nil
}

func (s *Source) GetSchema(ctx context.Context, table string) (*adapters.TableSchema, error) {
	return getSchema(ctx, s.db, table)
}

func (s *Source) GetRowCount(ctx context.Context, table string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quote(table))).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count(%s): %w", table, err)
	}
	return count, nil
}

func (s *Source) ReadBatch(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	query := fmt.Sprintf(`SELECT * FROM %s LIMIT %d OFFSET %d`, quote(table), opts.Limit, opts.Offset)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var records []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rec := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rec[col] = vals[i]
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Ensure deterministic column ordering in each record by sorting keys.
	// This is a no-op on the data but keeps WriteBatch column lists stable.
	_ = sort.StringSlice(cols) // cols already ordered by DB; kept for documentation intent

	return &adapters.Batch{Records: records, Size: len(records)}, nil
}

func (s *Source) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	check := &adapters.PermissionCheck{}

	if _, err := s.db.QueryContext(ctx, `SELECT name FROM sqlite_master LIMIT 1`); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _xferdb_probe (id INTEGER PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	check.CanCreateTable = true

	_, err = s.db.ExecContext(ctx, `INSERT INTO _xferdb_probe DEFAULT VALUES`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("write check failed: %v", err))
	} else {
		check.CanWrite = true
	}

	s.db.ExecContext(ctx, `DROP TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
	return check, nil
}
