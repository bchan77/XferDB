package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Source implements adapters.SourceAdapter for PostgreSQL.
type Source struct {
	db     *sql.DB
	config adapters.ConnectionConfig
}

func NewSource() adapters.SourceAdapter {
	return &Source{}
}

func (s *Source) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := sql.Open("postgres", buildDSN(config))
	if err != nil {
		return fmt.Errorf("postgres open: %w", err)
	}
	s.db = db
	s.config = config
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
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s`, quote(table))).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count(%s): %w", table, err)
	}
	return count, nil
}

func (s *Source) GetPKRange(ctx context.Context, table, pkColumn string) (int64, int64, error) {
	q := fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`, quote(pkColumn), quote(pkColumn), quote(table))
	var minVal, maxVal sql.NullInt64
	if err := s.db.QueryRowContext(ctx, q).Scan(&minVal, &maxVal); err != nil {
		return 0, 0, fmt.Errorf("pk range(%s.%s): %w", table, pkColumn, err)
	}
	if !minVal.Valid {
		return 0, 0, fmt.Errorf("table %s is empty", table)
	}
	return minVal.Int64, maxVal.Int64, nil
}

func (s *Source) ReadBatch(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	var query string
	if opts.PKCol != "" {
		op := "<"
		if opts.PKMaxIncl {
			op = "<="
		}
		query = fmt.Sprintf(
			`SELECT * FROM %s WHERE %s >= %d AND %s %s %d ORDER BY %s LIMIT %d`,
			quote(table),
			quote(opts.PKCol), opts.PKMin,
			quote(opts.PKCol), op, opts.PKMax,
			quote(opts.PKCol), opts.Limit,
		)
	} else {
		query = fmt.Sprintf(`SELECT * FROM %s LIMIT %d OFFSET %d`, quote(table), opts.Limit, opts.Offset)
	}

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
	batch := &adapters.Batch{Records: records, Size: len(records)}
	if opts.PKCol != "" && len(records) > 0 {
		batch.LastPK = pkInt64(records[len(records)-1][opts.PKCol])
	}
	return batch, nil
}

// pkInt64 coerces a PK value returned by the database driver to int64.
func pkInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func (s *Source) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	check := &adapters.PermissionCheck{}

	// Probe: read
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM information_schema.tables LIMIT 1`).Err(); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	// Probe: create table (use a temp table so we leave no permanent objects)
	_, err := s.db.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS _xferdb_probe (id SERIAL PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	check.CanCreateTable = true

	// Probe: write
	_, err = s.db.ExecContext(ctx, `INSERT INTO _xferdb_probe DEFAULT VALUES`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("write check failed: %v", err))
	} else {
		check.CanWrite = true
	}

	s.db.ExecContext(ctx, `DROP TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
	return check, nil
}

func (s *Source) GetInfo(ctx context.Context) (*adapters.DatabaseInfo, error) {
	info := &adapters.DatabaseInfo{Type: "PostgreSQL"}

	// Get version
	var version string
	if err := s.db.QueryRowContext(ctx, `SHOW server_version`).Scan(&version); err == nil {
		info.Version = version
	}

	// Get current database name
	var dbName string
	if err := s.db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&dbName); err == nil {
		info.Database = dbName
	}

	// Get host info from config or inet_server_addr()
	if s.config.Host != "" {
		port := s.config.Port
		if port == 0 {
			port = 5432
		}
		info.Host = fmt.Sprintf("%s:%d", s.config.Host, port)
	} else {
		var host sql.NullString
		var port sql.NullInt64
		s.db.QueryRowContext(ctx, `SELECT inet_server_addr(), inet_server_port()`).Scan(&host, &port)
		if host.Valid {
			info.Host = fmt.Sprintf("%s:%d", host.String, port.Int64)
		}
	}

	// Count tables
	tables, err := listTables(ctx, s.db)
	if err == nil {
		info.Tables = len(tables)
	}

	// Get database size
	var sizeBytes int64
	if err := s.db.QueryRowContext(ctx, `SELECT pg_database_size(current_database())`).Scan(&sizeBytes); err == nil {
		info.SizeBytes = sizeBytes
		info.SizeHuman = humanSize(sizeBytes)
	}

	// Check SSL status
	var ssl string
	if err := s.db.QueryRowContext(ctx, `SHOW ssl`).Scan(&ssl); err == nil {
		if ssl == "on" {
			info.SSL = "enabled"
		} else {
			info.SSL = "disabled"
		}
	}

	return info, nil
}
