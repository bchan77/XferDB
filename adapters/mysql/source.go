package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Source implements adapters.SourceAdapter for MySQL.
type Source struct {
	db     *sql.DB
	config adapters.ConnectionConfig
}

func NewSource() adapters.SourceAdapter {
	return &Source{}
}

func (s *Source) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	db, err := sql.Open("mysql", buildDSN(config))
	if err != nil {
		return fmt.Errorf("mysql open: %w", err)
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
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quote(pkColumn), quote(pkColumn), quote(table))
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
			"SELECT * FROM %s WHERE %s >= %d AND %s %s %d ORDER BY %s LIMIT %d",
			quote(table),
			quote(opts.PKCol), opts.PKMin,
			quote(opts.PKCol), op, opts.PKMax,
			quote(opts.PKCol), opts.Limit,
		)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", quote(table), opts.Limit, opts.Offset)
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

	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM information_schema.tables LIMIT 1`).Err(); err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("read check failed: %v", err))
		return check, nil
	}
	check.CanRead = true

	_, err := s.db.ExecContext(ctx, `CREATE TEMPORARY TABLE IF NOT EXISTS _xferdb_probe (id INT PRIMARY KEY)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("create table check failed: %v", err))
		return check, nil
	}
	check.CanCreateTable = true

	_, err = s.db.ExecContext(ctx, `INSERT INTO _xferdb_probe (id) VALUES (1)`)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("write check failed: %v", err))
	} else {
		check.CanWrite = true
	}

	s.db.ExecContext(ctx, `DROP TEMPORARY TABLE IF EXISTS _xferdb_probe`) //nolint:errcheck
	return check, nil
}

func (s *Source) GetInfo(ctx context.Context) (*adapters.DatabaseInfo, error) {
	info := &adapters.DatabaseInfo{Type: "MySQL"}

	// Get version
	var version string
	if err := s.db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&version); err == nil {
		info.Version = version
	}

	// Get current database name
	var dbName sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&dbName); err == nil && dbName.Valid {
		info.Database = dbName.String
	}

	// Get host from config
	if s.config.Host != "" {
		port := s.config.Port
		if port == 0 {
			port = 3306
		}
		info.Host = fmt.Sprintf("%s:%d", s.config.Host, port)
	}

	// Count tables
	tables, err := listTables(ctx, s.db)
	if err == nil {
		info.Tables = len(tables)
	}

	// Get database size
	if info.Database != "" {
		var sizeBytes sql.NullInt64
		err := s.db.QueryRowContext(ctx, `
			SELECT SUM(data_length + index_length)
			FROM information_schema.tables
			WHERE table_schema = ?`, info.Database).Scan(&sizeBytes)
		if err == nil && sizeBytes.Valid {
			info.SizeBytes = sizeBytes.Int64
			info.SizeHuman = humanSize(sizeBytes.Int64)
		}
	}

	// Check SSL status
	var sslCipher sql.NullString
	if err := s.db.QueryRowContext(ctx, `SHOW STATUS LIKE 'Ssl_cipher'`).Scan(new(string), &sslCipher); err == nil {
		if sslCipher.Valid && sslCipher.String != "" {
			info.SSL = "enabled"
		} else {
			info.SSL = "disabled"
		}
	}

	return info, nil
}
