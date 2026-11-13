package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	driver "github.com/go-sql-driver/mysql"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

var integerTypeRe = regexp.MustCompile(`^(tinyint|smallint|mediumint|int|integer|bigint)(\(\d+\))?( unsigned)?( zerofill)?$`)

// isIntegerType reports whether a MySQL column type can be AUTO_INCREMENT.
func isIntegerType(t string) bool {
	return integerTypeRe.MatchString(strings.ToLower(strings.TrimSpace(t)))
}

// autoIncrementColumn picks the column CreateTable makes AUTO_INCREMENT,
// or "" for none. MySQL allows one per table, and only on a column that
// leads an index; CreateTable's only index is the primary key (the others
// come after the copy), so it has to be the key's first column.
func autoIncrementColumn(cols []adapters.ColumnDef) string {
	for _, c := range cols {
		if c.PrimaryKey {
			if c.Identity {
				return c.Name
			}
			return ""
		}
	}
	return ""
}

// targetDSN adds NO_AUTO_VALUE_ON_ZERO to the target session's sql_mode,
// unless the DSN sets sql_mode itself: without it, a row copied with a key
// of 0 into an AUTO_INCREMENT column gets a newly generated key instead.
func targetDSN(dsn string) string {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return dsn
	}
	if _, ok := cfg.Params["sql_mode"]; ok {
		return dsn
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["sql_mode"] = "CONCAT(@@sql_mode,',NO_AUTO_VALUE_ON_ZERO')"
	return cfg.FormatDSN()
}

// autoIncrement returns table's AUTO_INCREMENT column, or "".
func autoIncrement(ctx context.Context, db *sql.DB, table string) (string, error) {
	var col string
	err := db.QueryRowContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND extra LIKE '%auto_increment%'`, table).Scan(&col)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return col, err
}

// ResetIdentity implements adapters.IdentityResetter. InnoDB already keeps
// its counter past the largest key inserted, so this matters for the
// source's floor; the largest key is taken too, in case the counter isn't.
func (t *Target) ResetIdentity(ctx context.Context, table string, floors map[string]int64) (int, error) {
	col, err := autoIncrement(ctx, t.db, table)
	if err != nil || col == "" {
		return 0, err
	}
	var max sql.NullInt64
	if err := t.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MAX(%s) FROM %s", quote(col), quote(table))).Scan(&max); err != nil {
		return 0, fmt.Errorf("largest %s.%s: %w", table, col, err)
	}
	next := max.Int64 + 1
	if f, ok := floors[col]; ok && f > next {
		next = f
	}
	if _, err := t.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", quote(table), next)); err != nil {
		return 0, fmt.Errorf("set %s AUTO_INCREMENT to %d: %w", table, next, err)
	}
	return 1, nil
}

// IdentityNext implements adapters.IdentitySource: the table's
// AUTO_INCREMENT counter. information_schema caches it for a day by
// default on 8.0 (information_schema_stats_expiry), so the session asks for
// a fresh value; a server without that variable (5.7, MariaDB) has no cache.
func (s *Source) IdentityNext(ctx context.Context, table string) (map[string]int64, error) {
	col, err := autoIncrement(ctx, s.db, table)
	if err != nil || col == "" {
		return nil, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, _ = conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0")
	var next sql.NullInt64
	if err := conn.QueryRowContext(ctx, `
		SELECT auto_increment FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&next); err != nil || !next.Valid {
		return nil, nil
	}
	return map[string]int64{col: next.Int64}, nil
}
