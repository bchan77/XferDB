package postgres

import (
	"context"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// ybIndexHash is YugabyteDB's pg_index.indoption bit for a HASH key column.
const ybIndexHash = 4

// HashColumns implements adapters.HashRangeSource: on YugabyteDB, the
// primary key columns flagged HASH; nil on PostgreSQL and for a
// range-sharded (ASC/DESC) or colocated key.
func (s *Source) HashColumns(ctx context.Context, table string) ([]string, error) {
	if s.config.Type != adapters.TypeYugabyte {
		return nil, nil
	}
	schema, name := splitName(table)
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index i
		CROSS JOIN LATERAL generate_subscripts(i.indkey, 1) AS k(n)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[k.n]
		WHERE i.indrelid = format('%I.%I', `+schemaExpr(2)+`, $1::text)::regclass
		  AND i.indisprimary
		  AND k.n < i.indnkeyatts
		  AND (i.indoption[k.n] & $3) <> 0
		ORDER BY k.n`, name, schema, ybIndexHash)
	if err != nil {
		return nil, fmt.Errorf("hash columns of %s: %w", table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// ReadHashRange implements adapters.HashRangeSource.
func (s *Source) ReadHashRange(ctx context.Context, table string, cols []string, lo, hi int) (*adapters.Batch, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("%s: no hash columns", table)
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quote(c)
	}
	code := "yb_hash_code(" + strings.Join(quoted, ", ") + ")"
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s WHERE %s >= $1 AND %s < $2`,
		qualify(table), code, code), lo, hi)
	if err != nil {
		return nil, fmt.Errorf("read hash range [%d, %d) of %s: %w", lo, hi, table, err)
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var records []map[string]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(names))
		ptrs := make([]interface{}, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rec := make(map[string]interface{}, len(names))
		for i, n := range names {
			rec[n] = vals[i]
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &adapters.Batch{Records: records, Size: len(records)}, nil
}
