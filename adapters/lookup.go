package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// RowLookup is an optional interface for source adapters that can fetch rows
// by primary key. Validation uses it to find, for a page of source rows, the
// target rows with the same keys (the target is read through the source
// adapter of its own type).
type RowLookup interface {
	// LookupRows returns the rows of table whose keyCols equal one of keys.
	// Each key holds one value per key column, in keyCols order. Rows come
	// back in no particular order and keys with no row are simply absent.
	LookupRows(ctx context.Context, table string, keyCols []string, keys [][]interface{}) ([]map[string]interface{}, error)
}

// MaxLookupParams caps the bound parameters in one lookup query: 900 stays
// under SQLite's limit on every version this project targets (older builds
// allow 999), and is far below PostgreSQL's and MySQL's.
const MaxLookupParams = 900

// LookupQuery builds the SELECT for a RowLookup: `col IN (…)` for a single
// key column, otherwise an OR of per-key equality groups, which every
// supported database accepts (SQLite only takes row values on the right of IN
// as a subquery). quote quotes an identifier; placeholder renders the n-th
// bound parameter, counting from 1.
func LookupQuery(quote func(string) string, placeholder func(n int) string, table string, keyCols []string, keys [][]interface{}) (string, []interface{}) {
	args := make([]interface{}, 0, len(keys)*len(keyCols))
	var where string
	if len(keyCols) == 1 {
		ph := make([]string, len(keys))
		for i, k := range keys {
			args = append(args, k[0])
			ph[i] = placeholder(len(args))
		}
		where = fmt.Sprintf("%s IN (%s)", quote(keyCols[0]), strings.Join(ph, ", "))
	} else {
		groups := make([]string, len(keys))
		for i, k := range keys {
			conds := make([]string, len(keyCols))
			for j, c := range keyCols {
				args = append(args, k[j])
				conds[j] = fmt.Sprintf("%s = %s", quote(c), placeholder(len(args)))
			}
			groups[i] = "(" + strings.Join(conds, " AND ") + ")"
		}
		where = strings.Join(groups, " OR ")
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", quote(table), where), args
}

// ScanRecords reads every row of rows into a column-name keyed map, the shape
// ReadBatch returns.
func ScanRecords(rows *sql.Rows) ([]map[string]interface{}, error) {
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
	return records, rows.Err()
}

// TableStreamer is an optional interface for source adapters that can read a
// whole table in one query, rows in no particular order. The validator's
// row fingerprint doesn't depend on order, and a single scan avoids paging,
// which on some tables costs a full scan per page: a YugabyteDB table whose
// primary key is hash-sharded can't be read in key order, so every "next
// page by key" query scanned and sorted the whole table.
type TableStreamer interface {
	// StreamTable calls fn with each row of table; an error from fn stops it.
	StreamTable(ctx context.Context, table string, fn func(map[string]interface{}) error) error
}

// StreamRecords passes each row of rows to fn as a column -> value map, the
// way ScanRecords collects them.
func StreamRecords(rows *sql.Rows, fn func(map[string]interface{}) error) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		rec := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rec[col] = vals[i]
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return rows.Err()
}
