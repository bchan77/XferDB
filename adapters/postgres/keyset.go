package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// keyAlias names the extra columns a keyset read selects: the key's text
// forms, which become the batch's LastKey and are removed from the rows.
const keyAlias = "xferdb_key_"

// KeyColumns implements adapters.KeysetSource. The key is looked up once
// per table, not on every batch.
func (s *Source) KeyColumns(ctx context.Context, table string) ([]string, error) {
	s.orderMu.Lock()
	defer s.orderMu.Unlock()
	if cols, ok := s.keyCols[table]; ok {
		return cols, nil
	}
	schema, err := getSchema(ctx, s.db, table)
	if err != nil {
		return nil, fmt.Errorf("primary key of %s: %w", table, err)
	}
	if s.keyCols == nil {
		s.keyCols = make(map[string][]string)
	}
	s.keyCols[table] = pkColumns(schema)
	return s.keyCols[table], nil
}

// MaxKey implements adapters.KeysetSource.
func (s *Source) MaxKey(ctx context.Context, table string) ([]string, error) {
	cols, err := s.KeyColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%s has no primary key", table)
	}
	// The text forms are aliased: an output column named like the key
	// column would be what ORDER BY sorts by, as text ("98" > "398").
	texts, desc := make([]string, len(cols)), make([]string, len(cols))
	for i, c := range cols {
		texts[i] = fmt.Sprintf("%s::text AS %s%d", quote(c), keyAlias, i)
		desc[i] = quote(c) + " DESC"
	}
	key := make([]string, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range key {
		ptrs[i] = &key[i]
	}
	err = s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT 1`,
		strings.Join(texts, ", "), qualify(table), strings.Join(desc, ", "))).Scan(ptrs...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("largest key of %s: %w", table, err)
	}
	return key, nil
}

// readKeyset reads the rows after opts.KeyAfter up to opts.KeyUpTo in key
// order. Keys are compared as row values, (a, b) > ($1, $2), with each
// parameter sent as text and typed by the column it is compared with, so
// any key type with a text form pages correctly, composite keys included.
func (s *Source) readKeyset(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	cols, err := s.KeyColumns(ctx, table)
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%s has no primary key to page by", table)
	}
	if opts.KeyUpTo == nil {
		return &adapters.Batch{}, nil // an empty table
	}
	if len(opts.KeyUpTo) != len(cols) {
		return nil, fmt.Errorf("%s: key bound has %d columns, the primary key %d", table, len(opts.KeyUpTo), len(cols))
	}
	quoted, texts := make([]string, len(cols)), make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quote(c)
		texts[i] = fmt.Sprintf("%s::text AS %s%d", quote(c), keyAlias, i)
	}
	row := "(" + strings.Join(quoted, ", ") + ")"
	params := func(first int) string {
		ps := make([]string, len(cols))
		for i := range ps {
			ps[i] = fmt.Sprintf("$%d", first+i)
		}
		return "(" + strings.Join(ps, ", ") + ")"
	}
	var args []interface{}
	conds := []string{row + " <= " + params(1)}
	for _, v := range opts.KeyUpTo {
		args = append(args, v)
	}
	if opts.KeyAfter != nil {
		if len(opts.KeyAfter) != len(cols) {
			return nil, fmt.Errorf("%s: resume key has %d columns, the primary key %d", table, len(opts.KeyAfter), len(cols))
		}
		conds = append(conds, row+" > "+params(len(args)+1))
		for _, v := range opts.KeyAfter {
			args = append(args, v)
		}
	}
	query := fmt.Sprintf(`SELECT *, %s FROM %s WHERE %s ORDER BY %s LIMIT %d`,
		strings.Join(texts, ", "), qualify(table), strings.Join(conds, " AND "), strings.Join(quoted, ", "), opts.Limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	records, err := adapters.ScanRecords(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	batch := &adapters.Batch{Records: records, Size: len(records)}
	for i, rec := range records {
		var last []string
		if i == len(records)-1 {
			last = make([]string, len(cols))
		}
		for k := range cols {
			name := fmt.Sprintf("%s%d", keyAlias, k)
			if last != nil {
				last[k] = keyText(rec[name])
			}
			delete(rec, name)
		}
		if last != nil {
			batch.LastKey = adapters.EncodeKey(last)
		}
	}
	return batch, nil
}

func keyText(v interface{}) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	}
	return fmt.Sprint(v)
}
