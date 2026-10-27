package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// ChangeApplier is an optional interface for target adapters that can replay
// a live migration's changes (#102). Changes are applied in transactions,
// and each transaction also records how far it got (SetApplied), in a small
// table on the target itself, so a batch and its position commit together:
// after a crash apply resumes exactly where the target is, never applying a
// change twice or skipping one.
type ChangeApplier interface {
	// PrepareChangeState creates the table that holds applied positions,
	// if it doesn't exist.
	PrepareChangeState(ctx context.Context) error
	// AppliedSeqs returns the last applied queue seq of each table of a
	// stream (a live migration's id); a table missing from it has none.
	AppliedSeqs(ctx context.Context, stream string) (map[string]uint64, error)
	// ClearChangeState removes a stream's positions, once it is finished.
	ClearChangeState(ctx context.Context, stream string) error
	BeginChanges(ctx context.Context) (ChangeTx, error)
}

// ChangeTx is one target transaction of replayed changes.
type ChangeTx interface {
	// Upsert writes whole rows, as WriteBatch does.
	Upsert(ctx context.Context, table string, rows []map[string]interface{}) error
	// Delete removes the rows with these keys; a key with no row is fine.
	Delete(ctx context.Context, table string, keys []map[string]interface{}) error
	// Update sets some columns of existing rows. It returns how many rows
	// it found; a row that isn't there is skipped, not an error.
	Update(ctx context.Context, table string, updates []RowUpdate) (int64, error)
	// Exists reports whether a row with this key is there.
	Exists(ctx context.Context, table string, key map[string]interface{}) (bool, error)
	// Truncate empties a table inside the transaction.
	Truncate(ctx context.Context, table string) error
	// SetApplied records seq as a table's last applied change.
	SetApplied(ctx context.Context, stream, table string, seq uint64) error
	Commit() error
	Rollback() error
}

// RowUpdate sets columns of the row whose key is Key. Set may include key
// columns, when the update changed the key.
type RowUpdate struct {
	Key map[string]interface{}
	Set map[string]interface{}
}

// SQLChangeTx implements ChangeTx over a database/sql transaction for the
// relational targets; each fills in its dialect.
type SQLChangeTx struct {
	Tx *sql.Tx
	// QuoteTable and QuoteColumn render identifiers; Placeholder the n-th
	// bound parameter, counting from 1.
	QuoteTable  func(string) string
	QuoteColumn func(string) string
	Placeholder func(n int) string
	// UpsertRows writes whole rows in Tx, with the target's upsert.
	UpsertRows func(ctx context.Context, tx *sql.Tx, table string, rows []map[string]interface{}) error
	// Bind converts a value before it is bound for table.col; nil leaves
	// values as they are.
	Bind func(ctx context.Context, table, col string, v interface{}) (interface{}, error)
	// TruncateSQL empties a table inside a transaction ("TRUNCATE %s", or
	// "DELETE FROM %s" where TRUNCATE would commit).
	TruncateSQL string
	// SetAppliedSQL upserts (stream, table, seq).
	SetAppliedSQL string
	// MaxParams caps the bound parameters of one statement.
	MaxParams int
}

func (s *SQLChangeTx) bind(ctx context.Context, table, col string, v interface{}) (interface{}, error) {
	if s.Bind == nil {
		return v, nil
	}
	return s.Bind(ctx, table, col, v)
}

func (s *SQLChangeTx) Upsert(ctx context.Context, table string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	return s.UpsertRows(ctx, s.Tx, table, rows)
}

func (s *SQLChangeTx) Delete(ctx context.Context, table string, keys []map[string]interface{}) error {
	if len(keys) == 0 {
		return nil
	}
	keyCols := sortedKeys(keys[0])
	if len(keyCols) == 0 {
		return fmt.Errorf("delete from %s: no key columns", table)
	}
	max := s.MaxParams
	if max <= 0 {
		max = MaxLookupParams
	}
	per := max / len(keyCols)
	if per < 1 {
		per = 1
	}
	for start := 0; start < len(keys); start += per {
		end := start + per
		if end > len(keys) {
			end = len(keys)
		}
		var args []interface{}
		groups := make([]string, 0, end-start)
		for _, k := range keys[start:end] {
			conds := make([]string, len(keyCols))
			for j, c := range keyCols {
				v, ok := k[c]
				if !ok {
					return fmt.Errorf("delete from %s: key is missing column %s", table, c)
				}
				v, err := s.bind(ctx, table, c, v)
				if err != nil {
					return err
				}
				args = append(args, v)
				conds[j] = s.QuoteColumn(c) + " = " + s.Placeholder(len(args))
			}
			groups = append(groups, "("+strings.Join(conds, " AND ")+")")
		}
		q := fmt.Sprintf("DELETE FROM %s WHERE %s", s.QuoteTable(table), strings.Join(groups, " OR "))
		if _, err := s.Tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("delete from %s: %w", table, err)
		}
	}
	return nil
}

func (s *SQLChangeTx) Update(ctx context.Context, table string, updates []RowUpdate) (int64, error) {
	var found int64
	for _, u := range updates {
		if len(u.Set) == 0 {
			continue
		}
		var args []interface{}
		setCols := sortedKeys(u.Set)
		sets := make([]string, len(setCols))
		for i, c := range setCols {
			v, err := s.bind(ctx, table, c, u.Set[c])
			if err != nil {
				return found, err
			}
			args = append(args, v)
			sets[i] = s.QuoteColumn(c) + " = " + s.Placeholder(len(args))
		}
		keyCols := sortedKeys(u.Key)
		if len(keyCols) == 0 {
			return found, fmt.Errorf("update %s: no key columns", table)
		}
		conds := make([]string, len(keyCols))
		for i, c := range keyCols {
			v, err := s.bind(ctx, table, c, u.Key[c])
			if err != nil {
				return found, err
			}
			args = append(args, v)
			conds[i] = s.QuoteColumn(c) + " = " + s.Placeholder(len(args))
		}
		q := fmt.Sprintf("UPDATE %s SET %s WHERE %s", s.QuoteTable(table), strings.Join(sets, ", "), strings.Join(conds, " AND "))
		res, err := s.Tx.ExecContext(ctx, q, args...)
		if err != nil {
			return found, fmt.Errorf("update %s: %w", table, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			found += n
		}
	}
	return found, nil
}

func (s *SQLChangeTx) Exists(ctx context.Context, table string, key map[string]interface{}) (bool, error) {
	keyCols := sortedKeys(key)
	if len(keyCols) == 0 {
		return false, fmt.Errorf("look up %s: no key columns", table)
	}
	var args []interface{}
	conds := make([]string, len(keyCols))
	for i, c := range keyCols {
		v, err := s.bind(ctx, table, c, key[c])
		if err != nil {
			return false, err
		}
		args = append(args, v)
		conds[i] = s.QuoteColumn(c) + " = " + s.Placeholder(len(args))
	}
	rows, err := s.Tx.QueryContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1", s.QuoteTable(table), strings.Join(conds, " AND ")), args...)
	if err != nil {
		return false, fmt.Errorf("look up %s: %w", table, err)
	}
	defer rows.Close()
	found := rows.Next()
	return found, rows.Err()
}

func (s *SQLChangeTx) Truncate(ctx context.Context, table string) error {
	if _, err := s.Tx.ExecContext(ctx, fmt.Sprintf(s.TruncateSQL, s.QuoteTable(table))); err != nil {
		return fmt.Errorf("truncate %s: %w", table, err)
	}
	return nil
}

func (s *SQLChangeTx) SetApplied(ctx context.Context, stream, table string, seq uint64) error {
	if _, err := s.Tx.ExecContext(ctx, s.SetAppliedSQL, stream, table, int64(seq)); err != nil {
		return fmt.Errorf("record applied position: %w", err)
	}
	return nil
}

func (s *SQLChangeTx) Commit() error   { return s.Tx.Commit() }
func (s *SQLChangeTx) Rollback() error { return s.Tx.Rollback() }

// QueryAppliedSeqs runs query (stream as its one parameter, selecting
// table_name and seq) for an AppliedSeqs implementation.
func QueryAppliedSeqs(ctx context.Context, db *sql.DB, query, stream string) (map[string]uint64, error) {
	rows, err := db.QueryContext(ctx, query, stream)
	if err != nil {
		return nil, fmt.Errorf("read applied positions: %w", err)
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var table string
		var seq int64
		if err := rows.Scan(&table, &seq); err != nil {
			return nil, err
		}
		out[table] = uint64(seq)
	}
	return out, rows.Err()
}

func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// LiveCheck is one item of a live migration's preflight: something the
// source (or target) needs before changes can be streamed from it.
type LiveCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
	// Fix says what to change when the check fails.
	Fix string `json:"fix,omitempty"`
	// Warning marks a check that doesn't block a live migration but is
	// worth knowing about, such as unlimited WAL retention.
	Warning bool `json:"warning,omitempty"`
}

// LivePreflighter is implemented by source adapters a live migration can
// stream from. LiveChecks checks what streaming tables needs; no tables
// means every table the source would migrate.
type LivePreflighter interface {
	LiveChecks(ctx context.Context, tables []string) ([]LiveCheck, error)
}
