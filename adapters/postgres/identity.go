package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// isIntegerType reports whether a PostgreSQL type can be an identity
// column's.
func isIntegerType(t string) bool {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "smallint", "integer", "int", "bigint", "int2", "int4", "int8":
		return true
	}
	return false
}

// identitySequences returns each identity (or serial) column of table with
// its sequence, as pg_get_serial_sequence names it (quoted as needed, ready
// to cast to regclass). A table that doesn't exist has none.
func identitySequences(ctx context.Context, db *sql.DB, name string) (map[string]string, error) {
	schemaName, table := splitName(name)
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname, s.seq
		FROM pg_catalog.pg_attribute a,
		LATERAL (SELECT pg_catalog.pg_get_serial_sequence(format('%I.%I', `+schemaExpr(2)+`, $1::text), a.attname) AS seq) s
		WHERE a.attrelid = pg_catalog.to_regclass(format('%I.%I', `+schemaExpr(2)+`, $1::text))
		  AND a.attnum > 0 AND NOT a.attisdropped AND s.seq IS NOT NULL`, table, schemaName)
	if err != nil {
		return nil, fmt.Errorf("identity columns of %s: %w", name, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var col, seq string
		if err := rows.Scan(&col, &seq); err != nil {
			return nil, err
		}
		out[col] = seq
	}
	return out, rows.Err()
}

// ResetIdentity implements adapters.IdentityResetter.
func (t *Target) ResetIdentity(ctx context.Context, table string, floors map[string]int64) (int, error) {
	seqs, err := identitySequences(ctx, t.db, table)
	if err != nil {
		return 0, err
	}
	set := 0
	for col, seq := range seqs {
		// A descending sequence counts down from its start; moving it past
		// the largest key would be wrong, and is out of its range anyway.
		var increment, maxValue int64
		if err := t.db.QueryRowContext(ctx, `SELECT seqincrement, seqmax FROM pg_catalog.pg_sequence WHERE seqrelid = $1::regclass`, seq).Scan(&increment, &maxValue); err != nil {
			return set, fmt.Errorf("sequence %s: %w", seq, err)
		}
		if increment < 0 {
			continue
		}
		var max sql.NullInt64
		if err := t.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT MAX(%s) FROM %s`, quote(col), qualify(table))).Scan(&max); err != nil {
			return set, fmt.Errorf("largest %s.%s: %w", table, col, err)
		}
		next := max.Int64 + 1
		if f, ok := floors[col]; ok && f > next {
			next = f
		}
		if next > maxValue {
			// The keys already reach the type's end (a smallint at
			// 32767): no value is left to hand out, and setval would fail.
			continue
		}
		// is_called false: next is the value the next insert gets.
		if _, err := t.db.ExecContext(ctx, `SELECT setval($1::regclass, $2, false)`, seq, next); err != nil {
			return set, fmt.Errorf("set %s to %d: %w", seq, next, err)
		}
		set++
	}
	return set, nil
}

// IdentityNext implements adapters.IdentitySource: the value each identity
// column's sequence hands out next. A sequence this login can't read is
// left out; the target then goes by its own rows.
func (s *Source) IdentityNext(ctx context.Context, table string) (map[string]int64, error) {
	seqs, err := identitySequences(ctx, s.db, table)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for col, seq := range seqs {
		// Read from the sequence itself: YugabyteDB's pg_sequences.last_value
		// is always NULL.
		var last, increment int64
		var called bool
		err := s.db.QueryRowContext(ctx, `SELECT last_value, is_called FROM `+seq).Scan(&last, &called)
		if err == nil {
			err = s.db.QueryRowContext(ctx, `SELECT seqincrement FROM pg_catalog.pg_sequence WHERE seqrelid = $1::regclass`, seq).Scan(&increment)
		}
		if err != nil || increment < 0 {
			continue
		}
		if called {
			last += increment
		}
		out[col] = last
	}
	return out, nil
}
