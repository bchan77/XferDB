package sqlite

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// changeStateTable holds a live migration's applied positions.
const changeStateTable = "xferdb_cdc_applied"

// PrepareChangeState implements adapters.ChangeApplier.
func (t *Target) PrepareChangeState(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+changeStateTable+` (
			stream     TEXT    NOT NULL,
			table_name TEXT    NOT NULL,
			seq        INTEGER NOT NULL,
			updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream, table_name)
		)`)
	if err != nil {
		return fmt.Errorf("create %s: %w", changeStateTable, err)
	}
	return nil
}

// AppliedSeqs implements adapters.ChangeApplier.
func (t *Target) AppliedSeqs(ctx context.Context, stream string) (map[string]uint64, error) {
	return adapters.QueryAppliedSeqs(ctx, t.db,
		`SELECT table_name, seq FROM `+changeStateTable+` WHERE stream = ?`, stream)
}

// ClearChangeState implements adapters.ChangeApplier.
func (t *Target) ClearChangeState(ctx context.Context, stream string) error {
	_, err := t.db.ExecContext(ctx, `DELETE FROM `+changeStateTable+` WHERE stream = ?`, stream)
	return err
}

// BeginChanges implements adapters.ChangeApplier.
func (t *Target) BeginChanges(ctx context.Context) (adapters.ChangeTx, error) {
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return &adapters.SQLChangeTx{
		Tx:          tx,
		QuoteTable:  quote,
		QuoteColumn: quote,
		Placeholder: func(int) string { return "?" },
		UpsertRows:  t.upsertTx,
		Bind: func(ctx context.Context, table, col string, v interface{}) (interface{}, error) {
			return t.bindColumn(ctx, tx, table, col, v)
		},
		TruncateSQL: "DELETE FROM %s",
		SetAppliedSQL: `INSERT INTO ` + changeStateTable + ` (stream, table_name, seq, updated_at)
			VALUES (?, ?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT (stream, table_name) DO UPDATE SET seq = excluded.seq, updated_at = CURRENT_TIMESTAMP`,
		MaxParams: 900,
	}, nil
}

// bindColumn applies bindValue for a key or updated column, so a key that
// arrives as []byte matches the text WriteBatch stored for it.
func (t *Target) bindColumn(ctx context.Context, q queryer, table, col string, v interface{}) (interface{}, error) {
	cols, keepBytes, err := t.writeColumnsQ(ctx, q, table)
	if err != nil {
		return nil, err
	}
	for i, c := range cols {
		if c == col {
			return bindValue(v, keepBytes[i]), nil
		}
	}
	return bindValue(v, false), nil
}
