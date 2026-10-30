package postgres

import (
	"context"
	"fmt"
	"strconv"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// A live migration's applied positions live in their own schema, so they
// never mix with the migrated tables (listTables leaves it out).
const changeStateSchema = "xferdb_cdc"

// PrepareChangeState implements adapters.ChangeApplier.
func (t *Target) PrepareChangeState(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS `+changeStateSchema+`;
		CREATE TABLE IF NOT EXISTS `+changeStateSchema+`.applied (
			stream     text        NOT NULL,
			table_name text        NOT NULL,
			seq        bigint      NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (stream, table_name)
		)`)
	if err != nil {
		return fmt.Errorf("create %s.applied: %w", changeStateSchema, err)
	}
	return nil
}

// AppliedSeqs implements adapters.ChangeApplier.
func (t *Target) AppliedSeqs(ctx context.Context, stream string) (map[string]uint64, error) {
	return adapters.QueryAppliedSeqs(ctx, t.db,
		`SELECT table_name, seq FROM `+changeStateSchema+`.applied WHERE stream = $1`, stream)
}

// ClearChangeState implements adapters.ChangeApplier.
func (t *Target) ClearChangeState(ctx context.Context, stream string) error {
	var exists bool
	if err := t.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, changeStateSchema+".applied").Scan(&exists); err != nil || !exists {
		return err // never prepared: nothing to clear
	}
	_, err := t.db.ExecContext(ctx, `DELETE FROM `+changeStateSchema+`.applied WHERE stream = $1`, stream)
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
		QuoteTable:  qualify,
		QuoteColumn: quote,
		Placeholder: func(n int) string { return "$" + strconv.Itoa(n) },
		UpsertRows:  t.upsertTx,
		TruncateSQL: "TRUNCATE %s",
		SetAppliedSQL: `INSERT INTO ` + changeStateSchema + `.applied (stream, table_name, seq, updated_at)
			VALUES ($1, $2, $3, now())
			ON CONFLICT (stream, table_name) DO UPDATE SET seq = EXCLUDED.seq, updated_at = now()`,
		MaxParams: 30000,
	}, nil
}
