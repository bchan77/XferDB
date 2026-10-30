package mysql

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// changeStateTable holds a live migration's applied positions. MySQL has no
// schemas within a database, so it sits beside the migrated tables.
const changeStateTable = "xferdb_cdc_applied"

// PrepareChangeState implements adapters.ChangeApplier.
func (t *Target) PrepareChangeState(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS `+changeStateTable+` (
			stream     VARCHAR(64)  NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			seq        BIGINT       NOT NULL,
			updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY (stream, table_name)
		) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin`)
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
		// TRUNCATE commits the transaction in MySQL.
		TruncateSQL: "DELETE FROM %s",
		SetAppliedSQL: `INSERT INTO ` + changeStateTable + ` (stream, table_name, seq) VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE seq = VALUES(seq)`,
		MaxParams: 30000,
	}, nil
}
