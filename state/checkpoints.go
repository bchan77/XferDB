package state

import (
	"context"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

type checkpointRow struct {
	MigrationID string    `db:"migration_id"`
	TableName   string    `db:"table_name"`
	BatchID     int       `db:"batch_id"`
	LastPK      string    `db:"last_pk"`
	RowsInBatch int       `db:"rows_in_batch"`
	CreatedAt   time.Time `db:"created_at"`
}

// SaveCheckpoint upserts a checkpoint after a successful batch write.
func (m *MetaDB) SaveCheckpoint(ctx context.Context, cp adapters.Checkpoint) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO checkpoints (migration_id, table_name, batch_id, last_pk, rows_in_batch, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (migration_id, table_name, batch_id) DO UPDATE SET
		    last_pk      = excluded.last_pk,
		    rows_in_batch = excluded.rows_in_batch,
		    created_at   = excluded.created_at`,
		cp.MigrationID, cp.TableName, cp.BatchID, cp.LastPK, cp.RowsInBatch, cp.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// GetLastCheckpoint returns the most recent checkpoint for a table within a migration.
// Returns nil, nil when no checkpoint exists (fresh start).
func (m *MetaDB) GetLastCheckpoint(ctx context.Context, migrationID, tableName string) (*adapters.Checkpoint, error) {
	var row checkpointRow
	err := m.db.GetContext(ctx, &row, `
		SELECT * FROM checkpoints
		WHERE migration_id = ? AND table_name = ?
		ORDER BY batch_id DESC
		LIMIT 1`, migrationID, tableName)
	if err != nil {
		// No checkpoint found — fresh table, start from the beginning.
		if isNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get last checkpoint: %w", err)
	}
	return &adapters.Checkpoint{
		MigrationID: row.MigrationID,
		TableName:   row.TableName,
		BatchID:     row.BatchID,
		LastPK:      row.LastPK,
		RowsInBatch: row.RowsInBatch,
		CreatedAt:   row.CreatedAt,
	}, nil
}

// DeleteCheckpoints removes all checkpoints for a migration (called on completion or cancellation).
func (m *MetaDB) DeleteCheckpoints(ctx context.Context, migrationID string) error {
	_, err := m.db.ExecContext(ctx,
		`DELETE FROM checkpoints WHERE migration_id = ?`, migrationID)
	if err != nil {
		return fmt.Errorf("delete checkpoints: %w", err)
	}
	return nil
}

// isNotFound reports whether err indicates a "no rows" result.
func isNotFound(err error) bool {
	return err != nil && err.Error() == "sql: no rows in result set"
}
