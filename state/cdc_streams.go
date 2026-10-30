package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CDCStream is a live migration's change stream (#102): what was created on
// the source and the positions the server records at setup and cutover.
// Positions are the source's own text form (a PostgreSQL LSN). Everything
// the capture and apply workers update as they run lives with them, in their
// queue and on the target, not here: the server is this database's only
// writer.
type CDCStream struct {
	MigrationID string    `db:"migration_id" json:"migration_id"`
	SlotName    string    `db:"slot_name" json:"slot_name"`
	Publication string    `db:"publication" json:"publication,omitempty"`
	StartPos    string    `db:"start_pos" json:"start_pos"`
	CatchupPos  string    `db:"catchup_pos" json:"catchup_pos,omitempty"`
	CutoverPos  string    `db:"cutover_pos" json:"cutover_pos,omitempty"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

// SaveCDCStream inserts or replaces a migration's stream record.
func (m *MetaDB) SaveCDCStream(ctx context.Context, s *CDCStream) error {
	s.UpdatedAt = time.Now()
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO cdc_streams (migration_id, slot_name, publication, start_pos, catchup_pos, cutover_pos, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (migration_id) DO UPDATE SET
		    slot_name   = excluded.slot_name,
		    publication = excluded.publication,
		    start_pos   = excluded.start_pos,
		    catchup_pos = excluded.catchup_pos,
		    cutover_pos = excluded.cutover_pos,
		    updated_at  = excluded.updated_at`,
		s.MigrationID, s.SlotName, s.Publication, s.StartPos, s.CatchupPos, s.CutoverPos, s.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save cdc stream: %w", err)
	}
	return nil
}

// GetCDCStream returns a migration's stream record, or nil if it has none.
func (m *MetaDB) GetCDCStream(ctx context.Context, migrationID string) (*CDCStream, error) {
	var s CDCStream
	err := m.db.GetContext(ctx, &s, `SELECT * FROM cdc_streams WHERE migration_id = ?`, migrationID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cdc stream: %w", err)
	}
	return &s, nil
}

// SetMigrationPhase records where a live migration is.
func (m *MetaDB) SetMigrationPhase(ctx context.Context, id, phase string) error {
	if _, err := m.db.ExecContext(ctx, `UPDATE migrations SET phase = ? WHERE id = ?`, phase, id); err != nil {
		return fmt.Errorf("set migration phase: %w", err)
	}
	return nil
}
