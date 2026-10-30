package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
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

// SetRunConfig saves a live run's effective transfer config.
func (m *MetaDB) SetRunConfig(ctx context.Context, id string, cfg adapters.TransferConfig) error {
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if _, err := m.db.ExecContext(ctx, `UPDATE migrations SET run_config = ? WHERE id = ?`, string(b), id); err != nil {
		return fmt.Errorf("save run config: %w", err)
	}
	return nil
}

// GetRunConfig returns the transfer config SetRunConfig saved, false if none.
func (m *MetaDB) GetRunConfig(ctx context.Context, id string) (adapters.TransferConfig, bool, error) {
	var cfg adapters.TransferConfig
	var text string
	if err := m.db.GetContext(ctx, &text, `SELECT run_config FROM migrations WHERE id = ?`, id); err != nil {
		return cfg, false, fmt.Errorf("get run config: %w", err)
	}
	if text == "" {
		return cfg, false, nil
	}
	if err := json.Unmarshal([]byte(text), &cfg); err != nil {
		return cfg, false, fmt.Errorf("run config of %s: %w", id, err)
	}
	return cfg, true, nil
}

// ActiveLiveMigrations returns the live migrations still running or paused,
// for the server to resume at startup.
func (m *MetaDB) ActiveLiveMigrations(ctx context.Context) ([]*adapters.Migration, error) {
	var rows []migrationRow
	if err := m.db.SelectContext(ctx, &rows,
		`SELECT * FROM migrations WHERE mode = ? AND status IN (?, ?, ?) ORDER BY created_at`,
		adapters.MigrationModeLive, string(adapters.StatusPending), string(adapters.StatusInProgress), string(adapters.StatusPaused)); err != nil {
		return nil, fmt.Errorf("list live migrations: %w", err)
	}
	out := make([]*adapters.Migration, len(rows))
	for i := range rows {
		out[i] = rows[i].toMigration()
	}
	return out, nil
}
