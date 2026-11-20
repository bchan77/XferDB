package state

import (
	"context"
	"database/sql"
	"time"
)

// SchemaStep is one index, foreign key or check constraint a migration
// created after copying the rows (post-schema), with its timing.
type SchemaStep struct {
	ID          int64      `json:"id"`
	MigrationID string     `json:"migration_id"`
	Kind        string     `json:"kind"` // index, foreign_key, check
	Table       string     `json:"table"`
	Name        string     `json:"name"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	Status      string     `json:"status"` // running, done, failed
	Attempts    int        `json:"attempts"`
	Error       string     `json:"error,omitempty"`
}

// StartSchemaStep records a step as running and returns its id.
func (m *MetaDB) StartSchemaStep(ctx context.Context, s SchemaStep) (int64, error) {
	res, err := m.db.ExecContext(ctx, `
		INSERT INTO migration_schema_steps (migration_id, kind, table_name, name, started_at, status)
		VALUES (?, ?, ?, ?, ?, 'running')`, s.MigrationID, s.Kind, s.Table, s.Name, s.StartedAt.UTC())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishSchemaStep records a step's end: status done or failed.
func (m *MetaDB) FinishSchemaStep(ctx context.Context, id int64, status string, attempts int, errText string, at time.Time) error {
	_, err := m.db.ExecContext(ctx, `
		UPDATE migration_schema_steps SET status = ?, attempts = ?, error = ?, finished_at = ? WHERE id = ?`,
		status, attempts, errText, at.UTC(), id)
	return err
}

// ListSchemaSteps returns a migration's steps in the order they started.
func (m *MetaDB) ListSchemaSteps(ctx context.Context, migrationID string) ([]SchemaStep, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT id, migration_id, kind, table_name, name, started_at, finished_at, status, attempts, error
		FROM migration_schema_steps WHERE migration_id = ? ORDER BY id`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	steps := []SchemaStep{}
	for rows.Next() {
		var s SchemaStep
		var fin sql.NullTime
		if err := rows.Scan(&s.ID, &s.MigrationID, &s.Kind, &s.Table, &s.Name, &s.StartedAt, &fin, &s.Status, &s.Attempts, &s.Error); err != nil {
			return nil, err
		}
		if fin.Valid {
			t := fin.Time
			s.FinishedAt = &t
		}
		steps = append(steps, s)
	}
	return steps, rows.Err()
}

// MarkSchemaStepsInterrupted closes steps a server restart left running.
func (m *MetaDB) MarkSchemaStepsInterrupted(ctx context.Context, migrationID string, at time.Time) error {
	_, err := m.db.ExecContext(ctx, `
		UPDATE migration_schema_steps SET status = 'failed', error = 'interrupted', finished_at = ?
		WHERE migration_id = ? AND status = 'running'`, at.UTC(), migrationID)
	return err
}
