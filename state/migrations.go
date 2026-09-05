package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

type migrationRow struct {
	ID          string       `db:"id"`
	ProjectID   string       `db:"project_id"`
	Status      string       `db:"status"`
	CreatedAt   time.Time    `db:"created_at"`
	StartedAt   sql.NullTime `db:"started_at"`
	CompletedAt sql.NullTime `db:"completed_at"`
	Error       string       `db:"error"`
	SchemaPlan  string       `db:"schema_plan"`
}

func (r *migrationRow) toMigration() *adapters.Migration {
	m := &adapters.Migration{
		ID:        r.ID,
		ProjectID: r.ProjectID,
		Status:    adapters.MigrationStatus(r.Status),
		CreatedAt: r.CreatedAt,
		Error:     r.Error,
	}
	if r.StartedAt.Valid {
		t := r.StartedAt.Time
		m.StartedAt = &t
	}
	if r.CompletedAt.Valid {
		t := r.CompletedAt.Time
		m.CompletedAt = &t
	}
	return m
}

type tableProgressRow struct {
	MigrationID     string       `db:"migration_id"`
	TableName       string       `db:"table_name"`
	Status          string       `db:"status"`
	RowsTotal       int64        `db:"rows_total"`
	RowsTransferred int64        `db:"rows_transferred"`
	StartedAt       sql.NullTime `db:"started_at"`
	CompletedAt     sql.NullTime `db:"completed_at"`
}

func (r *tableProgressRow) toTableProgress() adapters.TableProgress {
	tp := adapters.TableProgress{
		TableName:       r.TableName,
		Status:          adapters.MigrationStatus(r.Status),
		RowsTotal:       r.RowsTotal,
		RowsTransferred: r.RowsTransferred,
	}
	if r.StartedAt.Valid {
		t := r.StartedAt.Time
		tp.StartedAt = &t
	}
	if r.CompletedAt.Valid {
		t := r.CompletedAt.Time
		tp.CompletedAt = &t
	}
	return tp
}

// CreateMigration inserts a new migration record.
func (m *MetaDB) CreateMigration(ctx context.Context, mg *adapters.Migration) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO migrations (id, project_id, status, created_at)
		VALUES (?, ?, ?, ?)`,
		mg.ID, mg.ProjectID, string(mg.Status), mg.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create migration: %w", err)
	}
	return nil
}

// GetMigration returns a migration by ID.
func (m *MetaDB) GetMigration(ctx context.Context, id string) (*adapters.Migration, error) {
	var row migrationRow
	if err := m.db.GetContext(ctx, &row, `SELECT * FROM migrations WHERE id = ?`, id); err != nil {
		return nil, fmt.Errorf("get migration %s: %w", id, err)
	}
	return row.toMigration(), nil
}

// ListMigrations returns all migrations for a project, newest first.
func (m *MetaDB) ListMigrations(ctx context.Context, projectID string) ([]adapters.Migration, error) {
	var rows []migrationRow
	if err := m.db.SelectContext(ctx, &rows,
		`SELECT * FROM migrations WHERE project_id = ? ORDER BY created_at DESC`, projectID); err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}
	migs := make([]adapters.Migration, 0, len(rows))
	for _, r := range rows {
		migs = append(migs, *r.toMigration())
	}
	return migs, nil
}

// MarkInterruptedMigrations marks any pending or in_progress migrations as failed.
// Called on server startup to clean up migrations that were running when the server last stopped.
func (m *MetaDB) MarkInterruptedMigrations(ctx context.Context) (int, error) {
	res, err := m.db.ExecContext(ctx,
		`UPDATE migrations SET status = ?, completed_at = ?, error = ?
		 WHERE status IN (?, ?)`,
		string(adapters.StatusFailed), time.Now(),
		"interrupted by server restart",
		string(adapters.StatusPending), string(adapters.StatusInProgress),
	)
	if err != nil {
		return 0, fmt.Errorf("mark interrupted migrations: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// SetMigrationStatus updates the status and optional timestamps of a migration.
func (m *MetaDB) SetMigrationStatus(ctx context.Context, id string, status adapters.MigrationStatus) error {
	var err error
	switch status {
	case adapters.StatusInProgress:
		_, err = m.db.ExecContext(ctx,
			`UPDATE migrations SET status = ?, started_at = ? WHERE id = ?`,
			string(status), time.Now(), id)
	case adapters.StatusCompleted, adapters.StatusFailed:
		_, err = m.db.ExecContext(ctx,
			`UPDATE migrations SET status = ?, completed_at = ? WHERE id = ?`,
			string(status), time.Now(), id)
	default:
		_, err = m.db.ExecContext(ctx,
			`UPDATE migrations SET status = ? WHERE id = ?`, string(status), id)
	}
	if err != nil {
		return fmt.Errorf("set migration status: %w", err)
	}
	return nil
}

// SetMigrationError records an error message and marks the migration failed.
func (m *MetaDB) SetMigrationError(ctx context.Context, id string, migErr error) error {
	_, err := m.db.ExecContext(ctx,
		`UPDATE migrations SET status = ?, completed_at = ?, error = ? WHERE id = ?`,
		string(adapters.StatusFailed), time.Now(), migErr.Error(), id,
	)
	if err != nil {
		return fmt.Errorf("set migration error: %w", err)
	}
	return nil
}

// UpsertTableProgress inserts or updates progress for a single table in a migration.
func (m *MetaDB) UpsertTableProgress(ctx context.Context, migrationID string, tp adapters.TableProgress) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO migration_tables
		    (migration_id, table_name, status, rows_total, rows_transferred, started_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (migration_id, table_name) DO UPDATE SET
		    status           = excluded.status,
		    rows_total       = excluded.rows_total,
		    rows_transferred = excluded.rows_transferred,
		    started_at       = COALESCE(migration_tables.started_at, excluded.started_at),
		    completed_at     = excluded.completed_at`,
		migrationID, tp.TableName, string(tp.Status),
		tp.RowsTotal, tp.RowsTransferred, tp.StartedAt, tp.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert table progress: %w", err)
	}
	return nil
}

// GetTableProgress returns progress for all tables in a migration.
func (m *MetaDB) GetTableProgress(ctx context.Context, migrationID string) ([]adapters.TableProgress, error) {
	var rows []tableProgressRow
	if err := m.db.SelectContext(ctx, &rows,
		`SELECT * FROM migration_tables WHERE migration_id = ? ORDER BY table_name`, migrationID); err != nil {
		return nil, fmt.Errorf("get table progress: %w", err)
	}
	result := make([]adapters.TableProgress, 0, len(rows))
	for _, r := range rows {
		result = append(result, r.toTableProgress())
	}
	return result, nil
}

// DeleteMigration removes a migration and all its associated data.
func (m *MetaDB) DeleteMigration(ctx context.Context, id string) error {
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete migration begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM checkpoints WHERE migration_id = ?`, id); err != nil {
		return fmt.Errorf("delete checkpoints: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM migration_tables WHERE migration_id = ?`, id); err != nil {
		return fmt.Errorf("delete migration_tables: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM migrations WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete migration: %w", err)
	}

	return tx.Commit()
}
