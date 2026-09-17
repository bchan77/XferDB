package state

import (
	"context"
	"database/sql"
	"time"
)

// ProjectTableStatus is the per-project, per-table status record that persists
// across migration runs so --status always shows the full picture.
type ProjectTableStatus struct {
	TableName       string
	Status          string // "not_started", "in_progress", "done", "failed", "cancelled"
	RowsTransferred int64
	RowsTotal       int64
	LastMigrationID string
	UpdatedAt       time.Time
}

// RegisterProjectTables ensures every name in tableNames has a row in
// project_tables. Existing rows are left untouched so previous migration
// results are preserved across runs.
func (m *MetaDB) RegisterProjectTables(ctx context.Context, projectID string, tableNames []string) error {
	for _, name := range tableNames {
		_, err := m.db.ExecContext(ctx, `
			INSERT OR IGNORE INTO project_tables (project_id, table_name, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)`,
			projectID, name)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateProjectTableStatus upserts the status for a single table in the project
// registry. Called when a migration starts, completes, or fails a table.
func (m *MetaDB) UpdateProjectTableStatus(ctx context.Context, projectID, tableName, status, migrationID string, rowsTransferred, rowsTotal int64) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO project_tables
		    (project_id, table_name, status, rows_transferred, rows_total, last_migration_id, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT (project_id, table_name) DO UPDATE SET
		    status            = excluded.status,
		    rows_transferred  = excluded.rows_transferred,
		    rows_total        = excluded.rows_total,
		    last_migration_id = excluded.last_migration_id,
		    updated_at        = CURRENT_TIMESTAMP`,
		projectID, tableName, status, rowsTransferred, rowsTotal, migrationID)
	return err
}

// GetProjectTables returns all table records for a project, ordered by name.
func (m *MetaDB) GetProjectTables(ctx context.Context, projectID string) ([]ProjectTableStatus, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT table_name, status, rows_transferred, rows_total,
		       COALESCE(last_migration_id, ''), updated_at
		FROM project_tables
		WHERE project_id = ?
		ORDER BY table_name`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ProjectTableStatus
	for rows.Next() {
		var s ProjectTableStatus
		var updatedAt sql.NullTime
		if err := rows.Scan(&s.TableName, &s.Status, &s.RowsTransferred, &s.RowsTotal,
			&s.LastMigrationID, &updatedAt); err != nil {
			return nil, err
		}
		if updatedAt.Valid {
			s.UpdatedAt = updatedAt.Time
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

// GetTableFinalProgress returns the final row counts for a specific table in a
// migration. Used to populate project_tables after a table transfer completes.
func (m *MetaDB) GetTableFinalProgress(ctx context.Context, migrationID, tableName string) (rowsTransferred, rowsTotal int64, err error) {
	err = m.db.QueryRowContext(ctx,
		`SELECT rows_transferred, rows_total FROM migration_tables
		 WHERE migration_id = ? AND table_name = ?`,
		migrationID, tableName).Scan(&rowsTransferred, &rowsTotal)
	return
}
