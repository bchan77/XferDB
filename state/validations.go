package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Validation results recorded per table.
const (
	ValidationPending = "pending"
	ValidationPass    = "pass"
	ValidationFail    = "fail"
	ValidationSkipped = "skipped"
)

// ValidationOptions are the caller's choices for one validation run.
type ValidationOptions struct {
	Tables []string `json:"tables,omitempty"`
}

// Validation is one run comparing a migration's source and target. Status
// uses the migration lifecycle values (pending, in_progress, completed,
// failed, cancelled); a completed run can still hold failed tables.
type Validation struct {
	ID          string                   `json:"id"`
	MigrationID string                   `json:"migration_id"`
	Level       string                   `json:"level"`
	Status      adapters.MigrationStatus `json:"status"`
	Options     ValidationOptions        `json:"options"`
	CreatedAt   time.Time                `json:"created_at"`
	StartedAt   *time.Time               `json:"started_at,omitempty"`
	CompletedAt *time.Time               `json:"completed_at,omitempty"`
	Error       string                   `json:"error,omitempty"`
}

// ValidationTable is the outcome for one table in a validation run.
type ValidationTable struct {
	ValidationID string `db:"validation_id" json:"-"`
	TableName    string `db:"table_name" json:"table_name"`
	Result       string `db:"result" json:"result"`
	SourceRows   *int64 `db:"source_rows" json:"source_rows,omitempty"`
	TargetRows   *int64 `db:"target_rows" json:"target_rows,omitempty"`
	SourceHash   string `db:"source_hash" json:"source_hash,omitempty"`
	TargetHash   string `db:"target_hash" json:"target_hash,omitempty"`
	RowsChecked  int64  `db:"rows_checked" json:"rows_checked"`
	LevelRun     string `db:"level_run" json:"level_run,omitempty"`
	LevelNote    string `db:"level_note" json:"level_note,omitempty"`
	Detail       string `db:"detail" json:"detail,omitempty"` // why a table failed or was skipped
}

type validationRow struct {
	ID          string       `db:"id"`
	MigrationID string       `db:"migration_id"`
	Level       string       `db:"level"`
	Status      string       `db:"status"`
	Options     string       `db:"options"`
	CreatedAt   time.Time    `db:"created_at"`
	StartedAt   sql.NullTime `db:"started_at"`
	CompletedAt sql.NullTime `db:"completed_at"`
	Error       string       `db:"error"`
}

func (r *validationRow) toValidation() *Validation {
	v := &Validation{
		ID:          r.ID,
		MigrationID: r.MigrationID,
		Level:       r.Level,
		Status:      adapters.MigrationStatus(r.Status),
		CreatedAt:   r.CreatedAt,
		Error:       r.Error,
	}
	json.Unmarshal([]byte(r.Options), &v.Options) //nolint:errcheck // written by CreateValidation
	if r.StartedAt.Valid {
		t := r.StartedAt.Time
		v.StartedAt = &t
	}
	if r.CompletedAt.Valid {
		t := r.CompletedAt.Time
		v.CompletedAt = &t
	}
	return v
}

// CreateValidation inserts a new validation run.
func (m *MetaDB) CreateValidation(ctx context.Context, v *Validation) error {
	opts, err := json.Marshal(v.Options)
	if err != nil {
		return fmt.Errorf("marshal validation options: %w", err)
	}
	_, err = m.db.ExecContext(ctx, `
		INSERT INTO validations (id, migration_id, level, status, options, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		v.ID, v.MigrationID, v.Level, string(v.Status), string(opts), v.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create validation: %w", err)
	}
	return nil
}

// GetValidation returns a validation run by ID.
func (m *MetaDB) GetValidation(ctx context.Context, id string) (*Validation, error) {
	var row validationRow
	if err := m.db.GetContext(ctx, &row, `SELECT * FROM validations WHERE id = ?`, id); err != nil {
		return nil, fmt.Errorf("get validation %s: %w", id, err)
	}
	return row.toValidation(), nil
}

// ListValidations returns all validation runs for a migration, newest first.
func (m *MetaDB) ListValidations(ctx context.Context, migrationID string) ([]Validation, error) {
	var rows []validationRow
	if err := m.db.SelectContext(ctx, &rows,
		`SELECT * FROM validations WHERE migration_id = ? ORDER BY created_at DESC`, migrationID); err != nil {
		return nil, fmt.Errorf("list validations: %w", err)
	}
	out := make([]Validation, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r.toValidation())
	}
	return out, nil
}

// SetValidationStatus moves a validation run to status, stamping started_at
// or completed_at as appropriate. errMsg is recorded for failed runs.
func (m *MetaDB) SetValidationStatus(ctx context.Context, id string, status adapters.MigrationStatus, errMsg string) error {
	var err error
	switch status {
	case adapters.StatusInProgress:
		_, err = m.db.ExecContext(ctx,
			`UPDATE validations SET status = ?, started_at = ? WHERE id = ?`,
			string(status), time.Now(), id)
	case adapters.StatusCompleted, adapters.StatusFailed, adapters.StatusCancelled:
		_, err = m.db.ExecContext(ctx,
			`UPDATE validations SET status = ?, completed_at = ?, error = ? WHERE id = ?`,
			string(status), time.Now(), errMsg, id)
	default:
		_, err = m.db.ExecContext(ctx,
			`UPDATE validations SET status = ? WHERE id = ?`, string(status), id)
	}
	if err != nil {
		return fmt.Errorf("set validation status: %w", err)
	}
	return nil
}

// MarkInterruptedValidations fails any validation left pending or in progress
// by a server restart.
func (m *MetaDB) MarkInterruptedValidations(ctx context.Context) (int, error) {
	res, err := m.db.ExecContext(ctx,
		`UPDATE validations SET status = ?, completed_at = ?, error = ?
		 WHERE status IN (?, ?)`,
		string(adapters.StatusFailed), time.Now(), "interrupted by server restart",
		string(adapters.StatusPending), string(adapters.StatusInProgress),
	)
	if err != nil {
		return 0, fmt.Errorf("mark interrupted validations: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// UpsertValidationTable records the outcome for one table.
func (m *MetaDB) UpsertValidationTable(ctx context.Context, t ValidationTable) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO validation_tables
			(validation_id, table_name, result, source_rows, target_rows,
			 source_hash, target_hash, rows_checked, level_run, level_note, detail)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (validation_id, table_name) DO UPDATE SET
			result       = excluded.result,
			source_rows  = excluded.source_rows,
			target_rows  = excluded.target_rows,
			source_hash  = excluded.source_hash,
			target_hash  = excluded.target_hash,
			rows_checked = excluded.rows_checked,
			level_run    = excluded.level_run,
			level_note   = excluded.level_note,
			detail       = excluded.detail`,
		t.ValidationID, t.TableName, t.Result, t.SourceRows, t.TargetRows,
		t.SourceHash, t.TargetHash, t.RowsChecked, t.LevelRun, t.LevelNote, t.Detail,
	)
	if err != nil {
		return fmt.Errorf("upsert validation table %s: %w", t.TableName, err)
	}
	return nil
}

// GetValidationTables returns the per-table outcomes of a validation run.
func (m *MetaDB) GetValidationTables(ctx context.Context, validationID string) ([]ValidationTable, error) {
	var rows []ValidationTable
	if err := m.db.SelectContext(ctx, &rows,
		`SELECT * FROM validation_tables WHERE validation_id = ? ORDER BY table_name`, validationID); err != nil {
		return nil, fmt.Errorf("get validation tables: %w", err)
	}
	return rows, nil
}
