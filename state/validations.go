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
	// ValidationExpectedLossy: the table matches once the plan's narrowing
	// type overrides are applied to the source, and some rows were changed
	// by them. Not a failure; the user chose those conversions.
	ValidationExpectedLossy = "expected_lossy"
)

// ValidationOptions are the caller's choices for one validation run.
type ValidationOptions struct {
	Tables []string `json:"tables,omitempty"`
	// Sample is the percentage of each table an L3 row diff checks; 0
	// means every row.
	Sample int `json:"sample,omitempty"`
	// Mode is "per_table" for a run that validated each table as the
	// migration completed it; empty for one started after the migration.
	Mode string `json:"mode,omitempty"`
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

	// L3 row-diff totals: rows only in the source, rows only in the target,
	// and rows on both sides whose values differ. Sample is the percentage
	// of the table the diff checked (0: every row). Diffs holds the first
	// examples, saved separately through SaveValidationDiffs.
	DiffMissing  int64            `db:"diff_missing" json:"diff_missing"`
	DiffExtra    int64            `db:"diff_extra" json:"diff_extra"`
	DiffMismatch int64            `db:"diff_mismatch" json:"diff_mismatch"`
	LossyRows    int64            `db:"lossy_rows" json:"lossy_rows"` // rows a plan conversion changed, as expected
	Sample       int              `db:"sample" json:"sample,omitempty"`
	Diffs        []ValidationDiff `db:"-" json:"-"`
}

// Kinds of row difference an L3 diff records.
const (
	DiffMissingInTarget = "missing_in_target"
	DiffExtraInTarget   = "extra_in_target"
	DiffValueMismatch   = "value_mismatch"
	// DiffLossyByPlan: a row whose values the plan's type overrides changed,
	// matching what the target holds. Shown for information, not a defect.
	DiffLossyByPlan = "lossy_by_plan"
)

// ValidationDiff is one example row an L3 diff found. Key, Source and Target
// map column names to canonical values (nil for NULL); Source is empty for a
// row only in the target and Target for a row only in the source. Columns
// lists the columns whose values differ in a value_mismatch.
type ValidationDiff struct {
	Kind    string             `json:"kind"`
	Key     map[string]*string `json:"key"`
	Columns []string           `json:"columns,omitempty"`
	Source  map[string]*string `json:"source,omitempty"`
	Target  map[string]*string `json:"target,omitempty"`
}

type validationDiffRow struct {
	Kind    string `db:"kind"`
	Key     string `db:"key"`
	Columns string `db:"columns"`
	Source  string `db:"source_row"`
	Target  string `db:"target_row"`
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
			 source_hash, target_hash, rows_checked, level_run, level_note, detail,
			 diff_missing, diff_extra, diff_mismatch, sample, lossy_rows)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (validation_id, table_name) DO UPDATE SET
			result       = excluded.result,
			source_rows  = excluded.source_rows,
			target_rows  = excluded.target_rows,
			source_hash  = excluded.source_hash,
			target_hash  = excluded.target_hash,
			rows_checked = excluded.rows_checked,
			level_run    = excluded.level_run,
			level_note   = excluded.level_note,
			detail       = excluded.detail,
			diff_missing  = excluded.diff_missing,
			diff_extra    = excluded.diff_extra,
			diff_mismatch = excluded.diff_mismatch,
			sample        = excluded.sample,
			lossy_rows    = excluded.lossy_rows`,
		t.ValidationID, t.TableName, t.Result, t.SourceRows, t.TargetRows,
		t.SourceHash, t.TargetHash, t.RowsChecked, t.LevelRun, t.LevelNote, t.Detail,
		t.DiffMissing, t.DiffExtra, t.DiffMismatch, t.Sample, t.LossyRows,
	)
	if err != nil {
		return fmt.Errorf("upsert validation table %s: %w", t.TableName, err)
	}
	return nil
}

// SaveValidationDiffs replaces the example diffs recorded for one table of a
// validation run.
func (m *MetaDB) SaveValidationDiffs(ctx context.Context, validationID, table string, diffs []ValidationDiff) error {
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("save validation diffs: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM validation_diffs WHERE validation_id = ? AND table_name = ?`, validationID, table); err != nil {
		return fmt.Errorf("save validation diffs: %w", err)
	}
	for i, d := range diffs {
		key, _ := json.Marshal(d.Key)
		cols, _ := json.Marshal(d.Columns)
		var src, tgt []byte
		if d.Source != nil {
			src, _ = json.Marshal(d.Source)
		}
		if d.Target != nil {
			tgt, _ = json.Marshal(d.Target)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO validation_diffs
				(validation_id, table_name, seq, kind, key, columns, source_row, target_row)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			validationID, table, i, d.Kind, string(key), string(cols), string(src), string(tgt)); err != nil {
			return fmt.Errorf("save validation diffs: %w", err)
		}
	}
	return tx.Commit()
}

// GetValidationDiffs returns the example diffs recorded for one table of a
// validation run, in the order they were found.
func (m *MetaDB) GetValidationDiffs(ctx context.Context, validationID, table string) ([]ValidationDiff, error) {
	var rows []validationDiffRow
	if err := m.db.SelectContext(ctx, &rows, `
		SELECT kind, key, columns, source_row, target_row FROM validation_diffs
		WHERE validation_id = ? AND table_name = ? ORDER BY seq`, validationID, table); err != nil {
		return nil, fmt.Errorf("get validation diffs: %w", err)
	}
	out := make([]ValidationDiff, 0, len(rows))
	for _, r := range rows {
		d := ValidationDiff{Kind: r.Kind}
		// All four were written by SaveValidationDiffs.
		json.Unmarshal([]byte(r.Key), &d.Key)         //nolint:errcheck
		json.Unmarshal([]byte(r.Columns), &d.Columns) //nolint:errcheck
		if r.Source != "" {
			json.Unmarshal([]byte(r.Source), &d.Source) //nolint:errcheck
		}
		if r.Target != "" {
			json.Unmarshal([]byte(r.Target), &d.Target) //nolint:errcheck
		}
		out = append(out, d)
	}
	return out, nil
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

// TableValidation is the latest validation outcome for one table of a
// project, across all of its migrations.
type TableValidation struct {
	TableName    string    `db:"table_name" json:"table_name"`
	Result       string    `db:"result" json:"result"`
	Detail       string    `db:"detail" json:"detail,omitempty"`
	LevelRun     string    `db:"level_run" json:"level_run,omitempty"`
	ValidationID string    `db:"validation_id" json:"validation_id"`
	MigrationID  string    `db:"migration_id" json:"migration_id"`
	CheckedAt    time.Time `db:"created_at" json:"checked_at"`
}

// LatestTableValidations returns, per table, the most recent pass, fail or
// expected_lossy recorded by any validation of the project's migrations.
// Skipped entries are ignored: a table a run did not check keeps its earlier
// result.
func (m *MetaDB) LatestTableValidations(ctx context.Context, projectID string) ([]TableValidation, error) {
	var rows []TableValidation
	if err := m.db.SelectContext(ctx, &rows, `
		SELECT vt.table_name, vt.result, vt.detail, vt.level_run,
		       v.id AS validation_id, v.migration_id, v.created_at
		FROM validation_tables vt
		JOIN validations v ON v.id = vt.validation_id
		JOIN migrations  m ON m.id = v.migration_id
		WHERE m.project_id = ? AND vt.result IN (?, ?, ?)
		ORDER BY v.created_at DESC`,
		projectID, ValidationPass, ValidationFail, ValidationExpectedLossy); err != nil {
		return nil, fmt.Errorf("latest table validations: %w", err)
	}
	seen := make(map[string]bool, len(rows))
	out := make([]TableValidation, 0, len(rows))
	for _, r := range rows {
		if !seen[r.TableName] {
			seen[r.TableName] = true
			out = append(out, r)
		}
	}
	return out, nil
}

// ActiveValidation returns the project's pending or running validation, or
// nil when there is none.
func (m *MetaDB) ActiveValidation(ctx context.Context, projectID string) (*Validation, error) {
	var rows []validationRow
	if err := m.db.SelectContext(ctx, &rows, `
		SELECT v.* FROM validations v
		JOIN migrations m ON m.id = v.migration_id
		WHERE m.project_id = ? AND v.status IN (?, ?)
		ORDER BY v.created_at DESC LIMIT 1`,
		projectID, string(adapters.StatusPending), string(adapters.StatusInProgress)); err != nil {
		return nil, fmt.Errorf("active validation: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0].toValidation(), nil
}
