package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SchemaPlanRow is one field decision in the schema plan for a MongoDB collection.
// Each row maps one Mongo field (dot-notation) to its target Postgres representation.
type SchemaPlanRow struct {
	ProjectID  string    `db:"project_id"  json:"project_id"`
	Collection string    `db:"collection"  json:"collection"`
	FieldName  string    `db:"field_name"  json:"field_name"`
	PgColumn   string    `db:"pg_column"   json:"pg_column"`
	PgType     string    `db:"pg_type"     json:"pg_type"`
	Strategy   string    `db:"strategy"    json:"strategy"` // direct | as_jsonb | flatten | skip
	IsPK       bool      `db:"is_pk"       json:"is_pk"`
	Nullable   bool      `db:"nullable"    json:"nullable"`
	Overridden bool      `db:"overridden"  json:"overridden"`
	CreatedAt  time.Time `db:"created_at"  json:"created_at"`
}

// SavePlan upserts a slice of SchemaPlanRow for a project+collection.
// Rows with Overridden=true that already exist in the DB are left untouched —
// re-analyze refreshes inferred rows only.
func (m *MetaDB) SavePlan(ctx context.Context, rows []SchemaPlanRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("save plan begin tx: %w", err)
	}
	defer tx.Rollback()

	const q = `
		INSERT INTO schema_plans
		    (project_id, collection, field_name, pg_column, pg_type, strategy, is_pk, nullable, overridden, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (project_id, collection, field_name) DO UPDATE SET
		    pg_column  = CASE WHEN schema_plans.overridden = 1 THEN schema_plans.pg_column  ELSE excluded.pg_column  END,
		    pg_type    = CASE WHEN schema_plans.overridden = 1 THEN schema_plans.pg_type    ELSE excluded.pg_type    END,
		    strategy   = CASE WHEN schema_plans.overridden = 1 THEN schema_plans.strategy   ELSE excluded.strategy   END,
		    is_pk      = CASE WHEN schema_plans.overridden = 1 THEN schema_plans.is_pk      ELSE excluded.is_pk      END,
		    nullable   = CASE WHEN schema_plans.overridden = 1 THEN schema_plans.nullable   ELSE excluded.nullable   END,
		    overridden = schema_plans.overridden`

	for _, r := range rows {
		if r.CreatedAt.IsZero() {
			r.CreatedAt = time.Now()
		}
		_, err := tx.ExecContext(ctx, q,
			r.ProjectID, r.Collection, r.FieldName,
			r.PgColumn, r.PgType, r.Strategy,
			boolToInt(r.IsPK), boolToInt(r.Nullable), boolToInt(r.Overridden),
			r.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("save plan row %s.%s: %w", r.Collection, r.FieldName, err)
		}
	}
	return tx.Commit()
}

// OverridePlan updates a single field decision and marks it overridden=true so
// future re-analyzes do not clobber it.
func (m *MetaDB) OverridePlan(ctx context.Context, projectID, collection, fieldName string, updates SchemaPlanRow) error {
	_, err := m.db.ExecContext(ctx, `
		UPDATE schema_plans
		SET pg_column = ?, pg_type = ?, strategy = ?, is_pk = ?, nullable = ?, overridden = 1
		WHERE project_id = ? AND collection = ? AND field_name = ?`,
		updates.PgColumn, updates.PgType, updates.Strategy,
		boolToInt(updates.IsPK), boolToInt(updates.Nullable),
		projectID, collection, fieldName,
	)
	if err != nil {
		return fmt.Errorf("override plan %s.%s: %w", collection, fieldName, err)
	}
	return nil
}

// schemaPlanDBRow is the raw SQLite scan target. Booleans are stored as INTEGER
// in SQLite and sqlx does not auto-convert INTEGER → bool, so we scan into int
// and convert explicitly.
type schemaPlanDBRow struct {
	ProjectID  string    `db:"project_id"`
	Collection string    `db:"collection"`
	FieldName  string    `db:"field_name"`
	PgColumn   string    `db:"pg_column"`
	PgType     string    `db:"pg_type"`
	Strategy   string    `db:"strategy"`
	IsPK       int       `db:"is_pk"`
	Nullable   int       `db:"nullable"`
	Overridden int       `db:"overridden"`
	CreatedAt  time.Time `db:"created_at"`
}

func (r schemaPlanDBRow) toRow() SchemaPlanRow {
	return SchemaPlanRow{
		ProjectID:  r.ProjectID,
		Collection: r.Collection,
		FieldName:  r.FieldName,
		PgColumn:   r.PgColumn,
		PgType:     r.PgType,
		Strategy:   r.Strategy,
		IsPK:       r.IsPK != 0,
		Nullable:   r.Nullable != 0,
		Overridden: r.Overridden != 0,
		CreatedAt:  r.CreatedAt,
	}
}

// GetPlan returns all schema plan rows for a project+collection, ordered by field_name.
func (m *MetaDB) GetPlan(ctx context.Context, projectID, collection string) ([]SchemaPlanRow, error) {
	var rows []schemaPlanDBRow
	if err := m.db.SelectContext(ctx, &rows, `
		SELECT project_id, collection, field_name, pg_column, pg_type, strategy,
		       is_pk, nullable, overridden, created_at
		FROM schema_plans
		WHERE project_id = ? AND collection = ?
		ORDER BY field_name`, projectID, collection); err != nil {
		return nil, fmt.Errorf("get plan %s/%s: %w", projectID, collection, err)
	}
	out := make([]SchemaPlanRow, len(rows))
	for i, r := range rows {
		out[i] = r.toRow()
	}
	return out, nil
}

// ListPlanCollections returns the distinct collection names that have a saved plan
// for the given project.
func (m *MetaDB) ListPlanCollections(ctx context.Context, projectID string) ([]string, error) {
	var collections []string
	if err := m.db.SelectContext(ctx, &collections, `
		SELECT DISTINCT collection FROM schema_plans
		WHERE project_id = ?
		ORDER BY collection`, projectID); err != nil {
		return nil, fmt.Errorf("list plan collections: %w", err)
	}
	return collections, nil
}

// DeletePlan removes all schema plan rows for a project+collection (wipes before re-analyze).
func (m *MetaDB) DeletePlan(ctx context.Context, projectID, collection string) error {
	_, err := m.db.ExecContext(ctx,
		`DELETE FROM schema_plans WHERE project_id = ? AND collection = ?`,
		projectID, collection)
	if err != nil {
		return fmt.Errorf("delete plan %s/%s: %w", projectID, collection, err)
	}
	return nil
}

// DeleteAllPlans removes every schema plan row for a project.
func (m *MetaDB) DeleteAllPlans(ctx context.Context, projectID string) error {
	_, err := m.db.ExecContext(ctx,
		`DELETE FROM schema_plans WHERE project_id = ?`, projectID)
	if err != nil {
		return fmt.Errorf("delete all plans %s: %w", projectID, err)
	}
	return nil
}

// SnapshotPlan serialises the current schema_plans for a project into the
// migrations.schema_plan JSON column. The engine reads this frozen copy during
// the migration so that user overrides mid-run cannot change field conversion.
func (m *MetaDB) SnapshotPlan(ctx context.Context, migrationID, projectID string) error {
	collections, err := m.ListPlanCollections(ctx, projectID)
	if err != nil {
		return err
	}

	all := make([]SchemaPlanRow, 0)
	for _, col := range collections {
		rows, err := m.GetPlan(ctx, projectID, col)
		if err != nil {
			return err
		}
		all = append(all, rows...)
	}

	b, err := json.Marshal(all)
	if err != nil {
		return fmt.Errorf("marshal schema plan snapshot: %w", err)
	}

	_, err = m.db.ExecContext(ctx,
		`UPDATE migrations SET schema_plan = ? WHERE id = ?`, string(b), migrationID)
	if err != nil {
		return fmt.Errorf("snapshot plan for migration %s: %w", migrationID, err)
	}
	return nil
}

// GetMigrationPlan returns the frozen schema plan stored on a migration record.
// Returns nil when the migration has no snapshot (relational-only migration).
func (m *MetaDB) GetMigrationPlan(ctx context.Context, migrationID string) ([]SchemaPlanRow, error) {
	var raw string
	if err := m.db.QueryRowContext(ctx,
		`SELECT schema_plan FROM migrations WHERE id = ?`, migrationID,
	).Scan(&raw); err != nil {
		return nil, fmt.Errorf("get migration plan %s: %w", migrationID, err)
	}
	if raw == "" {
		return nil, nil
	}
	var rows []SchemaPlanRow
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("unmarshal migration plan %s: %w", migrationID, err)
	}
	return rows, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
