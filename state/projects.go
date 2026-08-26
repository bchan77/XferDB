package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

type projectRow struct {
	ID             string    `db:"id"`
	Name           string    `db:"name"`
	Description    string    `db:"description"`
	SourceConfig   string    `db:"source_config"`
	TargetConfig   string    `db:"target_config"`
	TransferConfig string    `db:"transfer_config"`
	CreatedAt      time.Time `db:"created_at"`
}

func (r *projectRow) toProject() (*adapters.Project, error) {
	var src, tgt adapters.ConnectionConfig
	var tr adapters.TransferConfig
	if err := json.Unmarshal([]byte(r.SourceConfig), &src); err != nil {
		return nil, fmt.Errorf("decode source_config: %w", err)
	}
	if err := json.Unmarshal([]byte(r.TargetConfig), &tgt); err != nil {
		return nil, fmt.Errorf("decode target_config: %w", err)
	}
	if err := json.Unmarshal([]byte(r.TransferConfig), &tr); err != nil {
		return nil, fmt.Errorf("decode transfer_config: %w", err)
	}
	return &adapters.Project{
		ID:             r.ID,
		Name:           r.Name,
		Description:    r.Description,
		SourceConfig:   src,
		TargetConfig:   tgt,
		TransferConfig: tr,
		CreatedAt:      r.CreatedAt,
	}, nil
}

// CreateProject persists a new project.
func (m *MetaDB) CreateProject(ctx context.Context, p *adapters.Project) error {
	src, err := json.Marshal(p.SourceConfig)
	if err != nil {
		return fmt.Errorf("encode source_config: %w", err)
	}
	tgt, err := json.Marshal(p.TargetConfig)
	if err != nil {
		return fmt.Errorf("encode target_config: %w", err)
	}
	tr, err := json.Marshal(p.TransferConfig)
	if err != nil {
		return fmt.Errorf("encode transfer_config: %w", err)
	}

	_, err = m.db.ExecContext(ctx, `
		INSERT INTO projects (id, name, description, source_config, target_config, transfer_config, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.ID, p.Name, p.Description, string(src), string(tgt), string(tr), p.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create project: %w", err)
	}
	return nil
}

// GetProject returns a project by ID.
func (m *MetaDB) GetProject(ctx context.Context, id string) (*adapters.Project, error) {
	var row projectRow
	err := m.db.GetContext(ctx, &row, `SELECT * FROM projects WHERE id = ?`, id)
	if err != nil {
		return nil, fmt.Errorf("get project %s: %w", id, err)
	}
	return row.toProject()
}

// GetProjectByName returns a project by its unique name.
func (m *MetaDB) GetProjectByName(ctx context.Context, name string) (*adapters.Project, error) {
	var row projectRow
	err := m.db.GetContext(ctx, &row, `SELECT * FROM projects WHERE name = ?`, name)
	if err != nil {
		return nil, fmt.Errorf("get project %q: %w", name, err)
	}
	return row.toProject()
}

// ListProjects returns all projects ordered by creation time.
func (m *MetaDB) ListProjects(ctx context.Context) ([]adapters.Project, error) {
	var rows []projectRow
	if err := m.db.SelectContext(ctx, &rows, `SELECT * FROM projects ORDER BY created_at`); err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	projects := make([]adapters.Project, 0, len(rows))
	for _, r := range rows {
		p, err := r.toProject()
		if err != nil {
			return nil, err
		}
		projects = append(projects, *p)
	}
	return projects, nil
}

// DeleteProject removes a project and all its migrations.
func (m *MetaDB) DeleteProject(ctx context.Context, id string) error {
	tx, err := m.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete project begin tx: %w", err)
	}
	defer tx.Rollback()

	// Remove checkpoints and migration_tables for all migrations under this project.
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM checkpoints WHERE migration_id IN
		    (SELECT id FROM migrations WHERE project_id = ?)`, id); err != nil {
		return fmt.Errorf("delete checkpoints: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM migration_tables WHERE migration_id IN
		    (SELECT id FROM migrations WHERE project_id = ?)`, id); err != nil {
		return fmt.Errorf("delete migration_tables: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM migrations WHERE project_id = ?`, id); err != nil {
		return fmt.Errorf("delete migrations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM projects WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete project: %w", err)
	}

	return tx.Commit()
}
