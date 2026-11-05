package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

// listDatabasesTimeout bounds a database listing, so a host that drops
// packets fails the request in seconds instead of hanging the form.
const listDatabasesTimeout = 10 * time.Second

// listDatabasesRequest is the body of POST /api/v1/connections/databases.
// ProjectID and Side are optional: the project settings form never shows the
// stored password, so it sends them to have an empty password filled from the
// project, as UpdateProject does on save.
type listDatabasesRequest struct {
	Config    adapters.ConnectionConfig `json:"config"`
	ProjectID string                    `json:"project_id,omitempty"`
	Side      string                    `json:"side,omitempty"` // "source" or "target"
}

// ListDatabases handles POST /api/v1/connections/databases. It connects with
// the given server settings (the database name may be empty) and returns the
// databases the login can see. A connection failure is a 422 whose error is
// the driver's message plus a hint, for the form to show next to the fields.
func (h *ProjectsHandler) ListDatabases(w http.ResponseWriter, r *http.Request) {
	var body listDatabasesRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	cfg := body.Config
	if !adapters.PostgresFamily(cfg.Type) && cfg.Type != "mysql" {
		writeError(w, http.StatusBadRequest, "listing databases is supported for postgres, yugabyte and mysql only")
		return
	}
	if strings.TrimSpace(cfg.Host) == "" {
		writeError(w, http.StatusBadRequest, "host is required")
		return
	}
	if cfg.Password == "" && body.ProjectID != "" {
		if err := h.fillStoredPassword(r.Context(), &cfg, body.ProjectID, body.Side); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), listDatabasesTimeout)
	defer cancel()
	names, err := listDatabases(ctx, cfg)
	if err != nil {
		h.Log.Info("connection.list_databases", "type", cfg.Type, "host", cfg.Host, "error", err)
		writeError(w, http.StatusUnprocessableEntity, friendlyConnectError(err))
		return
	}
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"databases": names})
}

// fillStoredPassword copies the project's stored password for side into cfg,
// but only when cfg still points at the same server and login: otherwise an
// edited host would receive a password meant for another server.
func (h *ProjectsHandler) fillStoredPassword(ctx context.Context, cfg *adapters.ConnectionConfig, projectID, side string) error {
	p, err := h.DB.GetProject(ctx, projectID)
	if err != nil {
		return errProjectNotFound
	}
	stored := p.SourceConfig
	if side == "target" {
		stored = p.TargetConfig
	}
	if stored.Type == cfg.Type && stored.Host == cfg.Host &&
		effectivePort(stored) == effectivePort(*cfg) && stored.Username == cfg.Username {
		cfg.Password = stored.Password
	}
	return nil
}

var errProjectNotFound = errors.New("project not found")

func effectivePort(c adapters.ConnectionConfig) int {
	if c.Port != 0 {
		return c.Port
	}
	switch c.Type {
	case "mysql":
		return 3306
	case adapters.TypeYugabyte:
		return 5433
	}
	return 5432
}

// onServer runs fn on a connection to cfg's server that doesn't need cfg's
// own database to exist: listing a server's databases, or creating the one
// that is missing. MySQL connects with no database. Postgres must name one
// and would otherwise default to a database named after the user, which
// usually doesn't exist, so it tries cfg's database (when set), then the
// "postgres" maintenance database (YugabyteDB: "yugabyte" first, the
// database it creates for its default user), then "template1", moving on
// only while the error is that the database doesn't exist.
func onServer(ctx context.Context, cfg adapters.ConnectionConfig, fn func(adapters.ConnectionConfig) error) error {
	cfg.DSN = ""
	var candidates []string
	switch {
	case cfg.Type == "mysql":
		candidates = []string{""}
	default:
		candidates = []string{"postgres", "template1"}
		if cfg.Type == adapters.TypeYugabyte {
			candidates = append([]string{"yugabyte"}, candidates...)
		}
		if cfg.Database != "" {
			candidates = append([]string{cfg.Database}, candidates...)
		}
	}
	var err error
	for _, db := range candidates {
		cfg.Database = db
		if err = fn(cfg); err == nil || !isMissingDatabase(err) {
			return err
		}
	}
	return err
}

// isMissingDatabase reports whether err is the server refusing a connection
// because its database doesn't exist: postgres 3D000, mysql 1049.
func isMissingDatabase(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "3D000") || strings.Contains(msg, "Error 1049")
}

func listDatabases(ctx context.Context, cfg adapters.ConnectionConfig) ([]string, error) {
	var names []string
	err := onServer(ctx, cfg, func(cfg adapters.ConnectionConfig) error {
		src, err := registry.NewSource(cfg.Type)
		if err != nil {
			return err
		}
		defer src.Close()
		if err := src.Connect(ctx, cfg); err != nil {
			return err
		}
		lister, ok := src.(adapters.DatabaseLister)
		if !ok {
			return fmt.Errorf("%s does not support listing databases", cfg.Type)
		}
		names, err = lister.ListDatabases(ctx)
		return err
	})
	return names, err
}

// CreateTargetDatabase handles POST /api/v1/projects/{id}/target-database:
// it creates the database the project's target config names, for a target
// whose preflight failed because that database doesn't exist yet. It
// connects to the server without that database (see onServer). An existing
// database is not an error, so a retried click is harmless.
func (h *ProjectsHandler) CreateTargetDatabase(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	cfg := p.TargetConfig
	if !adapters.PostgresFamily(cfg.Type) && cfg.Type != "mysql" {
		writeError(w, http.StatusBadRequest, "creating the target database is supported for postgres, yugabyte and mysql only")
		return
	}
	if cfg.Database == "" {
		writeError(w, http.StatusBadRequest, "the target connection names no database")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), listDatabasesTimeout)
	defer cancel()
	if err := createDatabase(ctx, cfg); err != nil {
		h.Log.Info("project.create_target_database", "project_id", id, "database", cfg.Database, "error", err)
		writeError(w, http.StatusUnprocessableEntity, friendlyCreateDatabaseError(err))
		return
	}
	h.Log.Info("project.create_target_database", "project_id", id, "database", cfg.Database)
	writeJSON(w, http.StatusCreated, map[string]string{"database": cfg.Database})
}

// CreateDatabase handles POST /api/v1/connections/create-database: the
// connection form's counterpart of CreateTargetDatabase, taking the fields
// as typed (same body as ListDatabases, so the settings form can have the
// stored password filled in) and creating config.database on that server.
func (h *ProjectsHandler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	var body listDatabasesRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	cfg := body.Config
	if !adapters.PostgresFamily(cfg.Type) && cfg.Type != "mysql" {
		writeError(w, http.StatusBadRequest, "creating a database is supported for postgres, yugabyte and mysql only")
		return
	}
	if strings.TrimSpace(cfg.Host) == "" || strings.TrimSpace(cfg.Database) == "" {
		writeError(w, http.StatusBadRequest, "host and database are required")
		return
	}
	if cfg.Password == "" && body.ProjectID != "" {
		if err := h.fillStoredPassword(r.Context(), &cfg, body.ProjectID, body.Side); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), listDatabasesTimeout)
	defer cancel()
	if err := createDatabase(ctx, cfg); err != nil {
		h.Log.Info("connection.create_database", "type", cfg.Type, "host", cfg.Host, "database", cfg.Database, "error", err)
		writeError(w, http.StatusUnprocessableEntity, friendlyCreateDatabaseError(err))
		return
	}
	h.Log.Info("connection.create_database", "type", cfg.Type, "host", cfg.Host, "database", cfg.Database)
	writeJSON(w, http.StatusCreated, map[string]string{"database": cfg.Database})
}

// createDatabase creates cfg.Database on cfg's server, connecting without it
// (see onServer). An existing database is not an error.
func createDatabase(ctx context.Context, cfg adapters.ConnectionConfig) error {
	name := cfg.Database
	return onServer(ctx, cfg, func(c adapters.ConnectionConfig) error {
		tgt, err := registry.NewTarget(c.Type)
		if err != nil {
			return err
		}
		defer tgt.Close()
		if err := tgt.Connect(ctx, c); err != nil {
			return err
		}
		creator, ok := tgt.(adapters.DatabaseCreator)
		if !ok {
			return fmt.Errorf("%s does not support creating databases", c.Type)
		}
		return creator.CreateDatabase(ctx, name)
	})
}

func friendlyCreateDatabaseError(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "42501") || strings.Contains(msg, "Error 1044") || strings.Contains(msg, "permission denied") {
		return msg + " — this user may not create databases: ask an administrator to create it, or to let this user create databases"
	}
	return friendlyConnectError(err)
}
