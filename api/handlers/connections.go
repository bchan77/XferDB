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
	if cfg.Type != "postgres" && cfg.Type != "mysql" {
		writeError(w, http.StatusBadRequest, "listing databases is supported for postgres and mysql only")
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
	if c.Type == "mysql" {
		return 3306
	}
	return 5432
}

// listDatabases connects with cfg and lists its server's databases. With no
// database given, postgres would default to one named after the user, which
// usually doesn't exist, so it connects to the "postgres" maintenance
// database, then "template1" if that one has been dropped. MySQL needs no
// database to connect.
func listDatabases(ctx context.Context, cfg adapters.ConnectionConfig) ([]string, error) {
	cfg.DSN = ""
	candidates := []string{cfg.Database}
	if cfg.Type == "postgres" && cfg.Database == "" {
		candidates = []string{"postgres", "template1"}
	}
	var lastErr error
	for _, db := range candidates {
		cfg.Database = db
		names, err := listDatabasesOnce(ctx, cfg)
		if err == nil {
			return names, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "does not exist") {
			break
		}
	}
	return nil, lastErr
}

func listDatabasesOnce(ctx context.Context, cfg adapters.ConnectionConfig) ([]string, error) {
	src, err := registry.NewSource(cfg.Type)
	if err != nil {
		return nil, err
	}
	defer src.Close()
	if err := src.Connect(ctx, cfg); err != nil {
		return nil, err
	}
	lister, ok := src.(adapters.DatabaseLister)
	if !ok {
		return nil, fmt.Errorf("%s does not support listing databases", cfg.Type)
	}
	return lister.ListDatabases(ctx)
}
