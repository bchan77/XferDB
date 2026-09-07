package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/analyzer"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ProjectsHandler holds shared dependencies for project-related endpoints.
type ProjectsHandler struct {
	DB  *state.MetaDB
	Log *slog.Logger
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// CreateProject handles POST /api/v1/projects.
func (h *ProjectsHandler) CreateProject(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name           string                  `json:"name"`
		Description    string                  `json:"description"`
		SourceConfig   adapters.ConnectionConfig `json:"source_config"`
		TargetConfig   adapters.ConnectionConfig `json:"target_config"`
		TransferConfig adapters.TransferConfig   `json:"transfer_config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	p := &adapters.Project{
		ID:             uuid.New().String(),
		Name:           body.Name,
		Description:    body.Description,
		SourceConfig:   body.SourceConfig,
		TargetConfig:   body.TargetConfig,
		TransferConfig: body.TransferConfig,
		CreatedAt:      time.Now(),
	}
	if err := h.DB.CreateProject(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.Log.Info("project.created",
		"project_id", p.ID,
		"name", p.Name,
		"source_type", p.SourceConfig.Type,
		"target_type", p.TargetConfig.Type,
	)
	writeJSON(w, http.StatusCreated, p)
}

// ListProjects handles GET /api/v1/projects.
func (h *ProjectsHandler) ListProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := h.DB.ListProjects(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, projects)
}

// GetProject handles GET /api/v1/projects/{id}.
func (h *ProjectsHandler) GetProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// DeleteProject handles DELETE /api/v1/projects/{id}.
func (h *ProjectsHandler) DeleteProject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Fetch name before deletion for the log entry.
	p, _ := h.DB.GetProject(r.Context(), id)
	if err := h.DB.DeleteProject(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	name := id
	if p != nil {
		name = p.Name
	}
	h.Log.Info("project.deleted", "project_id", id, "name", name)
	w.WriteHeader(http.StatusNoContent)
}

// Preflight handles POST /api/v1/projects/{id}/preflight.
func (h *ProjectsHandler) Preflight(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	ctx := r.Context()
	type checkResult struct {
		Source     *adapters.PermissionCheck `json:"source"`
		Target     *adapters.PermissionCheck `json:"target"`
		SourceInfo *adapters.DatabaseInfo    `json:"source_info,omitempty"`
		TargetInfo *adapters.DatabaseInfo    `json:"target_info,omitempty"`
		Status     string                    `json:"status"`
	}

	result := checkResult{Status: "ready"}

	// Source — capture connection errors as structured errors instead of aborting.
	if src, err := registry.NewSource(p.SourceConfig.Type); err != nil {
		result.Source = &adapters.PermissionCheck{Errors: []string{"unsupported adapter: " + err.Error()}}
		result.Status = "failed"
	} else if err := src.Connect(ctx, p.SourceConfig); err != nil {
		result.Source = &adapters.PermissionCheck{Errors: []string{friendlyConnectError(err)}}
		result.Status = "failed"
	} else {
		defer src.Close()
		result.Source, _ = src.CheckPermissions(ctx)
		if result.Source != nil && len(result.Source.Errors) > 0 {
			result.Status = "failed"
		}
		// Collect database info.
		result.SourceInfo, _ = src.GetInfo(ctx)
	}

	// Target — same pattern.
	if tgt, err := registry.NewTarget(p.TargetConfig.Type); err != nil {
		result.Target = &adapters.PermissionCheck{Errors: []string{"unsupported adapter: " + err.Error()}}
		result.Status = "failed"
	} else if err := tgt.Connect(ctx, p.TargetConfig); err != nil {
		result.Target = &adapters.PermissionCheck{Errors: []string{friendlyConnectError(err)}}
		result.Status = "failed"
	} else {
		defer tgt.Close()
		result.Target, _ = tgt.CheckPermissions(ctx)
		if result.Target != nil && len(result.Target.Errors) > 0 {
			result.Status = "failed"
		}
		// Collect database info.
		result.TargetInfo, _ = tgt.GetInfo(ctx)
	}

	h.Log.Info("project.preflight",
		"project_id", id,
		"status", result.Status,
		"source_errors", len(func() []string {
			if result.Source != nil {
				return result.Source.Errors
			}
			return nil
		}()),
		"target_errors", len(func() []string {
			if result.Target != nil {
				return result.Target.Errors
			}
			return nil
		}()),
	)
	writeJSON(w, http.StatusOK, result)
}

// Analyze handles POST /api/v1/projects/{id}/analyze.
// When the source type is "mongodb" the request is delegated to AnalyzeMongo
// which samples documents and infers a schema plan. All other source types
// use the existing relational diff path.
func (h *ProjectsHandler) Analyze(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	if p.SourceConfig.Type == "mongodb" {
		h.AnalyzeMongo(w, r, p.ID, p.SourceConfig.DSN)
		return
	}

	ctx := r.Context()

	src, err := registry.NewSource(p.SourceConfig.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := src.Connect(ctx, p.SourceConfig); err != nil {
		writeError(w, http.StatusBadGateway, "connect source: "+err.Error())
		return
	}
	defer src.Close()

	tgt, err := registry.NewTarget(p.TargetConfig.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := tgt.Connect(ctx, p.TargetConfig); err != nil {
		writeError(w, http.StatusBadGateway, "connect target: "+err.Error())
		return
	}
	defer tgt.Close()

	a := analyzer.New()
	result, err := a.Analyze(ctx, src, tgt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result.SourceType = p.SourceConfig.Type
	result.TargetType = p.TargetConfig.Type

	issueCount := 0
	for _, t := range result.Tables {
		issueCount += len(t.Issues)
	}
	h.Log.Info("project.analyzed",
		"project_id", id,
		"tables", len(result.Tables),
		"compatible", result.Compatible,
		"issues", issueCount,
	)
	writeJSON(w, http.StatusOK, result)
}

// friendlyConnectError translates raw driver errors into actionable messages.
func friendlyConnectError(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "does not exist") && (strings.Contains(msg, "3D000") || strings.Contains(msg, "database")):
		// Extract database name from the error if present.
		return msg + " — create the database first: CREATE DATABASE <name>;"
	case strings.Contains(msg, "pg_hba.conf") || strings.Contains(msg, "SSL off"):
		return msg + " — add ?sslmode=require to the DSN"
	case strings.Contains(msg, "authentication failed") || strings.Contains(msg, "password authentication"):
		return msg + " — check the username and password"
	case strings.Contains(msg, "connection refused"):
		return msg + " — check that the host and port are correct and the server is running"
	case strings.Contains(msg, "no such host") || strings.Contains(msg, "hostname"):
		return msg + " — check the hostname"
	case strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "context deadline"):
		return msg + " — connection timed out, check firewall rules or network connectivity"
	default:
		return msg
	}
}

// connectSource is a helper used by Preflight and Analyze.
func connectSource(ctx context.Context, cfg adapters.ConnectionConfig) (adapters.SourceAdapter, error) {
	src, err := registry.NewSource(cfg.Type)
	if err != nil {
		return nil, err
	}
	return src, src.Connect(ctx, cfg)
}
