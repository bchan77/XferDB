package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/analyzer"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ProjectsHandler holds shared dependencies for project-related endpoints.
type ProjectsHandler struct {
	DB *state.MetaDB
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
	if err := h.DB.DeleteProject(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
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
		Source *adapters.PermissionCheck `json:"source"`
		Target *adapters.PermissionCheck `json:"target"`
		Status string                    `json:"status"`
	}

	src, err := registry.NewSource(p.SourceConfig.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, "source adapter: "+err.Error())
		return
	}
	if err := src.Connect(ctx, p.SourceConfig); err != nil {
		writeError(w, http.StatusBadGateway, "connect source: "+err.Error())
		return
	}
	defer src.Close()

	tgt, err := registry.NewTarget(p.TargetConfig.Type)
	if err != nil {
		writeError(w, http.StatusBadRequest, "target adapter: "+err.Error())
		return
	}
	if err := tgt.Connect(ctx, p.TargetConfig); err != nil {
		writeError(w, http.StatusBadGateway, "connect target: "+err.Error())
		return
	}
	defer tgt.Close()

	srcCheck, _ := src.CheckPermissions(ctx)
	tgtCheck, _ := tgt.CheckPermissions(ctx)

	status := "ready"
	if srcCheck != nil && len(srcCheck.Errors) > 0 {
		status = "failed"
	}
	if tgtCheck != nil && len(tgtCheck.Errors) > 0 {
		status = "failed"
	}

	writeJSON(w, http.StatusOK, checkResult{
		Source: srcCheck,
		Target: tgtCheck,
		Status: status,
	})
}

// Analyze handles POST /api/v1/projects/{id}/analyze.
func (h *ProjectsHandler) Analyze(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
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

	writeJSON(w, http.StatusOK, result)
}

// connectSource is a helper used by Preflight and Analyze.
func connectSource(ctx context.Context, cfg adapters.ConnectionConfig) (adapters.SourceAdapter, error) {
	src, err := registry.NewSource(cfg.Type)
	if err != nil {
		return nil, err
	}
	return src, src.Connect(ctx, cfg)
}
