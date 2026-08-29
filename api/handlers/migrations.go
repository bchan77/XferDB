package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/stats"
)

// MigrationsHandler holds shared engine/collector maps for migration endpoints.
type MigrationsHandler struct {
	DB         *state.MetaDB
	Mu         *sync.Mutex
	Engines    map[string]*engine.Engine
	Collectors map[string]*stats.Collector
	Cancels    map[string]context.CancelFunc
}

// StartMigration handles POST /api/v1/projects/{id}/migrations.
func (h *MigrationsHandler) StartMigration(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	p, err := h.DB.GetProject(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	// Prevent concurrent migrations for the same project.
	existing, err := h.DB.ListMigrations(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, m := range existing {
		if m.Status == adapters.StatusPending || m.Status == adapters.StatusInProgress {
			writeError(w, http.StatusConflict,
				fmt.Sprintf("migration %s is already %s for this project — wait for it to finish or delete it first", m.ID, m.Status))
			return
		}
	}

	// Optional per-run TransferConfig overrides in the request body.
	var overrides struct {
		RecreateSchema *bool    `json:"recreate_schema"`
		Truncate       *bool    `json:"truncate"`
		DataOnly       *bool    `json:"data_only"`
		SchemaOnly     *bool    `json:"schema_only"`
		BatchSize      *int     `json:"batch_size"`
		Workers        *int     `json:"workers"`
		Tables         []string `json:"tables"`
	}
	json.NewDecoder(r.Body).Decode(&overrides) // ignore decode error — body is optional
	if overrides.RecreateSchema != nil {
		p.TransferConfig.RecreateSchema = *overrides.RecreateSchema
	}
	if overrides.Truncate != nil {
		p.TransferConfig.Truncate = *overrides.Truncate
	}
	if overrides.DataOnly != nil {
		p.TransferConfig.DataOnly = *overrides.DataOnly
	}
	if overrides.SchemaOnly != nil {
		p.TransferConfig.SchemaOnly = *overrides.SchemaOnly
	}
	if overrides.BatchSize != nil && *overrides.BatchSize > 0 {
		p.TransferConfig.BatchSize = *overrides.BatchSize
	}
	if overrides.Workers != nil && *overrides.Workers > 0 {
		p.TransferConfig.Workers = *overrides.Workers
	}
	if len(overrides.Tables) > 0 {
		p.TransferConfig.Tables = overrides.Tables
	}

	migrationID := uuid.New().String()
	mig := &adapters.Migration{
		ID:        migrationID,
		ProjectID: projectID,
		Status:    adapters.StatusPending,
		CreatedAt: time.Now(),
	}
	if err := h.DB.CreateMigration(r.Context(), mig); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	eng := engine.New(migrationID, p, h.DB)
	col := stats.NewCollector(migrationID, eng.Events())

	ctx, cancel := context.WithCancel(context.Background())

	h.Mu.Lock()
	h.Engines[migrationID] = eng
	h.Collectors[migrationID] = col
	h.Cancels[migrationID] = cancel
	h.Mu.Unlock()

	col.Start(ctx)
	go func() {
		defer func() {
			cancel()
			h.Mu.Lock()
			delete(h.Engines, migrationID)
			delete(h.Cancels, migrationID)
			h.Mu.Unlock()
		}()
		eng.Run(ctx) //nolint:errcheck
	}()

	writeJSON(w, http.StatusCreated, mig)
}

// ListMigrations handles GET /api/v1/projects/{id}/migrations.
func (h *MigrationsHandler) ListMigrations(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	migs, err := h.DB.ListMigrations(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, migs)
}

// GetMigration handles GET /api/v1/migrations/{id}.
func (h *MigrationsHandler) GetMigration(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mig, err := h.DB.GetMigration(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "migration not found")
		return
	}
	writeJSON(w, http.StatusOK, mig)
}

// PatchMigration handles PATCH /api/v1/migrations/{id} for pause/resume.
func (h *MigrationsHandler) PatchMigration(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var body struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	h.Mu.Lock()
	eng, ok := h.Engines[id]
	h.Mu.Unlock()

	if !ok {
		writeError(w, http.StatusNotFound, "no active engine for migration")
		return
	}

	switch body.Action {
	case "pause":
		eng.Pause()
	case "resume":
		eng.Resume()
	case "cancel":
		h.Mu.Lock()
		cancel, hasCancel := h.Cancels[id]
		h.Mu.Unlock()
		if hasCancel {
			cancel()
		}
		h.DB.SetMigrationError(r.Context(), id, fmt.Errorf("cancelled by user"))
	default:
		writeError(w, http.StatusBadRequest, "action must be 'pause', 'resume', or 'cancel'")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// DeleteMigration handles DELETE /api/v1/migrations/{id}.
func (h *MigrationsHandler) DeleteMigration(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.Mu.Lock()
	cancel, hasCancel := h.Cancels[id]
	h.Mu.Unlock()

	if hasCancel {
		cancel()
	}

	if err := h.DB.DeleteMigration(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetStats handles GET /api/v1/migrations/{id}/stats.
func (h *MigrationsHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	h.Mu.Lock()
	col, ok := h.Collectors[id]
	h.Mu.Unlock()

	if !ok {
		writeError(w, http.StatusNotFound, "no stats collector for migration")
		return
	}

	writeJSON(w, http.StatusOK, col.Snapshot())
}
