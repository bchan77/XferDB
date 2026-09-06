package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
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
	Log        *slog.Logger
}

// statsSaverAdapter wraps MetaDB to implement stats.StatsSaver.
type statsSaverAdapter struct {
	db *state.MetaDB
}

func (a *statsSaverAdapter) SaveMigrationStats(ctx context.Context, rec stats.StatsRecord) error {
	return a.db.SaveMigrationStats(ctx, state.MigrationStatsRecord{
		MigrationID:     rec.MigrationID,
		Timestamp:       rec.Timestamp,
		ElapsedSecs:     rec.ElapsedSecs,
		Phase:           rec.Phase,
		RowsTotal:       rec.RowsTotal,
		RowsTransferred: rec.RowsTransferred,
		RatePerSec:      rec.RatePerSec,
		ReadRate:        rec.ReadRate,
		WriteRate:       rec.WriteRate,
		TablesTotal:     rec.TablesTotal,
		TablesDone:      rec.TablesDone,
		TablesFailed:    rec.TablesFailed,
		Goroutines:      rec.Goroutines,
		MemAllocMB:      rec.MemAllocMB,
		MemSysMB:        rec.MemSysMB,
		CPUPercent:      rec.CPUPercent,
	})
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
		TableWorkers   *int     `json:"table_workers"`
		SegmentWorkers *int     `json:"segment_workers"`
		OffsetFallback *bool    `json:"offset_fallback"`
		BulkCopy       *bool    `json:"bulk_copy"`
		AccurateCounts *bool    `json:"accurate_counts"`
		AsyncPipeline  *bool    `json:"async_pipeline"`
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
	if overrides.TableWorkers != nil && *overrides.TableWorkers > 0 {
		p.TransferConfig.TableWorkers = *overrides.TableWorkers
	}
	if overrides.SegmentWorkers != nil && *overrides.SegmentWorkers > 0 {
		p.TransferConfig.SegmentWorkers = *overrides.SegmentWorkers
	}
	if overrides.OffsetFallback != nil {
		p.TransferConfig.OffsetFallback = *overrides.OffsetFallback
	}
	if overrides.BulkCopy != nil {
		p.TransferConfig.BulkCopy = *overrides.BulkCopy
	}
	if overrides.AccurateCounts != nil {
		p.TransferConfig.AccurateCounts = *overrides.AccurateCounts
	}
	if overrides.AsyncPipeline != nil {
		p.TransferConfig.AsyncPipeline = *overrides.AsyncPipeline
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

	// Normalise effective config values so the display reflects what the engine will use.
	effectiveBatchSize := p.TransferConfig.BatchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 1000 // engine defaultBatchSize
	}
	effectiveTableWorkers := p.TransferConfig.TableWorkers
	if effectiveTableWorkers < 1 {
		effectiveTableWorkers = 1
	}
	effectiveSegmentWorkers := p.TransferConfig.SegmentWorkers
	if effectiveSegmentWorkers < 1 {
		effectiveSegmentWorkers = 1
	}
	migLog := h.Log.With("migration_id", migrationID, "project", p.Name, "project_id", projectID)
	col := stats.NewCollector(migrationID, projectID, eng.Events(), stats.MigrationConfig{
		BatchSize:      effectiveBatchSize,
		TableWorkers:   effectiveTableWorkers,
		SegmentWorkers: effectiveSegmentWorkers,
	}, migLog)

	ctx, cancel := context.WithCancel(context.Background())

	h.Mu.Lock()
	h.Engines[migrationID] = eng
	h.Collectors[migrationID] = col
	h.Cancels[migrationID] = cancel
	h.Mu.Unlock()

	cfg := p.TransferConfig
	tables := cfg.Tables
	if len(tables) == 0 {
		tables = []string{"(all)"}
	}
	migLog.Info("migration.started",
		"tables", tables,
		"table_workers", effectiveTableWorkers,
		"segment_workers", effectiveSegmentWorkers,
		"batch_size", effectiveBatchSize,
		"recreate_schema", cfg.RecreateSchema,
		"truncate", cfg.Truncate,
		"bulk_copy", cfg.BulkCopy,
		"data_only", cfg.DataOnly,
	)

	col.SetSaver(&statsSaverAdapter{db: h.DB})
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
		h.Log.Info("migration.paused", "migration_id", id)
	case "resume":
		eng.Resume()
		h.Log.Info("migration.resumed", "migration_id", id)
	case "cancel":
		h.Mu.Lock()
		cancel, hasCancel := h.Cancels[id]
		h.Mu.Unlock()
		if hasCancel {
			cancel()
		}
		h.DB.SetMigrationError(r.Context(), id, fmt.Errorf("cancelled by user"))
		h.Log.Info("migration.cancelled", "migration_id", id)
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

	snap := col.Snapshot()

	// Augment the snapshot with project-level table records for tables that are
	// not part of this migration's scope (e.g. when --tables filters to a subset).
	// This gives --status a full project-wide view across all migration runs.
	if snap.ProjectID != "" {
		projectTables, err := h.DB.GetProjectTables(r.Context(), snap.ProjectID)
		if err == nil && len(projectTables) > 0 {
			inScope := make(map[string]bool, len(snap.TableDetails))
			for _, td := range snap.TableDetails {
				inScope[td.Name] = true
			}
			for _, pt := range projectTables {
				if !inScope[pt.TableName] {
					// If this table's status is from a different migration, show it as
					// "pending" for the current migration rather than the old status.
					status := pt.Status
					transferred := pt.RowsTransferred
					if pt.LastMigrationID != id {
						status = "pending"
						transferred = 0
					}
					snap.TableDetails = append(snap.TableDetails, stats.TableDetail{
						Name:        pt.TableName,
						Status:      status,
						Transferred: transferred,
						Total:       pt.RowsTotal,
					})
				}
			}
			sort.Slice(snap.TableDetails, func(i, j int) bool {
				return snap.TableDetails[i].Name < snap.TableDetails[j].Name
			})
		}
	}

	writeJSON(w, http.StatusOK, snap)
}

// GetStatsHistory handles GET /api/v1/migrations/{id}/stats/history.
// Returns all recorded stats snapshots for the migration.
func (h *MigrationsHandler) GetStatsHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	records, err := h.DB.GetMigrationStats(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, records)
}
