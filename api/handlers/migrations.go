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
	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/metrics"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/stats"
	"gitea.homelab.local/nextdevops/XferDB/validate"
)

// MigrationsHandler holds shared engine/collector maps for migration endpoints.
type MigrationsHandler struct {
	DB         *state.MetaDB
	Mu         *sync.Mutex
	Engines    map[string]*engine.Engine
	Collectors map[string]*stats.Collector
	Cancels    map[string]context.CancelFunc
	Log        *slog.Logger
	// Validations validates a migration once it completes, unless the
	// effective validation mode is off.
	Validations *ValidationsHandler
	// Metrics, when set, records each migration for GET /metrics.
	Metrics *metrics.Metrics
	// Live runs live migrations (#102); nil refuses them.
	Live *live.Manager
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
		// Per-run validation overrides; the project's settings apply otherwise.
		Validation      *string `json:"validation"`
		ValidationLevel  *string `json:"validation_level"`
		ValidationSample *int    `json:"validation_sample"`
		// Mode is "snapshot" (the default) or "live" (#102).
		Mode *string `json:"mode"`
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
	if overrides.Validation != nil {
		p.TransferConfig.Validation = *overrides.Validation
	}
	if overrides.ValidationLevel != nil {
		p.TransferConfig.ValidationLevel = *overrides.ValidationLevel
	}
	if overrides.ValidationSample != nil {
		p.TransferConfig.ValidationSample = *overrides.ValidationSample
	}
	if err := validate.CheckSettings(p.TransferConfig); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode := adapters.MigrationModeSnapshot
	if overrides.Mode != nil && *overrides.Mode != "" {
		mode = *overrides.Mode
	}
	switch mode {
	case adapters.MigrationModeSnapshot:
	case adapters.MigrationModeLive:
		if !h.checkLiveStart(w, r.Context(), p) {
			return
		}
	default:
		writeError(w, http.StatusBadRequest, `mode must be "snapshot" or "live"`)
		return
	}
	validationMode := p.TransferConfig.Validation
	if validationMode == "" {
		validationMode = validate.DefaultMode
	}
	validationLevel := p.TransferConfig.ValidationLevel
	if validationLevel == "" {
		validationLevel = validate.DefaultLevel
	}

	migrationID := uuid.New().String()
	mig := &adapters.Migration{
		ID:        migrationID,
		ProjectID: projectID,
		Status:    adapters.StatusPending,
		CreatedAt: time.Now(),
		Mode:      mode,
	}
	if mode == adapters.MigrationModeLive {
		mig.Phase = live.PhaseSetup
	}
	if err := h.DB.CreateMigration(r.Context(), mig); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if mode == adapters.MigrationModeLive {
		// Kept so a server restart can resume the run as it was started.
		if err := h.DB.SetRunConfig(r.Context(), migrationID, p.TransferConfig); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		// A live migration is validated at cutover, against a quiet source:
		// while it streams, differences would only be lag.
		validationMode = validate.ModeOff
	}
	h.launch(mig, p, validationMode, validationLevel)

	// The effective validation settings tell the client whether to wait for
	// a validation once the migration completes.
	writeJSON(w, http.StatusCreated, struct {
		*adapters.Migration
		Validation      string `json:"validation"`
		ValidationLevel string `json:"validation_level"`
	}{mig, validationMode, validationLevel})
}

// launch runs a migration's engine in the background, with its stats
// collector, metrics and validation; for a live migration, the snapshot,
// after which the stream carries on under h.Live.
func (h *MigrationsHandler) launch(mig *adapters.Migration, p *adapters.Project, validationMode, validationLevel string) {
	migrationID, projectID := mig.ID, p.ID
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
		"mode", mig.Mode,
		"tables", tables,
		"table_workers", effectiveTableWorkers,
		"segment_workers", effectiveSegmentWorkers,
		"batch_size", effectiveBatchSize,
		"recreate_schema", cfg.RecreateSchema,
		"truncate", cfg.Truncate,
		"bulk_copy", cfg.BulkCopy,
		"data_only", cfg.DataOnly,
		"validation", validationMode,
		"validation_level", validationLevel,
	)

	col.SetSaver(&statsSaverAdapter{db: h.DB})
	if h.Metrics != nil {
		col.SetObserver(h.Metrics.Track(migrationID, p.Name, col))
	}
	col.Start(ctx)

	isLive := mig.Mode == adapters.MigrationModeLive
	if isLive {
		eng.SetLive(&engine.Live{Setup: func(ctx context.Context, tables []adapters.TableSchema) error {
			return h.Live.Setup(ctx, migrationID, p, tables)
		}})
	}

	// In per_table mode each table is validated as soon as the engine
	// records it completed, while the rest are still copying.
	var perTable *perTableRun
	if h.Validations != nil && validationMode == validate.ModePerTable {
		perTable = h.Validations.beginPerTable(migrationID, p, validationLevel, p.TransferConfig.ValidationSample, effectiveTableWorkers, migLog)
		eng.OnTableDone(perTable.tableDone)
	}
	go func() {
		defer func() {
			cancel()
			h.Mu.Lock()
			delete(h.Engines, migrationID)
			delete(h.Cancels, migrationID)
			h.Mu.Unlock()
		}()
		err := eng.Run(ctx)
		if isLive {
			h.afterLiveSnapshot(migrationID, p, err, migLog)
			return
		}
		switch {
		case perTable != nil:
			perTable.finish()
		case h.Validations != nil:
			h.Validations.startAfterMigration(migrationID, p, validationMode, validationLevel, p.TransferConfig.ValidationSample, migLog)
		}
	}()
}

// afterLiveSnapshot moves a live migration on once its snapshot run ends:
// to catch-up when it succeeded, or, when it failed or was cancelled, it
// ends the stream so the source doesn't keep WAL for it.
func (h *MigrationsHandler) afterLiveSnapshot(id string, p *adapters.Project, runErr error, log *slog.Logger) {
	ctx := context.Background()
	if runErr == nil {
		if err := h.Live.SnapshotDone(ctx, id); err != nil {
			log.Error("live.snapshot_done", "error", err)
			h.DB.SetMigrationError(ctx, id, fmt.Errorf("start catch-up: %w", err)) //nolint:errcheck
		} else {
			log.Info("live.catch_up")
			return
		}
	}
	if err := h.Live.Stop(ctx, id, p); err != nil {
		log.Warn("live.stop", "error", err)
	}
}

// checkLiveStart answers false, having written the response, when a live
// migration of p can't start: live mode unsupported here, settings it can't
// run with, or failing readiness checks.
func (h *MigrationsHandler) checkLiveStart(w http.ResponseWriter, ctx context.Context, p *adapters.Project) bool {
	if h.Live == nil {
		writeError(w, http.StatusBadRequest, "live migration isn't enabled on this server")
		return false
	}
	if err := engine.CheckLive(p.TransferConfig); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return false
	}
	var src adapters.SourceAdapter
	s, srcErr := connectSource(ctx, p.SourceConfig)
	if srcErr == nil {
		defer s.Close()
		src = s
	}
	var tgt adapters.TargetAdapter
	t, tgtErr := registry.NewTarget(p.TargetConfig.Type)
	if tgtErr == nil {
		if tgtErr = t.Connect(ctx, p.TargetConfig); tgtErr == nil {
			defer t.Close()
			tgt = t
		}
	}
	res := liveChecks(ctx, p, src, tgt)
	if srcErr != nil || tgtErr != nil || !res.Ready {
		writeJSON(w, http.StatusBadRequest, struct {
			Error string      `json:"error"`
			Live  *liveResult `json:"live"`
		}{"the project isn't ready for a live migration: run the live readiness check", res})
		return false
	}
	return true
}

// ResumeLive picks up the live migrations a server restart interrupted:
// one still in setup or snapshot reruns its snapshot, which resumes each
// table from its checkpoint while capture keeps queueing; a later one is
// supervised again, its workers reattached or restarted.
func (h *MigrationsHandler) ResumeLive(ctx context.Context) {
	if h.Live == nil {
		return
	}
	migs, err := h.DB.ActiveLiveMigrations(ctx)
	if err != nil {
		h.Log.Warn("live.resume", "error", err)
		return
	}
	for _, mig := range migs {
		p, err := live.ProjectFor(ctx, h.DB, mig)
		if err != nil {
			h.Log.Warn("live.resume", "migration_id", mig.ID, "error", err)
			continue
		}
		switch mig.Phase {
		case live.PhaseCatchUp, live.PhaseStreaming:
			h.Live.Supervise(mig.ID, p)
		default:
			mode, level := validate.ModeOff, p.TransferConfig.ValidationLevel
			h.launch(mig, p, mode, level)
		}
		h.Log.Info("live.resumed", "migration_id", mig.ID, "phase", mig.Phase)
	}
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
	// Each table's outcome, from migration_tables: the only per-table record
	// of a finished migration, whose stats collector may be gone.
	tables, err := h.DB.GetTableProgress(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tables == nil {
		tables = []adapters.TableProgress{}
	}
	// A live migration's stream, while the server supervises it.
	var liveStatus *live.Status
	if h.Live != nil && mig.Mode == adapters.MigrationModeLive {
		if st, ok := h.Live.Status(id); ok {
			liveStatus = &st
		}
	}
	writeJSON(w, http.StatusOK, struct {
		*adapters.Migration
		Tables []adapters.TableProgress `json:"tables"`
		Live   *live.Status             `json:"live,omitempty"`
	}{mig, tables, liveStatus})
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

	// Once a live migration's snapshot is done, its stream is paused,
	// resumed and cancelled through h.Live; until then, through its engine.
	if !ok && h.Live != nil && h.Live.Active(id) {
		h.patchLive(w, r.Context(), id, body.Action)
		return
	}
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
		// Fast-path write so the DB reflects "cancelled" immediately; the
		// engine goroutine converges to the same state itself once it notices
		// ctx is cancelled (see Engine.fail), so this is a safety net rather
		// than the only writer.
		h.DB.SetMigrationCancelled(r.Context(), id) //nolint:errcheck
		h.Log.Info("migration.cancelled", "migration_id", id)
	default:
		writeError(w, http.StatusBadRequest, "action must be 'pause', 'resume', or 'cancel'")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// patchLive pauses, resumes or cancels a live migration's stream.
func (h *MigrationsHandler) patchLive(w http.ResponseWriter, ctx context.Context, id, action string) {
	var err error
	switch action {
	case "pause":
		err = h.Live.Pause(ctx, id)
	case "resume":
		err = h.Live.Resume(ctx, id)
	case "cancel":
		if err = h.Live.Stop(ctx, id, nil); err == nil {
			err = h.DB.SetMigrationCancelled(ctx, id)
		}
	default:
		writeError(w, http.StatusBadRequest, "action must be 'pause', 'resume', or 'cancel'")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	event := map[string]string{"pause": "migration.paused", "resume": "migration.resumed", "cancel": "migration.cancelled"}[action]
	h.Log.Info(event, "migration_id", id, "mode", adapters.MigrationModeLive)
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
	// A live migration's stream goes with it: the source must not keep
	// WAL for a migration nobody can see any more.
	if mig, err := h.DB.GetMigration(r.Context(), id); err == nil && mig.Mode == adapters.MigrationModeLive && h.Live != nil {
		if p, err := live.ProjectFor(r.Context(), h.DB, mig); err == nil {
			if err := h.Live.Stop(r.Context(), id, p); err != nil {
				h.Log.Warn("live.stop", "migration_id", id, "error", err)
			}
		}
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
			// Recompute totals after augmentation to ensure consistency.
			var totalRows, totalTransferred int64
			for _, td := range snap.TableDetails {
				totalRows += td.Total
				totalTransferred += td.Transferred
			}
			snap.Rows.Total = totalRows
			snap.Rows.Transferred = totalTransferred
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
