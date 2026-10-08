package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"gitea.homelab.local/nextdevops/XferDB/validate"
	"github.com/google/uuid"
)

// ValidationsHandler runs validations in the background, at most one per
// project at a time: each reads every row of both databases, so two at once
// would only compete for the same I/O.
type ValidationsHandler struct {
	DB  *state.MetaDB
	Log *slog.Logger

	mu        sync.Mutex
	cancels   map[string]context.CancelFunc // validation ID -> cancel
	byProject map[string]string             // project ID -> active validation ID
}

// errValidationBusy means the project already has a validation running.
var errValidationBusy = errors.New("busy")

// NewValidationsHandler returns a handler with no active validations.
func NewValidationsHandler(db *state.MetaDB, log *slog.Logger) *ValidationsHandler {
	return &ValidationsHandler{
		DB:        db,
		Log:       log,
		cancels:   make(map[string]context.CancelFunc),
		byProject: make(map[string]string),
	}
}

// start creates a validation run for migrationID and runs it in the
// background. Only tables that reached completed in that migration are
// validated; any other requested table is recorded as skipped with the reason.
func (h *ValidationsHandler) start(ctx context.Context, migrationID string, p *adapters.Project, level string, sample int, tables []string) (*state.Validation, error) {
	if level == "" {
		level = validate.DefaultLevel
	}
	if err := validate.CheckLevel(level); err != nil {
		return nil, err
	}
	if err := validate.CheckSample(sample); err != nil {
		return nil, err
	}
	// The sample only shapes an L3 diff; 100% is every row.
	if level != validate.LevelL3 || sample == 100 {
		sample = 0
	}

	progress, err := h.DB.GetTableProgress(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	statusOf := make(map[string]adapters.MigrationStatus, len(progress))
	var eligible, skipped []string
	for _, tp := range progress {
		statusOf[tp.TableName] = tp.Status
	}
	wanted := tables
	if len(wanted) == 0 {
		for _, tp := range progress {
			wanted = append(wanted, tp.TableName)
		}
	}
	for _, t := range wanted {
		if statusOf[t] == adapters.StatusCompleted {
			eligible = append(eligible, t)
		} else {
			skipped = append(skipped, t)
		}
	}

	// The plan frozen on the migration when there is one; otherwise the
	// project's current plan, which is what the engine itself reads.
	plan, err := h.DB.GetMigrationPlan(ctx, migrationID)
	if err != nil {
		return nil, err
	}
	if len(plan) == 0 {
		if plan, err = h.DB.GetPlanForProject(ctx, p.ID); err != nil {
			return nil, err
		}
	}

	h.mu.Lock()
	if _, busy := h.byProject[p.ID]; busy {
		h.mu.Unlock()
		return nil, errValidationBusy
	}
	v := &state.Validation{
		ID:          uuid.New().String(),
		MigrationID: migrationID,
		Level:       level,
		Status:      adapters.StatusPending,
		Options:     state.ValidationOptions{Tables: tables, Sample: sample},
		CreatedAt:   time.Now(),
	}
	if err := h.DB.CreateValidation(ctx, v); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	h.cancels[v.ID] = cancel
	h.byProject[p.ID] = v.ID
	h.mu.Unlock()

	for _, t := range skipped {
		reason := "table was not part of this migration"
		if st, ok := statusOf[t]; ok {
			reason = fmt.Sprintf("table did not complete in this migration (status %s)", st)
		}
		h.DB.UpsertValidationTable(ctx, state.ValidationTable{ //nolint:errcheck
			ValidationID: v.ID, TableName: t, Result: state.ValidationSkipped, Detail: reason,
		})
	}

	log := h.Log.With("validation_id", v.ID, "migration_id", migrationID, "project_id", p.ID, "level", level)
	go func() {
		defer func() {
			cancel()
			h.mu.Lock()
			delete(h.cancels, v.ID)
			delete(h.byProject, p.ID)
			h.mu.Unlock()
		}()
		writeCtx := context.WithoutCancel(runCtx)
		h.DB.SetValidationStatus(writeCtx, v.ID, adapters.StatusInProgress, "") //nolint:errcheck
		log.Info("validation.started", "tables", len(eligible), "skipped", len(skipped))

		var failed int
		err := validate.Run(runCtx, validate.Request{
			Project:   p,
			Plan:      plan,
			Tables:    eligible,
			Level:     level,
			BatchSize: p.TransferConfig.BatchSize,
			Sample:    sample,
		}, func(t state.ValidationTable) {
			t.ValidationID = v.ID
			if t.Result == state.ValidationFail {
				failed++
				log.Warn("validation.table_failed", "table", t.TableName, "detail", t.Detail)
			}
			if err := h.DB.UpsertValidationTable(writeCtx, t); err != nil {
				log.Error("validation.save_failed", "table", t.TableName, "error", err)
			}
			if len(t.Diffs) > 0 {
				if err := h.DB.SaveValidationDiffs(writeCtx, v.ID, t.TableName, t.Diffs); err != nil {
					log.Error("validation.save_diffs_failed", "table", t.TableName, "error", err)
				}
			}
		})
		switch {
		case runCtx.Err() != nil:
			h.DB.SetValidationStatus(writeCtx, v.ID, adapters.StatusCancelled, "cancelled by user") //nolint:errcheck
			log.Info("validation.cancelled")
		case err != nil:
			h.DB.SetValidationStatus(writeCtx, v.ID, adapters.StatusFailed, err.Error()) //nolint:errcheck
			log.Error("validation.failed", "error", err)
		default:
			h.DB.SetValidationStatus(writeCtx, v.ID, adapters.StatusCompleted, "") //nolint:errcheck
			log.Info("validation.completed", "tables", len(eligible), "failed_tables", failed)
		}
	}()
	return v, nil
}

// StartValidation handles POST /api/v1/migrations/{id}/validations.
func (h *ValidationsHandler) StartValidation(w http.ResponseWriter, r *http.Request) {
	migrationID := r.PathValue("id")
	mig, err := h.DB.GetMigration(r.Context(), migrationID)
	if err != nil {
		writeError(w, http.StatusNotFound, "migration not found")
		return
	}
	p, err := h.DB.GetProject(r.Context(), mig.ProjectID)
	if err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}

	var body struct {
		Level  string   `json:"level"`
		Sample *int     `json:"sample"`
		Tables []string `json:"tables"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
	}
	level := body.Level
	if level == "" {
		level = p.TransferConfig.ValidationLevel
	}
	if err := validate.CheckLevel(level); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sample := p.TransferConfig.ValidationSample
	if body.Sample != nil {
		if level != validate.LevelL3 && *body.Sample != 0 {
			writeError(w, http.StatusBadRequest, "sample applies only to level L3")
			return
		}
		sample = *body.Sample
	}
	if err := validate.CheckSample(sample); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	v, err := h.start(r.Context(), migrationID, p, level, sample, body.Tables)
	if errors.Is(err, errValidationBusy) {
		writeError(w, http.StatusConflict, "a validation is already running for this project — wait for it to finish or cancel it first")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

// startAfterMigration is called when a migration finishes. It validates the
// migration when the effective mode asks for it and the migration completed.
func (h *ValidationsHandler) startAfterMigration(migrationID string, p *adapters.Project, mode, level string, sample int, log *slog.Logger) {
	if mode == "" {
		mode = validate.DefaultMode
	}
	if mode != validate.ModeAfter {
		return
	}
	ctx := context.Background()
	mig, err := h.DB.GetMigration(ctx, migrationID)
	if err != nil || mig.Status != adapters.StatusCompleted {
		return
	}
	if _, err := h.start(ctx, migrationID, p, level, sample, nil); err != nil {
		log.Warn("validation.auto_start_failed", "error", err)
	}
}

// ListValidations handles GET /api/v1/migrations/{id}/validations.
func (h *ValidationsHandler) ListValidations(w http.ResponseWriter, r *http.Request) {
	vs, err := h.DB.ListValidations(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, vs)
}

// validationTableView adds Stale: true when a later migration has re-migrated
// the table, so this run's result no longer describes what is in the target.
type validationTableView struct {
	state.ValidationTable
	Stale bool `json:"stale"`
}

// GetValidation handles GET /api/v1/validations/{id}.
func (h *ValidationsHandler) GetValidation(w http.ResponseWriter, r *http.Request) {
	v, err := h.DB.GetValidation(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "validation not found")
		return
	}
	tables, err := h.DB.GetValidationTables(r.Context(), v.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	lastMigration := map[string]string{}
	if mig, err := h.DB.GetMigration(r.Context(), v.MigrationID); err == nil {
		if pts, err := h.DB.GetProjectTables(r.Context(), mig.ProjectID); err == nil {
			for _, pt := range pts {
				lastMigration[pt.TableName] = pt.LastMigrationID
			}
		}
	}
	views := make([]validationTableView, 0, len(tables))
	for _, t := range tables {
		last, known := lastMigration[t.TableName]
		views = append(views, validationTableView{
			ValidationTable: t,
			Stale:           known && last != "" && last != v.MigrationID,
		})
	}

	writeJSON(w, http.StatusOK, struct {
		*state.Validation
		Tables []validationTableView `json:"tables"`
	}{v, views})
}

// GetValidationDiffs handles GET /api/v1/validations/{id}/tables/{table}/diffs:
// the example rows an L3 diff recorded for one table, up to 100. The table's
// totals are on its entry in GET /api/v1/validations/{id}.
func (h *ValidationsHandler) GetValidationDiffs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.DB.GetValidation(r.Context(), id); err != nil {
		writeError(w, http.StatusNotFound, "validation not found")
		return
	}
	diffs, err := h.DB.GetValidationDiffs(r.Context(), id, r.PathValue("table"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, diffs)
}

// PatchValidation handles PATCH /api/v1/validations/{id} ({"action":"cancel"}).
func (h *ValidationsHandler) PatchValidation(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Action != "cancel" {
		writeError(w, http.StatusBadRequest, "action must be 'cancel'")
		return
	}
	h.mu.Lock()
	cancel, ok := h.cancels[id]
	h.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "no running validation with that ID")
		return
	}
	cancel()
	w.WriteHeader(http.StatusAccepted)
}

// ValidationStatus handles GET /api/v1/projects/{id}/validation-status: the
// latest pass/fail per table across the project's migrations, each marked
// stale once a later migration re-migrated that table, plus the project's
// running validation if there is one. A table absent from "tables" has never
// been validated.
func (h *ValidationsHandler) ValidationStatus(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	if _, err := h.DB.GetProject(r.Context(), projectID); err != nil {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	latest, err := h.DB.LatestTableValidations(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	active, err := h.DB.ActiveValidation(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	lastMigration := map[string]string{}
	if pts, err := h.DB.GetProjectTables(r.Context(), projectID); err == nil {
		for _, pt := range pts {
			lastMigration[pt.TableName] = pt.LastMigrationID
		}
	}

	type tableStatus struct {
		state.TableValidation
		Stale bool `json:"stale"`
	}
	tables := make([]tableStatus, 0, len(latest))
	for _, t := range latest {
		last := lastMigration[t.TableName]
		tables = append(tables, tableStatus{TableValidation: t, Stale: last != "" && last != t.MigrationID})
	}
	writeJSON(w, http.StatusOK, struct {
		Active *state.Validation `json:"active"`
		Tables []tableStatus     `json:"tables"`
	}{active, tables})
}
