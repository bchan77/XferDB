package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	mongoanalyzer "gitea.homelab.local/nextdevops/XferDB/analyzer/mongo"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// AnalyzeMongo handles POST /api/v1/projects/{id}/analyze when the source is MongoDB.
// It samples collections, infers the schema plan, persists it as a draft, and
// returns the full InferredSchema list for review.
func (h *ProjectsHandler) AnalyzeMongo(w http.ResponseWriter, r *http.Request, projectID string, dsn string) {
	var req struct {
		SampleSize      int     `json:"sample_size"`
		SamplePct       float64 `json:"sample_pct"`
		SampleThreshold int64   `json:"sample_threshold"`
		AI              bool    `json:"ai"`
	}
	// Decode optional body; ignore errors (all fields have safe defaults).
	json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck

	ctx := r.Context()

	sampler, err := mongoanalyzer.NewSampler(ctx, dsn, mongoanalyzer.SamplerOptions{
		SampleSize: req.SampleSize,
		Threshold:  req.SampleThreshold,
		SamplePct:  req.SamplePct,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, "connect to MongoDB: "+err.Error())
		return
	}
	defer sampler.Close(ctx) //nolint:errcheck

	collections, err := sampler.ListSampleableCollections(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list collections: "+err.Error())
		return
	}

	annotator := mongoanalyzer.NewAnnotator()

	type collectionResult struct {
		Collection string                       `json:"collection"`
		Schema     *mongoanalyzer.InferredSchema `json:"schema"`
		Warnings   []string                      `json:"warnings,omitempty"`
	}

	var results []collectionResult
	var allPlanRows []state.SchemaPlanRow

	for _, coll := range collections {
		sample, err := sampler.SampleCollection(ctx, coll)
		if err != nil {
			h.Log.Warn("mongo.analyze: sample failed", "collection", coll, "error", err)
			results = append(results, collectionResult{
				Collection: coll,
				Warnings:   []string{"sampling failed: " + err.Error()},
			})
			continue
		}

		schema, err := mongoanalyzer.InferSchema(sample, projectID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "infer schema for "+coll+": "+err.Error())
			return
		}

		if req.AI {
			annotations, err := annotator.Annotate(ctx, sample, schema)
			if err != nil {
				h.Log.Warn("mongo.analyze: AI annotation failed", "collection", coll, "error", err)
			} else {
				mongoanalyzer.ApplyAnnotations(schema, annotations)
			}
		}

		// Persist draft plan — does not clobber overridden rows.
		if err := h.DB.SavePlan(ctx, schema.PlanRows()); err != nil {
			writeError(w, http.StatusInternalServerError, "save plan for "+coll+": "+err.Error())
			return
		}

		allPlanRows = append(allPlanRows, schema.PlanRows()...)
		results = append(results, collectionResult{Collection: coll, Schema: schema})
	}

	h.Log.Info("project.mongo.analyzed",
		"project_id", projectID,
		"collections", len(collections),
		"fields", len(allPlanRows),
	)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"type":        "mongo_infer",
		"collections": results,
	})
}

// GetSchemaPlan handles GET /api/v1/projects/{id}/schema-plan.
func (h *ProjectsHandler) GetSchemaPlan(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	ctx := r.Context()

	collections, err := h.DB.ListPlanCollections(ctx, projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := make(map[string][]state.SchemaPlanRow, len(collections))
	for _, coll := range collections {
		rows, err := h.DB.GetPlan(ctx, projectID, coll)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		result[coll] = rows
	}

	writeJSON(w, http.StatusOK, result)
}

// OverrideSchemaPlan handles PUT /api/v1/projects/{id}/schema-plan.
// Body is a list of field overrides; each sets overridden=true.
func (h *ProjectsHandler) OverrideSchemaPlan(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	ctx := r.Context()

	var overrides []struct {
		Collection string `json:"collection"`
		FieldName  string `json:"field_name"`
		PgColumn   string `json:"pg_column"`
		PgType     string `json:"pg_type"`
		Strategy   string `json:"strategy"`
		IsPK       bool   `json:"is_pk"`
		Nullable   bool   `json:"nullable"`
	}
	if err := json.NewDecoder(r.Body).Decode(&overrides); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	validStrategies := map[string]bool{"direct": true, "as_jsonb": true, "flatten": true, "skip": true}

	for _, o := range overrides {
		if o.Collection == "" || o.FieldName == "" {
			writeError(w, http.StatusBadRequest, "collection and field_name are required")
			return
		}
		if o.Strategy != "" && !validStrategies[o.Strategy] {
			writeError(w, http.StatusBadRequest, "invalid strategy: "+o.Strategy)
			return
		}
		if err := h.DB.OverridePlan(ctx, projectID, o.Collection, o.FieldName, state.SchemaPlanRow{
			PgColumn: o.PgColumn,
			PgType:   o.PgType,
			Strategy: o.Strategy,
			IsPK:     o.IsPK,
			Nullable: o.Nullable,
		}); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	h.Log.Info("project.schema_plan.overridden",
		"project_id", projectID,
		"overrides", len(overrides),
	)
	w.WriteHeader(http.StatusNoContent)
}

// DeleteSchemaPlan handles DELETE /api/v1/projects/{id}/schema-plan.
// Query param ?collection=<name> wipes one collection; omit to wipe all.
func (h *ProjectsHandler) DeleteSchemaPlan(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("id")
	ctx := r.Context()

	coll := strings.TrimSpace(r.URL.Query().Get("collection"))
	var err error
	if coll != "" {
		err = h.DB.DeletePlan(ctx, projectID, coll)
	} else {
		err = h.DB.DeleteAllPlans(ctx, projectID)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.Log.Info("project.schema_plan.deleted",
		"project_id", projectID,
		"collection", coll,
	)
	w.WriteHeader(http.StatusNoContent)
}
