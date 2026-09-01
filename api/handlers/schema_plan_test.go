package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// openHandlerTestDB creates a temporary SQLite state DB and returns it with a
// cleanup function. Mirrors the helper in state/schema_plan_test.go.
func openHandlerTestDB(t *testing.T) (*state.MetaDB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "xferdb-handler-test-*.db")
	if err != nil {
		t.Fatalf("create temp db: %v", err)
	}
	f.Close()
	db, err := state.Open(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("state.Open: %v", err)
	}
	return db, func() { db.Close(); os.Remove(f.Name()) }
}

func newTestHandler(t *testing.T) (*ProjectsHandler, *state.MetaDB, func()) {
	t.Helper()
	db, cleanup := openHandlerTestDB(t)
	h := &ProjectsHandler{DB: db, Log: slog.Default()}
	return h, db, cleanup
}

// seedPlan inserts a handful of plan rows directly via the state layer.
func seedPlan(t *testing.T, db *state.MetaDB, projectID, collection string) []state.SchemaPlanRow {
	t.Helper()
	rows := []state.SchemaPlanRow{
		{
			ProjectID:  projectID,
			Collection: collection,
			FieldName:  "_id",
			PgColumn:   "_id",
			PgType:     "text",
			Strategy:   "direct",
			IsPK:       true,
			Nullable:   false,
			CreatedAt:  time.Now(),
		},
		{
			ProjectID:  projectID,
			Collection: collection,
			FieldName:  "amount",
			PgColumn:   "amount",
			PgType:     "double precision",
			Strategy:   "direct",
			IsPK:       false,
			Nullable:   true,
			CreatedAt:  time.Now(),
		},
		{
			ProjectID:  projectID,
			Collection: collection,
			FieldName:  "status",
			PgColumn:   "status",
			PgType:     "text",
			Strategy:   "direct",
			IsPK:       false,
			Nullable:   true,
			CreatedAt:  time.Now(),
		},
	}
	if err := db.SavePlan(context.Background(), rows); err != nil {
		t.Fatalf("seedPlan SavePlan: %v", err)
	}
	return rows
}

// --------------------------------------------------------------------------
// GetSchemaPlan

func TestGetSchemaPlan_ReturnsSeededRows(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-1"
	seedPlan(t, db, projID, "orders")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/"+projID+"/schema-plan", nil)
	req.SetPathValue("id", projID)
	w := httptest.NewRecorder()
	h.GetSchemaPlan(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string][]map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	fields, ok := body["orders"]
	if !ok {
		t.Fatal("response missing 'orders' collection key")
	}
	if len(fields) != 3 {
		t.Errorf("fields count = %d, want 3", len(fields))
	}
}

func TestGetSchemaPlan_EmptyWhenNoRows(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects/ghost/schema-plan", nil)
	req.SetPathValue("id", "ghost")
	w := httptest.NewRecorder()
	h.GetSchemaPlan(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if len(body) != 0 {
		t.Errorf("expected empty map, got %v", body)
	}
}

// --------------------------------------------------------------------------
// OverrideSchemaPlan

func TestOverrideSchemaPlan_UpdatesRow(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-2"
	seedPlan(t, db, projID, "orders")

	overrides := []map[string]any{
		{
			"collection": "orders",
			"field_name": "amount",
			"pg_type":    "numeric",
			"strategy":   "direct",
		},
	}
	body, _ := json.Marshal(overrides)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/"+projID+"/schema-plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", projID)
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body: %s", w.Code, w.Body.String())
	}

	// Verify the overridden flag is set and pg_type changed.
	rows, err := db.GetPlan(context.Background(), projID, "orders")
	if err != nil {
		t.Fatalf("GetPlan: %v", err)
	}
	for _, r := range rows {
		if r.FieldName == "amount" {
			if !r.Overridden {
				t.Error("overridden = false, want true")
			}
			if r.PgType != "numeric" {
				t.Errorf("pg_type = %q, want \"numeric\"", r.PgType)
			}
			return
		}
	}
	t.Error("did not find 'amount' field in plan after override")
}

func TestOverrideSchemaPlan_InvalidStrategy(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	overrides := []map[string]any{
		{
			"collection": "orders",
			"field_name": "amount",
			"strategy":   "bogus_strategy",
		},
	}
	body, _ := json.Marshal(overrides)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/p1/schema-plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "p1")
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestOverrideSchemaPlan_MissingCollection(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	overrides := []map[string]any{{"field_name": "amount"}}
	body, _ := json.Marshal(overrides)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/p1/schema-plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "p1")
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestOverrideSchemaPlan_MissingFieldName(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	overrides := []map[string]any{{"collection": "orders"}}
	body, _ := json.Marshal(overrides)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/p1/schema-plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "p1")
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestOverrideSchemaPlan_InvalidJSON(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/p1/schema-plan",
		bytes.NewBufferString("{not json}"))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", "p1")
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestOverrideSchemaPlan_MultipleOverrides(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-multi"
	seedPlan(t, db, projID, "orders")

	overrides := []map[string]any{
		{"collection": "orders", "field_name": "amount", "pg_type": "numeric", "strategy": "direct"},
		{"collection": "orders", "field_name": "status", "strategy": "skip"},
	}
	body, _ := json.Marshal(overrides)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/projects/"+projID+"/schema-plan", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", projID)
	w := httptest.NewRecorder()
	h.OverrideSchemaPlan(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body: %s", w.Code, w.Body.String())
	}

	rows, _ := db.GetPlan(context.Background(), projID, "orders")
	overriddenCount := 0
	for _, r := range rows {
		if r.Overridden {
			overriddenCount++
		}
	}
	if overriddenCount != 2 {
		t.Errorf("overridden count = %d, want 2", overriddenCount)
	}
}

// --------------------------------------------------------------------------
// DeleteSchemaPlan

func TestDeleteSchemaPlan_WipesAll(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-del"
	seedPlan(t, db, projID, "orders")
	seedPlan(t, db, projID, "customers")

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/projects/"+projID+"/schema-plan", nil)
	req.SetPathValue("id", projID)
	w := httptest.NewRecorder()
	h.DeleteSchemaPlan(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}

	colls, _ := db.ListPlanCollections(context.Background(), projID)
	if len(colls) != 0 {
		t.Errorf("collections after delete = %v, want empty", colls)
	}
}

func TestDeleteSchemaPlan_ScopedToCollection(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-scoped"
	seedPlan(t, db, projID, "orders")
	seedPlan(t, db, projID, "customers")

	req := httptest.NewRequest(http.MethodDelete,
		"/api/v1/projects/"+projID+"/schema-plan?collection=orders", nil)
	req.SetPathValue("id", projID)
	w := httptest.NewRecorder()
	h.DeleteSchemaPlan(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}

	colls, _ := db.ListPlanCollections(context.Background(), projID)
	if len(colls) != 1 || colls[0] != "customers" {
		t.Errorf("collections after scoped delete = %v, want [customers]", colls)
	}
}

func TestDeleteSchemaPlan_NoOpOnMissing(t *testing.T) {
	h, _, cleanup := newTestHandler(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/projects/ghost/schema-plan", nil)
	req.SetPathValue("id", "ghost")
	w := httptest.NewRecorder()
	h.DeleteSchemaPlan(w, req)

	// Deleting a non-existent plan is not an error.
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
}

// --------------------------------------------------------------------------
// Round-trip: seed → override → delete scoped → verify survivors

func TestSchemaPlan_RoundTrip(t *testing.T) {
	h, db, cleanup := newTestHandler(t)
	defer cleanup()

	const projID = "proj-rt"
	seedPlan(t, db, projID, "orders")
	seedPlan(t, db, projID, "products")

	// Override one field in orders.
	overrides := []map[string]any{
		{"collection": "orders", "field_name": "amount", "pg_type": "numeric", "strategy": "direct"},
	}
	oBody, _ := json.Marshal(overrides)
	putReq := httptest.NewRequest(http.MethodPut, "/api/v1/projects/"+projID+"/schema-plan", bytes.NewReader(oBody))
	putReq.Header.Set("Content-Type", "application/json")
	putReq.SetPathValue("id", projID)
	h.OverrideSchemaPlan(httptest.NewRecorder(), putReq)

	// Delete only the orders collection.
	delReq := httptest.NewRequest(http.MethodDelete,
		"/api/v1/projects/"+projID+"/schema-plan?collection=orders", nil)
	delReq.SetPathValue("id", projID)
	h.DeleteSchemaPlan(httptest.NewRecorder(), delReq)

	// GET should only return products.
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/projects/"+projID+"/schema-plan", nil)
	getReq.SetPathValue("id", projID)
	gw := httptest.NewRecorder()
	h.GetSchemaPlan(gw, getReq)

	var body map[string][]map[string]any
	json.NewDecoder(gw.Body).Decode(&body)

	if _, hasOrders := body["orders"]; hasOrders {
		t.Error("orders collection should have been deleted")
	}
	if _, hasProducts := body["products"]; !hasProducts {
		t.Error("products collection should still exist")
	}
}
