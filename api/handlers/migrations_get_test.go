package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestGetMigrationIncludesTables: the migration page of a finished migration
// has no stats collector to ask, and showed "No tables yet"; the record now
// carries each table's outcome from migration_tables.
func TestGetMigrationIncludesTables(t *testing.T) {
	_, db, cleanup := newTestHandler(t)
	defer cleanup()
	ctx := context.Background()
	if err := db.CreateProject(ctx, &adapters.Project{ID: "p", Name: "p"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateMigration(ctx, &adapters.Migration{ID: "m", ProjectID: "p", Status: adapters.StatusCompleted, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, tp := range []adapters.TableProgress{
		{TableName: "loadtest.orders", Status: adapters.StatusCompleted, RowsTotal: 1200000, RowsTransferred: 1200000},
		{TableName: "loadtest.products", Status: adapters.StatusFailed, RowsTotal: 10000, RowsTransferred: 4000},
	} {
		if err := db.UpsertTableProgress(ctx, "m", tp); err != nil {
			t.Fatal(err)
		}
	}
	h := &MigrationsHandler{DB: db}
	get := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/migrations/"+id, nil)
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		h.GetMigration(rec, req)
		return rec
	}
	rec := get("m")
	var out struct {
		ID     string                   `json:"id"`
		Status string                   `json:"status"`
		Tables []adapters.TableProgress `json:"tables"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("status %d, %v: %s", rec.Code, err, rec.Body)
	}
	if out.ID != "m" || out.Status != "completed" || len(out.Tables) != 2 {
		t.Fatalf("got %+v, want migration m with 2 tables", out)
	}
	byName := map[string]adapters.TableProgress{}
	for _, tp := range out.Tables {
		byName[tp.TableName] = tp
	}
	if tp := byName["loadtest.products"]; tp.Status != adapters.StatusFailed || tp.RowsTransferred != 4000 {
		t.Errorf("products = %+v", tp)
	}
	if rec := get("nope"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown migration: status %d", rec.Code)
	}
}
