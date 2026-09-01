package commands

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --------------------------------------------------------------------------
// runSchemaSet parsing

func TestRunSchemaSet_PgType(t *testing.T) {
	var captured []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		captured, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	ServerAddr = ts.URL

	err := runSchemaSet("proj-1", []string{"orders.amount=numeric"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var overrides []map[string]any
	if err := json.Unmarshal(captured, &overrides); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(overrides) != 1 {
		t.Fatalf("len(overrides) = %d, want 1", len(overrides))
	}
	o := overrides[0]
	if o["collection"] != "orders" {
		t.Errorf("collection = %v, want \"orders\"", o["collection"])
	}
	if o["field_name"] != "amount" {
		t.Errorf("field_name = %v, want \"amount\"", o["field_name"])
	}
	if o["pg_type"] != "numeric" {
		t.Errorf("pg_type = %v, want \"numeric\"", o["pg_type"])
	}
	// Non-strategy values always get strategy=direct.
	if o["strategy"] != "direct" {
		t.Errorf("strategy = %v, want \"direct\"", o["strategy"])
	}
}

func TestRunSchemaSet_Strategy(t *testing.T) {
	var captured []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	ServerAddr = ts.URL

	err := runSchemaSet("proj-1", []string{"orders.meta=as_jsonb"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var overrides []map[string]any
	json.Unmarshal(captured, &overrides)

	o := overrides[0]
	if o["strategy"] != "as_jsonb" {
		t.Errorf("strategy = %v, want \"as_jsonb\"", o["strategy"])
	}
	// Strategy values should not set pg_type.
	if _, hasPgType := o["pg_type"]; hasPgType {
		t.Errorf("pg_type should be absent when value is a strategy, got %v", o["pg_type"])
	}
}

func TestRunSchemaSet_AllStrategies(t *testing.T) {
	for _, strat := range []string{"direct", "as_jsonb", "flatten", "skip"} {
		t.Run(strat, func(t *testing.T) {
			var captured []byte
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer ts.Close()
			ServerAddr = ts.URL

			if err := runSchemaSet("p", []string{"coll.field=" + strat}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var overrides []map[string]any
			json.Unmarshal(captured, &overrides)
			if overrides[0]["strategy"] != strat {
				t.Errorf("strategy = %v, want %q", overrides[0]["strategy"], strat)
			}
		})
	}
}

func TestRunSchemaSet_MultipleFlags(t *testing.T) {
	var captured []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	ServerAddr = ts.URL

	err := runSchemaSet("proj-1", []string{
		"orders.amount=numeric",
		"orders.status=skip",
		"products.price=double precision",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var overrides []map[string]any
	json.Unmarshal(captured, &overrides)
	if len(overrides) != 3 {
		t.Errorf("len(overrides) = %d, want 3", len(overrides))
	}
}

func TestRunSchemaSet_MissingEquals(t *testing.T) {
	err := runSchemaSet("p", []string{"orders.amount"})
	if err == nil {
		t.Fatal("expected error for missing '='")
	}
	if !strings.Contains(err.Error(), "collection.field=value") {
		t.Errorf("error %q should describe expected format", err.Error())
	}
}

func TestRunSchemaSet_MissingDot(t *testing.T) {
	err := runSchemaSet("p", []string{"orders=numeric"})
	if err == nil {
		t.Fatal("expected error for missing '.'")
	}
	if !strings.Contains(err.Error(), "collection.field") {
		t.Errorf("error %q should describe expected format", err.Error())
	}
}

func TestRunSchemaSet_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"db unavailable"}`))
	}))
	defer ts.Close()
	ServerAddr = ts.URL

	err := runSchemaSet("proj-1", []string{"orders.amount=numeric"})
	if err == nil {
		t.Fatal("expected error on server 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error %q should mention HTTP status", err.Error())
	}
}

// --------------------------------------------------------------------------
// printMongoAnalysis output

func TestPrintMongoAnalysis_NoCollections(t *testing.T) {
	// Should not panic on empty collections list.
	printMongoAnalysis(map[string]any{
		"type":        "mongo_infer",
		"collections": []any{},
	})
}

func TestPrintMongoAnalysis_CollectionWithNoSchema(t *testing.T) {
	// A collection that failed sampling has warnings instead of a schema.
	printMongoAnalysis(map[string]any{
		"type": "mongo_infer",
		"collections": []any{
			map[string]any{
				"collection": "broken",
				"warnings":   []any{"sampling failed: connection refused"},
			},
		},
	})
}

func TestPrintMongoAnalysis_FullCollection(t *testing.T) {
	printMongoAnalysis(map[string]any{
		"type": "mongo_infer",
		"collections": []any{
			map[string]any{
				"collection": "orders",
				"schema": map[string]any{
					"fields": []any{
						map[string]any{
							"plan_row": map[string]any{
								"field_name": "_id",
								"pg_type":    "text",
								"strategy":   "direct",
							},
							"frequency": map[string]any{
								"total_docs":  float64(100),
								"occurrences": float64(100),
								"polymorphic": false,
							},
						},
						map[string]any{
							"plan_row": map[string]any{
								"field_name": "meta",
								"pg_type":    "jsonb",
								"strategy":   "as_jsonb",
							},
							"frequency": map[string]any{
								"total_docs":  float64(100),
								"occurrences": float64(80),
								"polymorphic": true,
							},
						},
					},
				},
			},
		},
	})
}
