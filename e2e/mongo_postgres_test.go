// Package e2e contains end-to-end integration tests.
//
// These tests require live databases and are skipped when the environment
// variables are not set:
//
//	XFERDB_TEST_MONGO_DSN  -- e.g. mongodb://localhost:27017/testdb
//	XFERDB_TEST_PG_DSN     -- e.g. postgres://user:pass@localhost/testdb?sslmode=disable
//
// Run with:
//
//	XFERDB_TEST_MONGO_DSN=... XFERDB_TEST_PG_DSN=... go test ./e2e/... -v -timeout 120s
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/api"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

const testCollection = "xferdb_e2e_orders"

// TestMongoToPostgres seeds a MongoDB collection, runs the full analyze →
// migrate pipeline through the XferDB REST API, then verifies that every
// seeded document landed in PostgreSQL with the expected field values.
func TestMongoToPostgres(t *testing.T) {
	mongoDSN := os.Getenv("XFERDB_TEST_MONGO_DSN")
	pgDSN := os.Getenv("XFERDB_TEST_PG_DSN")
	if mongoDSN == "" || pgDSN == "" {
		t.Skip("XFERDB_TEST_MONGO_DSN and XFERDB_TEST_PG_DSN not set — skipping e2e test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ------------------------------------------------------------------ seed
	t.Log("seeding MongoDB...")
	seedDocs, cleanup := seedMongo(t, ctx, mongoDSN)
	defer cleanup()
	t.Logf("seeded %d documents in collection %q", len(seedDocs), testCollection)

	// ----------------------------------------------------------- in-process API
	t.Log("starting in-process API server...")
	stateDB := openStateDB(t)
	defer stateDB.Close()

	srv := api.NewServer(stateDB, slog.Default())
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	base := ts.URL

	// --------------------------------------------------------- create project
	t.Log("creating project via API...")
	projID := createProject(t, base, mongoDSN, pgDSN)
	t.Logf("project id: %s", projID)

	// ---------------------------------------------------------------- analyze
	t.Log("running analyze (infer schema)...")
	analyzeProject(t, base, projID)

	// --------------------------------------------------------------- migrate
	t.Log("starting migration...")
	migID := startMigration(t, base, projID)
	t.Logf("migration id: %s", migID)

	// ------------------------------------------------------------------ poll
	t.Log("waiting for migration to complete...")
	waitForMigration(t, ctx, base, migID)

	// --------------------------------------------------------------- verify
	t.Log("verifying rows in PostgreSQL...")
	verifyPostgres(t, pgDSN, seedDocs)

	t.Log("e2e test passed")
}

// --------------------------------------------------------------------------
// helpers

type orderDoc struct {
	ID     bson.ObjectID `bson:"_id"`
	Amount float64       `bson:"amount"`
	Status string        `bson:"status"`
}

// seedMongo inserts test documents and returns them plus a cleanup function.
func seedMongo(t *testing.T, ctx context.Context, dsn string) ([]orderDoc, func()) {
	t.Helper()

	client, err := mongo.Connect(options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("mongo ping: %v", err)
	}

	// Extract database name from DSN (last path segment).
	dbName := "test"
	if u, err := parseDBName(dsn); err == nil && u != "" {
		dbName = u
	}

	coll := client.Database(dbName).Collection(testCollection)

	// Drop any leftover data from a previous failed run.
	_ = coll.Drop(ctx)

	docs := []orderDoc{
		{ID: bson.NewObjectID(), Amount: 19.99, Status: "pending"},
		{ID: bson.NewObjectID(), Amount: 49.00, Status: "paid"},
		{ID: bson.NewObjectID(), Amount: 5.50, Status: "pending"},
		{ID: bson.NewObjectID(), Amount: 120.00, Status: "shipped"},
		{ID: bson.NewObjectID(), Amount: 0.99, Status: "refunded"},
	}

	var ifaces []interface{}
	for _, d := range docs {
		ifaces = append(ifaces, d)
	}
	if _, err := coll.InsertMany(ctx, ifaces); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	cleanup := func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = coll.Drop(dropCtx)
		_ = client.Disconnect(dropCtx)
	}
	return docs, cleanup
}

// openStateDB creates a temp file-backed MetaDB for the test server.
func openStateDB(t *testing.T) *state.MetaDB {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "xferdb-e2e-*.db")
	if err != nil {
		t.Fatalf("create temp state db: %v", err)
	}
	f.Close()
	db, err := state.Open(f.Name())
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	return db
}

// createProject POSTs to /api/v1/projects and returns the project ID.
func createProject(t *testing.T, base, mongoDSN, pgDSN string) string {
	t.Helper()

	body := map[string]any{
		"name":        "e2e-mongo-pg",
		"description": "e2e test project",
		"source_config": map[string]any{
			"type": "mongodb",
			"dsn":  mongoDSN,
		},
		"target_config": map[string]any{
			"type": "postgres",
			"dsn":  pgDSN,
		},
		"transfer_config": map[string]any{
			"batch_size":    100,
			"table_workers": 1,
		},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(base+"/api/v1/projects", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST /projects: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create project: status %d, error: %v", resp.StatusCode, result["error"])
	}
	id, _ := result["id"].(string)
	if id == "" {
		t.Fatal("create project returned empty id")
	}
	return id
}

// analyzeProject POSTs to /analyze and asserts it succeeds.
func analyzeProject(t *testing.T, base, projID string) {
	t.Helper()
	body := map[string]any{"sample_size": 200}
	data, _ := json.Marshal(body)
	resp, err := http.Post(base+"/api/v1/projects/"+projID+"/analyze", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST /analyze: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("analyze: status %d, error: %v", resp.StatusCode, result["error"])
	}

	colls, _ := result["collections"].([]any)
	t.Logf("analyze returned %d collections", len(colls))

	// Confirm our test collection appears in the results.
	found := false
	for _, c := range colls {
		m, _ := c.(map[string]any)
		if m["collection"] == testCollection {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("analyze result did not include collection %q", testCollection)
	}
}

// startMigration POSTs to /migrations and returns the migration ID.
func startMigration(t *testing.T, base, projID string) string {
	t.Helper()

	body := map[string]any{
		"recreate_schema": true,
		"tables":          []string{testCollection},
	}
	data, _ := json.Marshal(body)
	resp, err := http.Post(
		base+"/api/v1/projects/"+projID+"/migrations",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		t.Fatalf("POST /migrations: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]any
	json.NewDecoder(resp.Body).Decode(&result)

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("start migration: status %d, error: %v", resp.StatusCode, result["error"])
	}
	id, _ := result["id"].(string)
	if id == "" {
		t.Fatal("start migration returned empty id")
	}
	return id
}

// waitForMigration polls the stats endpoint until the migration completes or fails.
func waitForMigration(t *testing.T, ctx context.Context, base, migID string) {
	t.Helper()
	url := fmt.Sprintf("%s/api/v1/migrations/%s/stats", base, migID)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for migration to complete")
		case <-time.After(time.Second):
		}

		resp, err := http.Get(url)
		if err != nil {
			t.Logf("stats poll: %v (retrying)", err)
			continue
		}
		var snap map[string]any
		json.NewDecoder(resp.Body).Decode(&snap)
		resp.Body.Close()

		phase, _ := snap["phase"].(string)
		t.Logf("migration phase: %s", phase)

		switch phase {
		case "complete":
			return
		case "failed":
			errs, _ := snap["errors"].([]any)
			t.Fatalf("migration failed: %v", errs)
		}
	}
}

// verifyPostgres queries PostgreSQL and asserts every seeded document is present.
func verifyPostgres(t *testing.T, pgDSN string, docs []orderDoc) {
	t.Helper()

	db, err := sql.Open("postgres", pgDSN)
	if err != nil {
		t.Fatalf("sql.Open postgres: %v", err)
	}
	defer db.Close()

	// Confirm row count.
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + testCollection).Scan(&count); err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if count != len(docs) {
		t.Errorf("postgres row count = %d, want %d", count, len(docs))
	}

	// Spot-check each document by _id.
	for _, d := range docs {
		var amount float64
		var status string
		err := db.QueryRow(
			`SELECT amount, status FROM `+testCollection+` WHERE _id = $1`,
			d.ID.Hex(),
		).Scan(&amount, &status)
		if err != nil {
			t.Errorf("row for _id=%s: %v", d.ID.Hex(), err)
			continue
		}
		if amount != d.Amount {
			t.Errorf("_id=%s amount = %v, want %v", d.ID.Hex(), amount, d.Amount)
		}
		if status != d.Status {
			t.Errorf("_id=%s status = %q, want %q", d.ID.Hex(), status, d.Status)
		}
	}
}

// parseDBName extracts the database name from a MongoDB DSN path.
func parseDBName(dsn string) (string, error) {
	// Find the path segment after the last slash in the authority.
	// mongodb://host/dbname or mongodb+srv://host/dbname
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			rest := dsn[i+1:]
			// Strip query string if present.
			if qi := len(rest); qi > 0 {
				for j, c := range rest {
					if c == '?' {
						rest = rest[:j]
						break
					}
				}
			}
			if rest != "" {
				return rest, nil
			}
		}
	}
	return "", fmt.Errorf("no database name in DSN")
}
