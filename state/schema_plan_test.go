package state

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func openTestDB(t *testing.T) (*MetaDB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "xferdb-state-test-*.db")
	if err != nil {
		t.Fatalf("create temp db file: %v", err)
	}
	f.Close()

	db, err := Open(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open state db: %v", err)
	}
	return db, func() {
		db.Close()
		os.Remove(f.Name())
	}
}

func seedProject(t *testing.T, db *MetaDB) string {
	t.Helper()
	ctx := context.Background()
	proj := &adapters.Project{
		ID:        "test-proj-id",
		Name:      "test-project",
		CreatedAt: time.Now(),
	}
	if err := db.CreateProject(ctx, proj); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	return proj.ID
}

func seedMigration(t *testing.T, db *MetaDB, migID, projID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	if err := db.CreateMigration(ctx, &adapters.Migration{
		ID: migID, ProjectID: projID,
		Status: adapters.StatusPending, CreatedAt: now,
	}); err != nil {
		t.Fatalf("seed migration: %v", err)
	}
}

// ── SavePlan / GetPlan ───────────────────────────────────────────────────────

func TestSavePlan_RoundTrip(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	rows := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true, Nullable: false},
		{ProjectID: projID, Collection: "orders", FieldName: "total",
			PgColumn: "total", PgType: "numeric", Strategy: "direct", IsPK: false, Nullable: false},
		{ProjectID: projID, Collection: "orders", FieldName: "address",
			PgColumn: "address", PgType: "jsonb", Strategy: "as_jsonb", IsPK: false, Nullable: true},
	}

	if err := db.SavePlan(ctx, rows); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	got, err := db.GetPlan(ctx, projID, "orders")
	if err != nil {
		t.Fatalf("GetPlan: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("GetPlan returned %d rows, want %d", len(got), len(rows))
	}

	// Results are sorted by field_name: _id, address, total.
	assertField := func(row SchemaPlanRow, wantField, wantPgType, wantStrategy string, wantPK, wantNullable bool) {
		t.Helper()
		if row.FieldName != wantField {
			t.Errorf("FieldName = %q, want %q", row.FieldName, wantField)
		}
		if row.PgType != wantPgType {
			t.Errorf("%s.PgType = %q, want %q", wantField, row.PgType, wantPgType)
		}
		if row.Strategy != wantStrategy {
			t.Errorf("%s.Strategy = %q, want %q", wantField, row.Strategy, wantStrategy)
		}
		if row.IsPK != wantPK {
			t.Errorf("%s.IsPK = %v, want %v", wantField, row.IsPK, wantPK)
		}
		if row.Nullable != wantNullable {
			t.Errorf("%s.Nullable = %v, want %v", wantField, row.Nullable, wantNullable)
		}
	}

	assertField(got[0], "_id", "text", "direct", true, false)
	assertField(got[1], "address", "jsonb", "as_jsonb", false, true)
	assertField(got[2], "total", "numeric", "direct", false, false)
}

func TestSavePlan_EmptySliceIsNoOp(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	if err := db.SavePlan(ctx, nil); err != nil {
		t.Fatalf("SavePlan(nil): %v", err)
	}
	got, err := db.GetPlan(ctx, projID, "orders")
	if err != nil {
		t.Fatalf("GetPlan: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 rows, got %d", len(got))
	}
}

func TestSavePlan_ReanalyzeDoesNotClobberOverridden(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	initial := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "total",
			PgColumn: "total", PgType: "double precision", Strategy: "direct"},
	}
	if err := db.SavePlan(ctx, initial); err != nil {
		t.Fatalf("initial SavePlan: %v", err)
	}

	// User overrides: changes type to numeric; marks overridden.
	if err := db.OverridePlan(ctx, projID, "orders", "total", SchemaPlanOverride{
		PgColumn: ptr("total"), PgType: ptr("numeric"), Strategy: ptr("direct"),
	}); err != nil {
		t.Fatalf("OverridePlan: %v", err)
	}

	// Re-analyze: SavePlan with the original inference again.
	if err := db.SavePlan(ctx, initial); err != nil {
		t.Fatalf("re-analyze SavePlan: %v", err)
	}

	got, err := db.GetPlan(ctx, projID, "orders")
	if err != nil {
		t.Fatalf("GetPlan after re-analyze: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0].PgType != "numeric" {
		t.Errorf("PgType = %q, want \"numeric\" (override must survive re-analyze)", got[0].PgType)
	}
	if !got[0].Overridden {
		t.Errorf("Overridden = false, want true")
	}
}

// ── OverridePlan ─────────────────────────────────────────────────────────────

func TestOverridePlan_SetsOverriddenFlag(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	if err := db.SavePlan(ctx, []SchemaPlanRow{
		{ProjectID: projID, Collection: "c", FieldName: "f",
			PgColumn: "f", PgType: "text", Strategy: "direct"},
	}); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	if err := db.OverridePlan(ctx, projID, "c", "f", SchemaPlanOverride{
		PgColumn: ptr("f"), PgType: ptr("jsonb"), Strategy: ptr("as_jsonb"),
	}); err != nil {
		t.Fatalf("OverridePlan: %v", err)
	}

	got, err := db.GetPlan(ctx, projID, "c")
	if err != nil || len(got) != 1 {
		t.Fatalf("GetPlan: err=%v rows=%d", err, len(got))
	}
	if got[0].PgType != "jsonb" {
		t.Errorf("PgType = %q, want \"jsonb\"", got[0].PgType)
	}
	if !got[0].Overridden {
		t.Error("Overridden = false, want true")
	}
}

// TestOverridePlan_CreatesRowWithoutPriorSavePlan covers relational tables,
// which have no SavePlan-seeded row (only the Mongo analyze path seeds one) —
// the first override for a table must still land.
func TestOverridePlan_CreatesRowWithoutPriorSavePlan(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	if err := db.OverridePlan(ctx, projID, "orders", "total", SchemaPlanOverride{
		PgColumn: ptr("total"), PgType: ptr("numeric"), Strategy: ptr("direct"),
		IsPK: ptr(false), Nullable: ptr(true),
	}); err != nil {
		t.Fatalf("OverridePlan: %v", err)
	}

	got, err := db.GetPlan(ctx, projID, "orders")
	if err != nil || len(got) != 1 {
		t.Fatalf("GetPlan: err=%v rows=%d", err, len(got))
	}
	if got[0].PgType != "numeric" {
		t.Errorf("PgType = %q, want \"numeric\"", got[0].PgType)
	}
	if !got[0].Overridden {
		t.Error("Overridden = false, want true")
	}
}

// TestOverridePlan_PartialKeepsUnsentFields is #113: `project schema --set
// ids_uuid._id=uuid` sends only pg_type and strategy, and used to blank
// pg_column (CREATE TABLE then failed on a "" column) and clear is_pk.
func TestOverridePlan_PartialKeepsUnsentFields(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	if err := db.SavePlan(ctx, []SchemaPlanRow{
		{ProjectID: projID, Collection: "ids_uuid", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true, Nullable: false},
		{ProjectID: projID, Collection: "ids_uuid", FieldName: "user",
			PgColumn: "user", PgType: "text", Strategy: "direct", Nullable: true},
	}); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	for field, pgType := range map[string]string{"_id": "uuid", "user": "varchar(80)"} {
		if err := db.OverridePlan(ctx, projID, "ids_uuid", field, SchemaPlanOverride{
			PgType: ptr(pgType), Strategy: ptr("direct"),
		}); err != nil {
			t.Fatalf("OverridePlan %s: %v", field, err)
		}
	}

	got, err := db.GetPlan(ctx, projID, "ids_uuid")
	if err != nil || len(got) != 2 {
		t.Fatalf("GetPlan: err=%v rows=%d", err, len(got))
	}
	id, user := got[0], got[1] // ordered by field_name
	if id.PgColumn != "_id" || id.PgType != "uuid" || !id.IsPK || id.Nullable || !id.Overridden {
		t.Errorf("_id = %+v, want pg_column _id, pg_type uuid, is_pk, NOT NULL, overridden", id)
	}
	if user.PgColumn != "user" || user.PgType != "varchar(80)" || user.IsPK || !user.Nullable {
		t.Errorf("user = %+v, want pg_column user, pg_type varchar(80), nullable, not PK", user)
	}
}

// TestOverridePlan_FullRowCanClearFlags covers the web UI, which always sends
// every field: an explicit false must still clear is_pk/nullable.
func TestOverridePlan_FullRowCanClearFlags(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	if err := db.SavePlan(ctx, []SchemaPlanRow{
		{ProjectID: projID, Collection: "c", FieldName: "f",
			PgColumn: "f", PgType: "text", Strategy: "direct", IsPK: true, Nullable: true},
	}); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}
	if err := db.OverridePlan(ctx, projID, "c", "f", SchemaPlanOverride{
		PgColumn: ptr("f_renamed"), PgType: ptr("integer"), Strategy: ptr("direct"),
		IsPK: ptr(false), Nullable: ptr(false),
	}); err != nil {
		t.Fatalf("OverridePlan: %v", err)
	}

	got, err := db.GetPlan(ctx, projID, "c")
	if err != nil || len(got) != 1 {
		t.Fatalf("GetPlan: err=%v rows=%d", err, len(got))
	}
	if r := got[0]; r.PgColumn != "f_renamed" || r.PgType != "integer" || r.IsPK || r.Nullable {
		t.Errorf("row = %+v, want every sent field applied", r)
	}
}

// TestOverridePlan_NewRowNeedsPKAndNullable: with no analyzed row to fall back
// on, guessing is_pk/nullable would silently drop a PK or add NOT NULL on a
// relational table (the engine applies both at CREATE TABLE).
func TestOverridePlan_NewRowNeedsPKAndNullable(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	err := db.OverridePlan(ctx, projID, "orders", "total", SchemaPlanOverride{
		PgType: ptr("numeric"), Strategy: ptr("direct"),
	})
	if !errors.Is(err, ErrIncompleteOverride) {
		t.Fatalf("err = %v, want ErrIncompleteOverride", err)
	}
	if got, _ := db.GetPlan(ctx, projID, "orders"); len(got) != 0 {
		t.Errorf("rejected override still wrote %d row(s)", len(got))
	}

	// With the flags given, pg_column defaults to the field name.
	if err := db.OverridePlan(ctx, projID, "orders", "total", SchemaPlanOverride{
		PgType: ptr("numeric"), IsPK: ptr(false), Nullable: ptr(true),
	}); err != nil {
		t.Fatalf("OverridePlan: %v", err)
	}
	got, err := db.GetPlan(ctx, projID, "orders")
	if err != nil || len(got) != 1 {
		t.Fatalf("GetPlan: err=%v rows=%d", err, len(got))
	}
	if r := got[0]; r.PgColumn != "total" || r.Strategy != "direct" || !r.Nullable {
		t.Errorf("row = %+v, want pg_column total, strategy direct, nullable", r)
	}
}

func ptr[T any](v T) *T { return &v }

// ── ListPlanCollections ──────────────────────────────────────────────────────

func TestListPlanCollections(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	rows := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
		{ProjectID: projID, Collection: "customers", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
		{ProjectID: projID, Collection: "orders", FieldName: "total",
			PgColumn: "total", PgType: "numeric", Strategy: "direct"},
	}
	if err := db.SavePlan(ctx, rows); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	cols, err := db.ListPlanCollections(ctx, projID)
	if err != nil {
		t.Fatalf("ListPlanCollections: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("got %d collections, want 2: %v", len(cols), cols)
	}
	if cols[0] != "customers" || cols[1] != "orders" {
		t.Errorf("collections = %v, want [customers orders]", cols)
	}
}

func TestListPlanCollections_EmptyWhenNoPlans(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	cols, err := db.ListPlanCollections(ctx, projID)
	if err != nil {
		t.Fatalf("ListPlanCollections: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("expected 0 collections, got %v", cols)
	}
}

// ── DeletePlan ───────────────────────────────────────────────────────────────

func TestDeletePlan_WipesOneCollection(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	rows := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
		{ProjectID: projID, Collection: "customers", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
	}
	if err := db.SavePlan(ctx, rows); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	if err := db.DeletePlan(ctx, projID, "orders"); err != nil {
		t.Fatalf("DeletePlan: %v", err)
	}

	cols, err := db.ListPlanCollections(ctx, projID)
	if err != nil {
		t.Fatalf("ListPlanCollections: %v", err)
	}
	if len(cols) != 1 || cols[0] != "customers" {
		t.Errorf("after DeletePlan(orders), collections = %v, want [customers]", cols)
	}
}

func TestDeleteAllPlans(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	rows := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
		{ProjectID: projID, Collection: "customers", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct"},
	}
	if err := db.SavePlan(ctx, rows); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	if err := db.DeleteAllPlans(ctx, projID); err != nil {
		t.Fatalf("DeleteAllPlans: %v", err)
	}

	cols, err := db.ListPlanCollections(ctx, projID)
	if err != nil {
		t.Fatalf("ListPlanCollections: %v", err)
	}
	if len(cols) != 0 {
		t.Errorf("after DeleteAllPlans, expected 0 collections, got %v", cols)
	}
}

// ── SnapshotPlan / GetMigrationPlan ─────────────────────────────────────────

func TestSnapshotPlan_FreezesCopyOnMigration(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	rows := []SchemaPlanRow{
		{ProjectID: projID, Collection: "orders", FieldName: "_id",
			PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
		{ProjectID: projID, Collection: "orders", FieldName: "total",
			PgColumn: "total", PgType: "numeric", Strategy: "direct"},
	}
	if err := db.SavePlan(ctx, rows); err != nil {
		t.Fatalf("SavePlan: %v", err)
	}

	migID := "mig-snapshot-test"
	seedMigration(t, db, migID, projID)

	if err := db.SnapshotPlan(ctx, migID, projID); err != nil {
		t.Fatalf("SnapshotPlan: %v", err)
	}

	// Override the live plan after the snapshot — the frozen copy must be unaffected.
	if err := db.OverridePlan(ctx, projID, "orders", "total", SchemaPlanOverride{
		PgColumn: ptr("total"), PgType: ptr("double precision"), Strategy: ptr("direct"),
	}); err != nil {
		t.Fatalf("OverridePlan after snapshot: %v", err)
	}

	snapshot, err := db.GetMigrationPlan(ctx, migID)
	if err != nil {
		t.Fatalf("GetMigrationPlan: %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("snapshot has %d rows, want 2", len(snapshot))
	}

	var totalRow *SchemaPlanRow
	for i := range snapshot {
		if snapshot[i].FieldName == "total" {
			totalRow = &snapshot[i]
		}
	}
	if totalRow == nil {
		t.Fatal("snapshot missing 'total' field")
	}
	// Frozen at snapshot time — must reflect "numeric", not the later override.
	if totalRow.PgType != "numeric" {
		t.Errorf("snapshot PgType = %q, want \"numeric\" (frozen copy must not reflect later overrides)", totalRow.PgType)
	}
}

func TestGetMigrationPlan_ReturnsNilForRelationalMigration(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	projID := seedProject(t, db)

	migID := "mig-no-plan"
	seedMigration(t, db, migID, projID)

	// No SnapshotPlan call — schema_plan column stays empty.
	plan, err := db.GetMigrationPlan(ctx, migID)
	if err != nil {
		t.Fatalf("GetMigrationPlan: %v", err)
	}
	if plan != nil {
		t.Errorf("expected nil plan for relational migration, got %v", plan)
	}
}
