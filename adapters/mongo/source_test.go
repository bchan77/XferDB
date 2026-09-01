package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ── encodeID / decodeLastKey ──────────────────────────────────────────────────

func TestEncodeID_ObjectId(t *testing.T) {
	oid := bson.NewObjectID()
	key, err := encodeID(oid)
	if err != nil {
		t.Fatalf("encodeID: %v", err)
	}
	if key != oid.Hex() {
		t.Errorf("got %q, want %q", key, oid.Hex())
	}
}

func TestEncodeID_String(t *testing.T) {
	key, err := encodeID("order-123")
	if err != nil {
		t.Fatalf("encodeID: %v", err)
	}
	if key != "order-123" {
		t.Errorf("got %q, want \"order-123\"", key)
	}
}

func TestEncodeID_Int32(t *testing.T) {
	key, err := encodeID(int32(42))
	if err != nil {
		t.Fatalf("encodeID: %v", err)
	}
	if key != "42" {
		t.Errorf("got %q, want \"42\"", key)
	}
}

func TestDecodeLastKey_ObjectIdHex(t *testing.T) {
	oid := bson.NewObjectID()
	plan := []state.SchemaPlanRow{{FieldName: "_id", PgType: "text"}}
	v, err := decodeLastKey(oid.Hex(), plan)
	if err != nil {
		t.Fatalf("decodeLastKey: %v", err)
	}
	decoded, ok := v.(bson.ObjectID)
	if !ok {
		t.Fatalf("decoded type = %T, want bson.ObjectID", v)
	}
	if decoded != oid {
		t.Errorf("decoded = %v, want %v", decoded, oid)
	}
}

func TestDecodeLastKey_IntType(t *testing.T) {
	plan := []state.SchemaPlanRow{{FieldName: "_id", PgType: "bigint"}}
	v, err := decodeLastKey("99", plan)
	if err != nil {
		t.Fatalf("decodeLastKey: %v", err)
	}
	if v != int64(99) {
		t.Errorf("decoded = %v (%T), want int64(99)", v, v)
	}
}

func TestDecodeLastKey_PlainString(t *testing.T) {
	plan := []state.SchemaPlanRow{{FieldName: "_id", PgType: "text"}}
	v, err := decodeLastKey("my-custom-id", plan)
	if err != nil {
		t.Fatalf("decodeLastKey: %v", err)
	}
	if v != "my-custom-id" {
		t.Errorf("decoded = %v, want \"my-custom-id\"", v)
	}
}

// ── encodeLastKey ─────────────────────────────────────────────────────────────

func TestEncodeLastKey_ExtractsID(t *testing.T) {
	oid := bson.NewObjectID()
	doc := bson.D{
		{Key: "name", Value: "test"},
		{Key: "_id", Value: oid},
	}
	key, err := encodeLastKey(doc)
	if err != nil {
		t.Fatalf("encodeLastKey: %v", err)
	}
	if key != oid.Hex() {
		t.Errorf("got %q, want %q", key, oid.Hex())
	}
}

func TestEncodeLastKey_MissingID(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	_, err := encodeLastKey(doc)
	if err == nil {
		t.Error("expected error for document without _id")
	}
}

// ── SetPlan ───────────────────────────────────────────────────────────────────

func TestSetPlan_BuildsCollectionIndex(t *testing.T) {
	src := &Source{}
	plan := []state.SchemaPlanRow{
		{Collection: "orders", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
		{Collection: "orders", FieldName: "total", PgColumn: "total", PgType: "numeric", Strategy: "direct"},
		{Collection: "customers", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
	}
	if err := src.SetPlan(plan); err != nil {
		t.Fatalf("SetPlan: %v", err)
	}
	if len(src.planByCollection["orders"]) != 2 {
		t.Errorf("orders plan rows = %d, want 2", len(src.planByCollection["orders"]))
	}
	if len(src.planByCollection["customers"]) != 1 {
		t.Errorf("customers plan rows = %d, want 1", len(src.planByCollection["customers"]))
	}
}

// ── requirePlan ───────────────────────────────────────────────────────────────

func TestRequirePlan_FailsBeforeSetPlan(t *testing.T) {
	src := &Source{}
	if err := src.requirePlan(); err == nil {
		t.Error("requirePlan must return error before SetPlan is called")
	}
}

func TestRequirePlan_PassesAfterSetPlan(t *testing.T) {
	src := &Source{}
	src.SetPlan(nil) //nolint:errcheck
	if err := src.requirePlan(); err != nil {
		t.Errorf("requirePlan after SetPlan: %v", err)
	}
}

// ── shouldSkipCollection ──────────────────────────────────────────────────────

func TestShouldSkipCollectionInSource(t *testing.T) {
	if !shouldSkipCollection("system.users", "collection") {
		t.Error("system.users must be skipped")
	}
	if !shouldSkipCollection("fs.files", "collection") {
		t.Error("fs.files must be skipped")
	}
	if shouldSkipCollection("orders", "collection") {
		t.Error("orders must not be skipped")
	}
}
