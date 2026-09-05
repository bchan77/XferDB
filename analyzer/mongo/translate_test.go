package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

func planWithFields(fields ...string) []state.SchemaPlanRow {
	rows := make([]state.SchemaPlanRow, len(fields))
	for i, f := range fields {
		rows[i] = state.SchemaPlanRow{FieldName: f, PgColumn: f, Strategy: "direct"}
	}
	return rows
}

func makeSpec(name string, key bson.D, extra ...bson.E) bson.D {
	spec := bson.D{
		{Key: "name", Value: name},
		{Key: "key", Value: key},
	}
	spec = append(spec, extra...)
	return spec
}

// ── _id_ index skipped ───────────────────────────────────────────────────────

func TestTranslate_IDIndexSkipped(t *testing.T) {
	specs := []bson.D{makeSpec("_id_", bson.D{{Key: "_id", Value: int32(1)}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("_id"))
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Index != nil {
		t.Error("_id_ index must be skipped (Index should be nil)")
	}
	if len(results[0].Warnings) != 0 {
		t.Errorf("_id_ skip must produce no warnings, got %v", results[0].Warnings)
	}
}

// ── single-field ascending ───────────────────────────────────────────────────

func TestTranslate_SingleFieldAscending(t *testing.T) {
	specs := []bson.D{makeSpec("email_1", bson.D{{Key: "email", Value: int32(1)}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("_id", "email"))

	if results[0].Index == nil {
		t.Fatal("expected translated index, got nil")
	}
	if results[0].Index.Name != "email_1" {
		t.Errorf("Name = %q, want \"email_1\"", results[0].Index.Name)
	}
	if len(results[0].Index.Columns) != 1 || results[0].Index.Columns[0] != "email" {
		t.Errorf("Columns = %v, want [email]", results[0].Index.Columns)
	}
	if results[0].Index.Unique {
		t.Error("index must not be unique")
	}
}

// ── unique index ─────────────────────────────────────────────────────────────

func TestTranslate_UniqueIndex(t *testing.T) {
	specs := []bson.D{makeSpec("email_1", bson.D{{Key: "email", Value: int32(1)}},
		bson.E{Key: "unique", Value: true})}
	results := TranslateRawIndexSpecs(specs, planWithFields("_id", "email"))

	if results[0].Index == nil {
		t.Fatal("expected index")
	}
	if !results[0].Index.Unique {
		t.Error("Unique = false, want true")
	}
}

// ── compound index ───────────────────────────────────────────────────────────

func TestTranslate_CompoundIndex(t *testing.T) {
	specs := []bson.D{makeSpec("a_1_b_1", bson.D{
		{Key: "a", Value: int32(1)},
		{Key: "b", Value: int32(1)},
	})}
	results := TranslateRawIndexSpecs(specs, planWithFields("_id", "a", "b"))

	if results[0].Index == nil {
		t.Fatal("expected index")
	}
	if len(results[0].Index.Columns) != 2 {
		t.Errorf("Columns = %v, want [a b]", results[0].Index.Columns)
	}
}

// ── descending → warning ─────────────────────────────────────────────────────

func TestTranslate_DescendingProducesWarning(t *testing.T) {
	specs := []bson.D{makeSpec("score_-1", bson.D{{Key: "score", Value: int32(-1)}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("_id", "score"))

	if results[0].Index == nil {
		t.Fatal("expected translated index (descending is allowed with warning)")
	}
	if len(results[0].Warnings) == 0 {
		t.Error("descending index must produce a warning")
	}
}

// ── unsupported types → skip with warning ────────────────────────────────────

func TestTranslate_TextIndexSkipped(t *testing.T) {
	specs := []bson.D{makeSpec("desc_text", bson.D{{Key: "description", Value: "text"}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("description"))

	if results[0].Index != nil {
		t.Error("text index must be skipped")
	}
	if len(results[0].Warnings) == 0 {
		t.Error("text index skip must produce a warning")
	}
}

func TestTranslate_TTLIndexSkipped(t *testing.T) {
	specs := []bson.D{makeSpec("expires_ttl",
		bson.D{{Key: "expires_at", Value: int32(1)}},
		bson.E{Key: "expireAfterSeconds", Value: int32(3600)})}
	results := TranslateRawIndexSpecs(specs, planWithFields("expires_at"))

	if results[0].Index != nil {
		t.Error("TTL index must be skipped")
	}
	if len(results[0].Warnings) == 0 {
		t.Error("TTL skip must produce a warning")
	}
}

func TestTranslate_HashedIndexSkipped(t *testing.T) {
	specs := []bson.D{makeSpec("user_hashed", bson.D{{Key: "user_id", Value: "hashed"}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("user_id"))

	if results[0].Index != nil {
		t.Error("hashed index must be skipped")
	}
}

func TestTranslate_FieldNotInPlanSkipsIndex(t *testing.T) {
	specs := []bson.D{makeSpec("x_1", bson.D{{Key: "x", Value: int32(1)}})}
	// Plan does not include "x" — field was skipped.
	results := TranslateRawIndexSpecs(specs, planWithFields("_id"))

	if results[0].Index != nil {
		t.Error("index on skipped field must be skipped")
	}
	if len(results[0].Warnings) == 0 {
		t.Error("skipped-field index must produce a warning")
	}
}

func TestTranslate_NestedPathSkipped(t *testing.T) {
	// "address.city" is a sub-path — jsonb nested path, cannot translate in v1.
	specs := []bson.D{makeSpec("addr_city_1", bson.D{{Key: "address.city", Value: int32(1)}})}
	results := TranslateRawIndexSpecs(specs, planWithFields("address.city"))

	if results[0].Index != nil {
		t.Error("nested path index must be skipped in v1")
	}
	if len(results[0].Warnings) == 0 {
		t.Error("nested path skip must produce a warning")
	}
}
