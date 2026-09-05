package mongo

import (
	"encoding/json"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

func makePlan(rows ...state.SchemaPlanRow) []state.SchemaPlanRow { return rows }

func row(field, col, pgType, strategy string) state.SchemaPlanRow {
	return state.SchemaPlanRow{FieldName: field, PgColumn: col, PgType: pgType, Strategy: strategy}
}

// ── ConvertDocument ───────────────────────────────────────────────────────────

func TestConvertDocument_DirectScalars(t *testing.T) {
	oid := bson.NewObjectID()
	doc := bson.D{
		{Key: "_id", Value: oid},
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: int32(30)},
		{Key: "score", Value: float64(9.5)},
		{Key: "active", Value: true},
	}
	plan := makePlan(
		row("_id", "_id", "text", "direct"),
		row("name", "name", "text", "direct"),
		row("age", "age", "integer", "direct"),
		row("score", "score", "double precision", "direct"),
		row("active", "active", "boolean", "direct"),
	)

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}

	if result["_id"] != oid.Hex() {
		t.Errorf("_id = %v, want %q", result["_id"], oid.Hex())
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %v, want \"Alice\"", result["name"])
	}
	if result["age"] != int64(30) {
		t.Errorf("age = %v (%T), want int64(30)", result["age"], result["age"])
	}
	if result["score"] != float64(9.5) {
		t.Errorf("score = %v, want 9.5", result["score"])
	}
	if result["active"] != true {
		t.Errorf("active = %v, want true", result["active"])
	}
}

func TestConvertDocument_SkipStrategy(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: "abc"},
		{Key: "internal", Value: "secret"},
	}
	plan := makePlan(
		row("_id", "_id", "text", "direct"),
		row("internal", "internal", "", "skip"),
	)

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}
	if _, ok := result["internal"]; ok {
		t.Error("skipped field must not appear in result")
	}
}

func TestConvertDocument_AsJsonbStrategy(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: "x"},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	}
	plan := makePlan(
		row("_id", "_id", "text", "direct"),
		row("address", "address", "jsonb", "as_jsonb"),
	)

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}

	// as_jsonb now returns string (not []byte) for PostgreSQL COPY compatibility.
	raw, ok := result["address"].(string)
	if !ok {
		t.Fatalf("address is %T, want string", result["address"])
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("address JSON invalid: %v", err)
	}
	if m["city"] != "NYC" {
		t.Errorf("address.city = %v, want \"NYC\"", m["city"])
	}
}

func TestConvertDocument_FlattenStrategy(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: "x"},
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	}
	plan := makePlan(
		row("_id", "_id", "text", "direct"),
		row("address", "", "", "flatten"),
		row("address.city", "address_city", "text", "direct"),
		row("address.zip", "address_zip", "text", "direct"),
	)

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}
	if result["address_city"] != "NYC" {
		t.Errorf("address_city = %v, want \"NYC\"", result["address_city"])
	}
	if result["address_zip"] != "10001" {
		t.Errorf("address_zip = %v, want \"10001\"", result["address_zip"])
	}
	// Parent flatten field must not appear as its own column.
	if _, ok := result["address"]; ok {
		t.Error("flatten parent must not appear as a column")
	}
}

func TestConvertDocument_UnknownFieldGoesToExtra(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: "x"},
		{Key: "surprise", Value: "unexpected"},
	}
	plan := makePlan(
		row("_id", "_id", "text", "direct"),
	)

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}

	// _extra now returns string (not []byte) for PostgreSQL COPY compatibility.
	raw, ok := result["_extra"].(string)
	if !ok {
		t.Fatalf("_extra is %T, want string", result["_extra"])
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("_extra JSON invalid: %v", err)
	}
	if m["surprise"] != "unexpected" {
		t.Errorf("_extra.surprise = %v, want \"unexpected\"", m["surprise"])
	}
}

func TestConvertDocument_NoExtraWhenAllFieldsKnown(t *testing.T) {
	doc := bson.D{{Key: "_id", Value: "x"}}
	plan := makePlan(row("_id", "_id", "text", "direct"))

	result, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}
	if _, ok := result["_extra"]; ok {
		t.Error("_extra must not appear when all fields are in the plan")
	}
}

// ── bsonToAny ─────────────────────────────────────────────────────────────────

func TestBsonToAny_NestedDocumentBecomesMap(t *testing.T) {
	v := bsonToAny(bson.D{{Key: "k", Value: "v"}})
	m, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("bsonToAny(bson.D) = %T, want map[string]interface{}", v)
	}
	if m["k"] != "v" {
		t.Errorf("m[\"k\"] = %v, want \"v\"", m["k"])
	}
}

func TestBsonToAny_ArrayBecomesSlice(t *testing.T) {
	v := bsonToAny(bson.A{"a", "b"})
	s, ok := v.([]interface{})
	if !ok {
		t.Fatalf("bsonToAny(bson.A) = %T, want []interface{}", v)
	}
	if len(s) != 2 {
		t.Errorf("slice len = %d, want 2", len(s))
	}
}

func TestBsonToAny_ObjectIDBecomesHex(t *testing.T) {
	oid := bson.NewObjectID()
	v := bsonToAny(oid)
	if v != oid.Hex() {
		t.Errorf("ObjectID = %v, want %q", v, oid.Hex())
	}
}

func TestBsonToAny_DateTimeBecomesRFC3339(t *testing.T) {
	dt := bson.DateTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	v := bsonToAny(dt)
	s, ok := v.(string)
	if !ok {
		t.Fatalf("DateTime = %T, want string", v)
	}
	if s == "" {
		t.Error("DateTime string must not be empty")
	}
}

func TestBsonToAny_Int32BecomesInt64(t *testing.T) {
	v := bsonToAny(int32(42))
	if v != int64(42) {
		t.Errorf("int32 = %v (%T), want int64(42)", v, v)
	}
}

// ── JSON safety: bson.D must not marshal as key-value array ──────────────────

func TestJsonMarshalBSON_DocumentIsObject(t *testing.T) {
	doc := bson.D{{Key: "city", Value: "NYC"}, {Key: "zip", Value: "10001"}}
	b, err := jsonMarshalBSON(doc)
	if err != nil {
		t.Fatalf("jsonMarshalBSON: %v", err)
	}
	// Must decode as an object, not an array.
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("result is not a JSON object: %v — raw: %s", err, b)
	}
	if m["city"] != "NYC" {
		t.Errorf("city = %v, want \"NYC\"", m["city"])
	}
}
