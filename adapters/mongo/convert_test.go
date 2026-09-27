package mongo

import (
	"encoding/json"
	"reflect"
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

// ── convertBinary (#97.3) ─────────────────────────────────────────────────────

func TestConvertBinary(t *testing.T) {
	uuidBytes := []byte{0x5f, 0x8d, 0x0b, 0x3c, 0x9a, 0x1e, 0x4f, 0x2b, 0x8c, 0x6d, 0x7e, 0x9f, 0x0a, 0x1b, 0x2c, 0x3d}
	uuid4 := bson.Binary{Subtype: bson.TypeBinaryUUID, Data: uuidBytes}
	generic := bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x8d, 0x00, 0xff}}
	const uuidText = "5f8d0b3c-9a1e-4f2b-8c6d-7e9f0a1b2c3d"

	cases := []struct {
		name   string
		v      bson.Binary
		pgType string
		want   interface{}
	}{
		{"uuid column", uuid4, "uuid", uuidText},
		{"uuid column, legacy subtype 3", bson.Binary{Subtype: bson.TypeBinaryUUIDOld, Data: uuidBytes}, "uuid", uuidText},
		{"text column, uuid subtype", uuid4, "text", uuidText},
		{"text column, generic subtype", generic, "text", "8d00ff"},
		{"bytea column keeps raw bytes", generic, "bytea", []byte{0x8d, 0x00, 0xff}},
		{"bytea column, uuid subtype keeps raw bytes", uuid4, "BYTEA", uuidBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertScalar(tc.v, tc.pgType)
			if err != nil {
				t.Fatalf("convertScalar: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestConvertBinary_UUIDColumnRejectsNonUUID(t *testing.T) {
	for _, v := range []bson.Binary{
		{Subtype: bson.TypeBinaryGeneric, Data: make([]byte, 16)},
		{Subtype: bson.TypeBinaryUUID, Data: make([]byte, 15)},
	} {
		if _, err := convertScalar(v, "uuid"); err == nil {
			t.Errorf("subtype %d, %d bytes: expected error for uuid column", v.Subtype, len(v.Data))
		}
	}
}

// The ids_uuid case from XferDB-jenkins: Binary _id overridden to uuid.
func TestConvertDocument_BinaryUUIDPrimaryKey(t *testing.T) {
	uuidBytes := []byte("0123456789abcdef")
	doc := bson.D{
		{Key: "_id", Value: bson.Binary{Subtype: bson.TypeBinaryUUID, Data: uuidBytes}},
		{Key: "user", Value: "alice"},
	}
	plan := makePlan(row("_id", "_id", "uuid", "direct"), row("user", "user", "text", "direct"))
	out, err := ConvertDocument(doc, plan, "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}
	if want := "30313233-3435-3637-3839-616263646566"; out["_id"] != want {
		t.Errorf("_id = %#v, want %q", out["_id"], want)
	}
}

// The ids_compound case: an embedded-document _id as a jsonb primary key.
func TestConvertDocument_CompoundIDAsJsonb(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: bson.D{{Key: "year", Value: int32(2026)}, {Key: "seq", Value: int32(4821)}}},
	}
	out, err := ConvertDocument(doc, makePlan(row("_id", "_id", "jsonb", "as_jsonb")), "_extra")
	if err != nil {
		t.Fatalf("ConvertDocument: %v", err)
	}
	s, ok := out["_id"].(string)
	if !ok {
		t.Fatalf("_id = %T, want JSON string", out["_id"])
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(s), &m); err != nil || m["year"] != float64(2026) || m["seq"] != float64(4821) {
		t.Errorf("_id = %s, want {year:2026, seq:4821} (err %v)", s, err)
	}
}
