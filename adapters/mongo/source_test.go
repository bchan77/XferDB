package mongo

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ── encodeID / decodeLastKey ──────────────────────────────────────────────────

// #97: the resume key must give back an _id of the same BSON type, or the
// next page's $gt matches nothing and the rest of the collection is skipped.
func TestEncodeID_RoundTripKeepsType(t *testing.T) {
	oid := bson.NewObjectID()
	cases := []struct {
		name string
		id   interface{}
	}{
		{"ObjectId", oid},
		{"String", "order-123"},
		{"String that looks like ObjectId hex", oid.Hex()},
		{"String that looks like an int", "42"},
		{"Int32", int32(42)},
		{"Int64", int64(1) << 40},
		{"Double", 3.5},
		{"Compound", bson.D{{Key: "year", Value: int32(2026)}, {Key: "seq", Value: int32(4821)}}},
		{"UUID Binary", bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte("0123456789abcdef")}},
		{"Date", bson.NewDateTimeFromTime(time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC))},
	}
	// The plan's pg_type must not matter for new-format keys.
	plan := []state.SchemaPlanRow{{FieldName: "_id", PgType: "bigint"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, err := encodeID(tc.id)
			if err != nil {
				t.Fatalf("encodeID: %v", err)
			}
			got, err := decodeLastKey(key, plan)
			if err != nil {
				t.Fatalf("decodeLastKey(%q): %v", key, err)
			}
			if !reflect.DeepEqual(got, tc.id) {
				t.Errorf("round trip via %q = %#v (%T), want %#v (%T)", key, got, got, tc.id, tc.id)
			}
		})
	}
}

func TestDecodeLastKey_BadExtJSON(t *testing.T) {
	if _, err := decodeLastKey(extJSONKeyPrefix+"not json", nil); err == nil {
		t.Error("expected error for malformed resume key")
	}
}

// Keys checkpointed by older versions have no prefix and must still resume.
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
	got, err := decodeLastKey(key, nil)
	if err != nil {
		t.Fatalf("decodeLastKey: %v", err)
	}
	if got != oid {
		t.Errorf("decoded %v, want %v", got, oid)
	}
}

// ── laterIDTypes ──────────────────────────────────────────────────────────────

func TestLaterIDTypes(t *testing.T) {
	later := laterIDTypes(int64(5))
	has := func(alias string) bool {
		for _, a := range later {
			if a == alias {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"string", "object", "binData", "objectId"} {
		if !has(want) {
			t.Errorf("types after a number %v must include %q", later, want)
		}
	}
	for _, notWant := range []string{"int", "long", "double", "decimal", "null", "minKey"} {
		if has(notWant) {
			t.Errorf("types after a number %v must not include %q", later, notWant)
		}
	}
	if got := laterIDTypes(bson.MaxKey{}); got != nil {
		t.Errorf("nothing sorts after MaxKey, got %v", got)
	}
	if got := laterIDTypes(struct{}{}); got != nil {
		t.Errorf("unknown type must give nil, got %v", got)
	}
}

// Every Go type a decoded _id can have must belong to a type group, or a
// polymorphic collection would stop paging at that type.
func TestIDTypeGroup_OrderMatchesMongoSort(t *testing.T) {
	ordered := []interface{}{
		bson.MinKey{}, nil, int32(1), "s", bson.D{}, bson.Binary{},
		bson.NewObjectID(), true, bson.DateTime(0), bson.Timestamp{}, bson.Regex{}, bson.MaxKey{},
	}
	prev := -1
	for _, v := range ordered {
		g := idTypeGroup(v)
		if g <= prev {
			t.Errorf("idTypeGroup(%T) = %d, want > %d", v, g, prev)
		}
		prev = g
	}
	if len(ordered) != len(idTypeOrder) {
		t.Errorf("test covers %d groups, idTypeOrder has %d", len(ordered), len(idTypeOrder))
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

// XferDB#98.2: Postgres truncates identifiers at 63 bytes, so two collections
// agreeing over their first 63 bytes used to land in one table.
func TestTableName(t *testing.T) {
	short := "compat_users"
	if got := TableName(short); got != short {
		t.Errorf("TableName(%q) = %q, want it unchanged", short, got)
	}

	fits := strings.Repeat("a", maxPgIdentLen)
	if got := TableName(fits); got != fits {
		t.Errorf("a name of exactly %d bytes must be left alone, got %q", maxPgIdentLen, got)
	}

	prefix := strings.Repeat("b", maxPgIdentLen)
	one, two := TableName(prefix+"_one"), TableName(prefix+"_two")
	if len(one) != maxPgIdentLen || len(two) != maxPgIdentLen {
		t.Errorf("over-long names must be cut to %d bytes, got %d and %d",
			maxPgIdentLen, len(one), len(two))
	}
	if one == two {
		t.Errorf("names differing after byte %d must not collide, both gave %q",
			maxPgIdentLen, one)
	}
	// Checkpoints are keyed by this name, so it must not move between runs.
	if TableName(prefix+"_one") != one {
		t.Error("TableName must be deterministic")
	}
}

func TestCollectionFor_MapsTruncatedNameBack(t *testing.T) {
	collection := strings.Repeat("c", maxPgIdentLen) + "_long"
	s := &Source{}
	if err := s.SetPlan([]state.SchemaPlanRow{
		{Collection: collection, FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct"},
		{Collection: "short_one", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct"},
	}); err != nil {
		t.Fatalf("SetPlan: %v", err)
	}

	table := TableName(collection)
	if table == collection {
		t.Fatal("test needs a name long enough to be truncated")
	}
	if got := s.collectionFor(table); got != collection {
		t.Errorf("collectionFor(%q) = %q, want the full collection name", table, got)
	}
	// A name that needed no truncation maps to itself.
	if got := s.collectionFor("short_one"); got != "short_one" {
		t.Errorf("collectionFor(\"short_one\") = %q, want it unchanged", got)
	}
	// An unknown name is returned as-is rather than erroring.
	if got := s.collectionFor("never_seen"); got != "never_seen" {
		t.Errorf("collectionFor(\"never_seen\") = %q, want it unchanged", got)
	}
}

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
