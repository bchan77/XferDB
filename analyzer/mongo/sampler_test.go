package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ── walkDocument ─────────────────────────────────────────────────────────────

func TestWalkDocument_ScalarTypes(t *testing.T) {
	doc := bson.D{
		{Key: "_id", Value: bson.ObjectID{}},
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: int32(30)},
		{Key: "score", Value: float64(9.5)},
		{Key: "active", Value: true},
	}

	freq := make(map[string]*fieldAcc)
	walkDocument(doc, "", freq)

	cases := []struct {
		field    string
		wantType string
	}{
		{"_id", "ObjectId"},
		{"name", "String"},
		{"age", "Int32"},
		{"score", "Double"},
		{"active", "Boolean"},
	}
	for _, c := range cases {
		acc, ok := freq[c.field]
		if !ok {
			t.Errorf("field %q not found in freq", c.field)
			continue
		}
		if acc.occurrences != 1 {
			t.Errorf("%q occurrences = %d, want 1", c.field, acc.occurrences)
		}
		if acc.typeCounts[c.wantType] != 1 {
			t.Errorf("%q type %q count = %d, want 1 (types: %v)", c.field, c.wantType, acc.typeCounts[c.wantType], acc.typeCounts)
		}
	}
}

func TestWalkDocument_NullValue(t *testing.T) {
	doc := bson.D{
		{Key: "notes", Value: nil},
	}
	freq := make(map[string]*fieldAcc)
	walkDocument(doc, "", freq)

	acc := freq["notes"]
	if acc == nil {
		t.Fatal("field 'notes' not in freq")
	}
	if acc.nullCount != 1 {
		t.Errorf("nullCount = %d, want 1", acc.nullCount)
	}
	if acc.occurrences != 0 {
		t.Errorf("occurrences = %d, want 0 (null is not an occurrence)", acc.occurrences)
	}
}

func TestWalkDocument_NestedDocument_DotNotation(t *testing.T) {
	doc := bson.D{
		{Key: "address", Value: bson.D{
			{Key: "city", Value: "NYC"},
			{Key: "zip", Value: "10001"},
		}},
	}
	freq := make(map[string]*fieldAcc)
	walkDocument(doc, "", freq)

	// Parent field recorded as Document type.
	if freq["address"] == nil || freq["address"].typeCounts["Document"] != 1 {
		t.Errorf("address not recorded as Document: %v", freq["address"])
	}
	// Child fields recorded with dot notation.
	if freq["address.city"] == nil || freq["address.city"].typeCounts["String"] != 1 {
		t.Errorf("address.city not recorded: %v", freq["address.city"])
	}
	if freq["address.zip"] == nil || freq["address.zip"].typeCounts["String"] != 1 {
		t.Errorf("address.zip not recorded: %v", freq["address.zip"])
	}
}

func TestWalkDocument_ArrayNotRecursed(t *testing.T) {
	doc := bson.D{
		{Key: "tags", Value: bson.A{"promo", "vip"}},
	}
	freq := make(map[string]*fieldAcc)
	walkDocument(doc, "", freq)

	acc := freq["tags"]
	if acc == nil {
		t.Fatal("field 'tags' not in freq")
	}
	if acc.typeCounts["Array"] != 1 {
		t.Errorf("tags type = %v, want Array count 1", acc.typeCounts)
	}
	// Array elements must not produce their own entries.
	for k := range freq {
		if k != "tags" {
			t.Errorf("unexpected field %q — arrays must not be recursed", k)
		}
	}
}

func TestWalkDocument_MultipleDocuments_Accumulates(t *testing.T) {
	// Simulate two documents being walked into the same freq map.
	doc1 := bson.D{
		{Key: "x", Value: "hello"},
		{Key: "y", Value: int32(1)},
	}
	doc2 := bson.D{
		{Key: "x", Value: "world"},
		// y is absent
	}

	freq := make(map[string]*fieldAcc)
	walkDocument(doc1, "", freq)
	walkDocument(doc2, "", freq)

	if freq["x"].occurrences != 2 {
		t.Errorf("x occurrences = %d, want 2", freq["x"].occurrences)
	}
	if freq["y"].occurrences != 1 {
		t.Errorf("y occurrences = %d, want 1 (only in doc1)", freq["y"].occurrences)
	}
}

// ── fieldAcc.typeList ─────────────────────────────────────────────────────────

func TestFieldAcc_TypeList_SortedByCountDesc(t *testing.T) {
	acc := &fieldAcc{
		typeCounts: map[string]int{
			"String":  5,
			"Int32":   3,
			"Double": 10,
		},
	}
	list := acc.typeList()
	if len(list) != 3 {
		t.Fatalf("typeList len = %d, want 3", len(list))
	}
	if list[0].TypeName != "Double" || list[0].Count != 10 {
		t.Errorf("list[0] = %+v, want {Double 10}", list[0])
	}
	if list[1].TypeName != "String" || list[1].Count != 5 {
		t.Errorf("list[1] = %+v, want {String 5}", list[1])
	}
	if list[2].TypeName != "Int32" || list[2].Count != 3 {
		t.Errorf("list[2] = %+v, want {Int32 3}", list[2])
	}
}

// ── shouldSkipCollection ──────────────────────────────────────────────────────

func TestShouldSkipCollection(t *testing.T) {
	cases := []struct {
		name     string
		collType string
		want     bool
	}{
		{"orders", "collection", false},
		{"customers", "collection", false},
		{"system.users", "collection", true},
		{"system.version", "collection", true},
		{"fs.files", "collection", true},
		{"fs.chunks", "collection", true},
		{"my_view", "view", true},
		{"sensor_data", "timeseries", true},
		{"logs.files_metadata", "collection", false}, // contains "files" but not as suffix without "."
		{"myfiles", "collection", false},              // ends with "files" but no preceding dot
	}
	for _, c := range cases {
		got := shouldSkipCollection(c.name, c.collType)
		if got != c.want {
			t.Errorf("shouldSkipCollection(%q, %q) = %v, want %v", c.name, c.collType, got, c.want)
		}
	}
}

// ── sortFields ───────────────────────────────────────────────────────────────

func TestSortFields_IdFirst(t *testing.T) {
	fields := []FieldFrequency{
		{FieldName: "total"},
		{FieldName: "_id"},
		{FieldName: "address"},
	}
	sortFields(fields)

	if fields[0].FieldName != "_id" {
		t.Errorf("first field = %q, want \"_id\"", fields[0].FieldName)
	}
	if fields[1].FieldName != "address" {
		t.Errorf("second field = %q, want \"address\"", fields[1].FieldName)
	}
	if fields[2].FieldName != "total" {
		t.Errorf("third field = %q, want \"total\"", fields[2].FieldName)
	}
}

func TestSortFields_AlphabeticWithoutId(t *testing.T) {
	fields := []FieldFrequency{
		{FieldName: "zebra"},
		{FieldName: "apple"},
		{FieldName: "mango"},
	}
	sortFields(fields)

	want := []string{"apple", "mango", "zebra"}
	for i, w := range want {
		if fields[i].FieldName != w {
			t.Errorf("fields[%d] = %q, want %q", i, fields[i].FieldName, w)
		}
	}
}

// ── extractDatabase ───────────────────────────────────────────────────────────

func TestExtractDatabase(t *testing.T) {
	cases := []struct {
		dsn     string
		wantDB  string
		wantErr bool
	}{
		{"mongodb://host:27017/mydb", "mydb", false},
		{"mongodb://user:pass@host:27017/mydb?replicaSet=rs0", "mydb", false},
		{"mongodb+srv://user:pass@cluster.example.net/mydb", "mydb", false},
		{"mongodb://host:27017/mydb?retryWrites=true", "mydb", false},
		// No database → error
		{"mongodb://host:27017", "", true},
		{"mongodb://host:27017/", "", true},
	}
	for _, c := range cases {
		db, err := extractDatabase(c.dsn)
		if c.wantErr {
			if err == nil {
				t.Errorf("extractDatabase(%q) expected error, got %q", c.dsn, db)
			}
			continue
		}
		if err != nil {
			t.Errorf("extractDatabase(%q) unexpected error: %v", c.dsn, err)
			continue
		}
		if db != c.wantDB {
			t.Errorf("extractDatabase(%q) = %q, want %q", c.dsn, db, c.wantDB)
		}
	}
}

// ── FieldFrequency.CoveragePct ────────────────────────────────────────────────

func TestCoveragePct(t *testing.T) {
	ff := FieldFrequency{Occurrences: 1500, TotalDocs: 2000}
	got := ff.CoveragePct()
	if got != 75.0 {
		t.Errorf("CoveragePct() = %v, want 75.0", got)
	}
}

func TestCoveragePct_ZeroTotal(t *testing.T) {
	ff := FieldFrequency{Occurrences: 10, TotalDocs: 0}
	if got := ff.CoveragePct(); got != 0 {
		t.Errorf("CoveragePct() with 0 total = %v, want 0", got)
	}
}

// ── polymorphic detection (via SampleCollection logic, unit-tested directly) ──

func TestPolymorphicDetection(t *testing.T) {
	// Simulate two documents with the same field holding different types.
	freq := make(map[string]*fieldAcc)
	doc1 := bson.D{{Key: "val", Value: float64(1.5)}}
	doc2 := bson.D{{Key: "val", Value: bson.Decimal128{}}}
	walkDocument(doc1, "", freq)
	walkDocument(doc2, "", freq)

	acc := freq["val"]
	list := acc.typeList()

	nonNullTypes := 0
	for _, tc := range list {
		if tc.TypeName != "Null" {
			nonNullTypes++
		}
	}
	if nonNullTypes <= 1 {
		t.Errorf("expected polymorphic field (>1 non-null types), got %d: %v", nonNullTypes, list)
	}
}

// ── bsonTypeName ─────────────────────────────────────────────────────────────

func TestBsonTypeName_KnownTypes(t *testing.T) {
	cases := []struct {
		value    interface{}
		wantName string
	}{
		{"hello", "String"},
		{int32(1), "Int32"},
		{int64(1), "Int64"},
		{float64(1.0), "Double"},
		{true, "Boolean"},
		{bson.D{}, "Document"},
		{bson.A{}, "Array"},
		{bson.ObjectID{}, "ObjectId"},
		{bson.DateTime(0), "Date"},
		{bson.Binary{}, "Binary"},
		{bson.Decimal128{}, "Decimal128"},
		{nil, "Null"},
	}
	for _, c := range cases {
		got := bsonTypeName(c.value)
		if got != c.wantName {
			t.Errorf("bsonTypeName(%T) = %q, want %q", c.value, got, c.wantName)
		}
	}
}
