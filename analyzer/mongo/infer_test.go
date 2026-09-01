package mongo

import (
	"strings"
	"testing"
)

// ── sanitizeName ─────────────────────────────────────────────────────────────

func TestSanitizeName_ValidName(t *testing.T) {
	seen := map[string]bool{}
	if got := sanitizeName("customer_id", seen); got != "customer_id" {
		t.Errorf("got %q, want \"customer_id\"", got)
	}
}

func TestSanitizeName_LeadingDigit(t *testing.T) {
	seen := map[string]bool{}
	got := sanitizeName("1field", seen)
	if strings.HasPrefix(got, "_") == false {
		t.Errorf("leading digit must produce underscore prefix, got %q", got)
	}
}

func TestSanitizeName_InvalidChars(t *testing.T) {
	seen := map[string]bool{}
	got := sanitizeName("my-field.name", seen)
	for _, r := range got {
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			t.Errorf("sanitized name %q contains invalid char %q", got, r)
		}
	}
}

func TestSanitizeName_TruncateTo63(t *testing.T) {
	seen := map[string]bool{}
	long := strings.Repeat("a", 100)
	got := sanitizeName(long, seen)
	if len(got) > 63 {
		t.Errorf("sanitized name length = %d, want <= 63", len(got))
	}
}

func TestSanitizeName_CollisionSuffix(t *testing.T) {
	seen := map[string]bool{"address_city": true}
	got := sanitizeName("address_city", seen)
	if got != "address_city_2" {
		t.Errorf("got %q, want \"address_city_2\"", got)
	}
}

func TestSanitizeName_MultipleCollisions(t *testing.T) {
	seen := map[string]bool{"x": true, "x_2": true, "x_3": true}
	got := sanitizeName("x", seen)
	if got != "x_4" {
		t.Errorf("got %q, want \"x_4\"", got)
	}
}

func TestSanitizeName_DotNotationToUnderscore(t *testing.T) {
	seen := map[string]bool{}
	// pgColumnName converts dots before sanitize is called.
	got := sanitizeName(pgColumnName("address.city"), seen)
	if got != "address_city" {
		t.Errorf("got %q, want \"address_city\"", got)
	}
}

// ── singleTypeMapping ────────────────────────────────────────────────────────

func TestSingleTypeMapping_AllTypes(t *testing.T) {
	cases := []struct {
		bsonType    string
		wantPgType  string
		wantStrategy string
	}{
		{"String", "text", "direct"},
		{"Int32", "integer", "direct"},
		{"Int64", "bigint", "direct"},
		{"Double", "double precision", "direct"},
		{"Decimal128", "numeric", "direct"},
		{"Boolean", "boolean", "direct"},
		{"Date", "timestamptz", "direct"},
		{"ObjectId", "text", "direct"},
		{"Binary", "bytea", "direct"},
		{"Array", "jsonb", "as_jsonb"},
		{"Document", "jsonb", "as_jsonb"},
	}
	for _, c := range cases {
		pgType, strategy, opts, _ := singleTypeMapping(c.bsonType)
		if pgType != c.wantPgType {
			t.Errorf("%s: pgType = %q, want %q", c.bsonType, pgType, c.wantPgType)
		}
		if strategy != c.wantStrategy {
			t.Errorf("%s: strategy = %q, want %q", c.bsonType, strategy, c.wantStrategy)
		}
		if len(opts) == 0 {
			t.Errorf("%s: options must not be empty", c.bsonType)
		}
	}
}

func TestSingleTypeMapping_DocumentIncludesFlattenOption(t *testing.T) {
	_, _, opts, _ := singleTypeMapping("Document")
	hasFlatten := false
	for _, o := range opts {
		if o.Strategy == "flatten" {
			hasFlatten = true
		}
	}
	if !hasFlatten {
		t.Error("Document options must include flatten strategy")
	}
}

// ── widenTypes ───────────────────────────────────────────────────────────────

func makeFF(types ...string) FieldFrequency {
	typeCounts := make([]TypeCount, len(types))
	for i, t := range types {
		typeCounts[i] = TypeCount{TypeName: t, Count: 10}
	}
	return FieldFrequency{FieldName: "f", Types: typeCounts, Polymorphic: true, TotalDocs: 100}
}

func TestWidenTypes_Int32AndInt64(t *testing.T) {
	pg, strategy, _, _ := widenTypes(makeFF("Int32", "Int64"))
	if pg != "bigint" || strategy != "direct" {
		t.Errorf("Int32+Int64 → got %q/%q, want bigint/direct", pg, strategy)
	}
}

func TestWidenTypes_DoubleAndDecimal128(t *testing.T) {
	pg, _, _, _ := widenTypes(makeFF("Double", "Decimal128"))
	if pg != "numeric" {
		t.Errorf("Double+Decimal128 → got %q, want numeric", pg)
	}
}

func TestWidenTypes_Int32Double(t *testing.T) {
	pg, _, _, _ := widenTypes(makeFF("Int32", "Double"))
	if pg != "double precision" {
		t.Errorf("Int32+Double → got %q, want double precision", pg)
	}
}

func TestWidenTypes_NumericPlusString(t *testing.T) {
	pg, strategy, _, _ := widenTypes(makeFF("Int32", "String"))
	if pg != "text" || strategy != "direct" {
		t.Errorf("Int32+String → got %q/%q, want text/direct", pg, strategy)
	}
}

func TestWidenTypes_AnyPlusDocument(t *testing.T) {
	pg, strategy, _, _ := widenTypes(makeFF("String", "Document"))
	if pg != "jsonb" || strategy != "as_jsonb" {
		t.Errorf("String+Document → got %q/%q, want jsonb/as_jsonb", pg, strategy)
	}
}

func TestWidenTypes_WarningIncluded(t *testing.T) {
	_, _, _, warns := widenTypes(makeFF("Int32", "String"))
	if len(warns) == 0 {
		t.Error("polymorphic field must produce at least one warning")
	}
}

// ── inferIDType ──────────────────────────────────────────────────────────────

func TestInferIDType_ObjectId(t *testing.T) {
	pg, _, warns := inferIDType([]TypeCount{{TypeName: "ObjectId", Count: 2000}})
	if pg != "text" {
		t.Errorf("ObjectId _id → %q, want text", pg)
	}
	if len(warns) > 0 {
		t.Errorf("ObjectId _id must have no warnings, got %v", warns)
	}
}

func TestInferIDType_DocumentWarning(t *testing.T) {
	_, _, warns := inferIDType([]TypeCount{{TypeName: "Document", Count: 100}})
	if len(warns) == 0 {
		t.Error("compound _id must produce a warning")
	}
}

// ── InferSchema end-to-end ───────────────────────────────────────────────────

func TestInferSchema_BasicCollection(t *testing.T) {
	sample := &CollectionSample{
		Collection:     "orders",
		EstimatedCount: 10000,
		SampleSize:     100,
		Fields: []FieldFrequency{
			{FieldName: "_id", Occurrences: 100, TotalDocs: 100,
				Types: []TypeCount{{TypeName: "ObjectId", Count: 100}}},
			{FieldName: "total", Occurrences: 100, TotalDocs: 100,
				Types: []TypeCount{{TypeName: "Double", Count: 100}}},
			{FieldName: "notes", Occurrences: 60, NullCount: 5, AbsentCount: 35, TotalDocs: 100,
				Types: []TypeCount{{TypeName: "String", Count: 60}}},
			{FieldName: "address", Occurrences: 100, TotalDocs: 100,
				Types: []TypeCount{{TypeName: "Document", Count: 100}}},
		},
	}

	schema, err := InferSchema(sample, "proj-1")
	if err != nil {
		t.Fatalf("InferSchema: %v", err)
	}

	if schema.Collection != "orders" {
		t.Errorf("Collection = %q, want \"orders\"", schema.Collection)
	}

	// Verify _id is PK and not nullable.
	var idField *InferredField
	for i := range schema.Fields {
		if schema.Fields[i].PlanRow.FieldName == "_id" {
			idField = &schema.Fields[i]
		}
	}
	if idField == nil {
		t.Fatal("_id field not found in InferredSchema")
	}
	if !idField.PlanRow.IsPK {
		t.Error("_id must be primary key")
	}
	if idField.PlanRow.Nullable {
		t.Error("_id must not be nullable")
	}
	if idField.PlanRow.PgType != "text" {
		t.Errorf("_id PgType = %q, want text", idField.PlanRow.PgType)
	}

	// notes is nullable (absent+null).
	var notesField *InferredField
	for i := range schema.Fields {
		if schema.Fields[i].PlanRow.FieldName == "notes" {
			notesField = &schema.Fields[i]
		}
	}
	if notesField == nil {
		t.Fatal("notes field not found")
	}
	if !notesField.PlanRow.Nullable {
		t.Error("notes must be nullable (has absent+null count)")
	}

	// TableSchema must include _extra jsonb column.
	hasExtra := false
	for _, col := range schema.TableSchema.Columns {
		if col.Name == "_extra" && col.Type == "jsonb" {
			hasExtra = true
		}
	}
	if !hasExtra {
		t.Error("TableSchema must include _extra jsonb column")
	}
}

func TestInferSchema_ColumnCollision(t *testing.T) {
	// address.city and a top-level address_city would produce the same pg_column name.
	sample := &CollectionSample{
		Collection: "test",
		SampleSize: 10,
		Fields: []FieldFrequency{
			{FieldName: "_id", Occurrences: 10, TotalDocs: 10,
				Types: []TypeCount{{TypeName: "ObjectId", Count: 10}}},
			{FieldName: "address_city", Occurrences: 10, TotalDocs: 10,
				Types: []TypeCount{{TypeName: "String", Count: 10}}},
			// address.city → pg_column candidate "address_city" — collision!
			{FieldName: "address.city", Occurrences: 10, TotalDocs: 10,
				Types: []TypeCount{{TypeName: "String", Count: 10}}},
		},
	}

	schema, err := InferSchema(sample, "proj-1")
	if err != nil {
		t.Fatalf("InferSchema: %v", err)
	}

	colNames := make(map[string]int)
	for _, col := range schema.TableSchema.Columns {
		colNames[col.Name]++
	}
	for name, count := range colNames {
		if count > 1 {
			t.Errorf("duplicate pg_column %q in TableSchema", name)
		}
	}
}

func TestInferSchema_PlanRowsCount(t *testing.T) {
	sample := &CollectionSample{
		Collection: "c",
		SampleSize: 5,
		Fields: []FieldFrequency{
			{FieldName: "_id", Occurrences: 5, TotalDocs: 5, Types: []TypeCount{{TypeName: "ObjectId", Count: 5}}},
			{FieldName: "x", Occurrences: 5, TotalDocs: 5, Types: []TypeCount{{TypeName: "String", Count: 5}}},
		},
	}
	schema, _ := InferSchema(sample, "p")
	rows := schema.PlanRows()
	if len(rows) != 2 {
		t.Errorf("PlanRows() = %d, want 2", len(rows))
	}
}
