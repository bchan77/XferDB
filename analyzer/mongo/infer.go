package mongo

import (
	"fmt"
	"strings"
	"unicode"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

const maxPgIdentLen = 63

// FieldOption is one alternative type+strategy mapping a user can select.
type FieldOption struct {
	PgType   string `json:"pg_type"`
	Strategy string `json:"strategy"`
}

// InferredField is the full inference result for one field.
type InferredField struct {
	Frequency FieldFrequency    `json:"frequency"`
	PlanRow   state.SchemaPlanRow `json:"plan_row"` // recommended mapping
	Options   []FieldOption       `json:"options"`
	Warnings  []string            `json:"warnings,omitempty"`
}

// InferredSchema is the full inference result for one collection.
type InferredSchema struct {
	Collection  string              `json:"collection"`
	Fields      []InferredField     `json:"fields"`
	TableSchema adapters.TableSchema `json:"table_schema"`
}

// PlanRows extracts just the SchemaPlanRows, ready for state.SavePlan.
func (s *InferredSchema) PlanRows() []state.SchemaPlanRow {
	rows := make([]state.SchemaPlanRow, 0, len(s.Fields))
	for _, f := range s.Fields {
		rows = append(rows, f.PlanRow)
	}
	return rows
}

// InferSchema derives a recommended schema plan from a CollectionSample.
// projectID is stored on each SchemaPlanRow so results can be passed directly
// to state.SavePlan.
func InferSchema(sample *CollectionSample, projectID string) (*InferredSchema, error) {
	seen := make(map[string]bool) // tracks assigned pg_column names for collision detection

	var fields []InferredField
	var columns []adapters.ColumnDef

	for _, ff := range sample.Fields {
		inf := inferField(ff, projectID, sample.Collection, seen)
		fields = append(fields, inf)

		// Flatten parent rows and skip rows do not produce target columns.
		if inf.PlanRow.Strategy == "direct" || inf.PlanRow.Strategy == "as_jsonb" {
			col := adapters.ColumnDef{
				Name:       inf.PlanRow.PgColumn,
				Type:       inf.PlanRow.PgType,
				Nullable:   inf.PlanRow.Nullable,
				PrimaryKey: inf.PlanRow.IsPK,
			}
			columns = append(columns, col)
		}
	}

	// _extra jsonb catches any field the sample missed at transfer time.
	extraName := sanitizeName("_extra", seen)
	columns = append(columns, adapters.ColumnDef{
		Name:     extraName,
		Type:     "jsonb",
		Nullable: true,
	})

	return &InferredSchema{
		Collection: sample.Collection,
		Fields:     fields,
		TableSchema: adapters.TableSchema{
			Name:    sample.Collection,
			Columns: columns,
		},
	}, nil
}

func inferField(ff FieldFrequency, projectID, collection string, seen map[string]bool) InferredField {
	nullable := ff.NullCount > 0 || ff.AbsentCount > 0

	// _id is always primary key, never nullable.
	if ff.FieldName == "_id" {
		pgType, opts, warns := inferIDType(ff.Types)
		seen["_id"] = true
		return InferredField{
			Frequency: ff,
			PlanRow: state.SchemaPlanRow{
				ProjectID:  projectID,
				Collection: collection,
				FieldName:  "_id",
				PgColumn:   "_id",
				PgType:     pgType,
				Strategy:   "direct",
				IsPK:       true,
				Nullable:   false,
			},
			Options:  opts,
			Warnings: warns,
		}
	}

	pgType, strategy, opts, warns := inferType(ff)
	pgCol := sanitizeName(pgColumnName(ff.FieldName), seen)
	seen[pgCol] = true

	return InferredField{
		Frequency: ff,
		PlanRow: state.SchemaPlanRow{
			ProjectID:  projectID,
			Collection: collection,
			FieldName:  ff.FieldName,
			PgColumn:   pgCol,
			PgType:     pgType,
			Strategy:   strategy,
			IsPK:       false,
			Nullable:   nullable,
		},
		Options:  opts,
		Warnings: warns,
	}
}

// inferIDType determines the Postgres type for the _id field.
func inferIDType(types []TypeCount) (pgType string, opts []FieldOption, warnings []string) {
	if len(types) == 0 {
		return "text", []FieldOption{{PgType: "text", Strategy: "direct"}}, nil
	}
	dominant := types[0].TypeName // typeList() is sorted by count desc

	switch dominant {
	case "ObjectId":
		return "text", []FieldOption{{PgType: "text", Strategy: "direct"}}, nil
	case "Int32":
		return "integer", []FieldOption{
			{PgType: "integer", Strategy: "direct"},
			{PgType: "bigint", Strategy: "direct"},
		}, nil
	case "Int64":
		return "bigint", []FieldOption{
			{PgType: "bigint", Strategy: "direct"},
			{PgType: "integer", Strategy: "direct"},
		}, nil
	case "String":
		return "text", []FieldOption{{PgType: "text", Strategy: "direct"}}, nil
	case "Binary":
		// Binary subtype 4 is UUID — can't distinguish from type name alone.
		return "text", []FieldOption{
			{PgType: "text", Strategy: "direct"},
			{PgType: "uuid", Strategy: "direct"},
		}, nil
	case "Document":
		return "jsonb", []FieldOption{{PgType: "jsonb", Strategy: "as_jsonb"}},
			[]string{"compound _id (Document type) not supported in v1 — migration will fail for this collection"}
	default:
		return "text", []FieldOption{{PgType: "text", Strategy: "direct"}},
			[]string{fmt.Sprintf("unusual _id type %q — defaulting to text", dominant)}
	}
}

// inferType returns the recommended pg_type, strategy, options, and warnings for a non-_id field.
func inferType(ff FieldFrequency) (pgType, strategy string, opts []FieldOption, warnings []string) {
	if len(ff.Types) == 0 {
		return "text", "direct", []FieldOption{{PgType: "text", Strategy: "direct"}}, nil
	}
	if !ff.Polymorphic {
		return singleTypeMapping(ff.Types[0].TypeName)
	}
	return widenTypes(ff)
}

// singleTypeMapping returns the mapping for a field with one observed non-null BSON type.
func singleTypeMapping(bsonType string) (pgType, strategy string, opts []FieldOption, warnings []string) {
	switch bsonType {
	case "String":
		return "text", "direct",
			[]FieldOption{{PgType: "text", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, nil
	case "Int32":
		return "integer", "direct",
			[]FieldOption{{PgType: "integer", Strategy: "direct"}, {PgType: "bigint", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Int64":
		return "bigint", "direct",
			[]FieldOption{{PgType: "bigint", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Double":
		return "double precision", "direct",
			[]FieldOption{{PgType: "double precision", Strategy: "direct"}, {PgType: "numeric", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Decimal128":
		return "numeric", "direct",
			[]FieldOption{{PgType: "numeric", Strategy: "direct"}, {PgType: "double precision", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Boolean":
		return "boolean", "direct",
			[]FieldOption{{PgType: "boolean", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Date":
		return "timestamptz", "direct",
			[]FieldOption{{PgType: "timestamptz", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "ObjectId":
		return "text", "direct",
			[]FieldOption{{PgType: "text", Strategy: "direct"}}, nil
	case "Binary":
		return "bytea", "direct",
			[]FieldOption{{PgType: "bytea", Strategy: "direct"}, {PgType: "text", Strategy: "direct"}}, nil
	case "Array":
		return "jsonb", "as_jsonb",
			[]FieldOption{{PgType: "jsonb", Strategy: "as_jsonb"}, {PgType: "", Strategy: "skip"}}, nil
	case "Document":
		return "jsonb", "as_jsonb",
			[]FieldOption{
				{PgType: "jsonb", Strategy: "as_jsonb"},
				{PgType: "", Strategy: "flatten"},
				{PgType: "", Strategy: "skip"},
			}, nil
	case "Regex":
		return "text", "direct",
			[]FieldOption{{PgType: "text", Strategy: "direct"}, {PgType: "", Strategy: "skip"}},
			[]string{"Regex stored as pattern string; flags are dropped"}
	default:
		return "jsonb", "as_jsonb",
			[]FieldOption{{PgType: "jsonb", Strategy: "as_jsonb"}, {PgType: "", Strategy: "skip"}},
			[]string{fmt.Sprintf("BSON type %q has no direct Postgres mapping — stored as jsonb", bsonType)}
	}
}

// widenTypes picks the widest compatible Postgres type for a polymorphic field.
func widenTypes(ff FieldFrequency) (pgType, strategy string, opts []FieldOption, warnings []string) {
	typeSet := make(map[string]bool)
	for _, tc := range ff.Types {
		if tc.TypeName != "Null" {
			typeSet[tc.TypeName] = true
		}
	}
	warnings = []string{fmt.Sprintf("polymorphic field: %s", typeSetStr(ff.Types))}

	// Numeric widening rules (ordered narrowest-first).
	switch {
	case only(typeSet, "Int32", "Int64"):
		return "bigint", "direct",
			[]FieldOption{{PgType: "bigint", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, warnings
	case only(typeSet, "Double", "Decimal128"):
		return "numeric", "direct",
			[]FieldOption{{PgType: "numeric", Strategy: "direct"}, {PgType: "double precision", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, warnings
	case onlyNumeric(typeSet): // Int32/Int64/Double mix → double precision
		return "double precision", "direct",
			[]FieldOption{{PgType: "double precision", Strategy: "direct"}, {PgType: "numeric", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, warnings
	case onlyNumericOrDecimal(typeSet): // any numeric + Decimal128 → numeric
		return "numeric", "direct",
			[]FieldOption{{PgType: "numeric", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, warnings
	}

	// Numeric + String → text (universal string fallback).
	if hasAnyNumeric(typeSet) && typeSet["String"] && !typeSet["Document"] && !typeSet["Array"] {
		return "text", "direct",
			[]FieldOption{{PgType: "text", Strategy: "direct"}, {PgType: "jsonb", Strategy: "as_jsonb"}}, warnings
	}

	// Anything + Document → jsonb (universal container).
	if typeSet["Document"] || typeSet["Array"] {
		return "jsonb", "as_jsonb",
			[]FieldOption{{PgType: "jsonb", Strategy: "as_jsonb"}, {PgType: "", Strategy: "skip"}}, warnings
	}

	// Fallback.
	return "jsonb", "as_jsonb",
		[]FieldOption{{PgType: "jsonb", Strategy: "as_jsonb"}, {PgType: "text", Strategy: "direct"}, {PgType: "", Strategy: "skip"}}, warnings
}

// only returns true when typeSet contains exactly the given types and no others.
func only(typeSet map[string]bool, types ...string) bool {
	if len(typeSet) != len(types) {
		return false
	}
	for _, t := range types {
		if !typeSet[t] {
			return false
		}
	}
	return true
}

var numericBSONTypes = map[string]bool{"Int32": true, "Int64": true, "Double": true}

func onlyNumeric(typeSet map[string]bool) bool {
	if len(typeSet) < 2 {
		return false
	}
	for t := range typeSet {
		if !numericBSONTypes[t] {
			return false
		}
	}
	return true
}

func onlyNumericOrDecimal(typeSet map[string]bool) bool {
	allAllowed := map[string]bool{"Int32": true, "Int64": true, "Double": true, "Decimal128": true}
	hasDecimal := typeSet["Decimal128"]
	if !hasDecimal {
		return false
	}
	for t := range typeSet {
		if !allAllowed[t] {
			return false
		}
	}
	return true
}

func hasAnyNumeric(typeSet map[string]bool) bool {
	for t := range numericBSONTypes {
		if typeSet[t] {
			return true
		}
	}
	return typeSet["Decimal128"]
}

func typeSetStr(types []TypeCount) string {
	parts := make([]string, 0, len(types))
	for _, tc := range types {
		parts = append(parts, fmt.Sprintf("%s(%d)", tc.TypeName, tc.Count))
	}
	return strings.Join(parts, ", ")
}

// pgColumnName converts a dot-notation Mongo field path to a column name candidate.
func pgColumnName(fieldName string) string {
	return strings.ReplaceAll(fieldName, ".", "_")
}

// sanitizeName produces a valid, unique Postgres identifier.
//   - Invalid chars → _
//   - Leading digit → prepend _
//   - Truncate to 63 bytes
//   - Collision → append _2, _3, …
func sanitizeName(candidate string, seen map[string]bool) string {
	var b strings.Builder
	first := true
	for _, r := range candidate {
		valid := r == '_' || unicode.IsLetter(r) || (!first && unicode.IsDigit(r))
		if first && unicode.IsDigit(r) {
			b.WriteRune('_')
			b.WriteRune(r)
		} else if valid {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
		first = false
	}
	name := b.String()
	if name == "" {
		name = "_field"
	}
	if len(name) > maxPgIdentLen {
		name = name[:maxPgIdentLen]
	}
	if !seen[name] {
		return name
	}
	base := name
	if len(base) > maxPgIdentLen-3 { // leave room for "_NN"
		base = base[:maxPgIdentLen-3]
	}
	for i := 2; ; i++ {
		candidate := fmt.Sprintf("%s_%d", base, i)
		if !seen[candidate] {
			return candidate
		}
	}
}
