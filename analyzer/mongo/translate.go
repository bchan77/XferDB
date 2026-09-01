package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// IndexTranslation is the result of translating one MongoDB index.
type IndexTranslation struct {
	Index    *adapters.IndexDef // nil when the index is skipped
	Warnings []string
}

// TranslateIndexes lists all indexes on a collection and translates the v1-
// supported subset to adapters.IndexDef. Unsupported types are skipped with
// a descriptive warning so the user can create them manually.
//
// plan maps mongo field_name → pg_column so we can use the sanitised name.
func TranslateIndexes(
	ctx interface{ Deadline() (interface{}, bool) },
	coll interface{ Indexes() mongo.IndexView },
	plan []state.SchemaPlanRow,
) ([]IndexTranslation, error) {
	return translateIndexesFromView(coll.Indexes(), plan)
}

// TranslateRawIndexSpecs is the testable core: takes already-decoded specs.
func TranslateRawIndexSpecs(specs []bson.D, plan []state.SchemaPlanRow) []IndexTranslation {
	// Build field_name → pg_column map from plan.
	colByField := make(map[string]string, len(plan))
	for _, row := range plan {
		if row.PgColumn != "" {
			colByField[row.FieldName] = row.PgColumn
		}
	}

	var results []IndexTranslation
	for _, spec := range specs {
		t := translateOneSpec(spec, colByField)
		results = append(results, t)
	}
	return results
}

func translateIndexesFromView(view mongo.IndexView, plan []state.SchemaPlanRow) ([]IndexTranslation, error) {
	cursor, err := view.List(nil, options.ListIndexes())
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	defer cursor.Close(nil)

	var specs []bson.D
	for cursor.Next(nil) {
		var spec bson.D
		if err := cursor.Decode(&spec); err != nil {
			return nil, fmt.Errorf("decode index spec: %w", err)
		}
		specs = append(specs, spec)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("index cursor: %w", err)
	}
	return TranslateRawIndexSpecs(specs, plan), nil
}

func translateOneSpec(spec bson.D, colByField map[string]string) IndexTranslation {
	name := specString(spec, "name")
	keyDoc := specDoc(spec, "key")
	isUnique := specBool(spec, "unique")
	_, isTTL := specField(spec, "expireAfterSeconds")
	_, isSparse := specField(spec, "sparse")
	_, isPartial := specField(spec, "partialFilterExpression")

	// Skip _id_ index — Postgres PKs are created by CreateTable.
	if name == "_id_" {
		return IndexTranslation{Warnings: nil}
	}

	// TTL indexes have no Postgres equivalent.
	if isTTL {
		return IndexTranslation{
			Warnings: []string{fmt.Sprintf("index %q: TTL index not supported in Postgres — create a pg_cron job or application-level expiry manually", name)},
		}
	}

	// Partial indexes need a WHERE clause that IndexDef cannot express in v1.
	if isPartial {
		return IndexTranslation{
			Warnings: []string{fmt.Sprintf("index %q: partial filter expression cannot be translated in v1 — create manually", name)},
		}
	}

	if isSparse {
		// Sparse index = WHERE field IS NOT NULL in Postgres; skip in v1.
		return IndexTranslation{
			Warnings: []string{fmt.Sprintf("index %q: sparse index not translated in v1 — create a partial index (WHERE %s IS NOT NULL) manually", name, name)},
		}
	}

	// Inspect key fields.
	var warns []string
	var pgCols []string

	for _, elem := range keyDoc {
		field := elem.Key

		// MongoDB index key values: int32 (1 asc, -1 desc, 2 for 2d) or
		// string ("text", "hashed", "2dsphere", "2d").
		switch dir := elem.Value.(type) {
		case string:
			switch dir {
			case "text":
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: text index not translated in v1 — create a GIN tsvector index manually", name)}}
			case "hashed":
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: hashed index not supported in Postgres — skipped", name)}}
			case "2dsphere":
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: 2dsphere geospatial index not supported — skipped", name)}}
			case "2d":
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: 2d geospatial index not supported — skipped", name)}}
			default:
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: unknown index type %q — skipped", name, dir)}}
			}
		case int32:
			switch dir {
			case 1: // ascending — OK
			case -1:
				warns = append(warns, fmt.Sprintf("index %q field %q: descending direction ignored (Postgres btree is bidirectional)", name, field))
			case 2:
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: 2d geospatial index not supported — skipped", name)}}
			default:
				return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: unknown direction %d — skipped", name, dir)}}
			}
		default:
			return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: unrecognised key value type %T — skipped", name, elem.Value)}}
		}

		// Map mongo field_name → pg_column.
		pgCol, ok := colByField[field]
		if !ok {
			// Field is skipped or not in plan — skip index.
			return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: field %q not in schema plan (skipped or missing) — index skipped", name, field)}}
		}

		// Nested path on a jsonb column (dot in field name) → cannot translate in v1.
		if isPlanFieldJsonb(field, colByField) {
			return IndexTranslation{Warnings: []string{fmt.Sprintf("index %q: nested path %q on a jsonb column requires an expression index — skipped in v1, create manually", name, field)}}
		}

		pgCols = append(pgCols, pgCol)
	}

	if len(pgCols) == 0 {
		return IndexTranslation{Warnings: warns}
	}

	return IndexTranslation{
		Index: &adapters.IndexDef{
			Name:    name,
			Columns: pgCols,
			Unique:  isUnique,
		},
		Warnings: warns,
	}
}

// isPlanFieldJsonb returns true when the field's dot-notation path implies it
// is a sub-path of a jsonb parent column (i.e. the field itself is not in the
// plan but a prefix of it is).
func isPlanFieldJsonb(field string, colByField map[string]string) bool {
	// If the field has dots and the field itself isn't directly in the plan,
	// it is a nested path. We already return early if the field isn't in the
	// plan, so this function only needs to catch the case where the field IS
	// in the plan but is a dot-notation sub-field of a jsonb parent.
	// For now: any dot in the field name is a nested path we cannot translate.
	for _, r := range field {
		if r == '.' {
			return true
		}
	}
	return false
}

// ── spec helpers ─────────────────────────────────────────────────────────────

func specField(spec bson.D, key string) (interface{}, bool) {
	for _, elem := range spec {
		if elem.Key == key {
			return elem.Value, true
		}
	}
	return nil, false
}

func specString(spec bson.D, key string) string {
	v, ok := specField(spec, key)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func specDoc(spec bson.D, key string) bson.D {
	v, ok := specField(spec, key)
	if !ok {
		return nil
	}
	d, _ := v.(bson.D)
	return d
}

func specBool(spec bson.D, key string) bool {
	v, ok := specField(spec, key)
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}
