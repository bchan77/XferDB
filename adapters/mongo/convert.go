// Package mongo implements the XferDB source adapter for MongoDB.
package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ConvertDocument converts a raw bson.D document into a flat map[string]interface{}
// using the provided schema plan. The returned map is ready to pass to a SQL
// target adapter's WriteBatch.
//
// Rules applied per plan row strategy:
//   - direct:   scalar BSON value converted to the nearest Go type
//   - as_jsonb: value serialised to JSON []byte
//   - flatten:  nested bson.D expanded one level into the parent map (child keys prefixed)
//   - skip:     field omitted entirely
//
// Fields not present in the plan are collected into "_extra" as a JSON object.
// The _extra key name is configurable (extraCol) to handle the sanitizer renaming it.
func ConvertDocument(doc bson.D, plan []state.SchemaPlanRow, extraCol string) (map[string]interface{}, error) {
	// Build lookup: mongo field_name → plan row.
	planByField := make(map[string]state.SchemaPlanRow, len(plan))
	for _, row := range plan {
		planByField[row.FieldName] = row
	}

	result := make(map[string]interface{}, len(doc))
	extra := make(map[string]interface{})

	if err := convertDoc(doc, "", planByField, result, extra); err != nil {
		return nil, err
	}

	if len(extra) > 0 {
		b, err := json.Marshal(extra)
		if err != nil {
			return nil, fmt.Errorf("marshal _extra: %w", err)
		}
		// Return as string for PostgreSQL COPY compatibility.
		result[extraCol] = string(b)
	}

	return result, nil
}

func convertDoc(doc bson.D, prefix string, plan map[string]state.SchemaPlanRow, out map[string]interface{}, extra map[string]interface{}) error {
	for _, elem := range doc {
		fieldPath := elem.Key
		if prefix != "" {
			fieldPath = prefix + "." + elem.Key
		}

		row, inPlan := plan[fieldPath]
		if !inPlan {
			extra[fieldPath] = bsonToAny(elem.Value)
			continue
		}

		switch row.Strategy {
		case "skip":
			// omit entirely

		case "as_jsonb":
			b, err := jsonMarshalBSON(elem.Value)
			if err != nil {
				return fmt.Errorf("as_jsonb %s: %w", fieldPath, err)
			}
			// Return as string for PostgreSQL COPY compatibility.
			// COPY expects JSON as text, not []byte.
			out[row.PgColumn] = string(b)

		case "flatten":
			nested, ok := elem.Value.(bson.D)
			if !ok {
				// Not a document at runtime — stash in _extra.
				extra[fieldPath] = bsonToAny(elem.Value)
				continue
			}
			// Expand one level: each child becomes a separate column.
			for _, child := range nested {
				childPath := fieldPath + "." + child.Key
				childRow, childInPlan := plan[childPath]
				if !childInPlan {
					extra[childPath] = bsonToAny(child.Value)
					continue
				}
				v, err := convertScalar(child.Value, childRow.PgType)
				if err != nil {
					return fmt.Errorf("flatten child %s: %w", childPath, err)
				}
				out[childRow.PgColumn] = v
			}

		default: // "direct"
			v, err := convertScalar(elem.Value, row.PgType)
			if err != nil {
				return fmt.Errorf("direct %s: %w", fieldPath, err)
			}
			out[row.PgColumn] = v
		}
	}
	return nil
}

// convertScalar converts a BSON value to the Go type that maps cleanly to
// the given Postgres type. The SQL driver handles the final encoding.
func convertScalar(v interface{}, pgType string) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case string:
		return val, nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case float64:
		return val, nil
	case bool:
		return val, nil
	case []byte:
		return val, nil
	case bson.ObjectID:
		return val.Hex(), nil
	case bson.DateTime:
		return val.Time(), nil
	case bson.Decimal128:
		// lib/pq accepts a string for a numeric column.
		return val.String(), nil
	case bson.Binary:
		return val.Data, nil
	case bson.Regex:
		// Store pattern only; flags dropped per design doc.
		return val.Pattern, nil
	case bson.D:
		// Nested document requested as direct — serialise via JSON.
		return jsonMarshalBSON(val)
	case bson.A:
		return jsonMarshalBSON(val)
	default:
		// Exotic types (MinKey, MaxKey, JavaScript, Timestamp, etc.) go through
		// Extended JSON serialisation.
		return jsonMarshalBSON(val)
	}
}

// jsonMarshalBSON converts a BSON value to a JSON []byte.
// It must NOT call json.Marshal(bson.D{}) directly because bson.D marshals
// as an array of {Key, Value} pairs, not as a JSON object.
func jsonMarshalBSON(v interface{}) ([]byte, error) {
	return json.Marshal(bsonToAny(v))
}

// bsonToAny recursively converts BSON types to plain Go types suitable for
// json.Marshal. This avoids the bson.D → [{Key,Value}…] serialisation bug.
func bsonToAny(v interface{}) interface{} {
	switch val := v.(type) {
	case bson.D:
		m := make(map[string]interface{}, len(val))
		for _, elem := range val {
			m[elem.Key] = bsonToAny(elem.Value)
		}
		return m
	case bson.A:
		s := make([]interface{}, len(val))
		for i, elem := range val {
			s[i] = bsonToAny(elem)
		}
		return s
	case bson.ObjectID:
		return val.Hex()
	case bson.DateTime:
		return val.Time().Format(time.RFC3339Nano)
	case bson.Decimal128:
		return val.String()
	case bson.Binary:
		return val.Data
	case bson.Regex:
		return val.Pattern
	case int32:
		return int64(val)
	case nil:
		return nil
	default:
		return val
	}
}
