// Package mongo provides MongoDB schema analysis for XferDB.
// It samples documents from a MongoDB source, builds a field frequency table,
// and produces the inferred schema plan used by the migration engine.
package mongo

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const DefaultSampleSize = 2000

// TypeCount records how many documents had a given BSON type for a field.
type TypeCount struct {
	TypeName string `json:"type_name"`
	Count    int    `json:"count"`
}

// FieldFrequency describes how often a field appears in the sample and
// what BSON types were observed for it.
type FieldFrequency struct {
	FieldName   string      `json:"field_name"`
	Occurrences int         `json:"occurrences"`  // documents where field was present and non-null
	NullCount   int         `json:"null_count"`   // explicit null values
	AbsentCount int         `json:"absent_count"` // documents where the field did not appear
	TotalDocs   int         `json:"total_docs"`
	Types       []TypeCount `json:"types"`
	Polymorphic bool        `json:"polymorphic"` // more than one distinct non-null BSON type
}

// CoveragePct returns the percentage of sampled documents that contained
// this field (present and non-null).
func (f *FieldFrequency) CoveragePct() float64 {
	if f.TotalDocs == 0 {
		return 0
	}
	return float64(f.Occurrences) / float64(f.TotalDocs) * 100
}

// CollectionSample is the result of sampling one MongoDB collection.
type CollectionSample struct {
	Collection     string           `json:"collection"`
	EstimatedCount int64            `json:"estimated_count"`
	SampleSize     int              `json:"sample_size"` // actual number of documents sampled
	Fields         []FieldFrequency `json:"fields"`
}

// Sampler connects to a MongoDB instance and samples documents to build
// field frequency tables for schema inference.
type Sampler struct {
	client     *mongo.Client
	database   string
	sampleSize int
}

// NewSampler connects to MongoDB using the full URI (stored verbatim in
// ConnectionConfig.DSN). sampleSize is the number of documents to sample
// per collection; pass 0 to use DefaultSampleSize.
func NewSampler(ctx context.Context, dsn string, sampleSize int) (*Sampler, error) {
	if sampleSize <= 0 {
		sampleSize = DefaultSampleSize
	}

	clientOpts := options.Client().
		ApplyURI(dsn).
		SetReadPreference(readpref.SecondaryPreferred())

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongo sampler connect: %w", err)
	}

	if err := client.Ping(ctx, readpref.SecondaryPreferred()); err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		return nil, fmt.Errorf("mongo sampler ping: %w", err)
	}

	// Extract database name from the URI path. ParseConnectionString already
	// does this, but we re-derive it here so Sampler is self-contained.
	dbName, err := extractDatabase(dsn)
	if err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		return nil, err
	}

	return &Sampler{
		client:     client,
		database:   dbName,
		sampleSize: sampleSize,
	}, nil
}

// Close disconnects from MongoDB.
func (s *Sampler) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// ListSampleableCollections returns the names of collections in the database
// that are suitable for sampling. Skipped:
//   - system.* (internal MongoDB collections)
//   - *.files / *.chunks (GridFS)
//   - views and time-series collections
func (s *Sampler) ListSampleableCollections(ctx context.Context) ([]string, error) {
	db := s.client.Database(s.database)

	// ListCollectionSpecifications returns type information alongside names.
	specs, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections in %q: %w", s.database, err)
	}

	var names []string
	for _, spec := range specs {
		if shouldSkipCollection(spec.Name, spec.Type) {
			continue
		}
		names = append(names, spec.Name)
	}
	return names, nil
}

// SampleCollection samples up to s.sampleSize documents from a collection and
// returns the field frequency table.
func (s *Sampler) SampleCollection(ctx context.Context, collection string) (*CollectionSample, error) {
	coll := s.client.Database(s.database).Collection(collection)

	estimatedCount, err := coll.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("estimated count %s: %w", collection, err)
	}

	// Use $sample to draw a random subset. For small collections the sample
	// may be the entire collection.
	pipeline := mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: s.sampleSize}}}},
	}
	cursor, err := coll.Aggregate(ctx, pipeline,
		options.Aggregate().SetBatchSize(int32(s.sampleSize)))
	if err != nil {
		return nil, fmt.Errorf("sample %s: %w", collection, err)
	}
	defer cursor.Close(ctx)

	// freq maps field path → accumulator.
	freq := make(map[string]*fieldAcc)
	totalDocs := 0

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode document in %s: %w", collection, err)
		}
		totalDocs++
		walkDocument(doc, "", freq)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error in %s: %w", collection, err)
	}

	// Mark absent counts: every field that was not in a document contributes
	// to absent_count (totalDocs − occurrences − nullCount).
	fields := make([]FieldFrequency, 0, len(freq))
	for fieldName, acc := range freq {
		absent := totalDocs - acc.occurrences - acc.nullCount
		if absent < 0 {
			absent = 0
		}
		ff := FieldFrequency{
			FieldName:   fieldName,
			Occurrences: acc.occurrences,
			NullCount:   acc.nullCount,
			AbsentCount: absent,
			TotalDocs:   totalDocs,
			Types:       acc.typeList(),
		}
		// Polymorphic: more than one distinct non-null BSON type observed.
		nonNullTypes := 0
		for _, tc := range ff.Types {
			if tc.TypeName != "Null" {
				nonNullTypes++
			}
		}
		ff.Polymorphic = nonNullTypes > 1
		fields = append(fields, ff)
	}

	// Sort by field name for deterministic output.
	sortFields(fields)

	return &CollectionSample{
		Collection:     collection,
		EstimatedCount: estimatedCount,
		SampleSize:     totalDocs,
		Fields:         fields,
	}, nil
}

// ── internal helpers ─────────────────────────────────────────────────────────

// fieldAcc accumulates type observations for a single field path.
type fieldAcc struct {
	occurrences int
	nullCount   int
	typeCounts  map[string]int
}

func (a *fieldAcc) typeList() []TypeCount {
	out := make([]TypeCount, 0, len(a.typeCounts))
	for name, count := range a.typeCounts {
		out = append(out, TypeCount{TypeName: name, Count: count})
	}
	// Sort by count descending for readable output.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Count > out[i].Count {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func ensureAcc(freq map[string]*fieldAcc, path string) *fieldAcc {
	if a, ok := freq[path]; ok {
		return a
	}
	a := &fieldAcc{typeCounts: make(map[string]int)}
	freq[path] = a
	return a
}

// walkDocument recursively visits every key in a bson.D, recording field
// paths (dot-notation) and their BSON types into freq.
func walkDocument(doc bson.D, prefix string, freq map[string]*fieldAcc) {
	for _, elem := range doc {
		path := elem.Key
		if prefix != "" {
			path = prefix + "." + elem.Key
		}
		acc := ensureAcc(freq, path)

		switch v := elem.Value.(type) {
		case nil:
			acc.nullCount++
		case bson.D:
			// Nested document — record as Document type and recurse.
			acc.occurrences++
			acc.typeCounts["Document"]++
			walkDocument(v, path, freq)
		case bson.A:
			// Array — record as Array type; do not recurse into elements.
			acc.occurrences++
			acc.typeCounts["Array"]++
		default:
			acc.occurrences++
			acc.typeCounts[bsonTypeName(elem.Value)]++
		}
	}
}

// bsonTypeName returns the human-readable BSON type name for a decoded value.
// go.mongodb.org/mongo-driver/v2 decodes BSON values into these Go types.
func bsonTypeName(v interface{}) string {
	switch v.(type) {
	case string:
		return "String"
	case int32:
		return "Int32"
	case int64:
		return "Int64"
	case float64:
		return "Double"
	case bool:
		return "Boolean"
	case bson.D:
		return "Document"
	case bson.A:
		return "Array"
	case bson.ObjectID:
		return "ObjectId"
	case bson.DateTime:
		return "Date"
	case bson.Binary:
		return "Binary"
	case bson.Decimal128:
		return "Decimal128"
	case bson.Regex:
		return "Regex"
	case bson.JavaScript:
		return "JavaScript"
	case bson.Timestamp:
		return "Timestamp"
	case bson.MinKey:
		return "MinKey"
	case bson.MaxKey:
		return "MaxKey"
	case bson.DBPointer:
		return "DBPointer"
	case bson.Symbol:
		return "Symbol"
	case bson.CodeWithScope:
		return "CodeWithScope"
	case bson.Undefined:
		return "Undefined"
	case nil:
		return "Null"
	default:
		return fmt.Sprintf("Unknown(%T)", v)
	}
}

// shouldSkipCollection returns true for collections that sampling must skip.
func shouldSkipCollection(name, collType string) bool {
	// system.* collections are MongoDB internals.
	if strings.HasPrefix(name, "system.") {
		return true
	}
	// GridFS bucket collections.
	if strings.HasSuffix(name, ".files") || strings.HasSuffix(name, ".chunks") {
		return true
	}
	// Views and time-series are not sampleable with $sample.
	if collType == "view" || collType == "timeseries" {
		return true
	}
	return false
}

// sortFields sorts a slice of FieldFrequency by field name, keeping "_id" first.
func sortFields(fields []FieldFrequency) {
	for i := 1; i < len(fields); i++ {
		for j := i; j > 0; j-- {
			a, b := fields[j-1].FieldName, fields[j].FieldName
			// _id is always first.
			if b == "_id" || (a != "_id" && a > b) {
				fields[j-1], fields[j] = fields[j], fields[j-1]
			} else {
				break
			}
		}
	}
}

// extractDatabase parses the database name from a mongodb:// or mongodb+srv:// URI.
func extractDatabase(dsn string) (string, error) {
	// Find the path component after the host(s).
	// URI form: mongodb://[user:pass@]host1[:port1][,host2[:port2]]/[database][?options]
	// We locate the first "/" after the scheme+authority section.
	afterScheme := strings.TrimPrefix(strings.TrimPrefix(dsn, "mongodb+srv://"), "mongodb://")
	// afterScheme looks like "user:pass@host/db?opts" or "host/db"
	slashIdx := strings.Index(afterScheme, "/")
	if slashIdx < 0 {
		// No database in URI — this is valid; the adapter will use the default db.
		return "", fmt.Errorf("mongodb URI has no database path: %q — specify the database in the connection string (e.g. mongodb://host:27017/mydb)", dsn)
	}
	afterSlash := afterScheme[slashIdx+1:]
	// Strip query string.
	if q := strings.Index(afterSlash, "?"); q >= 0 {
		afterSlash = afterSlash[:q]
	}
	db := strings.TrimSpace(afterSlash)
	if db == "" {
		return "", fmt.Errorf("mongodb URI has no database path: %q — specify the database in the connection string (e.g. mongodb://host:27017/mydb)", dsn)
	}
	return db, nil
}
