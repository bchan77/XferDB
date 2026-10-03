package mongo

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestSampleCollection_FlagsDottedKeyCollision stores a literal "a.b" key
// (legal since MongoDB 3.6) next to a nested a: {b: ...} and checks the real
// sampler reports the collision and inference warns about it (#117). The
// unit tests cover walkDocument; this covers the server round trip and the
// FieldFrequency wiring in SampleCollectionWithProgress.
//
// Requires a live MongoDB; skipped unless XFERDB_TEST_MONGO_DSN is set, e.g.
// mongodb://user:pass@host:27017/testdb?authSource=admin. Creates and drops a
// uniquely named collection.
func TestSampleCollection_FlagsDottedKeyCollision(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_MONGO_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_MONGO_DSN not set — skipping MongoDB integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	dbName, err := extractDatabase(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("xferdb_117_%d", time.Now().UnixNano())
	coll := client.Database(dbName).Collection(name)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if _, err := coll.InsertMany(ctx, []interface{}{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "a.b", Value: "literal"}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "a", Value: bson.D{{Key: "b", Value: "nested"}}}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	sampler, err := NewSampler(ctx, dsn, SamplerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sampler.Close(context.Background()) })
	sample, err := sampler.SampleCollection(ctx, name)
	if err != nil {
		t.Fatal(err)
	}

	var ab *FieldFrequency
	for i := range sample.Fields {
		if sample.Fields[i].FieldName == "a.b" {
			ab = &sample.Fields[i]
		}
	}
	if ab == nil {
		t.Fatalf("no a.b field in %+v", sample.Fields)
	}
	if want := [][]string{{"a", "b"}, {"a.b"}}; !reflect.DeepEqual(ab.PathShapes, want) {
		t.Errorf("a.b PathShapes = %q, want %q", ab.PathShapes, want)
	}
	schema, err := InferSchema(sample, "p")
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Warnings) != 1 {
		t.Errorf("collection warnings = %q, want exactly one", schema.Warnings)
	}
}
