package mongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestReadBatch_PagesEveryIDShape seeds one collection per _id shape from #97
// and pages through it with a batch size far below its row count, so every
// shape crosses several page boundaries (and, for the mixed collection,
// several BSON type boundaries). Every document must come back exactly once.
//
// Requires a live MongoDB; skipped unless XFERDB_TEST_MONGO_DSN is set, e.g.
// mongodb://user:pass@host:27017/testdb?authSource=admin. Each run creates its
// own uniquely named collections and drops them afterwards.
func TestReadBatch_PagesEveryIDShape(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_MONGO_DSN")
	if dsn == "" {
		t.Skip("XFERDB_TEST_MONGO_DSN not set — skipping MongoDB integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Registered before the drops below, so it runs after them (LIFO); a plain
	// defer would disconnect before t.Cleanup gets to drop anything.
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database(databaseFromDSN(dsn))

	uuid := func(i int) bson.Binary {
		b := make([]byte, 16)
		b[15] = byte(i)
		return bson.Binary{Subtype: bson.TypeBinaryUUID, Data: b}
	}
	ids := func(n int, f func(i int) interface{}) []interface{} {
		out := make([]interface{}, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	concat := func(parts ...[]interface{}) []interface{} {
		var out []interface{}
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}

	shapes := []struct {
		name     string
		pgType   string
		strategy string
		ids      []interface{}
	}{
		{"objectid", "text", "direct", ids(25, func(int) interface{} { return bson.NewObjectID() })},
		{"compound", "jsonb", "as_jsonb", ids(25, func(i int) interface{} {
			return bson.D{{Key: "year", Value: int32(2026)}, {Key: "seq", Value: int32(i)}}
		})},
		{"uuid", "uuid", "direct", ids(25, func(i int) interface{} { return uuid(i) })},
		{"double", "text", "direct", ids(25, func(i int) interface{} { return float64(i) + 0.5 })},
		{"polymorphic", "text", "direct", concat(
			ids(50, func(i int) interface{} { return int64(100000 + i) }),
			ids(3, func(i int) interface{} { return fmt.Sprintf("legacy-import-%03d", i) }),
		)},
		// One run of each type group between numbers and dates, sized so
		// page boundaries land both inside and exactly at the end of a run.
		{"mixed", "jsonb", "as_jsonb", concat(
			ids(9, func(i int) interface{} { return int32(i) }),
			ids(5, func(i int) interface{} { return fmt.Sprintf("s%02d", i) }),
			ids(7, func(i int) interface{} { return bson.D{{Key: "k", Value: int32(i)}} }),
			ids(3, func(i int) interface{} { return uuid(i) }),
			ids(4, func(int) interface{} { return bson.NewObjectID() }),
			ids(2, func(i int) interface{} { return i == 1 }),
			ids(8, func(i int) interface{} { return bson.NewDateTimeFromTime(time.Unix(int64(i), 0)) }),
		)},
	}

	run := time.Now().UnixNano()
	var plan []state.SchemaPlanRow
	for _, sh := range shapes {
		coll := fmt.Sprintf("xferdb_it97_%d_%s", run, sh.name)
		docs := make([]interface{}, len(sh.ids))
		for i, id := range sh.ids {
			docs[i] = bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(i)}}
		}
		if _, err := db.Collection(coll).InsertMany(ctx, docs); err != nil {
			t.Fatalf("seed %s: %v", coll, err)
		}
		t.Cleanup(func() {
			if err := db.Collection(coll).Drop(context.Background()); err != nil {
				t.Errorf("drop %s: %v", coll, err)
			}
		})
		plan = append(plan,
			state.SchemaPlanRow{Collection: coll, FieldName: "_id", PgColumn: "_id", PgType: sh.pgType, Strategy: sh.strategy, IsPK: true},
			state.SchemaPlanRow{Collection: coll, FieldName: "n", PgColumn: "n", PgType: "integer", Strategy: "direct"},
		)
	}

	src := &Source{}
	if err := src.Connect(ctx, adapters.ConnectionConfig{DSN: dsn}); err != nil {
		t.Fatalf("source connect: %v", err)
	}
	defer src.Close()
	if err := src.SetPlan(plan); err != nil {
		t.Fatalf("SetPlan: %v", err)
	}

	const batchSize = 7
	for _, sh := range shapes {
		sh := sh
		t.Run(sh.name, func(t *testing.T) {
			coll := fmt.Sprintf("xferdb_it97_%d_%s", run, sh.name)
			seen := make(map[int64]bool, len(sh.ids))
			lastKey := ""
			for page := 0; ; page++ {
				if page > len(sh.ids) {
					t.Fatalf("still paging after %d pages — resume key is not advancing", page)
				}
				batch, err := src.ReadBatch(ctx, coll, adapters.BatchOptions{LastPK: lastKey, Limit: batchSize})
				if err != nil {
					t.Fatalf("ReadBatch page %d (after %q): %v", page, lastKey, err)
				}
				if batch.Size == 0 {
					break
				}
				for _, rec := range batch.Records {
					n := rec["n"].(int64)
					if seen[n] {
						t.Errorf("document n=%d (_id %v) read twice", n, rec["_id"])
					}
					seen[n] = true
				}
				lastKey = batch.LastKey
			}
			if len(seen) != len(sh.ids) {
				var missing []interface{}
				for i, id := range sh.ids {
					if !seen[int64(i)] {
						missing = append(missing, id)
					}
				}
				t.Errorf("read %d of %d documents; missing _ids: %v", len(seen), len(sh.ids), missing)
			}
		})
	}
}
