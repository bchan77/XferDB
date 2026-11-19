package mongo

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Through mongos, a document moved to another shard while the shards are
// read can come back from both (matrix #442: mongo80-sh-yb2026-live), side by
// side in _id order and differing in the shard key. A batch keeps the first,
// so a PostgreSQL target's upsert never sees one key twice (21000).
func TestCollectBatchDropsRepeatedID(t *testing.T) {
	docs := []any{
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "account_id", Value: int32(10)}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "account_id", Value: int32(10)}},
		bson.D{{Key: "_id", Value: int32(2)}, {Key: "account_id", Value: int32(300)}, {Key: "kind", Value: "moved"}},
		bson.D{{Key: "_id", Value: int32(3)}, {Key: "account_id", Value: int32(300)}},
		bson.D{{Key: "_id", Value: int64(3)}, {Key: "account_id", Value: int32(10)}}, // the same key to MongoDB and the target
		bson.D{{Key: "_id", Value: "3"}, {Key: "account_id", Value: int32(10)}},      // a string: another key
	}
	cursor, err := mongo.NewCursorFromDocuments(docs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := []state.SchemaPlanRow{
		{Collection: "c", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
		{Collection: "c", FieldName: "account_id", PgColumn: "account_id", PgType: "integer", Strategy: "direct", Nullable: true},
		{Collection: "c", FieldName: "kind", PgColumn: "kind", PgType: "text", Strategy: "direct", Nullable: true},
	}
	records, last, err := collectBatch(context.Background(), cursor, plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("got %d records, want 4 (repeated _ids 2 and 3 dropped): %v", len(records), records)
	}
	if got := records[1]["account_id"]; got != int32(10) && got != int64(10) {
		t.Errorf("_id 2 kept account_id %v, want the first copy's 10", got)
	}
	if v, _ := idOf(last); v == "" {
		t.Errorf("no last document")
	}
}

func TestSameID(t *testing.T) {
	a := bson.D{{Key: "x", Value: 1}, {Key: "_id", Value: "k"}}
	b := bson.D{{Key: "_id", Value: "k"}, {Key: "x", Value: 2}}
	c := bson.D{{Key: "_id", Value: "j"}}
	if !sameID(a, b) || sameID(a, c) || sameID(a, bson.D{{Key: "x", Value: 1}}) {
		t.Error("sameID compares documents' _id only, and needs both to have one")
	}
	if !sameID(bson.D{{Key: "_id", Value: int32(7)}}, bson.D{{Key: "_id", Value: int64(7)}}) {
		t.Error("int32 7 and int64 7 are one _id")
	}
}
