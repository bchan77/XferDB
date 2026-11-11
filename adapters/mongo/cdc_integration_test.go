package mongo

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// mongoRS returns XFERDB_TEST_MONGO_RS with its database replaced by a new
// one, dropped when the test ends, and a client on it.
func mongoRS(t *testing.T) (string, *mongo.Database) {
	t.Helper()
	uri := os.Getenv("XFERDB_TEST_MONGO_RS")
	if uri == "" {
		// The same tests run through a sharded cluster's mongos.
		uri = os.Getenv("XFERDB_TEST_MONGO_SHARDED")
	}
	if uri == "" {
		t.Skip("XFERDB_TEST_MONGO_RS / XFERDB_TEST_MONGO_SHARDED not set; skipping MongoDB change stream test")
	}
	name := fmt.Sprintf("xferdb_cs_%d", time.Now().UnixNano())
	scheme := strings.Index(uri, "://") + 3
	query := ""
	if q := strings.Index(uri, "?"); q >= 0 {
		query = uri[q:]
	}
	host := strings.TrimSuffix(uri[scheme:], query)
	if i := strings.Index(host, "/"); i >= 0 {
		host = host[:i]
	}
	dsn := uri[:scheme] + host + "/" + name + query
	client, err := mongo.Connect(options.Client().ApplyURI(dsn))
	if err != nil {
		t.Fatal(err)
	}
	db := client.Database(name)
	t.Cleanup(func() {
		db.Drop(context.Background()) //nolint:errcheck
		client.Disconnect(context.Background())
	})
	return dsn, db
}

// A stream resumes from its high-water mark, which writes to collections it
// doesn't stream move on: on a quiet stream the last change's own token may
// leave the oplog long before the stream is behind.
func TestStreamResumesFromHighWaterMark(t *testing.T) {
	dsn, db := mongoRS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	quiet, busy := db.Collection("quiet"), db.Collection("busy")
	if _, err := quiet.InsertOne(ctx, bson.D{{Key: "_id", Value: 0}}); err != nil {
		t.Fatal(err)
	}
	newSource := func() *ChangeSource {
		s, err := NewChangeSource(ctx, adapters.ConnectionConfig{Type: "mongodb", DSN: dsn}, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetPlan([]state.SchemaPlanRow{
			{Collection: "quiet", FieldName: "_id", PgColumn: "_id", PgType: "bigint", Strategy: "direct", IsPK: true},
			{Collection: "quiet", FieldName: "v", PgColumn: "v", PgType: "text", Strategy: "direct"},
		}); err != nil {
			t.Fatal(err)
		}
		return s
	}
	src := newSource()
	defer src.Close()
	start, err := src.Prepare(ctx, []string{"quiet"})
	if err != nil {
		t.Fatal(err)
	}

	stream := func(s *ChangeSource, from cdc.Position) (chan cdc.Change, context.CancelFunc, chan error) {
		sctx, stop := context.WithCancel(ctx)
		out, done := make(chan cdc.Change, 16), make(chan error, 1)
		go func() { done <- s.Stream(sctx, from, out) }()
		return out, stop, done
	}
	recv := func(out chan cdc.Change, done chan error) cdc.Change {
		t.Helper()
		select {
		case c := <-out:
			return c
		case err := <-done:
			t.Fatalf("stream ended: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("no change within 30s")
		}
		return cdc.Change{}
	}

	out, stop, done := stream(src, start)
	if _, err := quiet.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "one"}}); err != nil {
		t.Fatal(err)
	}
	// The start is the cluster time of the last write, which the stream
	// includes: the insert of 0 may come first, harmlessly copied twice.
	c := recv(out, done)
	if c.Key["_id"] == int64(0) {
		c = recv(out, done)
	}
	if c.Op != cdc.OpInsert || c.Key["_id"] != int64(1) || c.After["v"] != "one" || c.After["_extra"] != nil {
		t.Fatalf("first change = %+v", c)
	}
	if err := src.Ack(ctx, c.Pos); err != nil {
		t.Fatal(err)
	}
	// Writes elsewhere move the high-water mark past the last change.
	deadline := time.Now().Add(30 * time.Second)
	for src.Behind(c.Pos, src.Confirmed()) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("confirmed stayed at %s", src.Confirmed())
		}
		if _, err := busy.InsertOne(ctx, bson.D{{Key: "x", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	hwm := src.Confirmed()
	if !strings.Contains(string(hwm), "/") {
		t.Fatalf("high-water mark %s has no token to resume after", hwm)
	}
	stop()
	<-done

	// A new stream from the high-water mark sees the next change, and not
	// the one before it again.
	src2 := newSource()
	defer src2.Close()
	out, stop, done = stream(src2, hwm)
	defer stop()
	if _, err := quiet.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: "two"}}}}); err != nil {
		t.Fatal(err)
	}
	c = recv(out, done)
	if c.Op != cdc.OpUpdate || c.Key["_id"] != int64(1) || c.After["v"] != "two" {
		t.Fatalf("change after resuming = %+v", c)
	}

	// The oplog reaches back past the start, so the stream isn't lost.
	if ret, err := src2.RetentionAt(ctx, start); err != nil || ret.Lost {
		t.Fatalf("retention from the start: %+v %v", ret, err)
	}
	// Before the oldest entry of a replica set's oplog is lost. Through
	// mongos it is a stream opened there, which a shard whose oplog was
	// never truncated still serves: nothing was lost.
	if topo, err := src2.topology(ctx); err != nil {
		t.Fatal(err)
	} else if topo == topoReplicaSet {
		if ret, err := src2.RetentionAt(ctx, position{T: 1, I: 1}.encode()); err != nil || !ret.Lost {
			t.Fatalf("retention from 1970: %+v %v, want lost", ret, err)
		}
	}
}

// Readiness on a replica set or through mongos: every check passes or only
// warns.
func TestLiveChecksMongo(t *testing.T) {
	dsn, db := mongoRS(t)
	ctx := context.Background()
	if _, err := db.Collection("c").InsertOne(ctx, bson.D{{Key: "a", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	s := &Source{}
	if err := s.Connect(ctx, adapters.ConnectionConfig{Type: "mongodb", DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	checks, err := s.LiveChecks(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, c := range checks {
		names[c.Name] = true
		if !c.OK && !c.Warning {
			t.Errorf("check failed: %+v", c)
		}
	}
	for _, want := range []string{"a replica set or sharded cluster", "MongoDB 4.2 or later", "can open a change stream", "oplog window"} {
		if !names[want] {
			t.Errorf("no %q check in %+v", want, checks)
		}
	}
}
