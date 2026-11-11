package api

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/cdc/live"
	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestLiveMigrationMongo is TestLiveMigration from a MongoDB replica set
// (#179): documents converted by the schema plan (a flattened subdocument,
// an array as jsonb, fields the plan doesn't know in _extra), a polymorphic
// _id, updates with and without post-images, $unset fields, replaces,
// deletes and multi-document transactions, while the snapshot copies and
// while streaming (apply killed, server restarted). Cutover under writes
// must stop at the quiet check; once the writer stops, cutover completes and
// the validation after it (which re-reads the source through the plan)
// passes.
//
// The sources are XFERDB_TEST_MONGO_RS, a mongodb:// URI of a replica set,
// and XFERDB_TEST_MONGO_SHARDED, one of a sharded cluster's mongos (any
// database in it; each run uses a new database, dropped afterwards). On the
// sharded cluster users is sharded by a hashed _id and things by a range of
// v split over the shards, so its events carry the shard key too and the
// writer's replaces move documents between shards. The targets are a SQLite
// file, and a new database on XFERDB_TEST_MONGO_PG (a postgres:// URL) and
// on XFERDB_TEST_YB when they are set.
func TestLiveMigrationMongo(t *testing.T) {
	ran := false
	for _, src := range []struct {
		name, env string
		sharded   bool
	}{
		{"replicaset", "XFERDB_TEST_MONGO_RS", false},
		{"sharded", "XFERDB_TEST_MONGO_SHARDED", true},
	} {
		uri := os.Getenv(src.env)
		if uri == "" {
			continue
		}
		ran = true
		t.Run(src.name, func(t *testing.T) { liveMongoTargets(t, uri, src.sharded) })
	}
	if !ran {
		t.Skip("XFERDB_TEST_MONGO_RS and XFERDB_TEST_MONGO_SHARDED not set; skipping MongoDB live migration test")
	}
}

func liveMongoTargets(t *testing.T, uri string, sharded bool) {
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "target.db")
		runLiveMongo(t, uri, sharded, map[string]any{"type": "sqlite", "dsn": path}, func(t *testing.T) *sql.DB {
			db, err := sql.Open("sqlite3", path)
			if err != nil {
				t.Fatal(err)
			}
			return db
		})
	})
	for _, tc := range []struct{ name, env, typ string }{
		{"postgres", "XFERDB_TEST_MONGO_PG", "postgres"},
		{"yugabyte", "XFERDB_TEST_YB", "yugabyte"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := os.Getenv(tc.env)
			if dsn == "" {
				t.Skip(tc.env + " not set")
			}
			admin, err := sql.Open("postgres", sqlDSNOf(dsn))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { admin.Close() })
			name := fmt.Sprintf("xferdb_mlive_%d", time.Now().UnixNano())
			mustExecDB(t, admin, `CREATE DATABASE `+name)
			t.Cleanup(func() { admin.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`) })
			tgtDSN := withDB(dsn, name)
			runLiveMongo(t, uri, sharded, map[string]any{"type": tc.typ, "dsn": tgtDSN}, func(t *testing.T) *sql.DB {
				db, err := sql.Open("postgres", sqlDSNOf(tgtDSN))
				if err != nil {
					t.Fatal(err)
				}
				return db
			})
		})
	}
}

// sqlDSNOf is dsn as lib/pq takes it: a yugabyte:// URL as postgres://.
func sqlDSNOf(dsn string) string {
	if strings.HasPrefix(dsn, "yugabyte://") {
		return "postgres://" + strings.TrimPrefix(dsn, "yugabyte://")
	}
	return dsn
}

// withDB replaces the database in a URL-style DSN (postgres://, mongodb://).
func withDB(dsn, name string) string {
	scheme := strings.Index(dsn, "://")
	path := strings.Index(dsn[scheme+3:], "/")
	query := ""
	if q := strings.Index(dsn, "?"); q >= 0 {
		query = dsn[q:]
	}
	if path < 0 {
		return strings.TrimSuffix(dsn, query) + "/" + name + query
	}
	return dsn[:scheme+3+path] + "/" + name + query
}

func runLiveMongo(t *testing.T, uri string, sharded bool, target map[string]any, openTarget func(*testing.T) *sql.DB) {
	ctx := context.Background()
	dbName := fmt.Sprintf("xferdb_mlive_%d", time.Now().UnixNano())
	srcURI := withDB(uri, dbName)
	client, err := mongo.Connect(options.Client().ApplyURI(srcURI))
	if err != nil {
		t.Fatal(err)
	}
	mdb := client.Database(dbName)
	t.Cleanup(func() {
		t0 := time.Now()
		mdb.Drop(context.Background()) //nolint:errcheck
		client.Disconnect(context.Background())
		t.Logf("dropped the source database in %s", time.Since(t0).Round(time.Millisecond))
	})

	// users: ObjectId _id, a subdocument the plan flattens, an array it
	// stores as jsonb. Created with post-images where the server has them
	// (6.0+); things has none, so its updates are looked up.
	if err := mdb.RunCommand(ctx, bson.D{{Key: "create", Value: "users"},
		{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}).Err(); err != nil {
		t.Logf("users without post-images: %v", err)
		mdb.CreateCollection(ctx, "users") //nolint:errcheck
	}
	users, things := mdb.Collection("users"), mdb.Collection("things")
	setupAt := time.Now()
	if sharded {
		shardMongo(t, client, dbName)
		t.Logf("sharded the collections in %s", time.Since(setupAt).Round(time.Millisecond))
	}
	var docs []interface{}
	for i := 0; i < 20000; i++ {
		docs = append(docs, bson.D{
			{Key: "_id", Value: bson.NewObjectID()},
			{Key: "n", Value: int64(i)},
			{Key: "name", Value: fmt.Sprint("user", i)},
			{Key: "address", Value: bson.D{{Key: "city", Value: fmt.Sprint("city", i%10)}, {Key: "zip", Value: fmt.Sprint(10000 + i)}}},
			{Key: "tags", Value: bson.A{"a", fmt.Sprint("t", i%7)}},
			{Key: "nick", Value: fmt.Sprint("nick", i)},
		})
	}
	if _, err := users.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		t.Fatal(err)
	}
	// things: an _id of three types (numbers, strings, ObjectIds), paged a
	// type group at a time.
	docs = nil
	for i := 0; i < 600; i++ {
		var id interface{} = int64(i)
		switch i % 3 {
		case 1:
			id = fmt.Sprintf("s%05d", i)
		case 2:
			id = bson.NewObjectID()
		}
		docs = append(docs, bson.D{{Key: "_id", Value: id}, {Key: "v", Value: int64(i)}, {Key: "label", Value: fmt.Sprint("thing", i)}})
	}
	if _, err := things.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		t.Fatal(err)
	}
	t.Logf("loaded the source in %s", time.Since(setupAt).Round(time.Millisecond))

	dir := t.TempDir()
	db, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	launcher := &live.InProcessLauncher{}
	defer func() {
		t0 := time.Now()
		launcher.StopAll()
		t.Logf("stopped the workers in %s", time.Since(t0).Round(time.Millisecond))
	}()
	newServer := func() (*Server, *httptest.Server) {
		s := NewServer(db, slog.New(slog.DiscardHandler))
		s.EnableLive(filepath.Join(dir, "cdc"), launcher)
		s.live.Interval = 200 * time.Millisecond
		s.live.QuietWait = 500 * time.Millisecond
		return s, httptest.NewServer(s.Handler())
	}
	srv, hs := newServer()

	var p struct {
		ID string `json:"id"`
	}
	post(t, hs.URL+"/api/v1/projects", map[string]any{
		"name":          "mlive-" + dbName,
		"source_config": map[string]any{"type": "mongodb", "dsn": srcURI},
		"target_config": target,
	}, http.StatusCreated, &p)
	began := time.Now()
	step := func(what string) { t.Logf("%6.1fs %s", time.Since(began).Seconds(), what) }
	analyzeMongo(t, hs, p.ID)
	step("analyzed")

	var ready struct {
		Live struct {
			Ready  bool `json:"ready"`
			Checks []struct {
				Name, Detail string
				OK           bool
			} `json:"checks"`
		} `json:"live"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/preflight", map[string]any{"mode": "live"}, http.StatusOK, &ready)
	if !ready.Live.Ready {
		t.Fatalf("live readiness: %+v", ready.Live.Checks)
	}

	stopWriter, writerDone, ops := mongoWriter(t, users, things, client)
	var m struct {
		ID   string `json:"id"`
		Mode string `json:"mode"`
	}
	post(t, hs.URL+"/api/v1/projects/"+p.ID+"/migrations", map[string]any{
		"mode": "live", "tables": []string{"users", "things"}, "batch_size": 100, "table_workers": 2,
	}, http.StatusCreated, &m)
	if m.Mode != "live" {
		t.Fatalf("mode = %q", m.Mode)
	}

	waitLive(t, hs, m.ID, "catch-up", func(l liveMig) bool {
		return l.Phase == "catch_up" || l.Phase == "streaming"
	})
	step(fmt.Sprintf("snapshot done after %d writer operations", ops.Load()))
	wdir := filepath.Join(dir, "cdc", m.ID)
	if err := launcher.Stop(ctx, wdir, worker.RoleApply); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	srv.live.Shutdown()
	hs.Close()
	srv, hs = newServer()
	defer func() {
		t0 := time.Now()
		srv.live.Shutdown()
		hs.Close()
		t.Logf("shut the server down in %s", time.Since(t0).Round(time.Millisecond))
	}()
	srv.ResumeLive(ctx)

	waitLive(t, hs, m.ID, "streaming", func(l liveMig) bool { return l.Phase == "streaming" })
	step("streaming")
	time.Sleep(time.Second)

	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	cut := waitLive(t, hs, m.ID, "the busy cutover to stop", func(l liveMig) bool {
		return l.Live.Cutover != nil && !l.Live.Cutover.Running
	})
	if !strings.Contains(cut.Live.Cutover.Error, "writes are still happening") || cut.Phase != "streaming" {
		t.Fatalf("cutover under writes: phase %s, error %q", cut.Phase, cut.Live.Cutover.Error)
	}

	close(stopWriter)
	<-writerDone
	step(fmt.Sprintf("busy cutover refused; writer stopped after %d operations", ops.Load()))
	marker := bson.NewObjectID()
	if _, err := users.InsertOne(ctx, bson.D{{Key: "_id", Value: marker}, {Key: "name", Value: "marker"}, {Key: "late_field", Value: "x"}}); err != nil {
		t.Fatal(err)
	}
	tgt := openTarget(t)
	defer tgt.Close()
	streamed := waitLive(t, hs, m.ID, "the last change to apply", func(l liveMig) bool {
		return count(tgt, `SELECT count(*) FROM users WHERE name = 'marker'`) == 1 && l.Live.LagChanges == 0
	})
	if a := streamed.Live.Apply; a == nil || a.Tables["users"].Inserts == 0 || a.Tables["users"].Updates == 0 || a.Tables["users"].Deletes == 0 {
		t.Errorf("stream activity = %+v", streamed.Live.Apply)
	}
	// A field the plan doesn't know went to _extra.
	var extra sql.NullString
	tgt.QueryRow(`SELECT _extra FROM users WHERE name = 'marker'`).Scan(&extra)
	if !strings.Contains(extra.String, "late_field") {
		t.Errorf("marker's _extra = %q, want late_field in it", extra.String)
	}
	// The counts match before cutover too.
	for _, c := range []*mongo.Collection{users, things} {
		n, _ := c.CountDocuments(ctx, bson.D{})
		if got := count(tgt, `SELECT count(*) FROM `+c.Name()); int64(got) != n {
			t.Errorf("%s: source has %d documents, target %d rows", c.Name(), n, got)
		}
	}

	post(t, hs.URL+"/api/v1/migrations/"+m.ID+"/cutover", map[string]any{}, http.StatusAccepted, nil)
	step("last change applied")
	waitMigration(t, hs, m.ID, "completed")
	step("cut over")
	if _, err := os.Stat(wdir); !os.IsNotExist(err) {
		t.Errorf("worker directory still there after cutover: %v", err)
	}
	var vs []validationResult
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		vs = listValidations(t, hs, m.ID)
		if len(vs) == 1 && vs[0].Status == "completed" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	step("validated")
	if len(vs) != 1 || vs[0].Status != "completed" {
		t.Fatalf("validation after cutover: %+v", vs)
	}
	checkValidationPassed(t, hs, vs[0].ID, "users", "things")
}

// shardMongo shards users by a hashed _id and things by a range of v, split
// at 300 with the upper half moved to another shard.
func shardMongo(t *testing.T, client *mongo.Client, db string) {
	t.Helper()
	ctx := context.Background()
	admin := client.Database("admin")
	run := func(cmd bson.D) error { return admin.RunCommand(ctx, cmd).Err() }
	if err := run(bson.D{{Key: "enableSharding", Value: db}}); err != nil {
		t.Fatalf("enableSharding: %v", err)
	}
	if err := run(bson.D{{Key: "shardCollection", Value: db + ".users"}, {Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}}}); err != nil {
		t.Fatalf("shard users: %v", err)
	}
	if err := run(bson.D{{Key: "shardCollection", Value: db + ".things"}, {Key: "key", Value: bson.D{{Key: "v", Value: 1}}}}); err != nil {
		t.Fatalf("shard things: %v", err)
	}
	if err := run(bson.D{{Key: "split", Value: db + ".things"}, {Key: "middle", Value: bson.D{{Key: "v", Value: 300}}}}); err != nil {
		t.Fatalf("split things: %v", err)
	}
	var shards struct {
		Shards []struct {
			ID string `bson:"_id"`
		} `bson:"shards"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&shards); err != nil {
		t.Fatal(err)
	}
	// A new sharded collection's one chunk is on the database's primary
	// shard: move the upper half to another.
	var dbInfo struct {
		Primary string `bson:"primary"`
	}
	if err := client.Database("config").Collection("databases").FindOne(ctx, bson.D{{Key: "_id", Value: db}}).Decode(&dbInfo); err != nil {
		t.Fatalf("primary shard of %s: %v", db, err)
	}
	for _, sh := range shards.Shards {
		if sh.ID == dbInfo.Primary {
			continue
		}
		if err := run(bson.D{{Key: "moveChunk", Value: db + ".things"}, {Key: "find", Value: bson.D{{Key: "v", Value: 300}}}, {Key: "to", Value: sh.ID}}); err != nil {
			t.Fatalf("move things' upper chunk to %s: %v", sh.ID, err)
		}
		return
	}
	t.Fatalf("no shard but the primary (%s) among %d", dbInfo.Primary, len(shards.Shards))
}

// transientWrite reports a write that failed only because another writer
// got there first, as happens on a sharded cluster when an update that
// moves a document between shards (a distributed transaction) conflicts.
func transientWrite(err error) bool {
	var se mongo.ServerError
	if errors.As(err, &se) && (se.HasErrorLabel("TransientTransactionError") || se.HasErrorCode(112) || se.HasErrorCode(251)) {
		return true // WriteConflict, NoSuchTransaction
	}
	return strings.Contains(err.Error(), "would move shards")
}

// analyzeMongo builds the project's schema plan, reading the whole
// streamed reply so the handler runs to the end.
func analyzeMongo(t *testing.T, hs *httptest.Server, projectID string) {
	t.Helper()
	resp, err := http.Post(hs.URL+"/api/v1/projects/"+projectID+"/analyze", "application/json", bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || bytes.Contains(b, []byte(`"type":"error"`)) {
		t.Fatalf("analyze: %d %s", resp.StatusCode, b)
	}
	if !bytes.Contains(b, []byte(`"type":"done"`)) {
		t.Fatalf("analyze didn't finish: %s", b)
	}
}

// mongoWriter changes both collections until stopped: inserts (some with a
// field the plan doesn't know), $set/$inc/$unset updates, replaces,
// deletes, and now and then a multi-document transaction.
func mongoWriter(t *testing.T, users, things *mongo.Collection, client *mongo.Client) (chan struct{}, chan struct{}, *atomic.Int64) {
	stop, done := make(chan struct{}), make(chan struct{})
	var ops atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			r := rand.New(rand.NewSource(int64(w)))
			pick := func(c *mongo.Collection) interface{} {
				var d bson.D
				cur, err := c.Aggregate(ctx, mongo.Pipeline{{{Key: "$sample", Value: bson.D{{Key: "size", Value: 1}}}}})
				if err != nil {
					return nil
				}
				defer cur.Close(ctx)
				if cur.Next(ctx) && cur.Decode(&d) == nil {
					for _, e := range d {
						if e.Key == "_id" {
							return e.Value
						}
					}
				}
				return nil
			}
			next := int64(100000 * (w + 1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				next++
				var err error
				switch k := r.Intn(10); {
				case k < 3:
					doc := bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "n", Value: next}, {Key: "name", Value: fmt.Sprint("new", next)},
						{Key: "address", Value: bson.D{{Key: "city", Value: "late"}, {Key: "zip", Value: "0"}}}, {Key: "tags", Value: bson.A{"new"}}}
					if k == 0 {
						doc = append(doc, bson.E{Key: "surprise", Value: bson.D{{Key: "deep", Value: next}}})
					}
					_, err = users.InsertOne(ctx, doc)
				case k < 5:
					if id := pick(users); id != nil {
						_, err = users.UpdateByID(ctx, id, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: fmt.Sprint("upd", next)}, {Key: "address.city", Value: "moved"}}},
							{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}})
					}
				case k == 5:
					if id := pick(users); id != nil {
						_, err = users.UpdateByID(ctx, id, bson.D{{Key: "$unset", Value: bson.D{{Key: "nick", Value: ""}, {Key: "tags", Value: ""}}}})
					}
				case k == 6:
					if id := pick(users); id != nil {
						_, err = users.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
					}
				case k == 7:
					if id := pick(things); id != nil {
						_, err = things.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "v", Value: next}, {Key: "label", Value: "replaced"}})
					}
				case k == 8:
					if id := pick(things); id != nil {
						if r.Intn(2) == 0 {
							_, err = things.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
						} else {
							_, err = things.UpdateByID(ctx, id, bson.D{{Key: "$set", Value: bson.D{{Key: "label", Value: fmt.Sprint("upd", next)}}}})
						}
					}
				default:
					// A transaction over both collections.
					sess, serr := client.StartSession()
					if serr != nil {
						err = serr
						break
					}
					_, err = sess.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
						if _, err := things.InsertOne(ctx, bson.D{{Key: "_id", Value: fmt.Sprintf("tx%07d", next)}, {Key: "v", Value: next}, {Key: "label", Value: "tx"}}); err != nil {
							return nil, err
						}
						if id := pick(users); id != nil {
							if _, err := users.UpdateByID(ctx, id, bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: fmt.Sprint("tx", next)}}}}); err != nil {
								return nil, err
							}
						}
						return nil, nil
					})
					sess.EndSession(ctx)
				}
				if err != nil && transientWrite(err) {
					continue // a concurrent writer won; try something else
				}
				if err != nil {
					t.Errorf("writer: %v", err)
					return
				}
				ops.Add(1)
			}
		}(w)
	}
	go func() { wg.Wait(); close(done) }()
	return stop, done, &ops
}
