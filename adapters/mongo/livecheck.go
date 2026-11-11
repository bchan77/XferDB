package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

type topology int

const (
	topoStandalone topology = iota
	topoReplicaSet
	topoSharded
)

// topologyOf tells a replica set member, a mongos and a standalone server
// apart by their hello reply.
func topologyOf(ctx context.Context, client *mongo.Client) (topology, error) {
	var hello struct {
		SetName string `bson:"setName"`
		Msg     string `bson:"msg"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return 0, fmt.Errorf("hello: %w", err)
	}
	switch {
	case hello.Msg == "isdbgrid":
		return topoSharded, nil
	case hello.SetName != "":
		return topoReplicaSet, nil
	}
	return topoStandalone, nil
}

// serverVersion returns the server's major and minor version.
func serverVersion(ctx context.Context, client *mongo.Client) (int, int, error) {
	var info struct {
		Version      string  `bson:"version"`
		VersionArray []int32 `bson:"versionArray"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info); err != nil {
		return 0, 0, fmt.Errorf("buildInfo: %w", err)
	}
	if len(info.VersionArray) < 2 {
		return 0, 0, fmt.Errorf("buildInfo: unexpected version %q", info.Version)
	}
	return int(info.VersionArray[0]), int(info.VersionArray[1]), nil
}

// supportsPostImages reports whether the server can return post-images
// (fullDocument: whenAvailable), MongoDB 6.0 and later.
func supportsPostImages(ctx context.Context, client *mongo.Client) (bool, error) {
	major, _, err := serverVersion(ctx, client)
	if err != nil {
		return false, err
	}
	return major >= 6, nil
}

// oplogRange returns the cluster times of the oldest and newest entries in
// the oplog of the replica set client is connected to; ok is false when
// there is no oplog to read (a mongos) or no privilege to read it.
func oplogRange(ctx context.Context, client *mongo.Client) (oldest, newest bson.Timestamp, ok bool, err error) {
	oplog := client.Database("local").Collection("oplog.rs")
	first := func(dir int) (bson.Timestamp, error) {
		var e struct {
			TS bson.Timestamp `bson:"ts"`
		}
		err := oplog.FindOne(ctx, bson.D{}, options.FindOne().
			SetSort(bson.D{{Key: "$natural", Value: dir}}).
			SetProjection(bson.D{{Key: "ts", Value: 1}})).Decode(&e)
		return e.TS, err
	}
	if oldest, err = first(1); err != nil {
		var se mongo.ServerError
		if errors.Is(err, mongo.ErrNoDocuments) || (errors.As(err, &se) && (se.HasErrorCode(13) || se.HasErrorCode(26) || se.HasErrorCode(20))) {
			// No oplog here, no find on local.oplog.rs, or a mongos
			// (IllegalOperation: no local database through mongos).
			return oldest, newest, false, nil
		}
		return oldest, newest, false, fmt.Errorf("read the oplog: %w", err)
	}
	if newest, err = first(-1); err != nil {
		return oldest, newest, false, fmt.Errorf("read the oplog: %w", err)
	}
	return oldest, newest, true, nil
}

func oldestOplog(ctx context.Context, client *mongo.Client) (bson.Timestamp, bool, error) {
	oldest, _, ok, err := oplogRange(ctx, client)
	return oldest, ok, err
}

// shardCount returns how many shards a sharded cluster has.
func shardCount(ctx context.Context, client *mongo.Client) (int, error) {
	var res struct {
		Shards []bson.Raw `bson:"shards"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&res); err != nil {
		return 0, err
	}
	return len(res.Shards), nil
}

// oplogWarnWindow is the oplog window below which readiness warns: the
// capture worker can't be down (or behind) longer than it.
const oplogWarnWindow = 24 * time.Hour

// LiveChecks implements adapters.LivePreflighter: what streaming changes
// from this database needs.
func (s *Source) LiveChecks(ctx context.Context, tables []string) ([]adapters.LiveCheck, error) {
	var checks []adapters.LiveCheck
	add := func(c adapters.LiveCheck) { checks = append(checks, c) }

	topo, err := topologyOf(ctx, s.client)
	if err != nil {
		return nil, err
	}
	switch topo {
	case topoReplicaSet:
		add(adapters.LiveCheck{Name: "a replica set or sharded cluster", OK: true, Detail: "the source is a replica set member"})
	case topoSharded:
		detail := "the source is a sharded cluster (mongos)"
		if n, err := shardCount(ctx, s.client); err == nil {
			detail = fmt.Sprintf("the source is a sharded cluster (mongos) with %d shards", n)
		}
		add(adapters.LiveCheck{Name: "a replica set or sharded cluster", OK: true, Detail: detail})
	default:
		add(adapters.LiveCheck{Name: "a replica set or sharded cluster", Detail: "the source is a standalone server, which keeps no oplog",
			Fix: "live migration reads the oplog through a change stream: run the server as a replica set (a single-member one will do)"})
		return checks, nil
	}

	major, minor, err := serverVersion(ctx, s.client)
	if err != nil {
		return nil, err
	}
	ver := fmt.Sprintf("MongoDB %d.%d", major, minor)
	if major < 4 || (major == 4 && minor < 2) {
		add(adapters.LiveCheck{Name: "MongoDB 4.2 or later", Detail: ver, Fix: "upgrade the source to MongoDB 4.2 or later"})
		return checks, nil
	}
	add(adapters.LiveCheck{Name: "MongoDB 4.2 or later", OK: true, Detail: ver})

	// The privilege is what opening a stream needs, so open one.
	db := s.client.Database(s.database)
	cs, err := db.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetMaxAwaitTime(time.Millisecond))
	if err != nil {
		add(adapters.LiveCheck{Name: "can open a change stream", Detail: err.Error(),
			Fix: fmt.Sprintf("grant the user the changeStream and find actions on database %s (the built-in read role has both)", s.database)})
	} else {
		cs.Close(ctx) //nolint:errcheck
		add(adapters.LiveCheck{Name: "can open a change stream", OK: true, Detail: "on database " + s.database})
	}

	if topo == topoSharded {
		add(adapters.LiveCheck{Name: "oplog window", Warning: true,
			Detail: "each shard keeps its own oplog, which can't be read through mongos",
			Fix:    "check every shard's oplog window (rs.printReplicationInfo() on each shard): the capture worker can't be down or behind for longer than the shortest one"})
	} else if oldest, newest, ok, err := oplogRange(ctx, s.client); err != nil {
		add(adapters.LiveCheck{Name: "oplog window", Warning: true, Detail: err.Error()})
	} else if !ok {
		add(adapters.LiveCheck{Name: "oplog window", Warning: true,
			Detail: "can't read local.oplog.rs to measure how long the oplog keeps changes",
			Fix:    "grant read on the local database to see the oplog window here"})
	} else {
		window := time.Duration(newest.T-oldest.T) * time.Second
		c := adapters.LiveCheck{Name: "oplog window", OK: true,
			Detail: fmt.Sprintf("the oplog holds %s of writes", window.Round(time.Minute))}
		if window < oplogWarnWindow {
			c.OK, c.Warning = false, true
			c.Fix = "the capture worker can't be down or behind for longer than this; raise the oplog size (replSetResizeOplog) or minimum retention (oplogMinRetentionHours)"
		}
		add(c)
	}

	if major >= 6 {
		with, total, err := s.postImageCollections(ctx, tables)
		if err != nil {
			return nil, err
		}
		detail := fmt.Sprintf("%d of %d collections keep post-images", with, total)
		if with < total {
			detail += "; for the others each update's document is read from the primary when it is streamed"
		}
		add(adapters.LiveCheck{Name: "update post-images", OK: true, Detail: detail})
	}
	return checks, nil
}

// postImageCollections counts the collections to stream that keep
// post-images (changeStreamPreAndPostImages), of how many.
func (s *Source) postImageCollections(ctx context.Context, tables []string) (with, total int, err error) {
	want := map[string]bool{}
	for _, t := range tables {
		want[s.collectionFor(t)] = true
	}
	specs, err := s.client.Database(s.database).ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return 0, 0, fmt.Errorf("list collections: %w", err)
	}
	for _, spec := range specs {
		if shouldSkipCollection(spec.Name, spec.Type) || (len(want) > 0 && !want[spec.Name]) {
			continue
		}
		total++
		if v, err := spec.Options.LookupErr("changeStreamPreAndPostImages", "enabled"); err == nil {
			if on, ok := v.BooleanOK(); ok && on {
				with++
			}
		}
	}
	return with, total, nil
}
