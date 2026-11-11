package mongo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// ChangeSource streams a MongoDB database's changes through a change stream
// (#179). It implements cdc.ChangeSource. Nothing is created on the source:
// a change stream reads the oplog, which a replica set (and each shard of a
// sharded cluster) keeps anyway, so Prepare only records where to start and
// Teardown has nothing to drop.
//
// On a sharded cluster the stream is opened through mongos, which merges the
// shards' oplogs in cluster-time order. Chunk migrations between shards
// aren't reported as changes, and an event's documentKey also carries the
// shard key, which lookups use as it is. The connection must reach one
// mongos: a stream's cursor lives on the mongos that opened it.
//
// Each document is converted with the frozen schema plan exactly as
// ReadBatch converts it (ConvertDocument), so a document the snapshot copied
// and one the stream replays reach the target as the same row. Fields the
// plan doesn't know go to _extra, as in the copy: MongoDB never sends an
// OpSchema change.
//
// Positions are cluster times, with the resume token of the event (or of the
// stream's high-water mark) that reached them: "<t>.<i>" or
// "<t>.<i>/<token>". A position without a token is a start point
// (startAtOperationTime); one with a token resumes after it.
type ChangeSource struct {
	cfg      adapters.ConnectionConfig
	client   *mongo.Client
	database string

	// From SetPlan: collection → its plan rows, target table, key columns
	// and every column a converted row has (to fill in the missing ones).
	plan     map[string][]state.SchemaPlanRow
	table    map[string]string
	keyCols  map[string][]string
	columns  map[string][]string
	extraCol map[string]string

	mu sync.Mutex
	// acked is the highest position the caller has stored (Ack), sent the
	// Pos of the last change handed to out and idle the stream's high-water
	// mark: read, with nothing of ours in it since sent. While acked has
	// reached sent, the stream is confirmed up to idle.
	acked, sent, idle position

	topo      topology
	topoKnown bool
	// The last probeRetention result, and when it was taken.
	probed   cdc.Retention
	probedAt time.Time
}

// NewChangeSource connects to the source for a live migration stream.
func NewChangeSource(ctx context.Context, cfg adapters.ConnectionConfig, _ string) (*ChangeSource, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("mongo change source: DSN is required (full mongodb:// URI)")
	}
	// Changes and the lookups that complete them come from the primary: a
	// secondary may not have the event's write yet.
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.DSN).SetReadPreference(readpref.Primary()))
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}
	db := cfg.Database
	if db == "" {
		db = DatabaseFromURI(cfg.DSN)
	}
	if db == "" {
		client.Disconnect(ctx) //nolint:errcheck
		return nil, fmt.Errorf("mongo change source: the connection string names no database")
	}
	return &ChangeSource{cfg: cfg, client: client, database: db}, nil
}

// SetPlan gives the stream the frozen schema plan, which Stream needs to
// convert documents. The capture worker calls it before Stream.
func (s *ChangeSource) SetPlan(plan []state.SchemaPlanRow) error {
	s.plan = map[string][]state.SchemaPlanRow{}
	s.table = map[string]string{}
	s.keyCols = map[string][]string{}
	s.columns = map[string][]string{}
	s.extraCol = map[string]string{}
	for _, row := range plan {
		c := row.Collection
		s.plan[c] = append(s.plan[c], row)
		s.table[c] = TableName(c)
		if row.IsPK {
			s.keyCols[c] = append(s.keyCols[c], row.PgColumn)
		}
		if row.Strategy != "skip" && row.Strategy != "flatten" {
			s.columns[c] = append(s.columns[c], row.PgColumn)
		}
	}
	for c := range s.plan {
		// As Source.SetPlan: the _extra column is always called that.
		s.extraCol[c] = "_extra"
		s.columns[c] = append(s.columns[c], "_extra")
		if len(s.keyCols[c]) == 0 {
			return fmt.Errorf("schema plan of %s has no primary key (_id)", c)
		}
	}
	return nil
}

// Close implements cdc.ChangeSource.
func (s *ChangeSource) Close() error {
	return s.client.Disconnect(context.Background())
}

// Prepare implements cdc.ChangeSource: it checks the source can stream
// changes and returns its cluster time now. Nothing is kept on the source,
// so there is no existing start point to return; the manager keeps it.
func (s *ChangeSource) Prepare(ctx context.Context, _ []string) (cdc.Position, error) {
	topo, err := topologyOf(ctx, s.client)
	if err != nil {
		return nil, err
	}
	if topo == topoStandalone {
		return nil, errStandalone
	}
	return s.CurrentPosition(ctx)
}

var errStandalone = errors.New("a standalone MongoDB server keeps no oplog, so it has no change stream: live migration needs a replica set or a sharded cluster")

// CurrentPosition implements cdc.ChangeSource: the cluster time of the
// source's last write, as a command reports it (operationTime).
func (s *ChangeSource) CurrentPosition(ctx context.Context) (cdc.Position, error) {
	raw, err := s.client.Database(s.database).RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Raw()
	if err != nil {
		return nil, fmt.Errorf("current cluster time: %w", err)
	}
	v, err := raw.LookupErr("operationTime")
	if err != nil {
		return nil, errors.New("current cluster time: the server reports no operationTime (is it a replica set member or mongos?)")
	}
	t, i, ok := v.TimestampOK()
	if !ok {
		return nil, fmt.Errorf("current cluster time: operationTime is %s", v.Type)
	}
	return position{T: t, I: i}.encode(), nil
}

// Stream implements cdc.ChangeSource.
func (s *ChangeSource) Stream(ctx context.Context, from cdc.Position, out chan<- cdc.Change) error {
	if s.plan == nil {
		return errors.New("mongo change source: schema plan not set")
	}
	start, err := decodePosition(from)
	if err != nil {
		return err
	}
	collections := make([]string, 0, len(s.plan))
	for c := range s.plan {
		collections = append(collections, c)
	}
	pipeline := mongo.Pipeline{{{Key: "$match", Value: bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: collections}}}},
		bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"dropDatabase", "invalidate"}}}}},
	}}}}}}

	opts := options.ChangeStream().SetMaxAwaitTime(time.Second)
	postImages, err := supportsPostImages(ctx, s.client)
	if err != nil {
		return err
	}
	if postImages {
		// The post-image where the collection keeps them; for the others
		// the update has no full document and is looked up below.
		opts.SetFullDocument(options.WhenAvailable)
	} else {
		opts.SetFullDocument(options.UpdateLookup)
	}
	if start.Token != "" {
		opts.SetResumeAfter(bson.D{{Key: "_data", Value: start.Token}})
	} else {
		opts.SetStartAtOperationTime(&bson.Timestamp{T: start.T, I: start.I})
	}
	cs, err := s.client.Database(s.database).Watch(ctx, pipeline, opts)
	if err != nil {
		return streamError(err)
	}
	defer cs.Close(context.Background())

	s.mu.Lock()
	if s.acked.less(start) {
		s.acked = start
	}
	s.sent, s.idle = s.acked, s.acked
	s.mu.Unlock()

	// A transaction's changes are sent whole, with Pos on the last: its
	// events (one session and transaction number; on a sharded cluster the
	// shards' parts have cluster times of their own) are held until one from
	// outside it comes, or the stream has nothing more for now. Resuming in
	// the middle would split a document's move between shards.
	txn := txnBuffer{recent: newTxnInserts(txnMemory)}
	send := func(c cdc.Change, pos position) error {
		select {
		case out <- c:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.mu.Lock()
		s.sent = pos
		s.mu.Unlock()
		return nil
	}
	flush := func() error {
		changes, last, any := txn.take()
		if !any {
			return nil
		}
		if len(changes) == 0 {
			s.noteIdle(last) // nothing in it to apply
			return nil
		}
		for i, c := range changes {
			if i == len(changes)-1 {
				c.Pos = last.encode()
			}
			if err := send(c, last); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		if !cs.TryNext(ctx) {
			if err := cs.Err(); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return streamError(err)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := flush(); err != nil {
				return err
			}
			// Nothing for us up to the high-water mark: an idle source
			// still moves it, by the replica set's periodic no-op writes.
			if p, err := tokenPosition(cs.ResumeToken()); err == nil {
				s.noteIdle(p)
			}
			continue
		}
		var ev changeEvent
		if err := cs.Decode(&ev); err != nil {
			return fmt.Errorf("decode change event: %w", err)
		}
		pos, err := tokenPosition(ev.ID)
		if err != nil {
			return err
		}
		id := ev.txnID()
		if !txn.part(id) {
			if err := flush(); err != nil {
				return err
			}
		}
		c, ok, err := s.change(ctx, &ev)
		if err != nil {
			return err
		}
		if id != "" {
			if ok && ev.OperationType == "delete" && txn.moved(&ev) {
				ok = false // the old copy of a document this transaction moved
			}
			txn.add(id, &ev, c, ok, pos)
			continue
		}
		if !ok {
			// Nothing to apply (an update whose document is gone by now:
			// its delete follows).
			s.noteIdle(pos)
			continue
		}
		c.Pos = pos.encode()
		if err := send(c, pos); err != nil {
			return err
		}
	}
}

// txnMemory is how many recent transactions' inserts are remembered to
// recognise a move's delete by: the delete may come after an unrelated
// event has already ended the buffered transaction.
const txnMemory = 4096

// txnBuffer holds the changes of one transaction until it ends.
type txnBuffer struct {
	id      string
	changes []cdc.Change
	last    position
	any     bool
	recent  *txnInserts
}

// part reports whether an event of transaction id belongs to the
// transaction held (none held: it can't).
func (b *txnBuffer) part(id string) bool {
	return b.any && id != "" && id == b.id
}

func (b *txnBuffer) add(id string, ev *changeEvent, c cdc.Change, ok bool, pos position) {
	b.id, b.last, b.any = id, pos, true
	if ok {
		b.changes = append(b.changes, c)
	}
	if ev.OperationType == "insert" {
		b.recent.add(id, ev.DocumentKey)
	}
}

// moved reports whether a delete is the old copy of a document its
// transaction moved between shards: an update that changes a document's
// shard key is streamed as an insert under the new shard key and a delete
// under the old one, in one transaction, in either order. Keyed by _id, the
// delete coming second would remove the row the insert just wrote. A
// transaction that inserts a document and deletes it again deletes it
// under the shard key it was inserted with.
func (b *txnBuffer) moved(ev *changeEvent) bool {
	ins, ok := b.recent.get(ev.txnID(), ev.DocumentKey)
	return ok && ins != docKeyString(ev.DocumentKey)
}

func (b *txnBuffer) take() ([]cdc.Change, position, bool) {
	changes, last, any := b.changes, b.last, b.any
	b.id, b.changes, b.last, b.any = "", nil, position{}, false
	return changes, last, any
}

// txnInserts remembers, for the last max transactions, the documentKey
// each _id was inserted with.
type txnInserts struct {
	max   int
	order []string
	byTxn map[string]map[string]string
}

func newTxnInserts(max int) *txnInserts {
	return &txnInserts{max: max, byTxn: map[string]map[string]string{}}
}

func (t *txnInserts) add(txn string, docKey bson.D) {
	id, ok := idOf(docKey)
	if !ok || txn == "" {
		return
	}
	m, seen := t.byTxn[txn]
	if !seen {
		if len(t.order) >= t.max {
			delete(t.byTxn, t.order[0])
			t.order = t.order[1:]
		}
		m = map[string]string{}
		t.byTxn[txn] = m
		t.order = append(t.order, txn)
	}
	m[id] = docKeyString(docKey)
}

func (t *txnInserts) get(txn string, docKey bson.D) (string, bool) {
	id, ok := idOf(docKey)
	if !ok || txn == "" {
		return "", false
	}
	k, ok := t.byTxn[txn][id]
	return k, ok
}

// idOf renders a documentKey's _id, for comparing.
func idOf(docKey bson.D) (string, bool) {
	for _, e := range docKey {
		if e.Key == "_id" {
			k, err := encodeID(e.Value)
			return k, err == nil
		}
	}
	return "", false
}

func docKeyString(docKey bson.D) string {
	b, err := bson.MarshalExtJSON(docKey, true, false)
	if err != nil {
		return fmt.Sprint(docKey)
	}
	return string(b)
}

// changeEvent is the part of a change event Stream reads.
type changeEvent struct {
	ID            bson.Raw       `bson:"_id"`
	OperationType string         `bson:"operationType"`
	ClusterTime   bson.Timestamp `bson:"clusterTime"`
	WallTime      *time.Time     `bson:"wallTime"`
	NS            struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey  bson.D `bson:"documentKey"`
	FullDocument bson.D `bson:"fullDocument"`
	// Set on a multi-document transaction's events.
	LSID      bson.Raw `bson:"lsid"`
	TxnNumber *int64   `bson:"txnNumber"`
}

// txnID identifies the transaction an event belongs to; "" when it is none.
func (ev *changeEvent) txnID() string {
	if ev.TxnNumber == nil || len(ev.LSID) == 0 {
		return ""
	}
	return fmt.Sprintf("%x/%d", []byte(ev.LSID), *ev.TxnNumber)
}

// change turns an event into a row change; ok is false when there is none
// to apply.
func (s *ChangeSource) change(ctx context.Context, ev *changeEvent) (cdc.Change, bool, error) {
	coll := ev.NS.Coll
	switch ev.OperationType {
	case "insert", "update", "replace", "delete":
	case "drop", "rename", "dropDatabase", "invalidate":
		what := "collection " + coll
		if coll == "" {
			what = "database " + ev.NS.DB
		}
		return cdc.Change{}, false, fmt.Errorf("%s: %s on the source; the live migration can't go on (start a new one)", what, ev.OperationType)
	default:
		// Index builds, collMod, shardCollection, ...: nothing for rows.
		return cdc.Change{}, false, nil
	}
	plan, ok := s.plan[coll]
	if !ok {
		return cdc.Change{}, false, nil
	}
	at := time.Unix(int64(ev.ClusterTime.T), 0).UTC()
	if ev.WallTime != nil {
		at = ev.WallTime.UTC()
	}
	c := cdc.Change{Table: s.table[coll], CommitAt: at}

	if ev.OperationType == "delete" {
		key, err := s.keyOf(coll, ev.DocumentKey)
		if err != nil {
			return cdc.Change{}, false, err
		}
		c.Op, c.Key = cdc.OpDelete, key
		return c, true, nil
	}

	doc := ev.FullDocument
	if doc == nil {
		// An update without a post-image: read the document as it is now.
		// It may be newer than this event, which the later events it
		// already shows rewrite the same way.
		var err error
		if doc, err = s.lookup(ctx, coll, ev.DocumentKey); err != nil {
			return cdc.Change{}, false, err
		}
		if doc == nil {
			return cdc.Change{}, false, nil
		}
	}
	row, err := ConvertDocument(doc, plan, s.extraCol[coll])
	if err != nil {
		return cdc.Change{}, false, fmt.Errorf("%s: %w", coll, err)
	}
	// The row replaces the target's whole row: a field the document no
	// longer has, or no _extra, sets its column to NULL.
	for _, col := range s.columns[coll] {
		if _, ok := row[col]; !ok {
			row[col] = nil
		}
	}
	c.Key = map[string]interface{}{}
	for _, k := range s.keyCols[coll] {
		c.Key[k] = row[k]
	}
	c.Op, c.After = cdc.OpUpdate, row
	if ev.OperationType == "insert" {
		c.Op = cdc.OpInsert
	}
	return c, true, nil
}

// keyOf converts an event's documentKey ({_id}, plus the shard key on a
// sharded collection) to the target's key columns.
func (s *ChangeSource) keyOf(coll string, docKey bson.D) (map[string]interface{}, error) {
	var id bson.D
	for _, e := range docKey {
		if e.Key == "_id" {
			id = bson.D{e}
		}
	}
	if id == nil {
		return nil, fmt.Errorf("%s: change event has no _id", coll)
	}
	row, err := ConvertDocument(id, s.plan[coll], s.extraCol[coll])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", coll, err)
	}
	key := map[string]interface{}{}
	for _, k := range s.keyCols[coll] {
		v, ok := row[k]
		if !ok {
			return nil, fmt.Errorf("%s: key column %s isn't in the converted _id", coll, k)
		}
		key[k] = v
	}
	return key, nil
}

// lookup reads a document by its documentKey; nil when it's gone.
//
// On a sharded collection the documentKey carries the shard key as it was
// before the update, so an update that changed the shard key's value finds
// nothing by it: the document is looked up by _id alone then (across every
// shard), and is gone only if that finds nothing either.
func (s *ChangeSource) lookup(ctx context.Context, coll string, docKey bson.D) (bson.D, error) {
	c := s.client.Database(s.database).Collection(coll)
	find := func(filter bson.D) (bson.D, error) {
		var doc bson.D
		err := c.FindOne(ctx, filter).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("look up %s %v: %w", coll, filter, err)
		}
		return doc, nil
	}
	doc, err := find(docKey)
	if doc != nil || err != nil || len(docKey) < 2 {
		return doc, err
	}
	for _, e := range docKey {
		if e.Key == "_id" {
			return find(bson.D{e})
		}
	}
	return nil, nil
}

// streamError explains the errors that end a stream for good.
func streamError(err error) error {
	if historyLost(err) {
		return fmt.Errorf("the source's oplog no longer has the changes this stream needs (it was down longer than the oplog window): %w", err)
	}
	return fmt.Errorf("change stream: %w", err)
}

// historyLost reports an error from opening a stream at a point the oplog
// (a shard's, through mongos) no longer has.
func historyLost(err error) bool {
	var se mongo.ServerError
	// ChangeStreamHistoryLost, ChangeStreamFatalError.
	return errors.As(err, &se) && (se.HasErrorCode(286) || se.HasErrorCode(280))
}

func (s *ChangeSource) noteIdle(p position) {
	s.mu.Lock()
	if s.idle.less(p) {
		s.idle = p
	}
	s.mu.Unlock()
}

// Ack implements cdc.ChangeSource. The source keeps nothing for the stream,
// so there's nothing to tell it.
func (s *ChangeSource) Ack(_ context.Context, upTo cdc.Position) error {
	p, err := decodePosition(upTo)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.acked.less(p) {
		s.acked = p
	}
	s.mu.Unlock()
	return nil
}

// Confirmed implements cdc.ChangeSource: what was acked, or the stream's
// high-water mark when everything sent is acked.
func (s *ChangeSource) Confirmed() cdc.Position {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.acked.less(s.sent) && s.acked.less(s.idle) {
		return s.idle.encode()
	}
	if s.acked.zero() {
		return nil
	}
	return s.acked.encode()
}

// Sequences implements cdc.ChangeSource: MongoDB has none.
func (s *ChangeSource) Sequences(context.Context) (map[string]int64, error) { return nil, nil }

// Teardown implements cdc.ChangeSource: nothing was created on the source.
func (s *ChangeSource) Teardown(context.Context) error { return nil }

// Columns implements cdc.ChangeSource. A document's fields aren't a schema:
// those the plan doesn't know go to _extra, so the stream never reports a
// schema change.
func (s *ChangeSource) Columns(context.Context, []string) (map[string][]cdc.Column, error) {
	return map[string][]cdc.Column{}, nil
}

// Retention implements cdc.ChangeSource. The oplog is a capped collection
// measured in time rather than held for the stream; see RetentionAt.
func (s *ChangeSource) Retention(context.Context) (cdc.Retention, error) {
	return cdc.Retention{}, nil
}

// RetentionAt implements cdc.PositionRetention: the stream is lost when the
// oplog no longer reaches back to resume, the position it would restart
// from. A replica set's oplog is read directly; a sharded cluster's can't be
// read through mongos, so a stream is opened at resume instead, as capture
// would (at most once a minute).
func (s *ChangeSource) RetentionAt(ctx context.Context, resume cdc.Position) (cdc.Retention, error) {
	p, err := decodePosition(resume)
	if err != nil || p.zero() {
		return cdc.Retention{}, err
	}
	topo, err := s.topology(ctx)
	if err != nil {
		return cdc.Retention{}, err
	}
	if topo == topoSharded {
		return s.probeRetention(ctx, p)
	}
	oldest, ok, err := oldestOplog(ctx, s.client)
	if err != nil || !ok {
		return cdc.Retention{}, err
	}
	ret := cdc.Retention{State: "oplog from " + time.Unix(int64(oldest.T), 0).UTC().Format(time.RFC3339)}
	if p.less(position{T: oldest.T, I: oldest.I}) {
		ret.Lost = true
	}
	return ret, nil
}

// probeInterval is how often RetentionAt opens a stream to see whether a
// sharded cluster still has the changes from a resume point.
const probeInterval = time.Minute

// probeRetention opens a change stream at p and reads once: one that needs
// changes a shard's oplog no longer has fails at once.
func (s *ChangeSource) probeRetention(ctx context.Context, p position) (cdc.Retention, error) {
	s.mu.Lock()
	if !s.probedAt.IsZero() && time.Since(s.probedAt) < probeInterval {
		ret := s.probed
		s.mu.Unlock()
		return ret, nil
	}
	s.mu.Unlock()
	opts := options.ChangeStream().SetMaxAwaitTime(time.Millisecond)
	if p.Token != "" {
		opts.SetResumeAfter(bson.D{{Key: "_data", Value: p.Token}})
	} else {
		opts.SetStartAtOperationTime(&bson.Timestamp{T: p.T, I: p.I})
	}
	ret := cdc.Retention{}
	cs, err := s.client.Database(s.database).Watch(ctx, mongo.Pipeline{}, opts)
	if err == nil {
		cs.TryNext(ctx)
		err = cs.Err()
		cs.Close(context.Background()) //nolint:errcheck
	}
	if err != nil {
		if !historyLost(err) {
			return cdc.Retention{}, fmt.Errorf("open a change stream at %s: %w", p.encode(), err)
		}
		ret.Lost, ret.State = true, err.Error()
	}
	s.mu.Lock()
	s.probed, s.probedAt = ret, time.Now()
	s.mu.Unlock()
	return ret, nil
}

// topology is the source's topology, asked once.
func (s *ChangeSource) topology(ctx context.Context) (topology, error) {
	s.mu.Lock()
	known, topo := s.topoKnown, s.topo
	s.mu.Unlock()
	if known {
		return topo, nil
	}
	topo, err := topologyOf(ctx, s.client)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.topo, s.topoKnown = topo, true
	s.mu.Unlock()
	return topo, nil
}

// PositionsAreTimes implements cdc.TimePositions: positions are cluster times.
func (s *ChangeSource) PositionsAreTimes() bool { return true }

// Behind implements cdc.ChangeSource, in microseconds of cluster time
// (seconds, with 1µs for each later increment within one second).
func (s *ChangeSource) Behind(pos, target cdc.Position) int64 {
	p, err1 := decodePosition(pos)
	t, err2 := decodePosition(target)
	if err1 != nil || err2 != nil || !p.less(t) {
		return 0
	}
	if t.T == p.T {
		return 1
	}
	return int64(t.T-p.T) * 1e6
}

// FormatPosition implements cdc.ChangeSource.
func (s *ChangeSource) FormatPosition(p cdc.Position) string { return string(p) }

// ParsePosition implements cdc.ChangeSource.
func (s *ChangeSource) ParsePosition(text string) (cdc.Position, error) {
	if text == "" {
		return nil, errors.New("empty MongoDB stream position")
	}
	p, err := decodePosition(cdc.Position(text))
	if err != nil {
		return nil, err
	}
	return p.encode(), nil
}

// position is a cluster time, and the resume token that reached it.
type position struct {
	T, I  uint32
	Token string
}

func (p position) zero() bool { return p.T == 0 && p.I == 0 }

func (p position) less(q position) bool {
	return p.T < q.T || (p.T == q.T && p.I < q.I)
}

func (p position) encode() cdc.Position {
	s := strconv.FormatUint(uint64(p.T), 10) + "." + strconv.FormatUint(uint64(p.I), 10)
	if p.Token != "" {
		s += "/" + p.Token
	}
	return cdc.Position(s)
}

func decodePosition(b cdc.Position) (position, error) {
	if len(b) == 0 {
		return position{}, nil
	}
	s := string(b)
	ts, token, _ := strings.Cut(s, "/")
	tt, ii, ok := strings.Cut(ts, ".")
	t, err1 := strconv.ParseUint(tt, 10, 32)
	i, err2 := strconv.ParseUint(ii, 10, 32)
	if !ok || err1 != nil || err2 != nil {
		return position{}, fmt.Errorf("invalid MongoDB stream position %q", s)
	}
	return position{T: uint32(t), I: uint32(i), Token: token}, nil
}

// tokenPosition reads a resume token's cluster time. A token's _data is a
// hex KeyString that starts with the event's cluster time: type byte 0x82
// (a Timestamp), then t and i, 4 bytes each, big-endian.
func tokenPosition(tok bson.Raw) (position, error) {
	data, ok := tok.Lookup("_data").StringValueOK()
	if !ok {
		return position{}, fmt.Errorf("resume token %s has no _data string", tok)
	}
	b, err := hex.DecodeString(data[:min(len(data), 18)])
	if err != nil || len(b) < 9 || b[0] != 0x82 {
		return position{}, fmt.Errorf("resume token %q doesn't start with a cluster time", data)
	}
	t := uint32(b[1])<<24 | uint32(b[2])<<16 | uint32(b[3])<<8 | uint32(b[4])
	i := uint32(b[5])<<24 | uint32(b[6])<<16 | uint32(b[7])<<8 | uint32(b[8])
	return position{T: t, I: i, Token: data}, nil
}
