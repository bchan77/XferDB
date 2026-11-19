package mongo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Source implements adapters.SourceAdapter for MongoDB.
// It also implements the PlanConsumer interface so the engine can inject
// the frozen schema plan after Connect.
type Source struct {
	client   *mongo.Client
	config   adapters.ConnectionConfig
	database string
	plan     []state.SchemaPlanRow

	// planByCollection maps collection name → rows, built on SetPlan.
	planByCollection map[string][]state.SchemaPlanRow
	// extraCol is the sanitised name of the _extra jsonb column (default "_extra").
	extraColByCollection map[string]string
	// collectionByTable reverses TableName: the engine uses one name for both
	// the read and the write side, so this adapter presents the target table
	// name outward and maps it back to the real collection to read from.
	collectionByTable map[string]string

	// accurateCounts uses CountDocuments instead of EstimatedDocumentCount.
	// Slower but accurate — useful when estimates are unreliable.
	accurateCounts bool
}

// NewSource returns an uninitialised Source. Call Connect before use.
func NewSource() adapters.SourceAdapter {
	return &Source{}
}

// SetPlan injects the frozen migration plan. Called by the engine after Connect.
func (s *Source) SetPlan(plan []state.SchemaPlanRow) error {
	s.plan = plan
	s.planByCollection = make(map[string][]state.SchemaPlanRow)
	s.extraColByCollection = make(map[string]string)
	s.collectionByTable = make(map[string]string)

	for _, row := range plan {
		s.planByCollection[row.Collection] = append(s.planByCollection[row.Collection], row)
		s.collectionByTable[TableName(row.Collection)] = row.Collection
	}

	// Detect the _extra column name per collection (sanitiser may have renamed it).
	for col, rows := range s.planByCollection {
		extra := "_extra"
		// The _extra column is not a plan row — it is always added by InferSchema.
		// We store the default and the adapter uses it unconditionally.
		_ = rows
		s.extraColByCollection[col] = extra
	}
	return nil
}

// ── SourceAdapter interface ───────────────────────────────────────────────────

func (s *Source) Connect(ctx context.Context, cfg adapters.ConnectionConfig) error {
	if cfg.DSN == "" {
		return fmt.Errorf("mongo source: DSN is required (full mongodb:// URI)")
	}

	clientOpts := options.Client().
		ApplyURI(cfg.DSN).
		SetReadPreference(readpref.SecondaryPreferred())

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return fmt.Errorf("mongo connect: %w", err)
	}

	s.client = client
	s.config = cfg
	s.database = cfg.Database
	// Extract database from DSN if not explicitly provided in config.
	if s.database == "" {
		s.database = DatabaseFromURI(cfg.DSN)
	}
	if s.database == "" {
		client.Disconnect(ctx) //nolint:errcheck
		s.client = nil
		return fmt.Errorf("mongo source: the connection string names no database; add it after the host, e.g. mongodb://host:27017/shop")
	}
	return nil
}

// DatabaseFromURI returns the database named in a mongodb:// or
// mongodb+srv:// URI's path, or "" if it names none. It is deliberately more
// lenient than url.Parse, which rejects userinfo the driver accepts (a
// password with a stray backslash, say) and so used to lose the database:
// credentials end at the last '@', the path at the first '?'.
func DatabaseFromURI(dsn string) string {
	i := strings.Index(dsn, "://")
	if i < 0 {
		return ""
	}
	rest := dsn[i+3:]
	if at := strings.LastIndex(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return ""
	}
	db := rest[slash+1:]
	if q := strings.Index(db, "?"); q >= 0 {
		db = db[:q]
	}
	if u, err := url.PathUnescape(db); err == nil {
		db = u
	}
	return db
}

func (s *Source) Close() error {
	if s.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.client.Disconnect(ctx)
}

func (s *Source) Ping(ctx context.Context) error {
	if s.client == nil {
		return fmt.Errorf("not connected")
	}
	return s.client.Ping(ctx, readpref.SecondaryPreferred())
}

// mongoSystemDatabases are the server's own databases, never a migration's
// subject.
var mongoSystemDatabases = map[string]bool{"admin": true, "config": true, "local": true}

// ListDatabases implements adapters.DatabaseLister: the databases this login
// is authorized for (a login without the listDatabases action sees only
// those), minus the system ones.
func (s *Source) ListDatabases(ctx context.Context) ([]string, error) {
	if s.client == nil {
		return nil, fmt.Errorf("not connected")
	}
	all, err := s.client.ListDatabaseNames(ctx, bson.D{},
		options.ListDatabases().SetAuthorizedDatabases(true))
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	var names []string
	for _, n := range all {
		if !mongoSystemDatabases[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *Source) ListTables(ctx context.Context) ([]adapters.TableSchema, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
	db := s.client.Database(s.database)
	specs, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}

	var tables []adapters.TableSchema
	for _, spec := range specs {
		if shouldSkipCollection(spec.Name, spec.Type) {
			continue
		}
		schema, err := s.GetSchema(ctx, spec.Name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, *schema)
	}
	return tables, nil
}

func (s *Source) GetSchema(ctx context.Context, table string) (*adapters.TableSchema, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
	collection := s.collectionFor(table)
	rows, ok := s.planByCollection[collection]
	if !ok {
		return nil, fmt.Errorf("no schema plan for collection %q — run 'project analyze' first", collection)
	}

	var cols []adapters.ColumnDef
	for _, row := range rows {
		if row.Strategy == "skip" || row.Strategy == "flatten" {
			continue
		}
		cols = append(cols, adapters.ColumnDef{
			Name:       row.PgColumn,
			Type:       row.PgType,
			Nullable:   row.Nullable,
			PrimaryKey: row.IsPK,
		})
	}
	// Append the _extra jsonb column.
	cols = append(cols, adapters.ColumnDef{
		Name:     s.extraColByCollection[collection],
		Type:     "jsonb",
		Nullable: true,
	})

	return &adapters.TableSchema{Name: TableName(collection), Columns: cols}, nil
}

// SetAccurateCounts enables accurate document counting using CountDocuments
// instead of EstimatedDocumentCount. Slower but accurate.
func (s *Source) SetAccurateCounts(enabled bool) {
	s.accurateCounts = enabled
}

func (s *Source) GetRowCount(ctx context.Context, table string) (int64, error) {
	coll := s.client.Database(s.database).Collection(s.collectionFor(table))
	if s.accurateCounts {
		// CountDocuments does a full collection scan — accurate but slow.
		// Use a longer timeout (5 min) since large collections can take a while.
		countCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		n, err := coll.CountDocuments(countCtx, bson.D{})
		if err != nil {
			// Fall back to estimated count if accurate count fails.
			return coll.EstimatedDocumentCount(ctx)
		}
		return n, nil
	}
	// EstimatedDocumentCount is fast (uses collection metadata) but can be inaccurate.
	return coll.EstimatedDocumentCount(ctx)
}

// GetPKRange is not supported for MongoDB — ObjectId range splits are deferred
// to a future milestone. Returning an error causes the engine to use sequential
// keyset mode instead.
func (s *Source) GetPKRange(_ context.Context, _, _ string) (int64, int64, error) {
	return 0, 0, fmt.Errorf("mongo: PK range splitting not supported; use sequential keyset mode")
}

// ReadBatch reads up to opts.Limit documents using keyset pagination on _id.
// opts.LastPK is the resume key of the last _id seen (empty on first call);
// see encodeID.
func (s *Source) ReadBatch(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
	if opts.Keyset {
		return s.readKeyset(ctx, table, opts)
	}
	collection := s.collectionFor(table)
	plan := s.planByCollection[collection]
	extraCol := s.extraColByCollection[collection]

	coll := s.client.Database(s.database).Collection(collection)

	var lastID interface{}
	hasLast := false
	filter := bson.D{}
	if lastKey, ok := opts.LastPK.(string); ok && lastKey != "" {
		id, err := decodeLastKey(lastKey, plan)
		if err != nil {
			return nil, fmt.Errorf("decode resume key: %w", err)
		}
		lastID, hasLast = id, true
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastID}}}}
	}

	limit := int64(opts.Limit)
	records, lastDoc, err := s.findBatch(ctx, coll, filter, limit, plan, extraCol)
	if err != nil {
		return nil, err
	}

	// $gt only matches _ids of the same BSON type as lastID ("type
	// bracketing"), so for a polymorphic _id an empty page means this type's
	// run is over, not the collection. The sort puts every remaining document
	// in a later type, so continue from the first of those.
	if len(records) == 0 && hasLast {
		if later := laterIDTypes(lastID); len(later) > 0 {
			filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: later}}}}
			records, lastDoc, err = s.findBatch(ctx, coll, filter, limit, plan, extraCol)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(records) == 0 {
		return &adapters.Batch{}, nil
	}

	lastKey, err := encodeLastKey(lastDoc)
	if err != nil {
		return nil, fmt.Errorf("encode last key: %w", err)
	}

	return &adapters.Batch{
		Records: records,
		Size:    len(records),
		LastKey: lastKey,
	}, nil
}

// StreamTable implements adapters.TableStreamer: every document of the
// collection, converted with its plan exactly as ReadBatch converts it, from
// one cursor in natural order. ReadBatch's keyset paging runs a new sorted
// find per batch (a seek and at least one round trip each, ~35 ms a batch on
// the size-L harness); a reader that doesn't need _id order, such as
// validation's order-independent fingerprint, has no use for that (#202).
func (s *Source) StreamTable(ctx context.Context, table string, fn func(map[string]interface{}) error) error {
	return s.stream(ctx, table, bson.D{}, fn)
}

// IntKeyBounds implements adapters.KeyRangeReader for the column the plan
// maps _id to. ok is false unless the smallest and the largest _id are both
// numbers: numbers sort together in BSON order, so then every _id is one,
// and a numeric range read sees every document.
func (s *Source) IntKeyBounds(ctx context.Context, table, col string) (int64, int64, bool, error) {
	if err := s.requirePlan(); err != nil {
		return 0, 0, false, err
	}
	collection := s.collectionFor(table)
	if !s.isIDColumn(collection, col) {
		return 0, 0, false, nil
	}
	coll := s.client.Database(s.database).Collection(collection)
	end := func(dir int) (float64, bool, error) {
		var doc bson.D
		err := coll.FindOne(ctx, bson.D{}, options.FindOne().
			SetSort(bson.D{{Key: "_id", Value: dir}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}})).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, fmt.Errorf("read %s _id bounds: %w", collection, err)
		}
		for _, e := range doc {
			if e.Key != "_id" {
				continue
			}
			switch v := e.Value.(type) {
			case int32:
				return float64(v), true, nil
			case int64:
				return float64(v), true, nil
			case float64:
				return v, true, nil
			}
		}
		return 0, false, nil
	}
	lo, okLo, err := end(1)
	if err != nil || !okLo {
		return 0, 0, false, err
	}
	hi, okHi, err := end(-1)
	if err != nil || !okHi {
		return 0, 0, false, err
	}
	return int64(math.Floor(lo)), int64(math.Floor(hi)), true, nil
}

// StreamKeyRange implements adapters.KeyRangeReader: the documents whose
// numeric _id is in [lo, hi), converted as StreamTable converts them.
func (s *Source) StreamKeyRange(ctx context.Context, table, col string, lo, hi *int64, fn func(map[string]interface{}) error) error {
	if err := s.requirePlan(); err != nil {
		return err
	}
	if !s.isIDColumn(s.collectionFor(table), col) {
		return fmt.Errorf("mongo: %s is not the column _id maps to; only _id ranges can be read", col)
	}
	cond := bson.D{}
	if lo != nil {
		cond = append(cond, bson.E{Key: "$gte", Value: *lo})
	}
	if hi != nil {
		cond = append(cond, bson.E{Key: "$lt", Value: *hi})
	}
	if len(cond) == 0 {
		// Both ends open would include non-numeric _ids, which a range never
		// does: still only the numbers.
		cond = bson.D{{Key: "$type", Value: "number"}}
	}
	return s.stream(ctx, table, bson.D{{Key: "_id", Value: cond}}, fn)
}

// isIDColumn reports whether col is the column the collection's plan maps
// _id to.
func (s *Source) isIDColumn(collection, col string) bool {
	for _, r := range s.planByCollection[collection] {
		if r.FieldName == "_id" {
			return r.PgColumn == col
		}
	}
	return false
}

// stream converts and passes on every document matching filter, from one
// cursor in natural order.
func (s *Source) stream(ctx context.Context, table string, filter bson.D, fn func(map[string]interface{}) error) error {
	if err := s.requirePlan(); err != nil {
		return err
	}
	collection := s.collectionFor(table)
	plan := s.planByCollection[collection]
	extraCol := s.extraColByCollection[collection]
	coll := s.client.Database(s.database).Collection(collection)
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return fmt.Errorf("find %s: %w", collection, err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("decode document: %w", err)
		}
		rec, err := ConvertDocument(doc, plan, extraCol)
		if err != nil {
			return fmt.Errorf("convert document: %w", err)
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	return nil
}

// findBatch reads up to limit documents matching filter in _id order and
// converts them with the collection's plan. lastDoc is the final raw document.
func (s *Source) findBatch(ctx context.Context, coll *mongo.Collection, filter bson.D, limit int64, plan []state.SchemaPlanRow, extraCol string) ([]map[string]interface{}, bson.D, error) {
	cursor, err := coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(limit))
	if err != nil {
		return nil, nil, fmt.Errorf("find %s: %w", coll.Name(), err)
	}
	defer cursor.Close(ctx)
	return collectBatch(ctx, cursor, plan, extraCol)
}

// collectBatch converts a findBatch cursor's documents, in _id order,
// returning the last one read for the next page's bound.
func collectBatch(ctx context.Context, cursor *mongo.Cursor, plan []state.SchemaPlanRow, extraCol string) ([]map[string]interface{}, bson.D, error) {
	var records []map[string]interface{}
	var lastDoc bson.D

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, fmt.Errorf("decode document: %w", err)
		}
		if lastDoc != nil && sameID(lastDoc, doc) {
			// Through mongos, a document whose shard key changes while the
			// shards are read can come from both of them (each answers at
			// its own point in time), the two side by side in _id order.
			// Keep the first: a live stream started before this read
			// replays the change, and one batch holding a key twice fails
			// a PostgreSQL upsert (21000).
			continue
		}
		rec, err := ConvertDocument(doc, plan, extraCol)
		if err != nil {
			return nil, nil, fmt.Errorf("convert document: %w", err)
		}
		records = append(records, rec)
		lastDoc = doc
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, fmt.Errorf("cursor: %w", err)
	}
	return records, lastDoc, nil
}

// sameID reports whether two documents have the same _id. Numbers compare
// by value (relaxed extended JSON): MongoDB's _id index treats 3 and
// NumberLong(3) as one key, and a target stores both as 3.
func sameID(a, b bson.D) bool {
	ka, okA := relaxedID(a)
	kb, okB := relaxedID(b)
	return okA && okB && ka == kb
}

func relaxedID(doc bson.D) (string, bool) {
	for _, e := range doc {
		if e.Key == "_id" {
			b, err := bson.MarshalExtJSON(bson.D{{Key: "_id", Value: e.Value}}, false, false)
			return string(b), err == nil
		}
	}
	return "", false
}

func (s *Source) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	result := &adapters.PermissionCheck{}
	db := s.client.Database(s.database)

	// Try a find to check read access.
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("cannot list collections: %v", err))
		return result, nil
	}

	result.CanRead = true
	result.CanWrite = false // source adapter — no writes
	result.CanCreateTable = false

	// Confirm at least one collection is readable.
	if len(collections) > 0 {
		cur, err := db.Collection(collections[0]).Find(ctx, bson.D{}, options.Find().SetLimit(1))
		if err != nil {
			result.CanRead = false
			result.Errors = append(result.Errors, fmt.Sprintf("cannot read from %q: %v", collections[0], err))
		} else {
			cur.Close(ctx)
		}
	}
	return result, nil
}

func (s *Source) GetInfo(ctx context.Context) (*adapters.DatabaseInfo, error) {
	info := &adapters.DatabaseInfo{
		Type:     "MongoDB",
		Database: s.database,
	}

	// Get host from config or parse from DSN
	if s.config.Host != "" {
		port := s.config.Port
		if port == 0 {
			port = 27017
		}
		info.Host = fmt.Sprintf("%s:%d", s.config.Host, port)
	} else if s.config.DSN != "" {
		// Parse host from mongodb:// URI
		info.Host = parseMongoHost(s.config.DSN)
	}

	// Get server version and info via buildInfo command
	var buildInfo bson.M
	if err := s.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err == nil {
		if version, ok := buildInfo["version"].(string); ok {
			info.Version = version
		}
	}

	// Count collections
	db := s.client.Database(s.database)
	specs, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err == nil {
		count := 0
		for _, spec := range specs {
			if !shouldSkipCollection(spec.Name, spec.Type) {
				count++
			}
		}
		info.Tables = count
	}

	// Get database stats for size
	var dbStats bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&dbStats); err == nil {
		if dataSize, ok := dbStats["dataSize"].(int64); ok {
			info.SizeBytes = dataSize
			info.SizeHuman = humanSize(dataSize)
		} else if dataSize, ok := dbStats["dataSize"].(float64); ok {
			info.SizeBytes = int64(dataSize)
			info.SizeHuman = humanSize(int64(dataSize))
		} else if dataSize, ok := dbStats["dataSize"].(int32); ok {
			info.SizeBytes = int64(dataSize)
			info.SizeHuman = humanSize(int64(dataSize))
		}
	}

	return info, nil
}

// humanSize formats bytes as a human-readable string.
func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.1f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// maxPgIdentLen is Postgres's NAMEDATALEN limit for an identifier.
const maxPgIdentLen = 63

// TableName maps a MongoDB collection name to the table name used on the
// target. A collection name is otherwise used verbatim, but Postgres truncates
// identifiers at NAMEDATALEN, so two collections agreeing over their first 63
// bytes would silently land in one table -- CreateTable uses CREATE TABLE IF
// NOT EXISTS, so the second one's rows join the first one's.
//
// Over-long names are truncated and given a suffix derived from the full name.
// Deriving it from the name rather than from a counter keeps the mapping
// deterministic and independent of which other collections exist, which
// matters because checkpoints are keyed by this name and must still resolve
// when a migration resumes.
func TableName(collection string) string {
	if len(collection) <= maxPgIdentLen {
		return collection
	}
	sum := sha256.Sum256([]byte(collection))
	suffix := "_" + hex.EncodeToString(sum[:4])
	return collection[:maxPgIdentLen-len(suffix)] + suffix
}

// collectionFor maps a name the engine handed back to the real collection.
// Names that need no truncation map to themselves, so an unknown name is
// returned unchanged rather than treated as an error.
func (s *Source) collectionFor(table string) string {
	if collection, ok := s.collectionByTable[table]; ok {
		return collection
	}
	return table
}

func (s *Source) requirePlan() error {
	if s.planByCollection == nil {
		return fmt.Errorf("mongo source: schema plan not set — call SetPlan before using this adapter")
	}
	return nil
}

// shouldSkipCollection mirrors the sampler logic — keep in sync.
func shouldSkipCollection(name, collType string) bool {
	if len(name) >= 7 && name[:7] == "system." {
		return true
	}
	if len(name) >= 6 && name[len(name)-6:] == ".files" {
		return true
	}
	if len(name) >= 7 && name[len(name)-7:] == ".chunks" {
		return true
	}
	return collType == "view" || collType == "timeseries"
}

// encodeLastKey serialises the _id of a document into a checkpoint string.
func encodeLastKey(doc bson.D) (string, error) {
	for _, elem := range doc {
		if elem.Key == "_id" {
			return encodeID(elem.Value)
		}
	}
	return "", fmt.Errorf("document has no _id field")
}

// extJSONKeyPrefix marks a resume key holding the _id as canonical Extended
// JSON. Keys without it were written by older versions, which stored only the
// ObjectId hex or the value's text and lost its BSON type (#97).
const extJSONKeyPrefix = "ejson:"

// encodeID serialises an _id as canonical Extended JSON so that decodeLastKey
// gets back the same BSON type. The type matters: $gt only matches _ids of the
// same type, so a compound or Binary _id resumed as a string matches nothing
// and the rest of the collection is silently skipped.
func encodeID(v interface{}) (string, error) {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "_id", Value: v}}, true, false)
	if err != nil {
		return "", fmt.Errorf("encode _id: %w", err)
	}
	return extJSONKeyPrefix + string(b), nil
}

// decodeLastKey converts a checkpoint string back to a native _id value
// for use in a $gt filter.
func decodeLastKey(key string, plan []state.SchemaPlanRow) (interface{}, error) {
	if ext, ok := strings.CutPrefix(key, extJSONKeyPrefix); ok {
		var doc bson.D
		if err := bson.UnmarshalExtJSON([]byte(ext), true, &doc); err != nil {
			return nil, fmt.Errorf("parse _id from %q: %w", key, err)
		}
		if len(doc) != 1 || doc[0].Key != "_id" {
			return nil, fmt.Errorf("resume key %q has no _id", key)
		}
		return doc[0].Value, nil
	}
	return decodeLegacyKey(key, plan)
}

// decodeLegacyKey reads a resume key checkpointed before extJSONKeyPrefix
// existed, guessing the _id's type from its pg_type in the plan.
func decodeLegacyKey(key string, plan []state.SchemaPlanRow) (interface{}, error) {
	idType := "text"
	for _, row := range plan {
		if row.FieldName == "_id" {
			idType = row.PgType
			break
		}
	}

	switch idType {
	case "integer", "bigint":
		var n int64
		if _, err := fmt.Sscanf(key, "%d", &n); err != nil {
			return nil, fmt.Errorf("parse int _id from %q: %w", key, err)
		}
		return n, nil
	default:
		// ObjectId hex → bson.ObjectID; otherwise plain string.
		if len(key) == 24 {
			oid, err := bson.ObjectIDFromHex(key)
			if err == nil {
				return oid, nil
			}
		}
		return key, nil
	}
}

// idTypeOrder lists the $type aliases an _id can have, grouped in the order
// MongoDB sorts BSON types. Types in one group compare with each other (all
// numbers, string and symbol); $gt never crosses a group. Arrays are not
// allowed as _id.
var idTypeOrder = [][]string{
	{"minKey"},
	{"null", "undefined"},
	{"int", "long", "double", "decimal"},
	{"string", "symbol"},
	{"object"},
	{"binData"},
	{"objectId"},
	{"bool"},
	{"date"},
	{"timestamp"},
	{"regex"},
	{"maxKey"},
}

// idTypeGroup returns the index in idTypeOrder of v's BSON type, or -1.
func idTypeGroup(v interface{}) int {
	switch v.(type) {
	case bson.MinKey:
		return 0
	case nil, bson.Null, bson.Undefined:
		return 1
	case int32, int64, float64, bson.Decimal128:
		return 2
	case string, bson.Symbol:
		return 3
	case bson.D, bson.M:
		return 4
	case bson.Binary:
		return 5
	case bson.ObjectID:
		return 6
	case bool:
		return 7
	case bson.DateTime:
		return 8
	case bson.Timestamp:
		return 9
	case bson.Regex:
		return 10
	case bson.MaxKey:
		return 11
	}
	return -1
}

// laterIDTypes returns the $type aliases of every type group that sorts after
// v's, or nil if v's type is unknown or already last.
func laterIDTypes(v interface{}) []string {
	g := idTypeGroup(v)
	if g < 0 {
		return nil
	}
	var later []string
	for _, group := range idTypeOrder[g+1:] {
		later = append(later, group...)
	}
	return later
}

// parseMongoHost extracts the host(s) from a mongodb:// or mongodb+srv:// URI.
func parseMongoHost(dsn string) string {
	// mongodb://user:pass@host1:27017,host2:27017/db?options
	// mongodb+srv://user:pass@cluster.mongodb.net/db?options

	// Find the start of the host portion (after ://)
	idx := 0
	if len(dsn) > 14 && dsn[:14] == "mongodb+srv://" {
		idx = 14
	} else if len(dsn) > 10 && dsn[:10] == "mongodb://" {
		idx = 10
	} else {
		return ""
	}

	rest := dsn[idx:]

	// Skip credentials if present (user:pass@)
	if atIdx := findByte(rest, '@'); atIdx >= 0 {
		rest = rest[atIdx+1:]
	}

	// Take everything up to the first / or ? (the host portion)
	endIdx := len(rest)
	if slashIdx := findByte(rest, '/'); slashIdx >= 0 && slashIdx < endIdx {
		endIdx = slashIdx
	}
	if qIdx := findByte(rest, '?'); qIdx >= 0 && qIdx < endIdx {
		endIdx = qIdx
	}

	return rest[:endIdx]
}

// findByte returns the index of the first occurrence of b in s, or -1.
func findByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
