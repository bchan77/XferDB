package mongo

import (
	"context"
	"fmt"
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
	database string
	plan     []state.SchemaPlanRow

	// planByCollection maps collection name → rows, built on SetPlan.
	planByCollection map[string][]state.SchemaPlanRow
	// extraCol is the sanitised name of the _extra jsonb column (default "_extra").
	extraColByCollection map[string]string

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

	for _, row := range plan {
		s.planByCollection[row.Collection] = append(s.planByCollection[row.Collection], row)
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
	s.database = cfg.Database
	return nil
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

func (s *Source) GetSchema(ctx context.Context, collection string) (*adapters.TableSchema, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
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

	return &adapters.TableSchema{Name: collection, Columns: cols}, nil
}

// SetAccurateCounts enables accurate document counting using CountDocuments
// instead of EstimatedDocumentCount. Slower but accurate.
func (s *Source) SetAccurateCounts(enabled bool) {
	s.accurateCounts = enabled
}

func (s *Source) GetRowCount(ctx context.Context, collection string) (int64, error) {
	coll := s.client.Database(s.database).Collection(collection)
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
// opts.LastPK is the hex-encoded last _id seen (empty on first call).
func (s *Source) ReadBatch(ctx context.Context, collection string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
	plan := s.planByCollection[collection]
	extraCol := s.extraColByCollection[collection]

	coll := s.client.Database(s.database).Collection(collection)

	filter := bson.D{}
	if lastKey, ok := opts.LastPK.(string); ok && lastKey != "" {
		lastID, err := decodeLastKey(lastKey, plan)
		if err != nil {
			return nil, fmt.Errorf("decode resume key: %w", err)
		}
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastID}}}}
	}

	limit := int64(opts.Limit)
	cursor, err := coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("find %s: %w", collection, err)
	}
	defer cursor.Close(ctx)

	var records []map[string]interface{}
	var lastDoc bson.D

	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode document: %w", err)
		}
		rec, err := ConvertDocument(doc, plan, extraCol)
		if err != nil {
			return nil, fmt.Errorf("convert document: %w", err)
		}
		records = append(records, rec)
		lastDoc = doc
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor: %w", err)
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
	result.CanWrite = false      // source adapter — no writes
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

// ── helpers ───────────────────────────────────────────────────────────────────

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

func encodeID(v interface{}) (string, error) {
	switch id := v.(type) {
	case bson.ObjectID:
		return id.Hex(), nil
	case string:
		return id, nil
	case int32:
		return fmt.Sprintf("%d", id), nil
	case int64:
		return fmt.Sprintf("%d", id), nil
	default:
		return fmt.Sprintf("%v", id), nil
	}
}

// decodeLastKey converts a checkpoint string back to a native _id value
// for use in a $gt filter. Looks at the plan to determine _id's pg_type.
func decodeLastKey(key string, plan []state.SchemaPlanRow) (interface{}, error) {
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
