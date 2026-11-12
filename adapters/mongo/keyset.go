package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// KeyColumns implements adapters.KeysetSource: a collection's key is _id.
func (s *Source) KeyColumns(_ context.Context, table string) ([]string, error) {
	if err := s.requirePlan(); err != nil {
		return nil, err
	}
	var cols []string
	for _, row := range s.planByCollection[s.collectionFor(table)] {
		if row.IsPK {
			cols = append(cols, row.PgColumn)
		}
	}
	return cols, nil
}

// MaxKey implements adapters.KeysetSource: the last _id in sort order, as
// encodeID writes it.
func (s *Source) MaxKey(ctx context.Context, table string) ([]string, error) {
	var doc bson.D
	err := s.primary(table).FindOne(ctx, bson.D{}, options.FindOne().
		SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetProjection(bson.D{{Key: "_id", Value: 1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("largest _id of %s: %w", table, err)
	}
	key, err := encodeLastKey(doc)
	if err != nil {
		return nil, err
	}
	return []string{key}, nil
}

// primary is table's collection, read from the primary with read concern
// majority. A live snapshot must not read from a secondary: one behind the
// stream's start would copy a document as it was before a change the
// stream never replays. Nor may it copy a write a failover could still roll
// back: the stream only carries majority-committed changes, so nothing
// would ever remove it from the target. The stream starts at the majority
// commit point (ChangeSource.Prepare), so it replays whatever a majority
// read here doesn't see yet.
func (s *Source) primary(table string) *mongo.Collection {
	return s.client.Database(s.database).Collection(s.collectionFor(table),
		options.Collection().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Majority()))
}

// readKeyset is ReadBatch for a live snapshot (BatchOptions.Keyset): the
// documents after KeyAfter up to KeyUpTo, in _id order.
//
// A range filter on _id only matches _ids of the bound's own type group
// ("type bracketing"), so a polymorphic _id is read a type group at a time:
// after's group up to the end, the groups between, then upTo's group up to
// upTo. Each step is a single bounded _id index range.
func (s *Source) readKeyset(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	if len(opts.KeyUpTo) != 1 {
		return &adapters.Batch{}, nil // empty when the copy began
	}
	collection := s.collectionFor(table)
	plan := s.planByCollection[collection]
	extraCol := s.extraColByCollection[collection]
	coll := s.primary(table)

	upTo, err := decodeLastKey(opts.KeyUpTo[0], plan)
	if err != nil {
		return nil, fmt.Errorf("decode key bound: %w", err)
	}
	upGroup := idTypeGroup(upTo)
	if upGroup < 0 {
		return nil, fmt.Errorf("%s: _id %v has a type that can't be paged", table, upTo)
	}
	lte := bson.D{{Key: "$lte", Value: upTo}}

	var steps []bson.D
	if len(opts.KeyAfter) == 0 {
		if g := typesBetween(-1, upGroup); len(g) > 0 {
			steps = append(steps, bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: g}}}})
		}
		steps = append(steps, bson.D{{Key: "_id", Value: lte}})
	} else {
		after, err := decodeLastKey(opts.KeyAfter[0], plan)
		if err != nil {
			return nil, fmt.Errorf("decode resume key: %w", err)
		}
		g := idTypeGroup(after)
		switch {
		case g < 0:
			return nil, fmt.Errorf("%s: _id %v has a type that can't be paged", table, after)
		case g == upGroup:
			steps = append(steps, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: after}, {Key: "$lte", Value: upTo}}}})
		case g < upGroup:
			steps = append(steps, bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: after}}}})
			if between := typesBetween(g, upGroup); len(between) > 0 {
				steps = append(steps, bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: between}}}})
			}
			steps = append(steps, bson.D{{Key: "_id", Value: lte}})
		}
		// g > upGroup: already past the bound.
	}

	for _, filter := range steps {
		records, lastDoc, err := s.findBatch(ctx, coll, filter, int64(opts.Limit), plan, extraCol)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			continue
		}
		key, err := encodeLastKey(lastDoc)
		if err != nil {
			return nil, fmt.Errorf("encode last key: %w", err)
		}
		return &adapters.Batch{Records: records, Size: len(records), LastKey: adapters.EncodeKey([]string{key})}, nil
	}
	return &adapters.Batch{}, nil
}

// typesBetween returns the $type aliases of the type groups strictly
// between groups from and to of idTypeOrder.
func typesBetween(from, to int) []string {
	var out []string
	for g := from + 1; g < to; g++ {
		out = append(out, idTypeOrder[g]...)
	}
	return out
}
