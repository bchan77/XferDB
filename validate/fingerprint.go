package validate

import (
	"fmt"
	"hash/fnv"
)

// fingerprint is an order-independent summary of a table's rows: the row
// count plus the sum (mod 2^64) of every row's hash. Addition commutes, so the
// two sides may return rows in any order -- MongoDB orders _id across BSON
// types, the target orders by its own PK type, and paged reads need not agree.
// It also compares rows as a multiset, so a table with no primary key is
// covered and a duplicated row changes the result.
type fingerprint struct {
	rows int64
	sum  uint64
}

// add folds one row into the fingerprint. cols must be sorted by name and
// identical for both sides, so the same values always hash the same; target
// says which side rec came from, since a column's key can differ in case.
func (f *fingerprint) add(rec map[string]interface{}, cols []column, target bool) {
	h := fnv.New64a()
	for _, c := range cols {
		key := c.name
		if target {
			key = c.tgtKey
		}
		h.Write([]byte(c.name))
		h.Write([]byte{0x1e})
		h.Write([]byte(canonicalize(rec[key], c)))
		h.Write([]byte{0x1f})
	}
	f.rows++
	f.sum += mix64(h.Sum64())
}

// String renders the fingerprint for storage and display.
func (f fingerprint) String() string {
	return fmt.Sprintf("%d:%016x", f.rows, f.sum)
}

// mix64 is the SplitMix64 finalizer. FNV-1a alone spreads a one-byte change
// over few output bits; summing such hashes would let two small differences
// cancel far more often than chance. Mixing first makes every row hash
// effectively independent before they are added.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
