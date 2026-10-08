package validate

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// L3 is a primary-key keyed row diff. It runs only on a table that failed L1
// or L2, to say which rows differ: rows only in the source, rows only in the
// target, and rows on both sides whose values differ.
//
// A table keyed by a single integer column is diffed in key ranges: a page of
// source rows fixes a range, and the target rows in that same range are read
// and paired by key, so all three kinds are found without holding the table
// in memory. Any other usable key is diffed by lookup: each page of source
// rows is looked up in the target by key, and a reverse pass over the target
// then finds the rows only it holds.

const (
	// maxExamples caps the differing rows recorded per table.
	maxExamples = 100
	// maxKeySet caps the source keys a lookup diff remembers for its
	// reverse pass (8 bytes each), so a huge table cannot exhaust memory.
	maxKeySet = 50_000_000
	// maxShownValue caps each value stored in an example.
	maxShownValue = 200
	// keySep separates key columns in a canonical key.
	keySep = "\x1f"
)

// CheckSample reports whether pct is a usable L3 sample percentage: 1-100,
// or 0 for every row (100 means the same).
func CheckSample(pct int) error {
	if pct < 0 || pct > 100 {
		return fmt.Errorf("validation sample %d%% must be between 1 and 100 (0 or 100: every row)", pct)
	}
	return nil
}

// diffKey returns the columns L3 pairs rows by: the source's primary key.
// It returns a reason instead when the table has no key L3 can trust to
// compare equal on both sides.
func diffKey(schema *adapters.TableSchema, cols []column) ([]column, string) {
	byName := make(map[string]column, len(cols))
	for _, c := range cols {
		byName[c.name] = c
	}
	var key []column
	for _, sc := range schema.Columns {
		if !sc.PrimaryKey {
			continue
		}
		c, ok := byName[sc.Name]
		if !ok {
			return nil, fmt.Sprintf("primary key column %s is not in the target", sc.Name)
		}
		if c.lossy != nil {
			// Converted keys can collide (1.4 and 1.2 both become 1), so
			// rows cannot be paired by them.
			return nil, fmt.Sprintf("primary key column %s is narrowed by a type override, so its values change in the target", sc.Name)
		}
		switch c.kind {
		case kindInt, kindText, kindChar, kindDecimal, kindUUID, kindDate:
		default:
			return nil, fmt.Sprintf("primary key column %s has type %s, which L3 cannot pair rows by", sc.Name, sc.Type)
		}
		key = append(key, c)
	}
	if len(key) == 0 {
		return nil, "the table has no primary key to pair rows by"
	}
	return key, ""
}

// differ runs one table's L3 diff and collects its outcome.
type differ struct {
	src, tgt  adapters.SourceAdapter
	table     string
	cols      []column
	key       []column
	batchSize int
	sample    int // percent of rows to check; 0 means all
	rng       *rand.Rand

	checked                    int64
	missing, extra, mismatched int64
	lossy                      int64 // rows that differ only by the plan's conversions
	examples                   map[string][]state.ValidationDiff
}

func newDiffer(src, tgt adapters.SourceAdapter, table string, cols, key []column, batchSize, sample int) *differ {
	if sample >= 100 {
		sample = 0
	}
	return &differ{
		src: src, tgt: tgt, table: table, cols: cols, key: key,
		batchSize: batchSize, sample: sample,
		rng:      rand.New(rand.NewSource(rand.Int63())),
		examples: make(map[string][]state.ValidationDiff),
	}
}

// keyOf renders a row's key canonically; target says which side rec is from.
func (d *differ) keyOf(rec map[string]interface{}, target bool) string {
	parts := make([]string, len(d.key))
	for i, c := range d.key {
		k := c.name
		if target {
			k = c.tgtKey
		}
		parts[i] = canonicalize(rec[k], c)
	}
	return strings.Join(parts, keySep)
}

// compare pairs one source row with its target row (nil when the target has
// none) and records any difference.
func (d *differ) compare(srcRec, tgtRec map[string]interface{}) {
	d.checked++
	if tgtRec == nil {
		d.missing++
		d.record(state.DiffMissingInTarget, srcRec, nil, nil)
		return
	}
	var differing, planned []string
	for _, c := range d.cols {
		t := canonicalize(tgtRec[c.tgtKey], c)
		if canonicalize(srcRec[c.name], c) == t {
			continue
		}
		if v, lossy := intended(srcRec[c.name], c); lossy && v == t {
			planned = append(planned, c.name)
			continue
		}
		differing = append(differing, c.name)
	}
	switch {
	case len(differing) > 0:
		d.mismatched++
		d.record(state.DiffValueMismatch, srcRec, tgtRec, differing)
	case len(planned) > 0:
		d.lossy++
		d.record(state.DiffLossyByPlan, srcRec, tgtRec, planned)
	}
}

// extraRow records a row found only in the target.
func (d *differ) extraRow(tgtRec map[string]interface{}) {
	d.extra++
	d.record(state.DiffExtraInTarget, nil, tgtRec, nil)
}

func (d *differ) record(kind string, srcRec, tgtRec map[string]interface{}, columns []string) {
	if len(d.examples[kind]) >= maxExamples {
		return
	}
	ex := state.ValidationDiff{Kind: kind, Columns: columns}
	if srcRec != nil {
		ex.Source = d.shown(srcRec, false, d.cols)
		ex.Key = d.shown(srcRec, false, d.key)
	}
	if tgtRec != nil {
		ex.Target = d.shown(tgtRec, true, d.cols)
		if ex.Key == nil {
			ex.Key = d.shown(tgtRec, true, d.key)
		}
	}
	d.examples[kind] = append(d.examples[kind], ex)
}

// shown renders cols of rec for display (see shownValues).
func (d *differ) shown(rec map[string]interface{}, target bool, cols []column) map[string]*string {
	return shownValues(rec, cols, target)
}

// shownValues renders cols of rec for display: canonical values, so an
// example shows exactly what was compared, nil for NULL, long values cut
// short. target says which side rec came from.
func shownValues(rec map[string]interface{}, cols []column, target bool) map[string]*string {
	out := make(map[string]*string, len(cols))
	for _, c := range cols {
		k := c.name
		if target {
			k = c.tgtKey
		}
		out[c.name] = shownValue(canonicalize(rec[k], c))
	}
	return out
}

// shownValue is one canonical value for display.
func shownValue(v string) *string {
	if v == nullToken {
		return nil
	}
	if r := []rune(v); len(r) > maxShownValue {
		v = string(r[:maxShownValue]) + "…"
	}
	return &v
}

// diffs merges the examples, taking from each kind in turn so one kind
// cannot crowd out the others.
func (d *differ) diffs() []state.ValidationDiff {
	kinds := []string{state.DiffMissingInTarget, state.DiffExtraInTarget, state.DiffValueMismatch, state.DiffLossyByPlan}
	var out []state.ValidationDiff
	for i := 0; len(out) < maxExamples; i++ {
		took := false
		for _, k := range kinds {
			if i < len(d.examples[k]) && len(out) < maxExamples {
				out = append(out, d.examples[k][i])
				took = true
			}
		}
		if !took {
			break
		}
	}
	return out
}

// pick reports whether a sampled lookup diff checks the next source row.
func (d *differ) pick() bool {
	return d.sample == 0 || d.rng.Intn(100) < d.sample
}

// byRange diffs a table keyed by one integer column, a key range at a time.
// srcRows sizes the sample. It returns a note on any limit of the diff.
func (d *differ) byRange(ctx context.Context, srcRows int64) (string, error) {
	k := d.key[0]
	lo, hi, okSrc := pkRange(ctx, d.src, d.table, k.name)
	tlo, thi, okTgt := pkRange(ctx, d.tgt, d.table, k.tgtKey)
	switch {
	case !okSrc && !okTgt:
		return "", nil
	case !okSrc:
		lo, hi = tlo, thi
	case okTgt:
		lo, hi = min(lo, tlo), max(hi, thi)
	}

	if d.sample == 0 {
		for from := lo; ; {
			next, done, err := d.window(ctx, from, true)
			if err != nil || done {
				return "", err
			}
			from = next
		}
	}

	// Sampled: probe enough random starting keys for the windows to cover
	// about sample% of the source rows, visiting them in key order so they
	// never overlap.
	probes := int(math.Ceil(float64(srcRows) * float64(d.sample) / 100 / float64(d.batchSize)))
	if probes < 1 {
		probes = 1
	}
	starts := make([]int64, probes)
	for i := range starts {
		starts[i] = lo + d.rng.Int63n(hi-lo+1)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	next := lo
	for _, s := range starts {
		if s < next {
			s = next
		}
		if s > hi {
			break
		}
		n, done, err := d.window(ctx, s, false)
		if err != nil {
			return "", err
		}
		if done {
			break
		}
		next = n
	}
	return "", nil
}

// window diffs one key range starting at from: up to a page of source rows,
// and every target row from `from` through the last of them. With toEnd, a
// short source page means the source is exhausted, so the range runs to the
// end of the key space to catch trailing target rows. It returns where the
// next window starts, or done when nothing follows.
func (d *differ) window(ctx context.Context, from int64, toEnd bool) (int64, bool, error) {
	k := d.key[0]
	b, err := d.src.ReadBatch(ctx, d.table, adapters.BatchOptions{
		Limit: d.batchSize, PKCol: k.name, PKMin: from, PKMax: math.MaxInt64, PKMaxIncl: true,
	})
	if err != nil {
		return 0, false, fmt.Errorf("read source rows: %w", err)
	}
	end := b.LastPK
	last := len(b.Records) < d.batchSize
	if len(b.Records) == 0 {
		end = from + int64(d.batchSize) - 1
		if end < from { // overflow
			end = math.MaxInt64
		}
	}
	if last && toEnd {
		end = math.MaxInt64
	}

	tgtRows := make(map[string]map[string]interface{})
	for cur := from; ; {
		tb, err := d.tgt.ReadBatch(ctx, d.table, adapters.BatchOptions{
			Limit: d.batchSize, PKCol: k.tgtKey, PKMin: cur, PKMax: end, PKMaxIncl: true,
		})
		if err != nil {
			return 0, false, fmt.Errorf("read target rows: %w", err)
		}
		for _, r := range tb.Records {
			tgtRows[d.keyOf(r, true)] = r
		}
		if len(tb.Records) < d.batchSize || tb.LastPK >= end {
			break
		}
		cur = tb.LastPK + 1
	}

	for _, r := range b.Records {
		key := d.keyOf(r, false)
		d.compare(r, tgtRows[key])
		delete(tgtRows, key)
	}
	extras := make([]string, 0, len(tgtRows))
	for key := range tgtRows {
		extras = append(extras, key)
	}
	sort.Strings(extras)
	for _, key := range extras {
		d.extraRow(tgtRows[key])
	}

	if end == math.MaxInt64 {
		return 0, true, nil
	}
	return end + 1, false, nil
}

// pkRange is GetPKRange with an empty table reported as ok == false.
func pkRange(ctx context.Context, a adapters.SourceAdapter, table, col string) (lo, hi int64, ok bool) {
	lo, hi, err := a.GetPKRange(ctx, table, col)
	return lo, hi, err == nil
}

// byLookup diffs a table by looking each page of source rows up in the
// target by key, then (when every source row was checked) scanning the
// target for rows whose key the source never had. found reports how many
// source rows had a target row, which tells the caller whether lookups work
// at all for this table.
func (d *differ) byLookup(ctx context.Context, lookup adapters.RowLookup, srcSchema, tgtSchema *adapters.TableSchema, isMongo bool, tgtRows int64) (found int64, note string, err error) {
	keyCols := make([]string, len(d.key))
	for i, c := range d.key {
		keyCols[i] = c.tgtKey
	}
	chunk := adapters.MaxLookupParams / len(d.key)
	if chunk > d.batchSize {
		chunk = d.batchSize
	}

	// Source key hashes, kept for the reverse pass.
	var keys []uint64
	keepKeys := d.sample == 0
	var pending []map[string]interface{}

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		args := make([][]interface{}, len(pending))
		for i, r := range pending {
			args[i] = make([]interface{}, len(d.key))
			for j, c := range d.key {
				args[i][j] = lookupArg(r[c.name])
			}
		}
		rows, err := lookup.LookupRows(ctx, d.table, keyCols, args)
		if err != nil {
			return fmt.Errorf("look up target rows: %w", err)
		}
		byKey := make(map[string]map[string]interface{}, len(rows))
		for _, r := range rows {
			byKey[d.keyOf(r, true)] = r
		}
		for _, r := range pending {
			t := byKey[d.keyOf(r, false)]
			if t != nil {
				found++
			}
			d.compare(r, t)
		}
		pending = pending[:0]
		return nil
	}

	var flushErr error
	err = scan(ctx, d.src, srcSchema, d.batchSize, isMongo, func(rec map[string]interface{}) {
		if flushErr != nil {
			return
		}
		if keepKeys {
			if len(keys) >= maxKeySet {
				keepKeys, keys = false, nil
			} else {
				keys = append(keys, keyHash(d.keyOf(rec, false)))
			}
		}
		// The first row is always checked, so a small sample of a small
		// table still checks something.
		if d.checked > 0 || len(pending) > 0 {
			if !d.pick() {
				return
			}
		}
		pending = append(pending, rec)
		if len(pending) >= chunk {
			flushErr = flush()
		}
	})
	if err == nil {
		err = flushErr
	}
	if err == nil {
		err = flush()
	}
	if err != nil {
		return found, "", err
	}

	switch {
	case found >= tgtRows:
		// Every target row was paired with a source row: none is extra.
		return found, "", nil
	case d.sample != 0:
		return found, "a sampled diff of a table without an integer key does not look for rows only in the target", nil
	case !keepKeys:
		return found, fmt.Sprintf("rows only in the target were not looked for: the table has more than %d rows", maxKeySet), nil
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	err = scan(ctx, d.tgt, tgtSchema, d.batchSize, false, func(rec map[string]interface{}) {
		h := keyHash(d.keyOf(rec, true))
		i := sort.Search(len(keys), func(i int) bool { return keys[i] >= h })
		if i == len(keys) || keys[i] != h {
			d.extraRow(rec)
		}
	})
	if err != nil {
		return found, "", fmt.Errorf("read target rows: %w", err)
	}
	return found, "", nil
}

// lookupArg turns a source value into a query argument for the target. It is
// the value the migration itself handed the target's driver, so it matches
// however the target stored it; only []byte becomes text, as the targets do
// for every non-binary column on write (no key column is binary).
func lookupArg(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// keyHash is a 64-bit hash of a canonical key, for the reverse pass's key set.
func keyHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return mix64(h.Sum64())
}
