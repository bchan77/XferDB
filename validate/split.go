package validate

import (
	"context"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Splitting one large table across workers (#200). Run's workers take whole
// tables first. A table large enough, keyed by a single integer column that
// both sides can read by range (adapters.KeyRangeReader), is read at L2 as
// key-range parts: its own worker works through them, and a worker that
// finds no whole table left takes parts too, so the largest table stops
// being the floor once the others are done. Each part fingerprints its
// range on both sides at once, and the parts' fingerprints add up to the
// table's (an order-independent sum), so the result is the same as one pass.
// Variables, not constants, so tests can split small tables.
var (
	splitMinRows  int64 = 500_000 // a table with fewer source rows is read in one pass
	splitPartRows int64 = 250_000 // rows per part, roughly: keys may be sparse
	splitMaxParts int64 = 64
)

// part is one key range of a split table, run on whichever worker's Session
// takes it.
type part func(w *Session) error

// partGroup is one split table's parts.
type partGroup struct {
	pending []part // not yet taken
	running int
	err     error // the first part to fail
}

// pool hands Run's workers their work: whole tables in the order given,
// then parts of tables other workers have split.
type pool struct {
	mu     sync.Mutex
	cond   *sync.Cond
	tables []string
	groups []*partGroup // split tables with pending parts
	owners int          // tables being validated, which may still be split
}

func newPool(tables []string) *pool {
	p := &pool{tables: append([]string(nil), tables...)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// wake makes every waiting worker look again, as cond.Wait doesn't watch ctx.
func (p *pool) wake() {
	p.mu.Lock()
	p.cond.Broadcast()
	p.mu.Unlock()
}

// work runs one worker until nothing is left to take. table validates a
// whole table on s and reports false to stop this worker (an error, already
// recorded by the caller).
func (p *pool) work(ctx context.Context, s *Session, table func(string) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for ctx.Err() == nil {
		if len(p.tables) > 0 {
			t := p.tables[0]
			p.tables = p.tables[1:]
			p.owners++
			p.mu.Unlock()
			ok := table(t)
			p.mu.Lock()
			p.owners--
			p.cond.Broadcast()
			if !ok {
				return
			}
			continue
		}
		if g, pt := p.takeLocked(nil); pt != nil {
			p.runLocked(s, g, pt)
			continue
		}
		if p.owners == 0 {
			return // no table left, and none that could still be split
		}
		p.cond.Wait()
	}
}

// split runs parts as one group on s and on any idle worker, and returns
// once every part is done, with the first part error. Called by the worker
// validating the table, which takes its own parts first and, while others
// finish the last of them, helps with other tables' parts.
func (p *pool) split(ctx context.Context, s *Session, parts []part) error {
	g := &partGroup{pending: parts}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.groups = append(p.groups, g)
	p.cond.Broadcast()
	for {
		if g.err != nil || ctx.Err() != nil {
			p.dropLocked(g) // what's left won't change the verdict
		}
		if pt := p.popLocked(g); pt != nil {
			p.runLocked(s, g, pt)
			continue
		}
		if g.running == 0 {
			if g.err == nil {
				return ctx.Err()
			}
			return g.err
		}
		if og, pt := p.takeLocked(g); pt != nil {
			p.runLocked(s, og, pt)
			continue
		}
		p.cond.Wait()
	}
}

// takeLocked takes a pending part of the group with the most left, other
// than skip.
func (p *pool) takeLocked(skip *partGroup) (*partGroup, part) {
	var best *partGroup
	for _, g := range p.groups {
		if g != skip && (best == nil || len(g.pending) > len(best.pending)) {
			best = g
		}
	}
	if best == nil {
		return nil, nil
	}
	return best, p.popLocked(best)
}

// popLocked takes g's next pending part, dropping g from the groups on
// offer once it has none left.
func (p *pool) popLocked(g *partGroup) part {
	if len(g.pending) == 0 {
		return nil
	}
	pt := g.pending[0]
	g.pending = g.pending[1:]
	g.running++
	if len(g.pending) == 0 {
		p.dropLocked(g)
	}
	return pt
}

func (p *pool) dropLocked(g *partGroup) {
	g.pending = nil
	for i, other := range p.groups {
		if other == g {
			p.groups = append(p.groups[:i], p.groups[i+1:]...)
			return
		}
	}
}

// runLocked runs pt on s with p.mu released.
func (p *pool) runLocked(s *Session, g *partGroup, pt part) {
	p.mu.Unlock()
	err := pt(s)
	p.mu.Lock()
	g.running--
	if err != nil && g.err == nil {
		g.err = err
	}
	p.cond.Broadcast()
}

// sideError is a part's read failure, saying which side failed.
type sideError struct {
	source bool
	err    error
}

func (e sideError) Error() string { return e.err.Error() }
func (e sideError) Unwrap() error { return e.err }

// keyRanges plans the parts of a table the caller may split: nil when it is
// too small, or isn't keyed by one integer column both sides can read by
// range. The ranges cover every key, the first and last open-ended, so a
// target row keyed outside the source's range is still read.
func keyRanges(ctx context.Context, src, tgt adapters.SourceAdapter, srcSchema, tgtSchema *adapters.TableSchema, cols []column, srcRows int64) (key, tgtKey string, bounds [][2]*int64) {
	if srcRows < splitMinRows {
		return "", "", nil
	}
	srcRange, ok := src.(adapters.KeyRangeReader)
	if !ok {
		return "", "", nil
	}
	if _, ok := tgt.(adapters.KeyRangeReader); !ok {
		return "", "", nil
	}
	key = integerPK(srcSchema)
	if key == "" {
		return "", "", nil
	}
	for _, c := range cols {
		if c.name == key {
			tgtKey = c.tgtKey
		}
	}
	tgtIsInt := false
	for _, c := range tgtSchema.Columns {
		if c.Name == tgtKey && kindOf(c.Type) == kindInt {
			tgtIsInt = true
		}
	}
	if !tgtIsInt {
		return "", "", nil
	}
	lo, hi, ok, err := srcRange.IntKeyBounds(ctx, srcSchema.Name, key)
	if err != nil || !ok || hi <= lo {
		return "", "", nil // read in one pass instead
	}
	n := srcRows / splitPartRows
	if n > splitMaxParts {
		n = splitMaxParts
	}
	span := uint64(hi) - uint64(lo) + 1 // as uint64: hi-lo can overflow int64
	if n < 2 || uint64(n) > span {
		return "", "", nil
	}
	step := span / uint64(n)
	cuts := make([]int64, n-1) // the n-1 boundaries between parts
	for i := range cuts {
		cuts[i] = int64(uint64(lo) + step*uint64(i+1))
	}
	for i := int64(0); i < n; i++ {
		var from, to *int64
		if i > 0 {
			from = &cuts[i-1]
		}
		if i < n-1 {
			to = &cuts[i]
		}
		bounds = append(bounds, [2]*int64{from, to})
	}
	return key, tgtKey, bounds
}

// readSplit is L2's read of a split table: each part fingerprints its range
// on both sides and adds what it found to the table's totals.
func readSplit(ctx context.Context, split func(context.Context, []part) error, table, key, tgtKey string, bounds [][2]*int64,
	srcSchema *adapters.TableSchema, cols []column, srcFP, tgtFP *fingerprint, planned *lossyLog, tr *tracker) (srcErr, tgtErr error) {
	var mu sync.Mutex
	parts := make([]part, len(bounds))
	for i, b := range bounds {
		lo, hi := b[0], b[1]
		parts[i] = func(w *Session) error {
			var sfp, tfp fingerprint
			pl := newLossyLog(srcSchema, cols)
			sErr, tErr := both(ctx,
				func(ctx context.Context) error {
					return w.src.(adapters.KeyRangeReader).StreamKeyRange(ctx, table, key, lo, hi, func(rec map[string]interface{}) error {
						pl.add(rec, sfp.add(rec, cols, false))
						tr.row()
						return ctx.Err()
					})
				},
				func(ctx context.Context) error {
					return w.tgt.(adapters.KeyRangeReader).StreamKeyRange(ctx, table, tgtKey, lo, hi, func(rec map[string]interface{}) error {
						tfp.add(rec, cols, true)
						tr.row()
						return ctx.Err()
					})
				})
			if sErr != nil {
				return sideError{source: true, err: sErr}
			}
			if tErr != nil {
				return sideError{err: tErr}
			}
			mu.Lock()
			srcFP.merge(sfp)
			tgtFP.merge(tfp)
			planned.merge(pl)
			mu.Unlock()
			return nil
		}
	}
	err := split(ctx, parts)
	if se, ok := err.(sideError); ok {
		if se.source {
			return se.err, nil
		}
		return nil, se.err
	}
	return err, nil
}

// merge adds another part's rows to f.
func (f *fingerprint) merge(o fingerprint) {
	f.rows += o.rows
	f.sum += o.sum
}

// merge adds another part's lossy rows to l, keeping the first examples.
func (l *lossyLog) merge(o *lossyLog) {
	l.rows += o.rows
	for c := range o.changed {
		l.changed[c] = true
	}
	for _, e := range o.examples {
		if len(l.examples) >= maxExamples {
			break
		}
		l.examples = append(l.examples, e)
	}
}
