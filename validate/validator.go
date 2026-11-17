// Package validate checks that a finished migration landed correctly: it
// compares the rows the migration intended to write against the rows the
// target actually holds.
//
// The intended rows come from re-reading the source through its own
// ReadBatch with the migration's schema plan, so every structural change the
// migration made (MongoDB flattening, skipped fields, _extra, table renames)
// is reproduced by the code that made it rather than re-implemented here. The
// actual rows come from opening the target with the source adapter of the
// target's type. What remains -- how each database represents a value -- is
// removed by canonicalize before anything is compared.
package validate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/mongo"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
	"github.com/lib/pq"
)

// Levels, cheapest first. L1 and L2 each run only on a table that passed the
// one below; L3 runs only on a table that failed one of them, to say which
// rows differ.
const (
	LevelL1 = "L1" // row counts
	LevelL2 = "L2" // order-independent fingerprint of every row
	LevelL3 = "L3" // PK-keyed row diff of a table that failed L1 or L2
)

// Modes decide when validation runs on its own after a migration.
const (
	ModeAfter    = "after"     // once the migration completes (default)
	ModePerTable = "per_table" // as each table completes, while the rest copy
	ModeOff      = "off"       // never; tables show as not validated
)

// DefaultLevel and DefaultMode apply when neither the request nor the
// project sets one.
const (
	DefaultLevel = LevelL2
	DefaultMode  = ModeAfter
)

const defaultBatchSize = 1000

// CheckLevel reports whether level can be run. Empty means the default.
func CheckLevel(level string) error {
	switch level {
	case "", LevelL1, LevelL2, LevelL3:
		return nil
	}
	return fmt.Errorf("validation level %q must be one of: L1, L2, L3", level)
}

// CheckSettings reports whether a project's or run's validation settings
// can be used.
func CheckSettings(t adapters.TransferConfig) error {
	if err := CheckMode(t.Validation); err != nil {
		return err
	}
	if err := CheckLevel(t.ValidationLevel); err != nil {
		return err
	}
	return CheckSample(t.ValidationSample)
}

// CheckMode reports whether mode can be used. Empty means the default.
func CheckMode(mode string) error {
	switch mode {
	case "", ModeAfter, ModePerTable, ModeOff:
		return nil
	}
	return fmt.Errorf("validation mode %q must be one of: after, per_table, off", mode)
}

// Request describes one validation run.
type Request struct {
	Project *adapters.Project
	// Plan is the schema plan the migration used. Required for a MongoDB
	// source; for a relational source it carries any column-type overrides.
	Plan []state.SchemaPlanRow
	// Tables to validate: the migration's tables that reached completed.
	Tables    []string
	Level     string
	BatchSize int
	// Sample is the percentage of a table an L3 diff checks; 0 means all.
	Sample int
	// OnProgress, when set, is called as each table's validation advances
	// (see Progress), from the goroutine validating it: with Workers > 1,
	// from several at once.
	OnProgress func(Progress)
	// Workers is how many tables Run validates at once, each on its own
	// source and target connections; 0 or 1 is one at a time. Tables are
	// started in the order given, so put the largest first.
	Workers int
}

// Run validates each table in req.Tables, req.Workers at a time, and hands
// every outcome to report as soon as that table is done (one call at a time).
// It returns early only on a connection failure or when ctx is cancelled; a
// table that fails validation is a result, not an error.
func Run(ctx context.Context, req Request, report func(state.ValidationTable)) error {
	workers := req.Workers
	if workers > len(req.Tables) {
		workers = len(req.Tables)
	}
	if workers < 1 {
		workers = 1
	}
	// Every session is opened before any table starts. The first must open;
	// a later one that can't (the server's connection limit, say) only means
	// fewer workers.
	sessions := make([]*Session, 0, workers)
	defer func() {
		for _, s := range sessions {
			s.Close()
		}
	}()
	for i := 0; i < workers; i++ {
		s, err := Open(ctx, req)
		if err != nil {
			if i == 0 {
				return err
			}
			break
		}
		sessions = append(sessions, s)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p := newPool(req.Tables)
	if len(sessions) > 1 {
		// With one worker nobody could take a part: read tables in one pass.
		for _, s := range sessions {
			s.pool = p
		}
	}
	go func() { <-ctx.Done(); p.wake() }()
	var (
		mu       sync.Mutex // serialises report and guards firstErr
		firstErr error
		wg       sync.WaitGroup
	)
	for _, s := range sessions {
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			p.work(ctx, s, func(table string) bool {
				res, err := s.Table(ctx, table)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					cancel()
					return false
				}
				report(res)
				return true
			})
		}(s)
	}
	wg.Wait()
	if firstErr == nil && ctx.Err() != nil {
		// Cancelled before every table was handed out.
		return ctx.Err()
	}
	return firstErr
}

// Session holds open connections to a project's source and target, so many
// tables can be validated one after another without reconnecting. A Session
// is not safe for concurrent use; open one per worker.
type Session struct {
	src, tgt  adapters.SourceAdapter
	isMongo   bool
	level     string
	batchSize int
	sample    int
	plan      []state.SchemaPlanRow
	srcType   string
	tgtType   string
	progress  func(Progress)
	// pool, set by Run when it has more than one worker, lets this session
	// split a large table into parts other workers can take.
	pool *pool
}

// Open connects to req's source and target. req.Tables is ignored; pass each
// table to Table.
func Open(ctx context.Context, req Request) (*Session, error) {
	level := req.Level
	if level == "" {
		level = DefaultLevel
	}
	if err := CheckLevel(level); err != nil {
		return nil, err
	}
	if err := CheckSample(req.Sample); err != nil {
		return nil, err
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	src, err := registry.NewSource(req.Project.SourceConfig.Type)
	if err != nil {
		return nil, fmt.Errorf("source adapter: %w", err)
	}
	if err := src.Connect(ctx, req.Project.SourceConfig); err != nil {
		return nil, fmt.Errorf("connect source: %w", err)
	}

	if mongoSrc, ok := src.(*mongo.Source); ok {
		if len(req.Plan) == 0 {
			src.Close()
			return nil, fmt.Errorf("no schema plan found for this MongoDB source — run 'xferdb project analyze' first")
		}
		if err := mongoSrc.SetPlan(req.Plan); err != nil {
			src.Close()
			return nil, fmt.Errorf("set schema plan: %w", err)
		}
		// An estimate is not a validation.
		mongoSrc.SetAccurateCounts(true)
	}

	// The target is read through the source adapter of its own type: every
	// supported target type also has a source adapter.
	tgt, err := registry.NewSource(req.Project.TargetConfig.Type)
	if err != nil {
		src.Close()
		return nil, fmt.Errorf("target reader: %w", err)
	}
	if err := tgt.Connect(ctx, req.Project.TargetConfig); err != nil {
		src.Close()
		return nil, fmt.Errorf("connect target: %w", err)
	}

	_, isMongo := src.(*mongo.Source)
	return &Session{
		src: src, tgt: tgt, isMongo: isMongo, level: level, batchSize: batchSize,
		sample: req.Sample, plan: req.Plan, srcType: req.Project.SourceConfig.Type,
		tgtType:  req.Project.TargetConfig.Type,
		progress: req.OnProgress,
	}, nil
}

// Table validates one table. The error is non-nil only when ctx is
// cancelled: a read cut short that way is not a verdict on the table.
func (s *Session) Table(ctx context.Context, table string) (state.ValidationTable, error) {
	if err := ctx.Err(); err != nil {
		return state.ValidationTable{}, err
	}
	tr := newTracker(s.progress, table, s.batchSize)
	var split func(context.Context, []part) error
	// Not on YugabyteDB: a range read of its default hash-sharded key scans
	// the whole table, which is why its tables are streamed in the first place.
	if s.pool != nil && s.srcType != "yugabyte" && s.tgtType != "yugabyte" {
		split = func(ctx context.Context, parts []part) error { return s.pool.split(ctx, s, parts) }
	}
	res := validateTable(ctx, s.src, s.tgt, table, s.level, s.batchSize, s.sample, s.plan, s.isMongo, s.tgtType, tr, split)
	if err := ctx.Err(); err != nil {
		return state.ValidationTable{}, err
	}
	return res, nil
}

// Close closes both connections.
func (s *Session) Close() {
	s.src.Close()
	s.tgt.Close()
}

// validateTable validates one table, trying again when a read failed on an
// error the database says is worth retrying: YugabyteDB reports a table whose
// schema changed under a connection's cache (just after its indexes were
// created) as SQLSTATE 40001, "schema version mismatch". The whole table is
// validated again, as a fingerprint can't resume halfway.
func validateTable(ctx context.Context, src, tgt adapters.SourceAdapter, table, level string, batchSize, sample int, plan []state.SchemaPlanRow, isMongo bool, targetType string, tr *tracker, split func(context.Context, []part) error) state.ValidationTable {
	var res state.ValidationTable
	for attempt := 0; ; attempt++ {
		var transient bool
		res, transient = validateAttempt(ctx, src, tgt, table, level, batchSize, sample, plan, isMongo, targetType, tr, split)
		if !transient || attempt >= len(retryDelays) {
			return res
		}
		select {
		case <-ctx.Done():
			return res
		case <-time.After(retryDelays[attempt]):
		}
	}
}

// retryDelays are the waits before each retry of a table that failed on a
// transient error.
var retryDelays = []time.Duration{time.Second, 3 * time.Second, 10 * time.Second}

// isTransient reports an error a retry may not hit: SQLSTATE 40001
// (serialization failure, YugabyteDB's schema version mismatch and restart
// read) or 40P01 (deadlock).
func isTransient(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "40001" || pqErr.Code == "40P01"
	}
	msg := err.Error()
	return strings.Contains(msg, "(40001)") || strings.Contains(msg, "schema version mismatch")
}

// split, when set, runs key-range parts of a large table on this and other
// workers (see split.go); nil reads every table in one pass.
func validateAttempt(ctx context.Context, src, tgt adapters.SourceAdapter, table, level string, batchSize, sample int, plan []state.SchemaPlanRow, isMongo bool, targetType string, tr *tracker, split func(context.Context, []part) error) (res state.ValidationTable, transient bool) {
	tr.step(StepCounting)
	// LevelRun starts at what L1/L2 reach; L3 raises it only if it runs.
	runLevel := level
	if level == LevelL3 {
		runLevel = LevelL2
	}
	res = state.ValidationTable{TableName: table, LevelRun: runLevel, Result: state.ValidationFail}

	srcSchema, err := src.GetSchema(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("read source schema: %v", err)
		return res, isTransient(err)
	}
	// The source's own types, before the plan's overrides replace them: a
	// narrowing override is converted by the target as it stores each value.
	// (A MongoDB source converts values itself, in ReadBatch.)
	var srcTypes map[string]string
	if !isMongo && len(plan) > 0 {
		srcTypes = make(map[string]string, len(srcSchema.Columns))
		for _, c := range srcSchema.Columns {
			srcTypes[c.Name] = c.Type
		}
		state.ApplyPlanOverrides(srcSchema, plan)
	}
	tgtSchema, err := tgt.GetSchema(ctx, table)
	if err != nil || len(tgtSchema.Columns) == 0 {
		if err == nil {
			err = fmt.Errorf("table not found")
		}
		res.Detail = fmt.Sprintf("read target schema: %v", err)
		return res, isTransient(err)
	}

	cols, problems := align(srcSchema, tgtSchema)
	if srcTypes != nil {
		planned := make(map[string]string, len(srcSchema.Columns))
		for _, c := range srcSchema.Columns {
			planned[c.Name] = c.Type
		}
		for i := range cols {
			cols[i].lossy = lossyRule(srcTypes[cols[i].name], planned[cols[i].name], targetType)
		}
	}

	// L1: row counts. Counted even when the columns disagree, since the
	// count is still useful when diagnosing the failure.
	srcRows, err := src.GetRowCount(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("count source rows: %v", err)
		return res, isTransient(err)
	}
	tgtRows, err := tgt.GetRowCount(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("count target rows: %v", err)
		return res, isTransient(err)
	}
	res.SourceRows, res.TargetRows = &srcRows, &tgtRows

	if len(problems) > 0 {
		res.Detail = strings.Join(problems, "; ")
		return res, false
	}
	if srcRows != tgtRows {
		res.Detail = fmt.Sprintf("row counts differ: source has %d, target has %d", srcRows, tgtRows)
		if level == LevelL3 {
			// L2 cannot pass with different counts; go straight to the diff.
			res.LevelRun = LevelL1
			tr.step(StepRowDiff)
			rowDiff(ctx, src, tgt, srcSchema, tgtSchema, cols, batchSize, sample, isMongo, &res)
		}
		return res, false
	}
	if level == LevelL1 {
		res.Result = state.ValidationPass
		return res, false
	}

	// L2: fingerprint every row on both sides.
	// The source side hashes each value as the plan meant it to be stored,
	// and notes the rows a lossy override changed.
	var srcFP, tgtFP fingerprint
	planned := newLossyLog(srcSchema, cols)
	tr.total(srcRows + tgtRows)
	tr.step(StepReading)
	// The two sides are read at once: each fingerprint is an
	// order-independent sum over its own side, and each side has its own
	// connection, so neither waits for the other (#200). A large table may
	// instead be read in key-range parts, several workers at once.
	var srcErr, tgtErr error
	var key, tgtKey string
	var bounds [][2]*int64
	if split != nil {
		key, tgtKey, bounds = keyRanges(ctx, src, tgt, srcSchema, tgtSchema, cols, srcRows)
	}
	if bounds != nil {
		srcErr, tgtErr = readSplit(ctx, split, table, key, tgtKey, bounds, srcSchema, cols, &srcFP, &tgtFP, planned, tr)
	} else {
		srcErr, tgtErr = both(ctx,
			func(ctx context.Context) error {
				return scan(ctx, src, srcSchema, batchSize, isMongo, func(rec map[string]interface{}) {
					planned.add(rec, srcFP.add(rec, cols, false))
					tr.row()
				})
			},
			func(ctx context.Context) error {
				return scan(ctx, tgt, tgtSchema, batchSize, false, func(rec map[string]interface{}) {
					tgtFP.add(rec, cols, true)
					tr.row()
				})
			})
	}
	if srcErr != nil {
		res.Detail = fmt.Sprintf("read source rows: %v", srcErr)
		return res, isTransient(srcErr)
	}
	if tgtErr != nil {
		res.Detail = fmt.Sprintf("read target rows: %v", tgtErr)
		return res, isTransient(tgtErr)
	}
	res.SourceHash, res.TargetHash = srcFP.String(), tgtFP.String()
	res.RowsChecked = srcFP.rows

	switch {
	case srcFP.rows != tgtFP.rows:
		// Counts matched a moment ago, so the table changed underneath us
		// (or an unordered OFFSET read skipped or repeated rows).
		res.Detail = fmt.Sprintf("read %d source rows and %d target rows although both counted %d; was a side being written to?", srcFP.rows, tgtFP.rows, srcRows)
	case srcFP.sum != tgtFP.sum:
		res.Detail = "row counts match but row contents differ; L2 cannot say which rows"
	case planned.rows > 0:
		res.Result = state.ValidationExpectedLossy
		res.LossyRows = planned.rows
		res.Detail = planned.detail()
		res.Diffs = planned.examples
		if level == LevelL3 {
			res.LevelNote = "matched L2 once the plan's conversions were applied, so no row diff was needed"
		}
	default:
		res.Result = state.ValidationPass
		if level == LevelL3 {
			res.LevelNote = "passed L2, so no row diff was needed"
		}
	}
	if res.Result == state.ValidationFail && level == LevelL3 {
		tr.step(StepRowDiff)
		rowDiff(ctx, src, tgt, srcSchema, tgtSchema, cols, batchSize, sample, isMongo, &res)
	}
	return res, false
}

// both runs a and b at once and returns their errors. When one fails, the
// other is cancelled, and its error -- only a consequence, and not always
// recognisable as one (lib/pq reports a cancelled query as its own error) --
// is dropped. When ctx itself is cancelled, both errors are returned.
func both(ctx context.Context, a, b func(context.Context) error) (errA, errB error) {
	inner, cancel := context.WithCancel(ctx)
	defer cancel()
	var once sync.Once
	first := 0 // 1 when a failed first, 2 when b did
	fail := func(side int) { once.Do(func() { first = side; cancel() }) }
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if errB = b(inner); errB != nil {
			fail(2)
		}
	}()
	if errA = a(inner); errA != nil {
		fail(1)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return errA, errB
	}
	switch first {
	case 1:
		errB = nil
	case 2:
		errA = nil
	}
	return errA, errB
}

// rowDiff runs L3 on a table that failed L1 or L2 and rewrites res with what
// it found. When the table cannot be diffed, res keeps the earlier failure
// and LevelNote says why L3 did not run.
func rowDiff(ctx context.Context, src, tgt adapters.SourceAdapter, srcSchema, tgtSchema *adapters.TableSchema, cols []column, batchSize, sample int, isMongo bool, res *state.ValidationTable) {
	unavailable := func(reason string) { res.LevelNote = "L3 row diff unavailable: " + reason }
	key, reason := diffKey(srcSchema, cols)
	if key == nil {
		unavailable(reason)
		return
	}
	d := newDiffer(src, tgt, srcSchema.Name, cols, key, batchSize, sample)

	var note string
	var err error
	if len(key) == 1 && key[0].kind == kindInt && !isMongo {
		note, err = d.byRange(ctx, *res.SourceRows)
	} else {
		lookup, ok := tgt.(adapters.RowLookup)
		if !ok {
			unavailable("the target cannot look rows up by key")
			return
		}
		var found int64
		found, note, err = d.byLookup(ctx, lookup, srcSchema, tgtSchema, isMongo, *res.TargetRows)
		if err == nil && found == 0 && d.checked > 0 && *res.TargetRows > 0 {
			// Not one key matched although the target has rows: the keys are
			// most likely stored in a form the lookup does not match, and
			// every row would be reported wrongly as missing and extra.
			unavailable("no source key was found in the target; its keys may be stored in a different form")
			return
		}
	}
	if err != nil {
		unavailable(err.Error())
		return
	}

	res.LevelRun = LevelL3
	res.LevelNote = note
	res.RowsChecked = d.checked
	res.DiffMissing, res.DiffExtra, res.DiffMismatch = d.missing, d.extra, d.mismatched
	res.LossyRows = d.lossy
	res.Diffs = d.diffs()
	if d.sample != 0 {
		res.Sample = d.sample
	}

	if d.missing+d.extra+d.mismatched == 0 {
		if d.sample != 0 {
			res.Detail = fmt.Sprintf("the %d%% sample found no differing rows; the difference is in rows it did not check", d.sample)
			return
		}
		res.Result = state.ValidationPass
		res.Detail = ""
		if d.lossy > 0 {
			res.Result = state.ValidationExpectedLossy
			res.Detail = fmt.Sprintf("%d row(s) changed by the plan's type overrides, as expected", d.lossy)
		}
		res.LevelNote = joinNotes("the row diff found no real difference, so the table changed after the earlier check failed", note)
		return
	}
	var parts []string
	if d.missing > 0 {
		parts = append(parts, fmt.Sprintf("%d row(s) missing in target", d.missing))
	}
	if d.extra > 0 {
		parts = append(parts, fmt.Sprintf("%d row(s) only in target", d.extra))
	}
	if d.mismatched > 0 {
		parts = append(parts, fmt.Sprintf("%d row(s) with different values", d.mismatched))
	}
	if d.lossy > 0 {
		parts = append(parts, fmt.Sprintf("%d more changed by the plan's type overrides, as expected", d.lossy))
	}
	res.Detail = strings.Join(parts, ", ")
	if d.sample != 0 {
		res.Detail += fmt.Sprintf(" (in a %d%% sample)", d.sample)
	}
}

func joinNotes(notes ...string) string {
	var out []string
	for _, n := range notes {
		if n != "" {
			out = append(out, n)
		}
	}
	return strings.Join(out, "; ")
}

// align pairs each intended column with the target column of the same name
// (exact match first, then case-insensitive, since some databases fold
// identifier case). A column on only one side is a defect: the comparison
// would otherwise pass while silently ignoring it.
func align(src, tgt *adapters.TableSchema) ([]column, []string) {
	exact := make(map[string]adapters.ColumnDef, len(tgt.Columns))
	folded := make(map[string]adapters.ColumnDef, len(tgt.Columns))
	for _, c := range tgt.Columns {
		exact[c.Name] = c
		folded[strings.ToLower(c.Name)] = c
	}

	var cols []column
	var missing []string
	matched := make(map[string]bool, len(tgt.Columns))
	for _, sc := range src.Columns {
		tc, ok := exact[sc.Name]
		if !ok {
			tc, ok = folded[strings.ToLower(sc.Name)]
		}
		if !ok {
			missing = append(missing, sc.Name)
			continue
		}
		matched[tc.Name] = true
		k := kindOf(sc.Type)
		frac := fracDigits(sc.Type)
		if f := fracDigits(tc.Type); f < frac {
			frac = f
		}
		cols = append(cols, column{name: sc.Name, tgtKey: tc.Name, kind: k, frac: frac})
	}
	var extra []string
	for _, tc := range tgt.Columns {
		if !matched[tc.Name] {
			extra = append(extra, tc.Name)
		}
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].name < cols[j].name })

	var problems []string
	if len(missing) > 0 {
		problems = append(problems, "columns missing in target: "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		problems = append(problems, "columns only in target: "+strings.Join(extra, ", "))
	}
	return cols, problems
}

// scan reads every row of a table and passes each to fn, in no particular
// order: the fingerprint and the diff's lookups don't depend on it. An
// adapter that can stream a table (adapters.TableStreamer) reads it in one
// query; paging cost a full table scan per page on a YugabyteDB table with a
// hash-sharded key (12s a page for 1.2M rows), and one query also reads a
// single snapshot. Every built-in adapter streams, MongoDB included (one
// cursor instead of a sorted find per batch, #202). An adapter that can't
// is paged by keyset where it can: MongoDB by _id, a relational table by its
// single integer primary key, and anything else by LIMIT/OFFSET, the same
// path the engine uses for such tables.
func scan(ctx context.Context, a adapters.SourceAdapter, schema *adapters.TableSchema, batchSize int, isMongo bool, fn func(map[string]interface{})) error {
	table := schema.Name
	if st, ok := a.(adapters.TableStreamer); ok {
		return st.StreamTable(ctx, table, func(r map[string]interface{}) error {
			fn(r)
			return ctx.Err()
		})
	}
	if isMongo {
		var lastKey string
		for {
			b, err := a.ReadBatch(ctx, table, adapters.BatchOptions{Limit: batchSize, LastPK: lastKey})
			if err != nil {
				return err
			}
			if len(b.Records) == 0 {
				return nil
			}
			for _, r := range b.Records {
				fn(r)
			}
			lastKey = b.LastKey
		}
	}

	if pk := integerPK(schema); pk != "" {
		if lo, hi, err := a.GetPKRange(ctx, table, pk); err == nil {
			for cur := lo; cur <= hi; {
				b, err := a.ReadBatch(ctx, table, adapters.BatchOptions{
					Limit: batchSize, PKCol: pk, PKMin: cur, PKMax: hi, PKMaxIncl: true,
				})
				if err != nil {
					return err
				}
				if len(b.Records) == 0 {
					return nil
				}
				for _, r := range b.Records {
					fn(r)
				}
				cur = b.LastPK + 1
			}
			return nil
		}
	}

	for offset := 0; ; {
		b, err := a.ReadBatch(ctx, table, adapters.BatchOptions{Limit: batchSize, Offset: offset})
		if err != nil {
			return err
		}
		if len(b.Records) == 0 {
			return nil
		}
		for _, r := range b.Records {
			fn(r)
		}
		offset += len(b.Records)
	}
}

// integerPK returns the table's primary key column when it is a single
// integer column, else "".
func integerPK(schema *adapters.TableSchema) string {
	var pk *adapters.ColumnDef
	for i := range schema.Columns {
		if schema.Columns[i].PrimaryKey {
			if pk != nil {
				return ""
			}
			pk = &schema.Columns[i]
		}
	}
	if pk == nil || kindOf(pk.Type) != kindInt {
		return ""
	}
	return pk.Name
}
