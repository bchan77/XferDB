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
	"fmt"
	"sort"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/adapters/mongo"
	"gitea.homelab.local/nextdevops/XferDB/registry"
	"gitea.homelab.local/nextdevops/XferDB/state"
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
	ModePerTable = "per_table" // as each table completes (not yet available)
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
	case "", ModeAfter, ModeOff:
		return nil
	case ModePerTable:
		return fmt.Errorf("validation mode per_table is not available yet; use after or off")
	}
	return fmt.Errorf("validation mode %q must be one of: after, off", mode)
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
}

// Run validates each table in req.Tables and hands every outcome to report as
// soon as that table is done. It returns early only on a connection failure
// or when ctx is cancelled; a table that fails validation is a result, not an
// error.
func Run(ctx context.Context, req Request, report func(state.ValidationTable)) error {
	level := req.Level
	if level == "" {
		level = DefaultLevel
	}
	if err := CheckLevel(level); err != nil {
		return err
	}
	if err := CheckSample(req.Sample); err != nil {
		return err
	}
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	src, err := registry.NewSource(req.Project.SourceConfig.Type)
	if err != nil {
		return fmt.Errorf("source adapter: %w", err)
	}
	if err := src.Connect(ctx, req.Project.SourceConfig); err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close()

	if mongoSrc, ok := src.(*mongo.Source); ok {
		if len(req.Plan) == 0 {
			return fmt.Errorf("no schema plan found for this MongoDB source — run 'xferdb project analyze' first")
		}
		if err := mongoSrc.SetPlan(req.Plan); err != nil {
			return fmt.Errorf("set schema plan: %w", err)
		}
		// An estimate is not a validation.
		mongoSrc.SetAccurateCounts(true)
	}

	// The target is read through the source adapter of its own type: every
	// supported target type also has a source adapter.
	tgt, err := registry.NewSource(req.Project.TargetConfig.Type)
	if err != nil {
		return fmt.Errorf("target reader: %w", err)
	}
	if err := tgt.Connect(ctx, req.Project.TargetConfig); err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer tgt.Close()

	_, isMongo := src.(*mongo.Source)
	for _, table := range req.Tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		res := validateTable(ctx, src, tgt, table, level, batchSize, req.Sample, req.Plan, isMongo, req.Project.TargetConfig.Type)
		if err := ctx.Err(); err != nil {
			// A read cut short by cancellation is not a verdict on the table.
			return err
		}
		report(res)
	}
	return nil
}

func validateTable(ctx context.Context, src, tgt adapters.SourceAdapter, table, level string, batchSize, sample int, plan []state.SchemaPlanRow, isMongo bool, targetType string) state.ValidationTable {
	// LevelRun starts at what L1/L2 reach; L3 raises it only if it runs.
	runLevel := level
	if level == LevelL3 {
		runLevel = LevelL2
	}
	res := state.ValidationTable{TableName: table, LevelRun: runLevel, Result: state.ValidationFail}

	srcSchema, err := src.GetSchema(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("read source schema: %v", err)
		return res
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
		return res
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
		return res
	}
	tgtRows, err := tgt.GetRowCount(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("count target rows: %v", err)
		return res
	}
	res.SourceRows, res.TargetRows = &srcRows, &tgtRows

	if len(problems) > 0 {
		res.Detail = strings.Join(problems, "; ")
		return res
	}
	if srcRows != tgtRows {
		res.Detail = fmt.Sprintf("row counts differ: source has %d, target has %d", srcRows, tgtRows)
		if level == LevelL3 {
			// L2 cannot pass with different counts; go straight to the diff.
			res.LevelRun = LevelL1
			rowDiff(ctx, src, tgt, srcSchema, tgtSchema, cols, batchSize, sample, isMongo, &res)
		}
		return res
	}
	if level == LevelL1 {
		res.Result = state.ValidationPass
		return res
	}

	// L2: fingerprint every row on both sides.
	// The source side hashes each value as the plan meant it to be stored,
	// and notes the rows a lossy override changed.
	var srcFP, tgtFP fingerprint
	planned := newLossyLog(srcSchema, cols)
	if err := scan(ctx, src, srcSchema, batchSize, isMongo, func(rec map[string]interface{}) {
		planned.add(rec, srcFP.add(rec, cols, false))
	}); err != nil {
		res.Detail = fmt.Sprintf("read source rows: %v", err)
		return res
	}
	if err := scan(ctx, tgt, tgtSchema, batchSize, false, func(rec map[string]interface{}) {
		tgtFP.add(rec, cols, true)
	}); err != nil {
		res.Detail = fmt.Sprintf("read target rows: %v", err)
		return res
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
		rowDiff(ctx, src, tgt, srcSchema, tgtSchema, cols, batchSize, sample, isMongo, &res)
	}
	return res
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

// scan reads every row of a table in pages and passes each to fn. It pages by
// keyset where it can: MongoDB by _id, a relational table by its single
// integer primary key. Anything else falls back to LIMIT/OFFSET, the same
// path the engine uses for such tables.
func scan(ctx context.Context, a adapters.SourceAdapter, schema *adapters.TableSchema, batchSize int, isMongo bool, fn func(map[string]interface{})) error {
	table := schema.Name
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
