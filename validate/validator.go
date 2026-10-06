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

// Levels, cheapest first. Each runs only on a table that passed the one
// below it.
const (
	LevelL1 = "L1" // row counts
	LevelL2 = "L2" // order-independent fingerprint of every row
	LevelL3 = "L3" // PK-keyed row diff (not yet available)
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
	case "", LevelL1, LevelL2:
		return nil
	case LevelL3:
		return fmt.Errorf("validation level L3 (row diff) is not available yet; use L1 or L2")
	}
	return fmt.Errorf("validation level %q must be one of: L1, L2", level)
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
		res := validateTable(ctx, src, tgt, table, level, batchSize, req.Plan, isMongo)
		if err := ctx.Err(); err != nil {
			// A read cut short by cancellation is not a verdict on the table.
			return err
		}
		report(res)
	}
	return nil
}

func validateTable(ctx context.Context, src, tgt adapters.SourceAdapter, table, level string, batchSize int, plan []state.SchemaPlanRow, isMongo bool) state.ValidationTable {
	res := state.ValidationTable{TableName: table, LevelRun: level, Result: state.ValidationFail}

	srcSchema, err := src.GetSchema(ctx, table)
	if err != nil {
		res.Detail = fmt.Sprintf("read source schema: %v", err)
		return res
	}
	if !isMongo && len(plan) > 0 {
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
		return res
	}
	if level == LevelL1 {
		res.Result = state.ValidationPass
		return res
	}

	// L2: fingerprint every row on both sides.
	var srcFP, tgtFP fingerprint
	if err := scan(ctx, src, srcSchema, batchSize, isMongo, func(rec map[string]interface{}) {
		srcFP.add(rec, cols, false)
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
	default:
		res.Result = state.ValidationPass
	}
	return res
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
