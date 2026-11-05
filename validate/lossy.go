package validate

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// Lossy by plan: a schema-plan override can narrow a column (numeric(10,4) to
// numeric(10,2), double to integer, timestamp to date), and the target then
// converts each value as it stores it. Such a difference is one the user
// chose, not a defect. For these columns the validator applies the same
// conversion to the source value, and a match after it is reported as
// expected_lossy.
//
// Only conversions a target actually performs silently are modelled, each as
// measured against PostgreSQL 16 and MySQL (strict mode) with real XferDB
// migrations. Where a target refuses the value instead (PostgreSQL storing
// 1.5 in an integer, any strict target storing text longer than VARCHAR(n))
// the migration fails, so there is nothing to model. SQLite stores every
// value as given, whatever the declared type.
//
//	override                  postgres                     mysql
//	decimal/float -> numeric  round half away from zero    same
//	  with fewer decimals
//	float/decimal -> integer  (refuses non-integers)       float value: half to even,
//	                                                       decimal text: half away
//	timestamp -> date         truncates (no rule needed)   rounds to whole seconds first

// convertFn returns the canonical value a target stores for a source value,
// or false when it cannot model this value (the plain comparison applies).
type convertFn func(v interface{}) (string, bool)

// lossyRule returns the conversion a target of type target applies when a
// plan override changes a column from fromType to toType, or nil when the
// override loses nothing that needs modelling.
func lossyRule(fromType, toType, target string) convertFn {
	if strings.EqualFold(strings.TrimSpace(fromType), strings.TrimSpace(toType)) {
		return nil
	}
	if !adapters.PostgresFamily(target) && target != "mysql" {
		return nil
	}
	from, to := kindOf(fromType), kindOf(toType)
	switch {
	case to == kindDecimal:
		scale, ok := decimalScale(toType)
		if !ok {
			return nil
		}
		switch from {
		case kindDecimal:
			if s, ok := decimalScale(fromType); ok && s <= scale {
				return nil
			}
		case kindFloat, kindFloat4:
		default:
			return nil
		}
		return func(v interface{}) (string, bool) {
			r, ok := decimalRat(v)
			if !ok {
				return "", false
			}
			return trimDecimal(roundHalfAway(r, scale)), true
		}

	case to == kindInt && target == "mysql" && (from == kindFloat || from == kindFloat4 || from == kindDecimal):
		return func(v interface{}) (string, bool) {
			// MySQL rounds a bound double with rint, but parses decimal
			// text (how lib/pq and the MySQL driver hand over numerics)
			// as exact and rounds it half away from zero.
			switch x := v.(type) {
			case float64:
				return strconv.FormatFloat(math.RoundToEven(x), 'f', 0, 64), true
			case float32:
				return strconv.FormatFloat(math.RoundToEven(float64(x)), 'f', 0, 64), true
			}
			r, ok := decimalRat(v)
			if !ok {
				return "", false
			}
			return trimDecimal(roundHalfAway(r, 0)), true
		}

	case to == kindDate && from == kindTimestamp && target == "mysql":
		return func(v interface{}) (string, bool) {
			t, ok := parseTime(v)
			if !ok {
				return "", false
			}
			return t.UTC().Round(time.Second).Format("2006-01-02"), true
		}
	}
	return nil
}

// intended returns the canonical value the migration meant to store for a
// source value of column c, and whether a lossy plan conversion changed it.
func intended(v interface{}, c column) (string, bool) {
	canon := canonicalize(v, c)
	if c.lossy == nil || v == nil {
		return canon, false
	}
	conv, ok := c.lossy(v)
	if !ok {
		return canon, false
	}
	return conv, conv != canon
}

// decimalScale returns the scale s of numeric(p,s) / decimal(p,s); a bare
// numeric has none. A money column is always 2.
func decimalScale(colType string) (int, bool) {
	lower := strings.ToLower(strings.TrimSpace(colType))
	if strings.HasPrefix(lower, "money") {
		return 2, true
	}
	open := strings.IndexByte(lower, '(')
	end := strings.IndexByte(lower, ')')
	if open < 0 || end < open {
		return 0, false
	}
	parts := strings.Split(lower[open+1:end], ",")
	if len(parts) == 1 {
		// numeric(p) is numeric(p,0).
		return 0, true
	}
	s, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	return s, err == nil
}

// decimalRat reads a driver value as an exact decimal. A float becomes its
// shortest decimal form, which is what lib/pq sends and what MySQL converts a
// bound double from.
func decimalRat(v interface{}) (*big.Rat, bool) {
	var s string
	switch x := v.(type) {
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(x), 'f', -1, 32)
	case int64:
		s = strconv.FormatInt(x, 10)
	default:
		s = strings.TrimSpace(asString(v))
	}
	return new(big.Rat).SetString(s)
}

// roundHalfAway renders r rounded to scale decimals, halves away from zero.
func roundHalfAway(r *big.Rat, scale int) string {
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(pow))
	num, den := scaled.Num(), scaled.Denom()
	neg := num.Sign() < 0
	q, m := new(big.Int).QuoRem(new(big.Int).Abs(num), den, new(big.Int))
	if new(big.Int).Mul(m, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if neg && q.Sign() != 0 {
		q.Neg(q)
	}
	return new(big.Rat).SetFrac(q, pow).FloatString(scale)
}

// lossyLog counts the source rows a lossy plan conversion changed and keeps
// the first of them as examples.
type lossyLog struct {
	key      []column // the table's primary key, to label examples; may be empty
	cols     []column
	rows     int64
	changed  map[string]bool
	examples []state.ValidationDiff
}

func newLossyLog(schema *adapters.TableSchema, cols []column) *lossyLog {
	byName := make(map[string]column, len(cols))
	for _, c := range cols {
		byName[c.name] = c
	}
	l := &lossyLog{cols: cols, changed: map[string]bool{}}
	for _, sc := range schema.Columns {
		if c, ok := byName[sc.Name]; ok && sc.PrimaryKey {
			l.key = append(l.key, c)
		}
	}
	return l
}

// add records a source row whose changed columns a plan conversion altered.
func (l *lossyLog) add(rec map[string]interface{}, changed []string) {
	if len(changed) == 0 {
		return
	}
	l.rows++
	for _, c := range changed {
		l.changed[c] = true
	}
	if len(l.examples) < maxExamples {
		l.examples = append(l.examples, state.ValidationDiff{
			Kind:    state.DiffLossyByPlan,
			Key:     shownValues(rec, l.key, false),
			Columns: changed,
			Source:  shownValues(rec, l.cols, false),
			Target:  shownIntended(rec, l.cols),
		})
	}
}

// detail says which rows and columns the plan changed.
func (l *lossyLog) detail() string {
	cols := make([]string, 0, len(l.changed))
	for c := range l.changed {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return fmt.Sprintf("%d row(s) changed by the plan's type overrides, as expected (%s)", l.rows, strings.Join(cols, ", "))
}

// shownIntended renders a source row as the plan meant it to be stored.
func shownIntended(rec map[string]interface{}, cols []column) map[string]*string {
	out := make(map[string]*string, len(cols))
	for _, c := range cols {
		v, _ := intended(rec[c.name], c)
		out[c.name] = shownValue(v)
	}
	return out
}
