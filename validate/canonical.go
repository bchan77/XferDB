package validate

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// kind is a column's logical type: what its values mean, independent of how
// any one database or driver represents them. Both sides of a comparison are
// canonicalized by the kind of the *intended* column (source schema with plan
// overrides applied), never by the target's physical type -- that is what
// lets a boolean match a SQLite INTEGER 1 or a MySQL TINYINT(1).
type kind int

const (
	kindText kind = iota
	kindChar      // fixed-width CHAR(n): trailing pad spaces are not data
	kindInt
	kindBool
	kindDecimal
	kindFloat  // double precision
	kindFloat4 // single precision (real, float)
	kindTimestamp
	kindDate
	kindTime
	kindJSON
	kindBinary
	kindUUID
	kindInterval // interval.go
)

// nullToken stands for SQL NULL (and a field absent from a MongoDB document,
// which the target stores as NULL). It cannot collide with a real value
// because canonical strings never contain a NUL byte prefix.
const nullToken = "\x00NULL"

// kindOf classifies a declared column type from any supported dialect
// (PostgreSQL, MySQL, SQLite, or a type the MongoDB analyzer inferred).
func kindOf(colType string) kind {
	lower := strings.Join(strings.Fields(strings.ToLower(colType)), " ")
	switch lower {
	case "tinyint(1)", "bit(1)", "boolean", "bool":
		return kindBool
	}
	base := lower
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	base = strings.TrimSuffix(base, " zerofill")
	base = strings.TrimSuffix(base, " unsigned")
	base = strings.TrimSuffix(base, " signed")

	switch base {
	case "integer", "int", "int2", "int4", "int8", "smallint", "bigint",
		"tinyint", "mediumint", "serial", "smallserial", "bigserial", "year":
		return kindInt
	case "numeric", "decimal", "money":
		return kindDecimal
	case "double precision", "double", "float8":
		return kindFloat
	case "real", "float", "float4":
		return kindFloat4
	case "date":
		return kindDate
	case "datetime":
		return kindTimestamp
	case "json", "jsonb":
		return kindJSON
	case "bytea", "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return kindBinary
	case "uuid":
		return kindUUID
	case "char", "character", "bpchar":
		return kindChar
	}
	if strings.HasPrefix(base, "interval") {
		return kindInterval
	}
	if strings.HasPrefix(base, "timestamp") {
		return kindTimestamp
	}
	// time[(p)] [with|without time zone], timetz. Must follow the timestamp check.
	if strings.HasPrefix(base, "time") {
		return kindTime
	}
	return kindText
}

// fracDigits returns how many fractional-second digits a temporal column
// keeps. Comparing two columns truncates both values to the smaller of the
// two, so a DATETIME(0) target is not reported as losing the microseconds it
// was never asked to keep.
func fracDigits(colType string) int {
	lower := strings.ToLower(strings.TrimSpace(colType))
	if open := strings.IndexByte(lower, '('); open >= 0 {
		if end := strings.IndexByte(lower[open:], ')'); end > 0 {
			if p, err := strconv.Atoi(strings.TrimSpace(lower[open+1 : open+end])); err == nil {
				return p
			}
		}
	}
	switch {
	case strings.HasPrefix(lower, "datetime"):
		return 0 // MySQL DATETIME without (fsp)
	case strings.HasPrefix(lower, "time"):
		// timestamp, timestamptz, time: PostgreSQL's default of microseconds.
		return 6
	}
	return 9
}

// column is a column as the comparison sees it: its logical kind and the
// fractional-second precision shared by both sides.
type column struct {
	name   string // intended column name; also the key in source rows
	tgtKey string // the same column's name in target rows
	kind   kind
	frac   int
	// lossy is the conversion the target applies because a plan override
	// narrowed this column (lossy.go); nil when there is none.
	lossy convertFn
}

// canonicalize renders v as a byte-stable string so that values meaning the
// same thing compare equal however a driver returned them.
func canonicalize(v interface{}, c column) string {
	if v == nil {
		return nullToken
	}
	switch c.kind {
	case kindInt:
		return canonInt(v)
	case kindBool:
		return canonBool(v)
	case kindDecimal:
		return canonDecimal(v)
	case kindFloat:
		return canonFloat(v, 15)
	case kindFloat4:
		return canonFloat4(v)
	case kindTimestamp:
		return canonTimestamp(v, c.frac)
	case kindDate:
		return canonDate(v)
	case kindTime:
		return canonTime(asString(v))
	case kindJSON:
		return canonJSON(v)
	case kindBinary:
		return canonBinary(v)
	case kindUUID:
		return canonUUID(v)
	case kindInterval:
		return canonInterval(v)
	case kindChar:
		return strings.TrimRight(asString(v), " ")
	}
	return asString(v)
}

// asString renders a driver value as text. []byte is how lib/pq and the
// MySQL text protocol return most non-numeric columns.
func asString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	}
	return fmt.Sprint(v)
}

func canonInt(v interface{}) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case bool:
		if x {
			return "1"
		}
		return "0"
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
	}
	s := strings.TrimSpace(asString(v))
	if n, ok := new(big.Int).SetString(s, 10); ok {
		return n.String()
	}
	return s
}

func canonBool(v interface{}) string {
	switch x := v.(type) {
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatBool(x != 0)
	case int32:
		return strconv.FormatBool(x != 0)
	case int:
		return strconv.FormatBool(x != 0)
	case float64:
		return strconv.FormatBool(x != 0)
	}
	s := strings.ToLower(strings.TrimSpace(asString(v)))
	switch s {
	case "t", "true", "y", "yes", "on", "1":
		return "true"
	case "f", "false", "n", "no", "off", "0", "\x00":
		return "false"
	}
	// A MySQL BIT(1) arrives as a single raw byte.
	if b, ok := v.([]byte); ok && len(b) == 1 {
		return strconv.FormatBool(b[0] != 0)
	}
	return s
}

// canonDecimal trims trailing fractional zeros so 1.50 and 1.5 match. A
// SQLite NUMERIC column hands back float64 or int64, so those are rendered
// without exponent first.
func canonDecimal(v interface{}) string {
	var s string
	switch x := v.(type) {
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case int64:
		s = strconv.FormatInt(x, 10)
	default:
		s = strings.TrimSpace(asString(v))
	}
	// PostgreSQL money renders with a currency symbol and grouping.
	s = strings.NewReplacer("$", "", ",", "").Replace(s)
	return trimDecimal(s)
}

func trimDecimal(s string) string {
	if strings.ContainsAny(s, "eE") {
		if r, ok := new(big.Rat).SetString(s); ok {
			s = r.FloatString(30)
		}
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	s = strings.TrimLeft(s, "0")
	if s == "" || strings.HasPrefix(s, ".") {
		s = "0" + s
	}
	if neg && s != "0" {
		s = "-" + s
	}
	return s
}

// canonFloat rounds to sig significant digits before hashing: re-encoding a
// float through another database's text or binary format can move the last
// bits, and a single-precision column only holds about 7 digits at all.
// canonFloat4 renders a single-precision value at 6 significant digits, the
// most MySQL shows for FLOAT over its text protocol (3.1415927 reads back as
// 3.14159, #131). The value is first snapped to single precision, so a source
// that hands over more digits (a Postgres real as text, a float64) rounds
// from the same stored value MySQL rounded from.
func canonFloat4(v interface{}) string {
	f, ok := parseFloat(v)
	if !ok {
		return asString(v)
	}
	if f == 0 {
		return "0"
	}
	return strconv.FormatFloat(float64(float32(f)), 'g', 6, 64)
}

// parseFloat reads a driver value as a float64.
func parseFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	p, err := strconv.ParseFloat(strings.TrimSpace(asString(v)), 64)
	return p, err == nil
}

func canonFloat(v interface{}, sig int) string {
	f, ok := parseFloat(v)
	if !ok {
		return asString(v)
	}
	if f == 0 {
		return "0" // folds -0 into 0
	}
	return strconv.FormatFloat(f, 'g', sig, 64)
}

// timeLayouts are the text forms drivers return when they do not parse a
// temporal column into time.Time themselves.
var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

func parseTime(v interface{}) (time.Time, bool) {
	if t, ok := v.(time.Time); ok {
		return t, true
	}
	s := strings.TrimSpace(asString(v))
	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func canonTimestamp(v interface{}, frac int) string {
	t, ok := parseTime(v)
	if !ok {
		return asString(v)
	}
	unit := time.Duration(1)
	for i := frac; i < 9; i++ {
		unit *= 10
	}
	// PostgreSQL and MySQL both round, not truncate, when a column keeps
	// fewer fractional digits (10:30:45.5 in TIMESTAMP(0) is 10:30:46).
	return t.UTC().Round(unit).Format(time.RFC3339Nano)
}

func canonDate(v interface{}) string {
	if t, ok := v.(time.Time); ok {
		// A date carries no zone; read its calendar fields as stored.
		return t.Format("2006-01-02")
	}
	s := strings.TrimSpace(asString(v))
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}

// canonTime drops trailing fractional zeros: MySQL TIME(6) pads to six
// digits where PostgreSQL prints none.
func canonTime(s string) string {
	s = strings.TrimSpace(s)
	if t, ok := parseTime(s); ok && len(s) > 10 {
		// A full timestamp in a time column (some drivers do this): keep the clock.
		s = t.Format("15:04:05.999999999")
	}
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		end := dot + 1
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
		fracPart := strings.TrimRight(s[dot+1:end], "0")
		rest := s[end:]
		if fracPart == "" {
			s = s[:dot] + rest
		} else {
			s = s[:dot+1] + fracPart + rest
		}
	}
	return s
}

// canonJSON re-serializes with sorted keys and normalized numbers, so key
// order, whitespace and 1.0 vs 1 (jsonb vs MySQL JSON) do not differ.
func canonJSON(v interface{}) string {
	raw := asString(v)
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var parsed interface{}
	if err := dec.Decode(&parsed); err != nil {
		return raw
	}
	b, err := json.Marshal(normalizeJSON(parsed))
	if err != nil {
		return raw
	}
	return string(b)
}

func normalizeJSON(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, e := range x {
			x[k] = normalizeJSON(e)
		}
		return x
	case []interface{}:
		for i, e := range x {
			x[i] = normalizeJSON(e)
		}
		return x
	case json.Number:
		return json.Number(trimDecimal(x.String()))
	}
	return v
}

func canonBinary(v interface{}) string {
	if b, ok := v.([]byte); ok {
		return hex.EncodeToString(b)
	}
	return hex.EncodeToString([]byte(asString(v)))
}

func canonUUID(v interface{}) string {
	if b, ok := v.([]byte); ok && len(b) == 16 {
		h := hex.EncodeToString(b)
		return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
	}
	return strings.ToLower(strings.TrimSpace(asString(v)))
}
