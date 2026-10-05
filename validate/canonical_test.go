package validate

import (
	"testing"
	"time"
)

func TestKindOf(t *testing.T) {
	cases := map[string]kind{
		"integer":                     kindInt,
		"int(11)":                     kindInt,
		"bigint unsigned":             kindInt,
		"tinyint(1)":                  kindBool,
		"boolean":                     kindBool,
		"BOOLEAN":                     kindBool,
		"numeric(10,2)":               kindDecimal,
		"decimal(65,30)":              kindDecimal,
		"double precision":            kindFloat,
		"real":                        kindFloat4,
		"float":                       kindFloat4,
		"timestamp without time zone": kindTimestamp,
		"timestamptz":                 kindTimestamp,
		"datetime(6)":                 kindTimestamp,
		"TIMESTAMP":                   kindTimestamp,
		"date":                        kindDate,
		"time(6)":                     kindTime,
		"jsonb":                       kindJSON,
		"json":                        kindJSON,
		"bytea":                       kindBinary,
		"longblob":                    kindBinary,
		"uuid":                        kindUUID,
		"character(10)":               kindChar,
		"char(36)":                    kindChar,
		"character varying(255)":      kindText,
		"text":                        kindText,
	}
	for typ, want := range cases {
		if got := kindOf(typ); got != want {
			t.Errorf("kindOf(%q) = %d, want %d", typ, got, want)
		}
	}
}

func TestFracDigits(t *testing.T) {
	cases := map[string]int{
		"datetime":                    0,
		"datetime(6)":                 6,
		"timestamp(3) with time zone": 3,
		"timestamp without time zone": 6,
		"timestamptz":                 6,
		"TIMESTAMP":                   6,
		"time(6)":                     6,
	}
	for typ, want := range cases {
		if got := fracDigits(typ); got != want {
			t.Errorf("fracDigits(%q) = %d, want %d", typ, got, want)
		}
	}
}

// TestCanonicalizeMatchesAcrossDrivers feeds each logical type in the Go forms
// different drivers actually return for the same stored value. Every form in
// a group must canonicalize identically, or L2 reports a false mismatch.
func TestCanonicalizeMatchesAcrossDrivers(t *testing.T) {
	ts := time.Date(2024, 3, 1, 10, 30, 0, 123456000, time.UTC)
	plus2 := time.FixedZone("", 2*3600)

	groups := []struct {
		name  string
		col   column
		forms []interface{}
	}{
		{"int", column{kind: kindInt}, []interface{}{int64(42), int32(42), []byte("42"), "42", float64(42)}},
		{"bool true", column{kind: kindBool}, []interface{}{true, int64(1), []byte("1"), "t", "true", []byte{1}}},
		{"bool false", column{kind: kindBool}, []interface{}{false, int64(0), []byte("0"), "f", "false"}},
		{"decimal", column{kind: kindDecimal}, []interface{}{"1.50", []byte("1.5000"), float64(1.5), "1.5", "$1.50"}},
		{"decimal integer", column{kind: kindDecimal}, []interface{}{"2.00", int64(2), float64(2), []byte("2")}},
		{"decimal negative zero", column{kind: kindDecimal}, []interface{}{"-0.00", "0", int64(0)}},
		{"double", column{kind: kindFloat}, []interface{}{0.1, "0.1", []byte("0.1"), 0.1 + 1e-17}},
		{"real", column{kind: kindFloat4}, []interface{}{float64(float32(0.1)), "0.1", float32(0.1)}},
		{"timestamp", column{kind: kindTimestamp, frac: 6}, []interface{}{
			ts, ts.In(plus2), "2024-03-01 10:30:00.123456+00:00", "2024-03-01T12:30:00.123456+02:00",
			"2024-03-01 10:30:00.123456789",
		}},
		{"timestamp to seconds", column{kind: kindTimestamp, frac: 0}, []interface{}{ts, time.Date(2024, 3, 1, 10, 30, 0, 0, time.UTC)}},
		{"date", column{kind: kindDate}, []interface{}{time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), "2024-03-01", []byte("2024-03-01"), "2024-03-01T00:00:00Z"}},
		{"time", column{kind: kindTime}, []interface{}{"10:30:00", []byte("10:30:00.000000"), "10:30:00."}},
		{"time fraction", column{kind: kindTime}, []interface{}{"10:30:00.5", "10:30:00.500000"}},
		{"json", column{kind: kindJSON}, []interface{}{
			`{"b": 1.0, "a": [1, 2]}`, []byte(`{"a":[1,2],"b":1}`), `{"a": [1.00, 2], "b": 1}`,
		}},
		{"binary", column{kind: kindBinary}, []interface{}{[]byte{0xde, 0xad}, "\xde\xad"}},
		{"uuid", column{kind: kindUUID}, []interface{}{
			"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", []byte("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
			[]byte{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11},
		}},
		{"char", column{kind: kindChar}, []interface{}{"ab", "ab   ", []byte("ab ")}},
		{"text", column{kind: kindText}, []interface{}{"héllo", []byte("héllo")}},
		{"null", column{kind: kindInt}, []interface{}{nil}},
	}
	for _, g := range groups {
		want := canonicalize(g.forms[0], g.col)
		for _, f := range g.forms[1:] {
			if got := canonicalize(f, g.col); got != want {
				t.Errorf("%s: canonicalize(%#v) = %q, want %q (from %#v)", g.name, f, got, want, g.forms[0])
			}
		}
	}
}

// TestCanonicalizeKeepsRealDifferences guards against rules so loose they
// turn a real defect into a pass.
func TestCanonicalizeKeepsRealDifferences(t *testing.T) {
	pairs := []struct {
		name string
		col  column
		a, b interface{}
	}{
		{"int", column{kind: kindInt}, int64(42), int64(43)},
		{"null vs empty text", column{kind: kindText}, nil, ""},
		{"null vs zero", column{kind: kindInt}, nil, int64(0)},
		{"bool", column{kind: kindBool}, true, false},
		{"decimal scale", column{kind: kindDecimal}, "1.5", "1.05"},
		{"double", column{kind: kindFloat}, 0.1, 0.1000001},
		{"timestamp microsecond", column{kind: kindTimestamp, frac: 6},
			time.Date(2024, 3, 1, 10, 30, 0, 1000, time.UTC), time.Date(2024, 3, 1, 10, 30, 0, 0, time.UTC)},
		{"timestamp zone", column{kind: kindTimestamp, frac: 6},
			time.Date(2024, 3, 1, 10, 30, 0, 0, time.UTC), time.Date(2024, 3, 1, 10, 30, 0, 0, time.FixedZone("", 3600))},
		{"json value", column{kind: kindJSON}, `{"a":1}`, `{"a":2}`},
		{"text case", column{kind: kindText}, "abc", "ABC"},
		{"text trailing space", column{kind: kindText}, "ab", "ab "},
	}
	for _, p := range pairs {
		if canonicalize(p.a, p.col) == canonicalize(p.b, p.col) {
			t.Errorf("%s: %#v and %#v canonicalize the same (%q)", p.name, p.a, p.b, canonicalize(p.a, p.col))
		}
	}
}
