package analyzer

import (
	"regexp"
	"strings"
)

// typeArgs matches a type's parenthesised arguments: (10), (19,0), (6).
var typeArgs = regexp.MustCompile(`\(([^)]*)\)`)

// typeSynonyms maps the spellings of one type (PostgreSQL's format_type, its
// internal names, SQL standard and short forms) to one name.
var typeSynonyms = map[string]string{
	"character varying":           "varchar",
	"character":                   "char",
	"bpchar":                      "char",
	"int":                         "integer",
	"int4":                        "integer",
	"int8":                        "bigint",
	"int2":                        "smallint",
	"float8":                      "double precision",
	"float4":                      "real",
	"bool":                        "boolean",
	"decimal":                     "numeric",
	"timestamp without time zone": "timestamp",
	"timestamp with time zone":    "timestamptz",
	"time without time zone":      "time",
	"time with time zone":         "timetz",
	"varbit":                      "bit varying",
}

// defaultArgs are arguments a type has when none are written: timestamp(6)
// is timestamp, and char is char(1).
var defaultArgs = map[string]string{
	"timestamp":   "6",
	"timestamptz": "6",
	"time":        "6",
	"timetz":      "6",
	"interval":    "6",
	"char":        "1",
}

// canonicalType is a type name in one spelling, so two spellings of one type
// compare equal: varchar(10) and character varying(10), numeric(19) and
// numeric(19,0), timestamp(6) and timestamp without time zone. Spellings it
// doesn't know are compared as written, ignoring case and spacing.
func canonicalType(t string) string {
	s := strings.ToLower(strings.TrimSpace(t))
	args := ""
	if m := typeArgs.FindStringSubmatch(s); m != nil {
		args = strings.ReplaceAll(m[1], " ", "")
		s = typeArgs.ReplaceAllString(s, " ")
	}
	base := strings.Join(strings.Fields(s), " ")
	if syn, ok := typeSynonyms[base]; ok {
		base = syn
	}
	if base == "numeric" && strings.HasSuffix(args, ",0") {
		args = strings.TrimSuffix(args, ",0") // numeric(19,0) is numeric(19)
	}
	if args == defaultArgs[base] {
		args = ""
	}
	if args == "" {
		return base
	}
	return base + "(" + args + ")"
}

// SameType reports whether two type names name the same type.
func SameType(a, b string) bool {
	return canonicalType(a) == canonicalType(b)
}
