package adapters

import (
	"regexp"
	"strings"
)

// Column defaults cross dialects here. A source reports a column's default
// as its own SQL (ColumnDef.DefaultValue) and says which dialect that is
// (ColumnDef.DefaultDialect). A target of the same dialect uses it as is; a
// different one gets it translated by TranslateDefault, or no default at all
// when it has no equivalent. Without this, PostgreSQL's now(), 'x'::text and
// nextval(...) went verbatim into SQLite and MySQL DDL and failed CREATE
// TABLE, so the migration failed before copying a row.
//
// Dropping a default never changes the data a migration copies, since every
// row carries its own value; it only changes what a later INSERT that leaves
// the column out gets.

// Dialects of a column default.
const (
	DialectPostgres = "postgres"
	DialectMySQL    = "mysql"
	DialectSQLite   = "sqlite"
)

// defaultKind is what a default means, whatever its dialect spelled.
type defaultKind int

const (
	defaultUnknown defaultKind = iota // an expression with no known equivalent
	defaultNull                       // NULL: the same as no default
	defaultNumber                     // a numeric literal
	defaultString                     // a string literal
	defaultBool                       // true or false
	defaultNow                        // the current timestamp
	defaultToday                      // the current date
	defaultUUID                       // a random UUID
)

type parsedDefault struct {
	kind  defaultKind
	value string // the number, the unquoted string, or "true"/"false"
}

var (
	numberRe = regexp.MustCompile(`^[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$`)
	// A PostgreSQL cast suffix: 'x'::text, 'x'::character varying(20), 5::bigint.
	pgCastRe = regexp.MustCompile(`^(.*?)(::[a-z_][a-z0-9_ ]*(\([0-9, ]*\))?(\[\])?)+$`)
	// now(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(6), transaction_timestamp() ...
	nowRe        = regexp.MustCompile(`^(now\(\)|current_timestamp(\(\d*\))?|localtimestamp(\(\d*\))?|transaction_timestamp\(\)|statement_timestamp\(\)|clock_timestamp\(\)|datetime\('now'\)|strftime\('%[^']*',\s*'now'\))$`)
	todayRe      = regexp.MustCompile(`^(current_date|curdate\(\)|current_date\(\)|date\('now'\)|'now'::date|\('now'::text\)::date)$`)
	introducerRe = regexp.MustCompile(`^_[A-Za-z0-9]+\s*('.*')$`)
	uuidRe       = regexp.MustCompile(`^(gen_random_uuid\(\)|uuid_generate_v4\(\)|uuid\(\))$`)
)

// parseDefault reads a default in its source dialect.
func parseDefault(expr, dialect string) parsedDefault {
	s := strings.TrimSpace(expr)
	// Expression defaults come wrapped in parentheses: MySQL's (uuid()),
	// SQLite's (datetime('now')), PostgreSQL's (-1).
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' && balanced(s[1:len(s)-1], dialect) {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if dialect == DialectPostgres {
		if m := pgCastRe.FindStringSubmatch(s); m != nil && m[1] != "" {
			bare := strings.TrimSpace(m[1])
			for len(bare) >= 2 && bare[0] == '(' && bare[len(bare)-1] == ')' && balanced(bare[1:len(bare)-1], dialect) {
				bare = strings.TrimSpace(bare[1 : len(bare)-1])
			}
			if strings.EqualFold(s, "('now'::text)::date") || strings.EqualFold(s, "'now'::date") {
				return parsedDefault{kind: defaultToday}
			}
			s = bare
		}
	}
	if dialect == DialectMySQL {
		// A string expression default carries a charset introducer:
		// _utf8mb4'it\'s'.
		if m := introducerRe.FindStringSubmatch(s); m != nil {
			s = m[1]
		}
	}
	lower := strings.ToLower(s)
	switch {
	case lower == "null":
		return parsedDefault{kind: defaultNull}
	case lower == "true" || lower == "false":
		return parsedDefault{kind: defaultBool, value: lower}
	case numberRe.MatchString(s):
		return parsedDefault{kind: defaultNumber, value: s}
	case len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'':
		if v, ok := unquote(s, dialect); ok {
			return parsedDefault{kind: defaultString, value: v}
		}
	case nowRe.MatchString(lower):
		return parsedDefault{kind: defaultNow}
	case todayRe.MatchString(lower):
		return parsedDefault{kind: defaultToday}
	case uuidRe.MatchString(lower):
		return parsedDefault{kind: defaultUUID}
	}
	return parsedDefault{kind: defaultUnknown}
}

// balanced reports whether s's parentheses pair up outside its string
// literals, so stripping an outer pair from "(a) + (b)" is refused.
func balanced(s, dialect string) bool {
	depth := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case inQuote && c == '\\' && dialect == DialectMySQL:
			i++ // an escaped character, perhaps a quote
		case c == '\'':
			inQuote = !inQuote
		case inQuote:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && !inQuote
}

// unquote reads a single-quoted SQL string literal: ” is a quote, and in
// MySQL a backslash escapes the next character.
func unquote(s, dialect string) (string, bool) {
	body := s[1 : len(s)-1]
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == '\'':
			if i+1 < len(body) && body[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return "", false // a quote ends the literal early: not one literal
		case c == '\\' && dialect == DialectMySQL && i+1 < len(body):
			i++
			switch body[i] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '0':
				b.WriteByte(0)
			default:
				b.WriteByte(body[i])
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}

// quoteLiteral renders s as a single-quoted string literal for dialect.
func quoteLiteral(s, dialect string) string {
	s = strings.ReplaceAll(s, "'", "''")
	if dialect == DialectMySQL {
		s = strings.ReplaceAll(s, `\`, `\\`)
	}
	return "'" + s + "'"
}

// TranslateDefault returns the DEFAULT expression for column c on a target
// of dialect target, whose column type there is colType, and false when
// the column should get no default. A default in the target's own dialect,
// or of unknown dialect, is returned as the source wrote it.
func TranslateDefault(c ColumnDef, target, colType string) (string, bool) {
	if c.DefaultValue == nil {
		return "", false
	}
	if c.DefaultDialect == "" || c.DefaultDialect == target {
		return *c.DefaultValue, true
	}
	d := parseDefault(*c.DefaultValue, c.DefaultDialect)
	t := strings.ToLower(strings.TrimSpace(colType))
	isBool := t == "boolean" || t == "bool" || t == "tinyint(1)"
	switch d.kind {
	case defaultNumber:
		if target == DialectPostgres && isBool {
			// MySQL and SQLite booleans are 0/1 integers; PostgreSQL's
			// boolean won't take an integer default.
			switch d.value {
			case "0":
				return "false", true
			case "1":
				return "true", true
			}
			return "", false
		}
		return d.value, true
	case defaultBool:
		if target == DialectPostgres {
			return d.value, true
		}
		if d.value == "true" {
			return "1", true
		}
		return "0", true
	case defaultString:
		lit := quoteLiteral(d.value, target)
		if target == DialectMySQL && mysqlNeedsExpressionDefault(t) {
			// TEXT, BLOB and JSON columns take only expression defaults
			// (MySQL 8.0.13+), a literal in parentheses.
			return "(" + lit + ")", true
		}
		return lit, true
	case defaultNow:
		switch target {
		case DialectMySQL:
			// CURRENT_TIMESTAMP only defaults a DATETIME or TIMESTAMP, and
			// must carry the column's fractional-second precision.
			if !strings.HasPrefix(t, "datetime") && !strings.HasPrefix(t, "timestamp") {
				return "", false
			}
			if i := strings.Index(t, "("); i >= 0 {
				return "CURRENT_TIMESTAMP" + t[i:], true
			}
			return "CURRENT_TIMESTAMP", true
		default:
			return "CURRENT_TIMESTAMP", true
		}
	case defaultToday:
		if target == DialectMySQL {
			return "(CURRENT_DATE)", true
		}
		return "CURRENT_DATE", true
	case defaultUUID:
		switch target {
		case DialectPostgres:
			return "gen_random_uuid()", true
		case DialectMySQL:
			return "(uuid())", true
		}
	}
	// NULL, a sequence (the target has its own key generation, or none),
	// or an expression this can't carry over.
	return "", false
}

func mysqlNeedsExpressionDefault(t string) bool {
	for _, p := range []string{"text", "tinytext", "mediumtext", "longtext", "blob", "tinyblob", "mediumblob", "longblob", "json"} {
		if t == p {
			return true
		}
	}
	return false
}
