// Package oracle is XferDB's Oracle source adapter (#190): Oracle 12c and
// later (tested on 21c XE and 23ai Free), through the pure-Go go-ora driver,
// so no Oracle Instant Client is needed.
//
// Oracle is a source only. Its column types are reported in the PostgreSQL
// spelling every target already maps (NUMBER(10) as bigint, VARCHAR2(20 CHAR)
// as varchar(20), DATE as timestamp(0), ...), the way the MongoDB source
// reports its inferred types, and ReadBatch returns values to match. Oracle's
// unquoted, upper-case names are folded to lower case (CUSTOMERS.ORDER_ID is
// customers.order_id on the target); a quoted mixed-case name keeps its case.
package oracle

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	_ "github.com/sijms/go-ora/v2" // registers the "oracle" database/sql driver
)

const defaultPort = 1521

// withDSN fills the config's empty fields from an oracle:// DSN
// (oracle://user:pw@host:1521/SERVICE?schema=HR&SSL=true): a DSN on its own
// (a config file, the CLI's --source) works like the separate fields, and a
// schema can ride along in it. Other query options are left in the DSN for
// buildURL to pass to go-ora.
func withDSN(cfg adapters.ConnectionConfig) adapters.ConnectionConfig {
	if !strings.HasPrefix(strings.ToLower(cfg.DSN), "oracle://") {
		return cfg
	}
	u, err := url.Parse(cfg.DSN)
	if err != nil {
		return cfg
	}
	if cfg.Host == "" {
		cfg.Host = u.Hostname()
	}
	if cfg.Port == 0 {
		cfg.Port, _ = strconv.Atoi(u.Port())
	}
	if cfg.Database == "" {
		cfg.Database = strings.TrimPrefix(u.Path, "/")
	}
	if u.User != nil {
		if cfg.Username == "" {
			cfg.Username = u.User.Username()
		}
		if pw, ok := u.User.Password(); ok && cfg.Password == "" {
			cfg.Password = pw
		}
	}
	if cfg.Schema == "" {
		cfg.Schema = u.Query().Get("schema")
	}
	return cfg
}

// buildURL makes a go-ora URL from the connection config (after withDSN):
// host, port, service name (Database) and credentials, the DSN's query
// options other than schema, and TLS when SSLMode asks for it.
func buildURL(cfg adapters.ConnectionConfig) string {
	port := cfg.Port
	if port == 0 {
		port = defaultPort
	}
	q := url.Values{}
	if cfg.DSN != "" {
		if u, err := url.Parse(cfg.DSN); err == nil {
			q = u.Query()
		}
	}
	q.Del("schema") // XferDB's, not a go-ora option
	switch strings.ToLower(cfg.SSLMode) {
	case "", "disable", "disabled", "false":
	case "verify-ca", "verify-full":
		q.Set("SSL", "true")
	default: // require, prefer, true
		q.Set("SSL", "true")
		q.Set("SSL VERIFY", "false")
	}
	// Built with net/url, not go-ora's BuildUrl, which doesn't escape the
	// password: one with an @ or / in it made a URL go-ora then misread.
	u := url.URL{
		Scheme:   "oracle",
		User:     url.UserPassword(cfg.Username, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, strconv.Itoa(port)),
		Path:     "/" + cfg.Database,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// quote returns a double-quoted Oracle identifier. Oracle identifiers can't
// contain a double quote, so there is nothing to escape.
func quote(s string) string {
	return `"` + s + `"`
}

// unquotedRe matches a name Oracle stores for an unquoted identifier: upper
// case, starting with a letter.
var unquotedRe = regexp.MustCompile(`^[A-Z][A-Z0-9_$#]*$`)

// foldNames maps each Oracle name to the name XferDB reports: lower case for
// an unquoted-style name, unless that would collide with another name in the
// same set, in which case both keep their case.
func foldNames(names []string) map[string]string {
	count := map[string]int{}
	for _, n := range names {
		count[strings.ToLower(n)]++
	}
	out := make(map[string]string, len(names))
	for _, n := range names {
		if unquotedRe.MatchString(n) && count[strings.ToLower(n)] == 1 {
			out[n] = strings.ToLower(n)
		} else {
			out[n] = n
		}
	}
	return out
}

// column is one column as the catalog describes it, plus what ReadBatch
// needs to convert its values.
type column struct {
	oracleName string // as Oracle stores it, for SQL
	name       string // as reported (folded)
	dataType   string // ALL_TAB_COLUMNS.DATA_TYPE
	typ        string // the PostgreSQL-spelled type XferDB reports
	conv       convKind
}

// convKind is how ReadBatch turns a go-ora value into the value a target
// stores for the reported type.
type convKind int

const (
	convNone     convKind = iota
	convInt               // NUMBER(p,0), p <= 18: decimal string -> int64
	convFloat             // FLOAT(b): decimal string -> float64
	convJSON              // JSON: bytes -> string
	convBool              // BOOLEAN (23ai): "1"/"0" -> bool
	convInterval          // INTERVAL ...: Oracle text -> ISO 8601 duration
)

// mapType turns a column's catalog description into the PostgreSQL-spelled
// type XferDB reports, and the conversion its values need. precision and
// scale are -1 when NULL in the catalog. An unsupported type (object types,
// VARRAY, nested tables, SDO_GEOMETRY, BFILE) is reported as such, so the
// target's CREATE TABLE names it when it fails; deselect the table.
func mapType(dataType string, charLength, precision, scale int) (string, convKind) {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	switch {
	case dt == "NUMBER":
		switch {
		case precision < 0 && scale < 0:
			return "numeric", convNone // NUMBER: any precision and scale
		case scale == 0 && precision < 0:
			return "numeric(38)", convNone // INTEGER: NUMBER(*,0)
		case scale == 0 && precision <= 4:
			return "smallint", convInt
		case scale == 0 && precision <= 9:
			return "integer", convInt
		case scale == 0 && precision <= 18:
			return "bigint", convInt
		case scale == 0:
			return fmt.Sprintf("numeric(%d)", precision), convNone
		case scale > 0 && precision >= scale:
			return fmt.Sprintf("numeric(%d,%d)", precision, scale), convNone
		default:
			// NUMBER(p,-s) rounds to tens or hundreds; NUMBER(p,s) with s > p
			// holds only small fractions. Neither has a portable equivalent.
			return "numeric", convNone
		}
	case dt == "FLOAT":
		return "double precision", convFloat
	case dt == "BINARY_FLOAT":
		return "real", convNone
	case dt == "BINARY_DOUBLE":
		return "double precision", convNone
	case dt == "VARCHAR2", dt == "NVARCHAR2", dt == "VARCHAR":
		if charLength <= 0 {
			return "text", convNone
		}
		return fmt.Sprintf("varchar(%d)", charLength), convNone
	case dt == "CHAR", dt == "NCHAR":
		if charLength <= 0 {
			charLength = 1
		}
		return fmt.Sprintf("char(%d)", charLength), convNone
	case dt == "CLOB", dt == "NCLOB", dt == "LONG":
		return "text", convNone
	case dt == "BLOB", dt == "RAW", dt == "LONG RAW":
		return "bytea", convNone
	case dt == "DATE":
		return "timestamp(0)", convNone // an Oracle DATE has a time of day
	case strings.HasPrefix(dt, "TIMESTAMP"):
		p := 6
		if scale >= 0 {
			p = scale
		}
		if strings.Contains(dt, "TIME ZONE") {
			return fmt.Sprintf("timestamp(%d) with time zone", p), convNone
		}
		return fmt.Sprintf("timestamp(%d)", p), convNone
	case strings.HasPrefix(dt, "INTERVAL"):
		return "interval", convInterval
	case dt == "JSON":
		return "jsonb", convJSON
	case dt == "XMLTYPE":
		return "xml", convNone
	case dt == "BOOLEAN":
		return "boolean", convBool
	case dt == "ROWID":
		return "varchar(18)", convNone
	case dt == "UROWID":
		return "varchar(4000)", convNone
	}
	return "unsupported Oracle type " + dt, convNone
}

// convert turns a value go-ora scanned into the value the reported type
// stores. go-ora returns NUMBER as its exact decimal text, LOBs as string or
// []byte, DATE and TIMESTAMP as time.Time, and ” as NULL (Oracle has no
// empty string).
func convert(v interface{}, k convKind) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch k {
	case convInt:
		if s, ok := v.(string); ok {
			return strconv.ParseInt(s, 10, 64)
		}
	case convFloat:
		if s, ok := v.(string); ok {
			return strconv.ParseFloat(s, 64)
		}
	case convJSON:
		if b, ok := v.([]byte); ok {
			return string(b), nil
		}
	case convBool:
		switch x := v.(type) {
		case string:
			return x != "0", nil
		case int64:
			return x != 0, nil
		case bool:
			return x, nil
		}
	case convInterval:
		if s, ok := v.(string); ok {
			return isoInterval(s)
		}
	}
	return v, nil
}

var (
	ymRe = regexp.MustCompile(`^([+-])?(\d+)-(\d+)$`)
	dsRe = regexp.MustCompile(`^([+-])?(\d+) (\d+):(\d+):(\d+)(\.\d+)?$`)
)

// isoInterval turns Oracle's interval text (+01-02, -03 04:05:06.789000)
// into an ISO 8601 duration (P1Y2M, -P3DT4H5M6.789S), which PostgreSQL's
// interval reads and other targets keep as text.
func isoInterval(s string) (string, error) {
	s = strings.TrimSpace(s)
	if m := ymRe.FindStringSubmatch(s); m != nil {
		y, _ := strconv.Atoi(m[2])
		mo, _ := strconv.Atoi(m[3])
		return sign(m[1]) + fmt.Sprintf("P%dY%dM", y, mo), nil
	}
	if m := dsRe.FindStringSubmatch(s); m != nil {
		d, _ := strconv.Atoi(m[2])
		h, _ := strconv.Atoi(m[3])
		mi, _ := strconv.Atoi(m[4])
		sec := strings.TrimLeft(m[5], "0")
		if sec == "" {
			sec = "0"
		}
		if frac := strings.TrimRight(m[6], "0"); frac != "." && frac != "" {
			sec += frac
		}
		return sign(m[1]) + fmt.Sprintf("P%dDT%dH%dM%sS", d, h, mi, sec), nil
	}
	return "", fmt.Errorf("unrecognised Oracle interval %q", s)
}

func sign(s string) string {
	if s == "-" {
		return "-"
	}
	return ""
}

// defaultSQL cleans ALL_TAB_COLUMNS.DATA_DEFAULT (a LONG, often with a
// trailing newline) into the expression XferDB passes on in the oracle
// dialect. Identity columns' sequence defaults are left out.
func defaultSQL(def string, identity bool) (string, bool) {
	d := strings.TrimSpace(def)
	if identity || d == "" || strings.EqualFold(d, "NULL") {
		return "", false
	}
	return d, true
}

// pkInt64 reads an integer key value as ReadBatch returns it.
func pkInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case float64:
		return int64(x)
	}
	return 0
}

// humanSize formats bytes as a human-readable string.
func humanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
