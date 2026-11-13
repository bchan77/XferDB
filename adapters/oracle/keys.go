package oracle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Primary keys as text (#195). A live migration's snapshot pages a table by
// its whole primary key (adapters.KeysetSource), and its stream reads rows
// back by the key values LogMiner reports. Both carry a key as text, in one
// fixed spelling per type, which a session with fixedNLS writes and reads:
// numbers as TO_CHAR prints them with a '.' decimal point, DATE as
// 'YYYY-MM-DD HH24:MI:SS', TIMESTAMP with nine fractional digits, RAW as
// hex, character types as they are. Other key types can't be used live.

const (
	dateFormat      = "YYYY-MM-DD HH24:MI:SS"
	timestampFormat = "YYYY-MM-DD HH24:MI:SS.FF9"
)

// fixedNLS makes a session spell numbers and dates the way keys are kept.
// DBMS_LOGMNR.MINE_VALUE formats by them too.
var fixedNLS = []string{
	`ALTER SESSION SET NLS_DATE_FORMAT = '` + dateFormat + `'`,
	`ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '` + timestampFormat + `'`,
	`ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'`,
	`ALTER SESSION SET NLS_SORT = BINARY`,
	`ALTER SESSION SET NLS_COMP = BINARY`,
}

// session returns a connection with fixedNLS set. Close it when done.
func (s *Source) session(ctx context.Context) (*sql.Conn, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	for _, stmt := range fixedNLS {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			conn.Close()
			return nil, fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return conn, nil
}

// keyKind is how a key column is written as text.
type keyKind int

const (
	keyUnsupported keyKind = iota
	keyNumber
	keyString
	keyChar // blank-padded to its length
	keyDate
	keyTimestamp
	keyRaw
)

func kindOfKey(dataType string) keyKind {
	dt := strings.ToUpper(dataType)
	switch {
	case dt == "NUMBER", dt == "FLOAT", dt == "BINARY_FLOAT", dt == "BINARY_DOUBLE":
		return keyNumber
	case dt == "VARCHAR2", dt == "NVARCHAR2", dt == "VARCHAR":
		return keyString
	case dt == "CHAR", dt == "NCHAR":
		return keyChar
	case dt == "DATE":
		return keyDate
	case strings.HasPrefix(dt, "TIMESTAMP") && !strings.Contains(dt, "TIME ZONE"):
		return keyTimestamp
	case dt == "RAW":
		return keyRaw
	}
	return keyUnsupported
}

// keyText is the SQL for a key column's text form.
func keyText(c column) string {
	q := quote(c.oracleName)
	switch kindOfKey(c.dataType) {
	case keyNumber:
		return "TO_CHAR(" + q + ", 'TM9')"
	case keyDate:
		return "TO_CHAR(" + q + ", '" + dateFormat + "')"
	case keyTimestamp:
		return "TO_CHAR(" + q + ", '" + timestampFormat + "')"
	case keyRaw:
		return "RAWTOHEX(" + q + ")"
	}
	return q
}

// keyParam is the SQL that turns bind parameter :n, a key's text form,
// back into the column's type.
func keyParam(c column, n int) string {
	p := ":" + strconv.Itoa(n)
	switch kindOfKey(c.dataType) {
	case keyNumber:
		return "TO_NUMBER(" + p + ")"
	case keyDate:
		return "TO_DATE(" + p + ", '" + dateFormat + "')"
	case keyTimestamp:
		return "TO_TIMESTAMP(" + p + ", '" + timestampFormat + "')"
	case keyRaw:
		return "HEXTORAW(" + p + ")"
	}
	return p
}

// keyValue turns a key's text form into the value ReadBatch returns for
// the column, for a delete, whose row can't be read back.
func keyValue(c column, text string) (interface{}, error) {
	switch kindOfKey(c.dataType) {
	case keyNumber:
		return convert(text, c.conv)
	case keyChar:
		if n := c.charLen - len([]rune(text)); n > 0 {
			return text + strings.Repeat(" ", n), nil
		}
		return text, nil
	case keyDate:
		return time.ParseInLocation("2006-01-02 15:04:05", text, time.UTC)
	case keyTimestamp:
		return time.ParseInLocation("2006-01-02 15:04:05.999999999", text, time.UTC)
	case keyRaw:
		return hex.DecodeString(text)
	}
	return text, nil
}

// tableKey is a table's primary key columns, in key order.
func (s *Source) tableKey(ctx context.Context, table string) ([]column, []column, string, error) {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return nil, nil, "", err
	}
	names, err := s.keyColumns(ctx, otable)
	if err != nil {
		return nil, nil, "", err
	}
	key := make([]column, 0, len(names))
	for _, n := range names {
		for _, c := range cols {
			if c.oracleName == n {
				key = append(key, c)
			}
		}
	}
	return cols, key, otable, nil
}

// KeyColumns implements adapters.KeysetSource.
func (s *Source) KeyColumns(ctx context.Context, table string) ([]string, error) {
	_, key, _, err := s.tableKey(ctx, table)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(key))
	for i, c := range key {
		names[i] = c.name
	}
	return names, nil
}

// MaxKey implements adapters.KeysetSource.
func (s *Source) MaxKey(ctx context.Context, table string) ([]string, error) {
	_, key, otable, err := s.tableKey(ctx, table)
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("%s has no primary key", table)
	}
	texts, desc := make([]string, len(key)), make([]string, len(key))
	for i, c := range key {
		texts[i] = keyText(c)
		desc[i] = quote(c.oracleName) + " DESC"
	}
	conn, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	out := make([]string, len(key))
	ptrs := make([]interface{}, len(key))
	for i := range out {
		ptrs[i] = &out[i]
	}
	err = conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s FETCH FIRST 1 ROWS ONLY`,
		strings.Join(texts, ", "), s.from(otable), strings.Join(desc, ", "))).Scan(ptrs...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("largest key of %s: %w", table, err)
	}
	return out, nil
}

// keyCompare is the condition key (op) bound, for op "<=" or ">", spelled
// out column by column, since Oracle has no row-value comparison:
// (a, b) > (x, y) is a > x OR (a = x AND b > y). go-ora binds placeholders
// by position, so each use of a value gets its own, appended to args.
func keyCompare(key []column, op string, bound []string, args *[]interface{}) string {
	strict := strings.TrimSuffix(op, "=")
	param := func(i int) string {
		*args = append(*args, bound[i])
		return keyParam(key[i], len(*args))
	}
	var ors []string
	for i := range key {
		var ands []string
		for j := 0; j < i; j++ {
			ands = append(ands, quote(key[j].oracleName)+" = "+param(j))
		}
		last := strict
		if i == len(key)-1 {
			last = op
		}
		ands = append(ands, quote(key[i].oracleName)+" "+last+" "+param(i))
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	return "(" + strings.Join(ors, " OR ") + ")"
}

// readKeyset reads the rows after opts.KeyAfter up to opts.KeyUpTo, in key
// order (adapters.BatchOptions.Keyset).
func (s *Source) readKeyset(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	cols, key, otable, err := s.tableKey(ctx, table)
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("%s has no primary key to page by", table)
	}
	if opts.KeyUpTo == nil {
		return &adapters.Batch{}, nil // an empty table
	}
	if len(opts.KeyUpTo) != len(key) || (opts.KeyAfter != nil && len(opts.KeyAfter) != len(key)) {
		return nil, fmt.Errorf("%s: a key bound doesn't have the primary key's %d columns", table, len(key))
	}
	var args []interface{}
	conds := []string{keyCompare(key, "<=", opts.KeyUpTo, &args)}
	if opts.KeyAfter != nil {
		conds = append(conds, keyCompare(key, ">", opts.KeyAfter, &args))
	}
	texts, order := make([]string, len(key)), make([]string, len(key))
	for i, c := range key {
		texts[i] = keyText(c)
		order[i] = quote(c.oracleName)
	}
	query := fmt.Sprintf(`SELECT %s, %s FROM %s WHERE %s ORDER BY %s FETCH FIRST %d ROWS ONLY`,
		selectList(cols), strings.Join(texts, ", "), s.from(otable), strings.Join(conds, " AND "),
		strings.Join(order, ", "), opts.Limit)
	conn, err := s.session(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	defer rows.Close()
	batch := &adapters.Batch{}
	var last []string
	err = scanRowsWithKey(rows, cols, len(key), func(rec map[string]interface{}, k []string) error {
		batch.Records = append(batch.Records, rec)
		last = k
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	batch.Size = len(batch.Records)
	if last != nil {
		batch.LastKey = adapters.EncodeKey(last)
	}
	return batch, nil
}

func nullStrings(texts []string) []sql.NullString {
	out := make([]sql.NullString, len(texts))
	for i, s := range texts {
		out[i] = sql.NullString{String: s, Valid: true}
	}
	return out
}

// lobLocatorKey reads a row's key texts from the SQL_REDO of a
// SEL_LOB_LOCATOR, the operation that starts a change to a LOB stored out
// of line:
//
//	select "NOTE" into loc_c from "OWNER"."T" where "ID" = '2100' and
//	"D" = TO_DATE('2025-02-03 04:05:06', 'YYYY-MM-DD HH24:MI:SS') for update;
//
// Mined on a fixedNLS session, each value is spelled as keyText spells it
// (RAW in lower case, which normKey allows for). Every key column must be
// there.
func lobLocatorKey(owner string, info *liveInfo, redo string) ([]string, error) {
	fail := func(why string) ([]string, error) {
		return nil, fmt.Errorf("%s: can't read the changed row's key from LogMiner's LOB operation (%s): %.300q", info.name, why, redo)
	}
	from := " from " + quote(owner) + "." + quote(info.otable) + " where "
	i := strings.Index(redo, from)
	if i < 0 {
		return fail("no WHERE clause")
	}
	rest := redo[i+len(from):]
	vals := map[string]string{}
	// literal reads a quoted string at the start of rest.
	literal := func(open, close byte) (string, bool) {
		if rest == "" || rest[0] != open {
			return "", false
		}
		var b strings.Builder
		for j := 1; j < len(rest); j++ {
			if rest[j] != close {
				b.WriteByte(rest[j])
				continue
			}
			if j+1 < len(rest) && rest[j+1] == close {
				b.WriteByte(close)
				j++
				continue
			}
			rest = rest[j+1:]
			return b.String(), true
		}
		return "", false
	}
	for {
		name, ok := literal('"', '"')
		if !ok {
			return fail("expected a column name")
		}
		if !strings.HasPrefix(rest, " = ") {
			return fail("expected = after " + name)
		}
		rest = rest[3:]
		// A value is a string, or one wrapped in TO_DATE, TO_TIMESTAMP or
		// HEXTORAW, whose other arguments (a format) are left out.
		fn := ""
		if j := strings.IndexByte(rest, '('); j > 0 && rest[0] != '\'' {
			fn, rest = rest[:j], rest[j+1:]
			switch fn {
			case "TO_DATE", "TO_TIMESTAMP", "HEXTORAW":
			default:
				return fail("unexpected " + fn)
			}
		}
		v, ok := literal('\'', '\'')
		if !ok {
			return fail("expected a value for " + name)
		}
		if fn != "" {
			for rest != "" && rest[0] != ')' {
				if rest[0] == '\'' {
					if _, ok := literal('\'', '\''); !ok {
						return fail("unterminated argument of " + fn)
					}
					continue
				}
				rest = rest[1:]
			}
			if rest == "" {
				return fail("unterminated " + fn)
			}
			rest = rest[1:]
		}
		vals[name] = v
		if strings.HasPrefix(rest, " and ") {
			rest = rest[5:]
			continue
		}
		if !strings.HasPrefix(rest, " for update") {
			return fail("expected AND or FOR UPDATE after " + name)
		}
		break
	}
	texts := make([]string, len(info.key))
	for k, c := range info.key {
		v, ok := vals[c.oracleName]
		if !ok {
			return fail("no value for key column " + c.oracleName)
		}
		texts[k] = v
	}
	return texts, nil
}
