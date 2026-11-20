package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Source implements adapters.SourceAdapter for Oracle. It reads the tables
// of one schema: ConnectionConfig.Schema when set (as typed, or upper-cased
// the way Oracle stores an unquoted name), else the login's own schema.
type Source struct {
	db     *sql.DB
	config adapters.ConnectionConfig
	owner  string // the schema read, as Oracle stores it

	mu     sync.Mutex
	tables map[string]string   // reported table name -> Oracle name
	cols   map[string][]column // reported table name -> its columns
}

func NewSource() adapters.SourceAdapter {
	return &Source{}
}

func (s *Source) Connect(ctx context.Context, config adapters.ConnectionConfig) error {
	config = withDSN(config)
	db, err := sql.Open("oracle", buildURL(config))
	if err != nil {
		return fmt.Errorf("oracle open: %w", err)
	}
	s.db = db
	s.config = config
	if err := s.Ping(ctx); err != nil {
		return err
	}
	owner, err := s.resolveOwner(ctx, config.Schema)
	if err != nil {
		return err
	}
	s.owner = owner
	return nil
}

// resolveOwner finds the schema to read: the configured one, matched as
// typed or as Oracle stores an unquoted name (upper case), else the
// session's current schema.
func (s *Source) resolveOwner(ctx context.Context, schema string) (string, error) {
	if schema == "" {
		var owner string
		err := s.db.QueryRowContext(ctx, `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual`).Scan(&owner)
		if err != nil {
			return "", fmt.Errorf("oracle current schema: %w", err)
		}
		return owner, nil
	}
	for _, candidate := range []string{schema, strings.ToUpper(schema)} {
		var n int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM all_users WHERE username = :1`, candidate).Scan(&n); err != nil {
			return "", fmt.Errorf("oracle schema %s: %w", schema, err)
		}
		if n > 0 {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("oracle schema %q not found (or not visible to this login)", schema)
}

func (s *Source) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Source) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// ListSchemas implements adapters.SchemaLister: the schemas (users) that own
// a table this login can see, minus Oracle's own (ORACLE_MAINTAINED, 12c+),
// the login's own schema first. An Oracle schema with no tables has nothing
// to migrate, so it isn't offered.
func (s *Source) ListSchemas(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT u.username
FROM all_users u
WHERE u.oracle_maintained = 'N'
  AND EXISTS (SELECT 1 FROM all_tables t WHERE t.owner = u.username AND t.dropped = 'NO')
ORDER BY CASE WHEN u.username = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') THEN 0 ELSE 1 END, u.username`)
	if err != nil {
		return nil, fmt.Errorf("oracle list schemas: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// tableQuery lists the schema's ordinary tables: not dropped (the recycle
// bin), temporary, nested, secondary (domain index storage), IOT overflow,
// external, a materialized view's container or a materialized view log.
const tableQuery = `
SELECT t.table_name
FROM all_tables t
WHERE t.owner = :1
  AND t.dropped = 'NO'
  AND t.temporary = 'N'
  AND t.nested = 'NO'
  AND t.secondary = 'N'
  AND (t.iot_type IS NULL OR t.iot_type = 'IOT')
  AND NOT EXISTS (SELECT 1 FROM all_external_tables e WHERE e.owner = t.owner AND e.table_name = t.table_name)
  AND NOT EXISTS (SELECT 1 FROM all_mviews m WHERE m.owner = t.owner AND m.container_name = t.table_name)
  AND NOT EXISTS (SELECT 1 FROM all_mview_logs l WHERE l.log_owner = t.owner AND l.log_table = t.table_name)
ORDER BY t.table_name`

// refreshTables reloads the table list and drops cached columns.
func (s *Source) refreshTables(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, tableQuery, s.owner)
	if err != nil {
		return nil, fmt.Errorf("oracle list tables: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	folded := foldNames(names)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = make(map[string]string, len(names))
	reported := make([]string, 0, len(names))
	for _, n := range names {
		s.tables[folded[n]] = n
		reported = append(reported, folded[n])
	}
	s.cols = map[string][]column{}
	return reported, nil
}

// oracleTable resolves a reported table name to Oracle's.
func (s *Source) oracleTable(ctx context.Context, table string) (string, error) {
	s.mu.Lock()
	name, ok := s.tables[table]
	s.mu.Unlock()
	if ok {
		return name, nil
	}
	// Not loaded yet, or a table created since the list was read.
	if _, err := s.refreshTables(ctx); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if name, ok := s.tables[table]; ok {
		return name, nil
	}
	return "", fmt.Errorf("oracle table %s not found in schema %s", table, s.owner)
}

func (s *Source) from(otable string) string {
	return quote(s.owner) + "." + quote(otable)
}

// columns returns a table's columns, cached until the table list reloads.
func (s *Source) columns(ctx context.Context, table string) ([]column, string, error) {
	otable, err := s.oracleTable(ctx, table)
	if err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	cached, ok := s.cols[table]
	s.mu.Unlock()
	if ok {
		return cached, otable, nil
	}
	// refs_identity: the column is in an enabled foreign key to an identity
	// column of this schema, which identityType may report as bigint, so
	// this column gets the same type: PostgreSQL refuses a foreign key from
	// numeric to bigint (42804).
	rows, err := s.db.QueryContext(ctx, `
SELECT t.column_name, t.data_type, NVL(t.char_length, 0), NVL(t.data_precision, -1), NVL(t.data_scale, -1), t.identity_column,
	CASE WHEN EXISTS (
		SELECT 1
		FROM all_constraints c
		JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
		JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name
		JOIN all_cons_columns rc ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name AND rc.position = cc.position
		JOIN all_tab_columns rt ON rt.owner = rc.owner AND rt.table_name = rc.table_name AND rt.column_name = rc.column_name
		WHERE c.owner = t.owner AND c.table_name = t.table_name AND cc.column_name = t.column_name
			AND c.constraint_type = 'R' AND c.status = 'ENABLED' AND r.owner = c.owner AND rt.identity_column = 'YES'
	) THEN 'YES' ELSE 'NO' END
FROM all_tab_columns t
WHERE t.owner = :1 AND t.table_name = :2
ORDER BY t.column_id`, s.owner, otable)
	if err != nil {
		return nil, "", fmt.Errorf("oracle columns(%s): %w", table, err)
	}
	defer rows.Close()
	var cols []column
	var names []string
	for rows.Next() {
		var c column
		var charLen, prec, scale int
		var identity, refsIdentity string
		if err := rows.Scan(&c.oracleName, &c.dataType, &charLen, &prec, &scale, &identity, &refsIdentity); err != nil {
			return nil, "", err
		}
		c.charLen = charLen
		c.typ, c.conv = mapType(c.dataType, charLen, prec, scale)
		if identity == "YES" || refsIdentity == "YES" {
			c.typ, c.conv = identityType(c.typ, c.conv, scale)
		}
		cols = append(cols, c)
		names = append(names, c.oracleName)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	folded := foldNames(names)
	for i := range cols {
		cols[i].name = folded[cols[i].oracleName]
	}
	s.mu.Lock()
	s.cols[table] = cols
	s.mu.Unlock()
	return cols, otable, nil
}

// forgetColumns drops a table's cached columns, after its definition
// changed.
func (s *Source) forgetColumns(table string) {
	s.mu.Lock()
	delete(s.cols, table)
	s.mu.Unlock()
}

// oracleColumn resolves a reported column name to Oracle's.
func oracleColumn(cols []column, name string) (string, error) {
	for _, c := range cols {
		if c.name == name {
			return c.oracleName, nil
		}
	}
	return "", fmt.Errorf("no column %s", name)
}

func (s *Source) ListTables(ctx context.Context) ([]adapters.TableSchema, error) {
	names, err := s.refreshTables(ctx)
	if err != nil {
		return nil, err
	}
	tables := make([]adapters.TableSchema, 0, len(names))
	for _, n := range names {
		schema, err := s.GetSchema(ctx, n)
		if err != nil {
			return nil, err
		}
		tables = append(tables, *schema)
	}
	return tables, nil
}

func (s *Source) GetSchema(ctx context.Context, table string) (*adapters.TableSchema, error) {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return nil, err
	}
	byOracle := make(map[string]string, len(cols))
	for _, c := range cols {
		byOracle[c.oracleName] = c.name
	}

	meta := map[string]struct {
		nullable bool
		def      sql.NullString
		identity bool
	}{}
	rows, err := s.db.QueryContext(ctx, `
SELECT column_name, nullable, data_default, identity_column
FROM all_tab_columns
WHERE owner = :1 AND table_name = :2`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle column details(%s): %w", table, err)
	}
	for rows.Next() {
		var name, nullable, identity string
		var def sql.NullString
		if err := rows.Scan(&name, &nullable, &def, &identity); err != nil {
			rows.Close()
			return nil, err
		}
		meta[name] = struct {
			nullable bool
			def      sql.NullString
			identity bool
		}{nullable == "Y", def, identity == "YES"}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	pk, err := s.primaryKey(ctx, otable)
	if err != nil {
		return nil, err
	}
	schema := &adapters.TableSchema{Name: table}
	for _, c := range cols {
		m := meta[c.oracleName]
		col := adapters.ColumnDef{Name: c.name, Type: c.typ, Nullable: m.nullable, PrimaryKey: pk[c.oracleName], Identity: m.identity}
		if m.def.Valid {
			if expr, ok := defaultSQL(m.def.String, m.identity); ok {
				col.DefaultValue = &expr
				col.DefaultDialect = adapters.DialectOracle
			}
		}
		schema.Columns = append(schema.Columns, col)
	}

	if schema.Indexes, err = s.indexes(ctx, otable, byOracle); err != nil {
		return nil, err
	}
	if schema.ForeignKeys, err = s.foreignKeys(ctx, otable, byOracle); err != nil {
		return nil, err
	}
	return schema, nil
}

// primaryKey returns the enabled primary key's columns, by Oracle name.
func (s *Source) primaryKey(ctx context.Context, otable string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT cc.column_name
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name
WHERE c.owner = :1 AND c.table_name = :2 AND c.constraint_type = 'P' AND c.status = 'ENABLED'`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle primary key(%s): %w", otable, err)
	}
	defer rows.Close()
	pk := map[string]bool{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		pk[n] = true
	}
	return pk, rows.Err()
}

// indexes returns the plain (B-tree, bitmap, reverse-key) indexes other than
// the primary key's. Function-based, domain and LOB indexes are left out:
// their definitions are Oracle SQL.
func (s *Source) indexes(ctx context.Context, otable string, byOracle map[string]string) ([]adapters.IndexDef, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT i.index_name, i.uniqueness, ic.column_name
FROM all_indexes i
JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name
WHERE i.table_owner = :1 AND i.table_name = :2
  AND i.index_type IN ('NORMAL', 'NORMAL/REV', 'BITMAP')
  AND NOT EXISTS (
    SELECT 1 FROM all_constraints c
    WHERE c.owner = i.table_owner AND c.table_name = i.table_name AND c.constraint_type = 'P'
      AND c.index_owner = i.owner AND c.index_name = i.index_name)
ORDER BY i.index_name, ic.column_position`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle indexes(%s): %w", otable, err)
	}
	defer rows.Close()
	var out []adapters.IndexDef
	for rows.Next() {
		var name, uniqueness, col string
		if err := rows.Scan(&name, &uniqueness, &col); err != nil {
			return nil, err
		}
		reported, ok := byOracle[col]
		if !ok {
			continue // a hidden column (e.g. of a virtual column index)
		}
		if n := len(out); n > 0 && out[n-1].Name == foldName(name) {
			out[n-1].Columns = append(out[n-1].Columns, reported)
			continue
		}
		out = append(out, adapters.IndexDef{Name: foldName(name), Columns: []string{reported}, Unique: uniqueness == "UNIQUE"})
	}
	return out, rows.Err()
}

// foldName folds one unquoted-style name.
func foldName(n string) string {
	if unquotedRe.MatchString(n) {
		return strings.ToLower(n)
	}
	return n
}

// foreignKeys returns the enabled foreign keys to tables in the same schema
// (a key into another schema can't be created on the target, which holds
// only this one).
func (s *Source) foreignKeys(ctx context.Context, otable string, byOracle map[string]string) ([]adapters.ForeignKey, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT c.constraint_name, cc.column_name, r.owner, r.table_name, rc.column_name, c.delete_rule
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name
JOIN all_cons_columns rc ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name AND rc.position = cc.position
WHERE c.owner = :1 AND c.table_name = :2 AND c.constraint_type = 'R' AND c.status = 'ENABLED'
ORDER BY c.constraint_name, cc.position`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle foreign keys(%s): %w", otable, err)
	}
	type fkRow struct{ name, col, refOwner, refTable, refCol, onDelete string }
	var list []fkRow
	for rows.Next() {
		var r fkRow
		if err := rows.Scan(&r.name, &r.col, &r.refOwner, &r.refTable, &r.refCol, &r.onDelete); err != nil {
			rows.Close()
			return nil, err
		}
		list = append(list, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var out []adapters.ForeignKey
	for _, r := range list {
		if r.refOwner != s.owner {
			continue
		}
		refTable := s.reportedTable(r.refTable)
		refCols, _, err := s.columns(ctx, refTable)
		if err != nil {
			continue // not a migrated table (e.g. a materialized view's)
		}
		refCol := r.refCol
		for _, c := range refCols {
			if c.oracleName == r.refCol {
				refCol = c.name
			}
		}
		col := byOracle[r.col]
		name := foldName(r.name)
		if n := len(out); n > 0 && out[n-1].Name == name {
			out[n-1].Columns = append(out[n-1].Columns, col)
			out[n-1].RefColumns = append(out[n-1].RefColumns, refCol)
			continue
		}
		onDelete := r.onDelete
		if onDelete == "NO ACTION" {
			onDelete = ""
		}
		out = append(out, adapters.ForeignKey{Name: name, Columns: []string{col}, RefTable: refTable,
			RefColumns: []string{refCol}, OnDelete: onDelete})
	}
	return out, nil
}

// reportedTable maps an Oracle table name to the name XferDB reports.
func (s *Source) reportedTable(otable string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for reported, o := range s.tables {
		if o == otable {
			return reported
		}
	}
	return foldName(otable)
}

func (s *Source) GetRowCount(ctx context.Context, table string) (int64, error) {
	otable, err := s.oracleTable(ctx, table)
	if err != nil {
		return 0, err
	}
	var n int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+s.from(otable)).Scan(&n); err != nil {
		return 0, fmt.Errorf("count(%s): %w", table, err)
	}
	return n, nil
}

func (s *Source) GetPKRange(ctx context.Context, table, pkColumn string) (int64, int64, error) {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return 0, 0, err
	}
	pk, err := oracleColumn(cols, pkColumn)
	if err != nil {
		return 0, 0, fmt.Errorf("pk range(%s): %w", table, err)
	}
	var lo, hi sql.NullString
	q := fmt.Sprintf(`SELECT TO_CHAR(MIN(%s)), TO_CHAR(MAX(%s)) FROM %s`, quote(pk), quote(pk), s.from(otable))
	if err := s.db.QueryRowContext(ctx, q).Scan(&lo, &hi); err != nil {
		return 0, 0, fmt.Errorf("pk range(%s.%s): %w", table, pkColumn, err)
	}
	if !lo.Valid {
		return 0, 0, fmt.Errorf("table %s is empty", table)
	}
	minV, err := strconv.ParseInt(lo.String, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("pk range(%s.%s): %w", table, pkColumn, err)
	}
	maxV, err := strconv.ParseInt(hi.String, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("pk range(%s.%s): %w", table, pkColumn, err)
	}
	return minV, maxV, nil
}

// selectList is the column list of a table's reads, in catalog order, each
// column read through readExprs.
func selectList(cols []column) string {
	var parts []string
	for _, c := range cols {
		parts = append(parts, readExprs(c)...)
	}
	return strings.Join(parts, ", ")
}

// jsonInline is the longest serialized JSON document read inline, as
// VARCHAR2 (a STANDARD max_string_size database's limit).
const jsonInline = 4000

// readExprs are the select-list expressions a column is read through; most
// columns are read as themselves.
//   - XMLTYPE: as its serialized text. go-ora can't decode an XMLTYPE value
//     (the read fails with "TTC error: received code 1/2 during response
//     reading"); NO INDENT keeps binary-stored XML compact.
//   - JSON: as two expressions, its text as VARCHAR2 when it fits
//     (jsonInline), else as a CLOB. go-ora fetches a native JSON value like a
//     LOB, with round trips of its own for every value: 10,000 rows with two
//     JSON columns took 34s that way and 0.3s as VARCHAR2. Only a document
//     too long for VARCHAR2 is fetched as a CLOB. scanRowsWithKey takes the
//     first that isn't NULL.
func readExprs(c column) []string {
	col := quote(c.oracleName)
	switch c.dataType {
	case "XMLTYPE":
		return []string{fmt.Sprintf("XMLSERIALIZE(CONTENT %s AS CLOB NO INDENT)", col)}
	case "JSON":
		inline := fmt.Sprintf("JSON_SERIALIZE(%s RETURNING VARCHAR2(%d) NULL ON ERROR)", col, jsonInline)
		return []string{inline,
			fmt.Sprintf("CASE WHEN %s IS NULL THEN JSON_SERIALIZE(%s RETURNING CLOB) END", inline, col)}
	}
	return []string{col}
}

func (s *Source) ReadBatch(ctx context.Context, table string, opts adapters.BatchOptions) (*adapters.Batch, error) {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return nil, err
	}
	var (
		query string
		args  []interface{}
	)
	if opts.Keyset {
		return s.readKeyset(ctx, table, opts)
	}
	if opts.PKCol != "" {
		pk, err := oracleColumn(cols, opts.PKCol)
		if err != nil {
			return nil, fmt.Errorf("read batch from %s: %w", table, err)
		}
		op := "<"
		if opts.PKMaxIncl {
			op = "<="
		}
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE %s >= :1 AND %s %s :2 ORDER BY %s FETCH FIRST %d ROWS ONLY`,
			selectList(cols), s.from(otable), quote(pk), quote(pk), op, quote(pk), opts.Limit)
		args = []interface{}{opts.PKMin, opts.PKMax}
	} else {
		// OFFSET paging needs a stable order: the primary key, else ROWID.
		order, err := s.orderBy(ctx, otable)
		if err != nil {
			return nil, err
		}
		query = fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY`,
			selectList(cols), s.from(otable), order, opts.Offset, opts.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	defer rows.Close()
	var records []map[string]interface{}
	if err := scanRows(rows, cols, func(rec map[string]interface{}) error {
		records = append(records, rec)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read batch from %s: %w", table, err)
	}
	batch := &adapters.Batch{Records: records, Size: len(records)}
	if opts.PKCol != "" && len(records) > 0 {
		batch.LastPK = pkInt64(records[len(records)-1][opts.PKCol])
	}
	return batch, nil
}

func (s *Source) orderBy(ctx context.Context, otable string) (string, error) {
	keys, err := s.keyColumns(ctx, otable)
	if err != nil {
		return "", err
	}
	if len(keys) == 0 {
		return "ROWID", nil
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = quote(k)
	}
	return strings.Join(parts, ", "), nil
}

// keyColumns returns the enabled primary key's columns by Oracle name, in
// key order; none for a table without one.
func (s *Source) keyColumns(ctx context.Context, otable string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT cc.column_name
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name AND cc.table_name = c.table_name
WHERE c.owner = :1 AND c.table_name = :2 AND c.constraint_type = 'P' AND c.status = 'ENABLED'
ORDER BY cc.position`, s.owner, otable)
	if err != nil {
		return nil, fmt.Errorf("oracle primary key(%s): %w", otable, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		keys = append(keys, n)
	}
	return keys, rows.Err()
}

// scanRows converts each row to a record keyed by the reported names.
func scanRows(rows *sql.Rows, cols []column, fn func(map[string]interface{}) error) error {
	return scanRowsWithKey(rows, cols, 0, func(rec map[string]interface{}, _ []string) error { return fn(rec) })
}

// scanRowsWithKey is scanRows for a query that selects nkey more columns
// after cols, the key's text forms, which fn gets apart from the record.
func scanRowsWithKey(rows *sql.Rows, cols []column, nkey int, fn func(map[string]interface{}, []string) error) error {
	width := 0 // the select list's expressions for cols (readExprs)
	for _, c := range cols {
		width += len(readExprs(c))
	}
	vals := make([]interface{}, width+nkey)
	ptrs := make([]interface{}, len(vals))
	for rows.Next() {
		for i := range vals {
			vals[i] = nil
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		rec := make(map[string]interface{}, len(cols))
		at := 0
		for _, c := range cols {
			n := len(readExprs(c))
			raw := vals[at]
			for j := 1; raw == nil && j < n; j++ {
				raw = vals[at+j]
			}
			at += n
			v, err := convert(raw, c.conv)
			if err != nil {
				return fmt.Errorf("column %s: %w", c.name, err)
			}
			rec[c.name] = v
		}
		var key []string
		if nkey > 0 {
			key = make([]string, nkey)
			for i := range key {
				key[i] = fmt.Sprint(vals[width+i])
			}
		}
		if err := fn(rec, key); err != nil {
			return err
		}
	}
	return rows.Err()
}

// StreamTable implements adapters.TableStreamer.
func (s *Source) StreamTable(ctx context.Context, table string, fn func(map[string]interface{}) error) error {
	cols, otable, err := s.columns(ctx, table)
	if err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s`, selectList(cols), s.from(otable)))
	if err != nil {
		return fmt.Errorf("read %s: %w", table, err)
	}
	defer rows.Close()
	return scanRows(rows, cols, fn)
}

func (s *Source) CheckPermissions(ctx context.Context) (*adapters.PermissionCheck, error) {
	check := &adapters.PermissionCheck{}
	names, err := s.refreshTables(ctx)
	if err != nil {
		check.Errors = append(check.Errors, fmt.Sprintf("cannot list the tables of schema %s: %v", s.owner, err))
		return check, nil
	}
	check.CanRead = true
	if len(names) > 0 {
		otable, _ := s.oracleTable(ctx, names[0])
		var one int
		err := s.db.QueryRowContext(ctx, `SELECT 1 FROM `+s.from(otable)+` WHERE ROWNUM = 1`).Scan(&one)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			check.CanRead = false
			check.Errors = append(check.Errors, fmt.Sprintf("cannot read %s.%s: %v", s.owner, otable, err))
		}
	}
	return check, nil
}

func (s *Source) GetInfo(ctx context.Context) (*adapters.DatabaseInfo, error) {
	info := &adapters.DatabaseInfo{Type: "Oracle", Schema: s.owner}
	var version string
	if err := s.db.QueryRowContext(ctx, `SELECT version_full FROM product_component_version WHERE product LIKE 'Oracle%' AND ROWNUM = 1`).Scan(&version); err == nil {
		info.Version = version
	} else if err := s.db.QueryRowContext(ctx, `SELECT version FROM product_component_version WHERE ROWNUM = 1`).Scan(&version); err == nil {
		info.Version = version
	}
	var service string
	if err := s.db.QueryRowContext(ctx, `SELECT SYS_CONTEXT('USERENV', 'SERVICE_NAME') FROM dual`).Scan(&service); err == nil {
		info.Database = service
	}
	if s.config.Host != "" {
		port := s.config.Port
		if port == 0 {
			port = defaultPort
		}
		info.Host = fmt.Sprintf("%s:%d", s.config.Host, port)
	}
	if names, err := s.refreshTables(ctx); err == nil {
		info.Tables = len(names)
	}
	// Segment sizes: dba_segments needs a DBA grant; a login reading its own
	// schema can use user_segments.
	var size sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT SUM(bytes) FROM dba_segments WHERE owner = :1`, s.owner).Scan(&size); err != nil {
		var me string
		if s.db.QueryRowContext(ctx, `SELECT USER FROM dual`).Scan(&me) == nil && me == s.owner {
			_ = s.db.QueryRowContext(ctx, `SELECT SUM(bytes) FROM user_segments`).Scan(&size)
		}
	}
	if size.Valid {
		info.SizeBytes = size.Int64
		info.SizeHuman = humanSize(size.Int64)
	}
	var proto string
	if err := s.db.QueryRowContext(ctx, `SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') FROM dual`).Scan(&proto); err == nil {
		if strings.EqualFold(proto, "tcps") {
			info.SSL = "enabled"
		} else {
			info.SSL = "disabled"
		}
	}
	return info, nil
}
