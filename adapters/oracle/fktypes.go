package oracle

import (
	"context"
	"fmt"
)

// A foreign key column is reported with the type of the column it
// references, when both are NUMBER. Oracle lets them differ (NUMBER(10)
// referencing NUMBER(19), or anything referencing an identity column, which
// identityType reports as bigint), but mapped separately they can land on
// either side of numeric/bigint, and PostgreSQL has no btree operator between
// those: the foreign key fails with "cannot be implemented" (42804). Every
// value of a validated foreign key is a value of the key it references, so
// the referenced column's type holds them; a NOVALIDATE key holding something
// else fails the read instead of being rounded.

// colRef names a column as Oracle stores it.
type colRef struct{ table, col string }

// numberCol is a NUMBER column's declaration.
type numberCol struct {
	prec, scale int // -1 when not declared
	identity    bool
}

// fkTypes holds the schema's NUMBER-to-NUMBER foreign key links: each
// referencing column's referenced column, and the referenced column's
// declaration.
type fkTypes struct {
	refs map[colRef]colRef
	cols map[colRef]numberCol
}

// numberType is the type and conversion a NUMBER column is reported with.
func numberType(c numberCol) (string, convKind) {
	typ, conv := mapType("NUMBER", 0, c.prec, c.scale)
	if c.identity {
		typ, conv = identityType(typ, conv, c.scale)
	}
	return typ, conv
}

// typeFor reports the type a referencing column takes: that of the column
// at the end of its chain of references (a key that is itself a foreign key
// takes its own referenced type). ok is false for a column that references
// no NUMBER column, or whose references go round in a cycle.
func (f *fkTypes) typeFor(c colRef) (typ string, conv convKind, ok bool) {
	if f == nil {
		return "", 0, false
	}
	seen := map[colRef]bool{c: true}
	ref, ok := f.refs[c]
	if !ok {
		return "", 0, false
	}
	for {
		next, more := f.refs[ref]
		if !more {
			break
		}
		if seen[ref] {
			return "", 0, false
		}
		seen[ref] = true
		ref = next
	}
	decl, ok := f.cols[ref]
	if !ok {
		return "", 0, false
	}
	typ, conv = numberType(decl)
	return typ, conv, true
}

// loadFKTypes reads the schema's enabled foreign keys between NUMBER columns
// within it (keys into other schemas aren't created on the target).
func (s *Source) loadFKTypes(ctx context.Context) (*fkTypes, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT cc.table_name, cc.column_name, rc.table_name, rc.column_name,
	NVL(rt.data_precision, -1), NVL(rt.data_scale, -1), rt.identity_column
FROM all_constraints c
JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
JOIN all_constraints r ON r.owner = c.r_owner AND r.constraint_name = c.r_constraint_name
JOIN all_cons_columns rc ON rc.owner = r.owner AND rc.constraint_name = r.constraint_name AND rc.position = cc.position
JOIN all_tab_columns t ON t.owner = cc.owner AND t.table_name = cc.table_name AND t.column_name = cc.column_name
JOIN all_tab_columns rt ON rt.owner = rc.owner AND rt.table_name = rc.table_name AND rt.column_name = rc.column_name
WHERE c.owner = :1 AND c.constraint_type = 'R' AND c.status = 'ENABLED' AND r.owner = c.owner
	AND t.data_type = 'NUMBER' AND rt.data_type = 'NUMBER'
ORDER BY c.constraint_name, cc.position`, s.owner)
	if err != nil {
		return nil, fmt.Errorf("oracle foreign key types: %w", err)
	}
	defer rows.Close()
	f := &fkTypes{refs: map[colRef]colRef{}, cols: map[colRef]numberCol{}}
	for rows.Next() {
		var from, to colRef
		var decl numberCol
		var identity string
		if err := rows.Scan(&from.table, &from.col, &to.table, &to.col, &decl.prec, &decl.scale, &identity); err != nil {
			return nil, err
		}
		decl.identity = identity == "YES"
		if _, dup := f.refs[from]; !dup { // a column in two keys: the first, by constraint name
			f.refs[from] = to
		}
		f.cols[to] = decl
	}
	return f, rows.Err()
}

// foreignKeyTypes returns the schema's foreign key links, read once per
// table list (refreshTables drops them with the column cache).
func (s *Source) foreignKeyTypes(ctx context.Context) (*fkTypes, error) {
	s.mu.Lock()
	f := s.fkTypes
	s.mu.Unlock()
	if f != nil {
		return f, nil
	}
	f, err := s.loadFKTypes(ctx)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.fkTypes = f
	s.mu.Unlock()
	return f, nil
}
