package adapters

import "context"

// Identity columns (#194). A source reports them (ColumnDef.Identity), a
// target creates them as its own kind, and once the rows are copied the
// target's generator is moved past them. Without that, the first insert
// after the switch-over that leaves the key out either fails (no
// generator) or reuses a key a copied row already has.

// IdentityResetter is implemented by targets whose identity columns keep a
// next value of their own (PostgreSQL sequences, MySQL's AUTO_INCREMENT
// counter). SQLite needs nothing: an INTEGER PRIMARY KEY takes the
// largest key plus one.
type IdentityResetter interface {
	// ResetIdentity sets the next value of each identity column the target
	// table has to one past the largest value it holds, or to floors[col]
	// when that is higher (the source's own next value: its generator can be
	// ahead of the rows, past deleted rows or rolled-back inserts). It
	// returns how many columns it set.
	ResetIdentity(ctx context.Context, table string, floors map[string]int64) (int, error)
}

// IdentitySource is implemented by sources that can tell the next value
// their identity columns' generators will hand out, by column name.
type IdentitySource interface {
	IdentityNext(ctx context.Context, table string) (map[string]int64, error)
}
