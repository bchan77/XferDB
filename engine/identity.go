package engine

import (
	"context"
	"fmt"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// ResetIdentities moves each table's identity columns on the target past
// the rows copied into them, and past the source's own next value where
// the source can tell it (#194), so an insert that leaves the key out gets
// a new one once the application switches over. It returns how many
// columns it set; a target without generators of its own (SQLite) sets
// none. An offline migration runs it once the copy is done, a live one at
// cutover (rows keep arriving until then). src may be nil.
func ResetIdentities(ctx context.Context, src adapters.SourceAdapter, tgt adapters.TargetAdapter, tables []string) (int, error) {
	resetter, ok := tgt.(adapters.IdentityResetter)
	if !ok {
		return 0, nil
	}
	var floorsOf adapters.IdentitySource
	if src != nil {
		floorsOf, _ = src.(adapters.IdentitySource)
	}
	set := 0
	for _, table := range tables {
		var floors map[string]int64
		if floorsOf != nil {
			// Best effort: without the source's value the target still
			// goes past every copied key.
			floors, _ = floorsOf.IdentityNext(ctx, table)
		}
		n, err := resetter.ResetIdentity(ctx, table, floors)
		if err != nil {
			return set, fmt.Errorf("identity columns of %s: %w", table, err)
		}
		set += n
	}
	return set, nil
}
