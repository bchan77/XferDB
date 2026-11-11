// Package changetest is a conformance test for adapters.ChangeApplier, run by
// each target adapter's tests against its own database.
package changetest

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// Run checks target's ChangeApplier against a scratch table it creates and
// drops. read returns the table's rows as id → name, with every value read
// back as text so the check is the same on every database.
func Run(t *testing.T, target adapters.TargetAdapter, read func(table string) map[string]string) {
	t.Helper()
	ctx := context.Background()
	ap, ok := target.(adapters.ChangeApplier)
	if !ok {
		t.Fatalf("%T does not implement adapters.ChangeApplier", target)
	}
	table := fmt.Sprintf("xferdb_changetest_%d", time.Now().UnixNano())
	schema := &adapters.TableSchema{Name: table, Columns: []adapters.ColumnDef{
		{Name: "id", Type: "integer", PrimaryKey: true},
		{Name: "tenant", Type: "varchar(20)", PrimaryKey: true},
		{Name: "name", Type: "varchar(50)", Nullable: true},
		{Name: "note", Type: "text", Nullable: true},
	}}
	if err := target.CreateTable(ctx, schema); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer target.DropTable(ctx, table) //nolint:errcheck
	// Clearing a target that may never have been prepared (a live migration
	// cancelled before its first apply) is fine.
	if err := ap.ClearChangeState(ctx, "never-prepared"); err != nil {
		t.Fatalf("ClearChangeState before PrepareChangeState: %v", err)
	}
	if err := ap.PrepareChangeState(ctx); err != nil {
		t.Fatalf("PrepareChangeState: %v", err)
	}
	if err := ap.PrepareChangeState(ctx); err != nil {
		t.Fatalf("PrepareChangeState twice: %v", err)
	}
	stream := "test-" + table
	defer ap.ClearChangeState(ctx, stream) //nolint:errcheck

	row := func(id int64, tenant, name string) map[string]interface{} {
		// []byte for text, as lib/pq and the MySQL driver hand it over.
		return map[string]interface{}{"id": id, "tenant": []byte(tenant), "name": name, "note": "n"}
	}
	key := func(id int64, tenant string) map[string]interface{} {
		return map[string]interface{}{"id": id, "tenant": []byte(tenant)}
	}
	apply := func(f func(tx adapters.ChangeTx) error) {
		t.Helper()
		tx, err := ap.BeginChanges(ctx)
		if err != nil {
			t.Fatalf("BeginChanges: %v", err)
		}
		if err := f(tx); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	want := func(step string, exp map[string]string) {
		t.Helper()
		got := read(table)
		if fmt.Sprint(sorted(got)) != fmt.Sprint(sorted(exp)) {
			t.Fatalf("%s: rows %v, want %v", step, sorted(got), sorted(exp))
		}
	}

	apply(func(tx adapters.ChangeTx) error {
		if err := tx.Upsert(ctx, table, []map[string]interface{}{row(1, "a", "one"), row(2, "a", "two"), row(3, "b", "three")}); err != nil {
			return err
		}
		return tx.SetApplied(ctx, stream, table, 3)
	})
	want("upsert", map[string]string{"1/a": "one", "2/a": "two", "3/b": "three"})

	apply(func(tx adapters.ChangeTx) error {
		// Upserting an existing key replaces it.
		if err := tx.Upsert(ctx, table, []map[string]interface{}{row(1, "a", "uno")}); err != nil {
			return err
		}
		n, err := tx.Update(ctx, table, []adapters.RowUpdate{
			{Key: key(2, "a"), Set: map[string]interface{}{"name": "dos"}},
			// A key change: the row moves to 4/b.
			{Key: key(3, "b"), Set: map[string]interface{}{"id": int64(4), "name": "four"}},
			// A row that isn't there is skipped.
			{Key: key(99, "z"), Set: map[string]interface{}{"name": "nobody"}},
		})
		if err != nil {
			return err
		}
		if n != 2 {
			return fmt.Errorf("Update found %d rows, want 2", n)
		}
		if err := tx.Delete(ctx, table, []map[string]interface{}{key(2, "a"), key(98, "z")}); err != nil {
			return err
		}
		for k, want := range map[int64]bool{1: true, 2: false, 4: true} {
			tenant := "a"
			if k == 4 {
				tenant = "b"
			}
			if got, err := tx.Exists(ctx, table, key(k, tenant)); err != nil || got != want {
				return fmt.Errorf("Exists(%d) = %v, %v; want %v", k, got, err, want)
			}
		}
		return tx.SetApplied(ctx, stream, table, 7)
	})
	want("update and delete", map[string]string{"1/a": "uno", "4/b": "four"})

	// A rolled-back transaction leaves rows and the position as they were.
	tx, err := ap.BeginChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, table, []map[string]interface{}{key(1, "a")}); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetApplied(ctx, stream, table, 8); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	want("rollback", map[string]string{"1/a": "uno", "4/b": "four"})
	seqs, err := ap.AppliedSeqs(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	if seqs[table] != 7 || len(seqs) != 1 {
		t.Fatalf("AppliedSeqs = %v, want %s: 7", seqs, table)
	}

	apply(func(tx adapters.ChangeTx) error {
		if err := tx.Truncate(ctx, table); err != nil {
			return err
		}
		return tx.Upsert(ctx, table, []map[string]interface{}{row(5, "c", "five")})
	})
	want("truncate", map[string]string{"5/c": "five"})

	// Many keys in one delete are chunked under the parameter limit.
	var rows, keys []map[string]interface{}
	for i := int64(100); i < 1600; i++ {
		rows = append(rows, row(i, "k", "x"))
		keys = append(keys, key(i, "k"))
	}
	apply(func(tx adapters.ChangeTx) error {
		if err := tx.Upsert(ctx, table, rows); err != nil {
			return err
		}
		return tx.Delete(ctx, table, keys)
	})
	want("bulk delete", map[string]string{"5/c": "five"})

	if err := ap.ClearChangeState(ctx, stream); err != nil {
		t.Fatal(err)
	}
	if seqs, _ := ap.AppliedSeqs(ctx, stream); len(seqs) != 0 {
		t.Fatalf("AppliedSeqs after clear = %v", seqs)
	}
}

func sorted(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}
