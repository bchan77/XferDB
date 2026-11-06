package state

import (
	"context"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// A table the source no longer has drops out of the project's table list,
// with its old results; the others keep theirs.
func TestPruneProjectTables(t *testing.T) {
	db, cleanup := openTestDB(t)
	defer cleanup()
	ctx := context.Background()
	if err := db.CreateProject(ctx, &adapters.Project{ID: "p", Name: "p"}); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterProjectTables(ctx, "p", []string{"commerce.orders", "loadtest.orders"}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateProjectTableStatus(ctx, "p", "commerce.orders", "done", "m1", 10, 10); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateProjectTableStatus(ctx, "p", "loadtest.orders", "done", "m0", 1200000, 1200000); err != nil {
		t.Fatal(err)
	}
	if err := db.PruneProjectTables(ctx, "p", []string{"commerce.orders", "commerce.products"}); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetProjectTables(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TableName != "commerce.orders" || got[0].Status != "done" || got[0].RowsTotal != 10 {
		t.Fatalf("project tables = %+v", got)
	}
}
