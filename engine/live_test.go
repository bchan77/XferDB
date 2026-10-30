package engine

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestStageOfBlock checks which stage each pg_dump post-data statement of a
// live migration runs at: what apply needs right after the copy, what
// replay could break only once caught up, and the rest at cutover.
func TestStageOfBlock(t *testing.T) {
	for _, c := range []struct {
		typ, sql string
		want     SchemaStage
	}{
		{"CONSTRAINT", `ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);`, StageSnapshot},
		{"INDEX", `CREATE INDEX orders_customer ON public.orders USING btree (customer_id);`, StageSnapshot},
		{"INDEX", `CREATE UNIQUE INDEX orders_ref ON public.orders USING btree (ref);`, StageCatchUp},
		{"CONSTRAINT", `ALTER TABLE ONLY public.users ADD CONSTRAINT users_email_key UNIQUE (email);`, StageCatchUp},
		{"CONSTRAINT", `ALTER TABLE ONLY public.rooms ADD CONSTRAINT no_overlap EXCLUDE USING gist (during WITH &&);`, StageCatchUp},
		{"CONSTRAINT", `ALTER TABLE public.orders ADD CONSTRAINT amount_positive CHECK ((amount > 0)) NOT VALID;`, StageCutover},
		{"FK CONSTRAINT", `ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(id);`, StageCutover},
		{"TRIGGER", `CREATE TRIGGER audit AFTER INSERT ON public.orders FOR EACH ROW EXECUTE FUNCTION public.audit();`, StageCutover},
		{"RULE", `CREATE RULE r AS ON DELETE TO public.orders DO INSTEAD NOTHING;`, StageCutover},
	} {
		if got := stageOfBlock(ddlBlock{Type: c.typ, SQL: c.sql}); got != c.want {
			t.Errorf("%s %q: stage %s, want %s", c.typ, c.sql, got, c.want)
		}
	}
}

func TestStageSchema(t *testing.T) {
	tables := []adapters.TableSchema{{
		Name: "orders",
		Indexes: []adapters.IndexDef{
			{Name: "by_customer", Columns: []string{"customer_id"}},
			{Name: "by_ref", Columns: []string{"ref"}, Unique: true},
		},
		ForeignKeys: []adapters.ForeignKey{{Name: "fk", Columns: []string{"customer_id"}, RefTable: "customers", RefColumns: []string{"id"}}},
		Checks:      []adapters.CheckConstraint{{Name: "positive", Expression: "amount > 0"}},
	}}
	names := func(ix []adapters.IndexDef) []string {
		var out []string
		for _, i := range ix {
			out = append(out, i.Name)
		}
		return out
	}
	snap := stageSchema(tables, StageSnapshot)[0]
	if got := names(snap.Indexes); len(got) != 1 || got[0] != "by_customer" || snap.ForeignKeys != nil || snap.Checks != nil {
		t.Errorf("snapshot stage: indexes %v, %d fks, %d checks", got, len(snap.ForeignKeys), len(snap.Checks))
	}
	catch := stageSchema(tables, StageCatchUp)[0]
	if got := names(catch.Indexes); len(got) != 1 || got[0] != "by_ref" || catch.ForeignKeys != nil || catch.Checks != nil {
		t.Errorf("catch-up stage: indexes %v, %d fks, %d checks", got, len(catch.ForeignKeys), len(catch.Checks))
	}
	cut := stageSchema(tables, StageCutover)[0]
	if len(cut.Indexes) != 0 || len(cut.ForeignKeys) != 1 || len(cut.Checks) != 1 {
		t.Errorf("cutover stage: %d indexes, %d fks, %d checks", len(cut.Indexes), len(cut.ForeignKeys), len(cut.Checks))
	}
	if len(tables[0].Indexes) != 2 {
		t.Errorf("stageSchema changed its input")
	}
}

func TestCheckLive(t *testing.T) {
	for _, c := range []struct {
		cfg adapters.TransferConfig
		ok  bool
	}{
		{adapters.TransferConfig{}, true},
		{adapters.TransferConfig{RecreateSchema: true, BulkCopy: true}, true},
		{adapters.TransferConfig{SchemaOnly: true}, false},
		{adapters.TransferConfig{DataOnly: true}, false},
		{adapters.TransferConfig{Truncate: true}, false},
	} {
		if err := CheckLive(c.cfg); (err == nil) != c.ok {
			t.Errorf("CheckLive(%+v) = %v, want ok=%v", c.cfg, err, c.ok)
		}
	}
}
