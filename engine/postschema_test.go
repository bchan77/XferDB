package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// ddlTarget records post-schema DDL: the order statements ran in, and any
// moment two ran on one table at once. failOnce names a statement whose first
// attempt fails as YugabyteDB fails DDL on a table another DDL still changes.
type ddlTarget struct {
	devNullTarget
	mu       sync.Mutex
	busy     map[string]int
	overlap  []string
	order    []string // kind:name, in the order they ran
	failOnce map[string]bool
	failErr  error // what a failOnce statement fails with (default: a DDL conflict)
	running  int   // statements running now, on any table
	maxRun   int   // the most that ever ran at once
}

func (t *ddlTarget) enter(kind, name string, tables ...string) error {
	t.mu.Lock()
	for _, tb := range tables {
		t.busy[tb]++
		if t.busy[tb] > 1 {
			t.overlap = append(t.overlap, tb)
		}
	}
	t.order = append(t.order, kind+":"+name)
	t.running++
	t.maxRun = max(t.maxRun, t.running)
	fail := t.failOnce[name]
	delete(t.failOnce, name)
	t.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	t.mu.Lock()
	for _, tb := range tables {
		t.busy[tb]--
	}
	t.running--
	t.mu.Unlock()
	if fail && t.failErr != nil {
		return t.failErr
	}
	if fail {
		return errors.New(`pq: DDL transaction x cannot continue as table carts is undergoing DDL transaction verification for y: TABLE_SCHEMA_CHANGE_IN_PROGRESS (XX000)`)
	}
	return nil
}

func (t *ddlTarget) CreateIndexes(_ context.Context, table string, ix []adapters.IndexDef) error {
	return t.enter("index", ix[0].Name, table)
}

func (t *ddlTarget) CreateConstraints(_ context.Context, table string, fks []adapters.ForeignKey, cks []adapters.CheckConstraint) error {
	if len(fks) > 0 {
		return t.enter("fk", fks[0].Name, table, fks[0].RefTable)
	}
	return t.enter("check", cks[0].Name, table)
}

func TestPostSchemaOrderLocksRetryAndSteps(t *testing.T) {
	tgt := &ddlTarget{busy: map[string]int{}, failOnce: map[string]bool{"cart_items_cart_fk": true}}
	eng, db, cleanup := newTestEngine(t, &offsetSource{}, tgt)
	defer cleanup()
	tables := []adapters.TableSchema{
		{Name: "carts", Indexes: []adapters.IndexDef{{Name: "carts_a"}, {Name: "carts_b"}, {Name: "carts_c"}}},
		{Name: "cart_items",
			Indexes:     []adapters.IndexDef{{Name: "cart_items_a"}},
			ForeignKeys: []adapters.ForeignKey{{Name: "cart_items_cart_fk", RefTable: "carts"}},
			Checks:      []adapters.CheckConstraint{{Name: "cart_items_qty_ck"}}},
		{Name: "orders", ForeignKeys: []adapters.ForeignKey{{Name: "orders_cart_fk", RefTable: "carts"}}},
	}
	ctx := context.Background()
	if err := eng.createPostSchema(ctx, tables, 4); err != nil {
		t.Fatal(err)
	}
	if len(tgt.overlap) > 0 {
		t.Errorf("DDL ran on a table another statement was changing: %v", tgt.overlap)
	}
	// Every index before any constraint.
	seenConstraint := false
	for _, o := range tgt.order {
		if o[:6] == "index:" {
			if seenConstraint {
				t.Errorf("an index ran after a constraint: %v", tgt.order)
				break
			}
		} else {
			seenConstraint = true
		}
	}

	steps, err := db.ListSchemaSteps(ctx, eng.migrationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 7 {
		t.Fatalf("recorded %d steps, want 7: %+v", len(steps), steps)
	}
	for _, st := range steps {
		if st.Status != "done" || st.FinishedAt == nil || st.FinishedAt.Before(st.StartedAt) {
			t.Errorf("step %s: %+v", st.Name, st)
		}
		want := 1
		if st.Name == "cart_items_cart_fk" {
			want = 2 // refused once, retried
		}
		if st.Attempts != want {
			t.Errorf("step %s took %d attempts, want %d", st.Name, st.Attempts, want)
		}
	}
}

// A statement that keeps failing for another reason fails the run, recorded
// as a failed step with its error.
func TestPostSchemaFailureRecorded(t *testing.T) {
	tgt := &failingDDLTarget{}
	eng, db, cleanup := newTestEngine(t, &offsetSource{}, tgt)
	defer cleanup()
	ctx := context.Background()
	err := eng.createPostSchema(ctx, []adapters.TableSchema{{Name: "t", Indexes: []adapters.IndexDef{{Name: "t_ix"}}}}, 2)
	if err == nil {
		t.Fatal("want an error")
	}
	steps, _ := db.ListSchemaSteps(ctx, eng.migrationID)
	if len(steps) != 1 || steps[0].Status != "failed" || steps[0].Error == "" {
		t.Errorf("steps %+v, want one failed with its error", steps)
	}
}

type failingDDLTarget struct {
	devNullTarget
	msg string
}

func (f failingDDLTarget) CreateIndexes(context.Context, string, []adapters.IndexDef) error {
	if f.msg != "" {
		return errors.New(f.msg)
	}
	return errors.New("pq: could not create unique index (23505)")
}

// YugabyteDB can abort an online index build with "Restart read required"
// (40001); the index is created on the next attempt instead of failing the
// migration. An error that won't go away still fails it at once.
func TestPostSchemaRetriesRestartRead(t *testing.T) {
	tgt := &ddlTarget{busy: map[string]int{}, failOnce: map[string]bool{"categories_slug_uk": true},
		failErr: errors.New("create index categories_slug_uk on categories: pq: Restart read required (40001)")}
	eng, db, cleanup := newTestEngine(t, &offsetSource{}, tgt)
	defer cleanup()
	ctx := context.Background()
	tables := []adapters.TableSchema{{Name: "categories", Indexes: []adapters.IndexDef{{Name: "categories_slug_uk"}}}}
	if err := eng.createPostSchema(ctx, tables, 2); err != nil {
		t.Fatal(err)
	}
	steps, _ := db.ListSchemaSteps(ctx, eng.migrationID)
	if len(steps) != 1 || steps[0].Status != "done" || steps[0].Attempts != 2 {
		t.Errorf("steps %+v, want one done in 2 attempts", steps)
	}

	for _, msg := range []string{"pq: could not create unique index (23505)", "pq: syntax error (42601)"} {
		if isRetryableDDL(errors.New(msg)) {
			t.Errorf("%q retried", msg)
		}
	}
}

// The error names the statement once, not once from the adapter and again
// from the engine.
func TestPostSchemaErrorNamesStatementOnce(t *testing.T) {
	tgt := &failingDDLTarget{msg: "create index t_ix on t: pq: boom (XX000)"}
	eng, _, cleanup := newTestEngine(t, &offsetSource{}, tgt)
	defer cleanup()
	err := eng.createPostSchema(context.Background(), []adapters.TableSchema{{Name: "t", Indexes: []adapters.IndexDef{{Name: "t_ix"}}}}, 1)
	if err == nil || strings.Count(err.Error(), "create index t_ix on t") != 1 {
		t.Errorf("error %v, want the statement named once", err)
	}
}

// On a YugabyteDB target post-schema runs one statement at a time, even on
// different tables: concurrent online index builds wait for each other's
// backends until they time out ("timed out waiting for postgres backends to
// catch up"). Another target still builds on several tables at once.
func TestPostSchemaSerialOnYugabyte(t *testing.T) {
	tables := []adapters.TableSchema{
		{Name: "accounts", Indexes: []adapters.IndexDef{{Name: "accounts_code_uk"}}},
		{Name: "categories", Indexes: []adapters.IndexDef{{Name: "categories_slug_uk"}}},
		{Name: "api_requests", Indexes: []adapters.IndexDef{{Name: "api_requests_at_ix"}}},
	}
	for _, tc := range []struct {
		typ     string
		wantMax func(int) bool
	}{
		{adapters.TypeYugabyte, func(n int) bool { return n == 1 }},
		{"postgres", func(n int) bool { return n > 1 }},
	} {
		tgt := &ddlTarget{busy: map[string]int{}}
		eng, _, cleanup := newTestEngine(t, &offsetSource{}, tgt)
		eng.project.TargetConfig.Type = tc.typ
		if err := eng.createPostSchema(context.Background(), tables, 3); err != nil {
			t.Fatal(err)
		}
		if !tc.wantMax(tgt.maxRun) {
			t.Errorf("%s target: %d statements ran at once", tc.typ, tgt.maxRun)
		}
		cleanup()
	}
}
