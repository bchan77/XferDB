package validate

import (
	"context"
	"strings"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/state"
)

func validateL3(t *testing.T, plant []string, sample int) map[string]state.ValidationTable {
	t.Helper()
	p, tgt := migrated(t)
	for _, stmt := range plant {
		if _, err := tgt.Exec(stmt); err != nil {
			t.Fatalf("plant: %v\n%s", err, stmt)
		}
	}
	got := map[string]state.ValidationTable{}
	err := Run(context.Background(), Request{
		Project: p, Tables: allTables, Level: LevelL3, BatchSize: 2, Sample: sample,
	}, func(r state.ValidationTable) { got[r.TableName] = r })
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	return got
}

// kinds counts r's examples by kind.
func kinds(r state.ValidationTable) map[string]int {
	n := map[string]int{}
	for _, d := range r.Diffs {
		n[d.Kind]++
	}
	return n
}

func TestL3FindsEachKindOfDifference(t *testing.T) {
	cases := []struct {
		name                     string
		plant                    []string
		table                    string
		missing, extra, mismatch int64
		mismatchCol              string
	}{
		// items: single integer key, diffed by key range.
		{"int key changed value", []string{`UPDATE items SET name = 'APPLE' WHERE id = 1`}, "items", 0, 0, 1, "name"},
		{"int key deleted row", []string{`DELETE FROM items WHERE id = 2`}, "items", 1, 0, 0, ""},
		{"int key extra rows", []string{`INSERT INTO items (id, name) VALUES (99, 'ghost'), (-5, 'before')`}, "items", 0, 2, 0, ""},
		{"int key all three", []string{
			`DELETE FROM items WHERE id = 3`,
			`INSERT INTO items (id, name) VALUES (4, 'new')`,
			`UPDATE items SET ratio = 9.5 WHERE id = 2`,
		}, "items", 1, 1, 1, "ratio"},
		// tags: text key, diffed by lookup plus a reverse pass.
		{"text key changed value", []string{`UPDATE tags SET label = 'BETA' WHERE code = 'b'`}, "tags", 0, 0, 1, "label"},
		{"text key missing and extra", []string{
			`DELETE FROM tags WHERE code = 'a'`, `INSERT INTO tags VALUES ('z', 'Zeta')`,
		}, "tags", 1, 1, 0, ""},
		// pairs: composite key.
		{"composite key", []string{
			`UPDATE pairs SET v = 7 WHERE a = 1 AND b = 'y'`, `DELETE FROM pairs WHERE a = 2`,
		}, "pairs", 1, 0, 1, "v"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := validateL3(t, c.plant, 0)
			r := got[c.table]
			if r.Result != state.ValidationFail || r.LevelRun != LevelL3 {
				t.Fatalf("result %s at %s (%s; note %q), want fail at L3", r.Result, r.LevelRun, r.Detail, r.LevelNote)
			}
			if r.DiffMissing != c.missing || r.DiffExtra != c.extra || r.DiffMismatch != c.mismatch {
				t.Errorf("missing/extra/mismatch = %d/%d/%d, want %d/%d/%d (%s)",
					r.DiffMissing, r.DiffExtra, r.DiffMismatch, c.missing, c.extra, c.mismatch, r.Detail)
			}
			k := kinds(r)
			if int64(k[state.DiffMissingInTarget]) != c.missing || int64(k[state.DiffExtraInTarget]) != c.extra || int64(k[state.DiffValueMismatch]) != c.mismatch {
				t.Errorf("examples by kind %v do not match the totals", k)
			}
			for _, d := range r.Diffs {
				if len(d.Key) == 0 {
					t.Errorf("%s example has no key", d.Kind)
				}
				if d.Kind == state.DiffValueMismatch && (len(d.Columns) != 1 || d.Columns[0] != c.mismatchCol) {
					t.Errorf("mismatch columns %v, want [%s]", d.Columns, c.mismatchCol)
				}
			}
			for other, o := range got {
				if other != c.table && o.Result != state.ValidationPass {
					t.Errorf("untouched table %s: result %s (%s)", other, o.Result, o.Detail)
				}
			}
		})
	}
}

func TestL3ShowsCanonicalValues(t *testing.T) {
	r := validateL3(t, []string{`UPDATE items SET name = NULL, price = 3 WHERE id = 1`}, 0)["items"]
	if len(r.Diffs) != 1 {
		t.Fatalf("diffs %+v", r.Diffs)
	}
	d := r.Diffs[0]
	if *d.Key["id"] != "1" || *d.Source["name"] != "apple" || d.Target["name"] != nil {
		t.Errorf("key %v, source name %v, target name %v", d.Key, d.Source["name"], d.Target["name"])
	}
	if *d.Source["price"] != "1.5" || *d.Target["price"] != "3" {
		t.Errorf("price source %s target %s, want canonical 1.5 and 3", *d.Source["price"], *d.Target["price"])
	}
}

func TestL3FallsBackWithoutAKey(t *testing.T) {
	r := validateL3(t, []string{`UPDATE notes SET n = 5 WHERE body = 'other'`}, 0)["notes"]
	if r.Result != state.ValidationFail || r.LevelRun != LevelL2 {
		t.Fatalf("result %s at %s, want fail at L2", r.Result, r.LevelRun)
	}
	if !strings.Contains(r.LevelNote, "no primary key") || !strings.Contains(r.Detail, "contents differ") {
		t.Errorf("note %q / detail %q should explain the fallback and keep the L2 failure", r.LevelNote, r.Detail)
	}
}

func TestL3PassingTableStaysAtL2(t *testing.T) {
	r := validateL3(t, nil, 0)["items"]
	if r.Result != state.ValidationPass || r.LevelRun != LevelL2 || len(r.Diffs) != 0 {
		t.Fatalf("result %s at %s with %d diffs, want a pass at L2 with none", r.Result, r.LevelRun, len(r.Diffs))
	}
}

func TestL3Sample(t *testing.T) {
	// Every row differs, so even a 1% sample must find some, on both the
	// key-range path (items) and the lookup path (tags).
	got := validateL3(t, []string{`UPDATE items SET name = 'x'`, `UPDATE tags SET label = 'x'`}, 1)
	for _, table := range []string{"items", "tags"} {
		r := got[table]
		if r.Result != state.ValidationFail || r.Sample != 1 || r.DiffMismatch == 0 {
			t.Errorf("%s: result %s, sample %d, mismatches %d; want a sampled fail with mismatches", table, r.Result, r.Sample, r.DiffMismatch)
		}
		if !strings.Contains(r.Detail, "1% sample") {
			t.Errorf("%s: detail %q should say it was sampled", table, r.Detail)
		}
	}
	if !strings.Contains(got["tags"].LevelNote, "only in the target") {
		t.Errorf("tags note %q should say extra rows were not looked for", got["tags"].LevelNote)
	}

	// 100% is a full diff.
	r := validateL3(t, []string{`UPDATE items SET name = 'x'`}, 100)["items"]
	if r.Sample != 0 || r.DiffMismatch != 3 {
		t.Errorf("sample 100: recorded sample %d, mismatches %d; want 0 and 3", r.Sample, r.DiffMismatch)
	}
}

func TestCheckSample(t *testing.T) {
	for _, ok := range []int{0, 1, 50, 100} {
		if err := CheckSample(ok); err != nil {
			t.Errorf("CheckSample(%d): %v", ok, err)
		}
	}
	for _, bad := range []int{-1, 101} {
		if CheckSample(bad) == nil {
			t.Errorf("CheckSample(%d) accepted", bad)
		}
	}
}
