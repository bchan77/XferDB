package engine

import (
	"reflect"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestFilterTablesSchemaQualified(t *testing.T) {
	tables := []adapters.TableSchema{{Name: "orders"}, {Name: "loadtest.orders"}, {Name: "loadtest.Products"}}
	names := func(ts []adapters.TableSchema) []string {
		var out []string
		for _, t := range ts {
			out = append(out, t.Name)
		}
		return out
	}
	for _, c := range []struct {
		filters []string
		want    []string
		missing []string
	}{
		{[]string{"orders"}, []string{"orders"}, nil},
		{[]string{"public.orders"}, []string{"orders"}, nil},
		{[]string{"loadtest.orders"}, []string{"loadtest.orders"}, nil},
		{[]string{"LOADTEST.products", "nope.x"}, []string{"loadtest.Products"}, []string{"nope.x"}},
	} {
		got := filterTables(tables, c.filters)
		if !reflect.DeepEqual(names(got), c.want) {
			t.Errorf("filter %v = %v, want %v", c.filters, names(got), c.want)
		}
		if m := missingRequestedTables(got, c.filters); !reflect.DeepEqual(m, c.missing) {
			t.Errorf("filter %v missing = %v, want %v", c.filters, m, c.missing)
		}
	}
}
