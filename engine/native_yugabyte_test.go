package engine

import (
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// pg_dump and psql reach a yugabyte source or target like a postgres one:
// its URL scheme rewritten, and its own default port (#158).
func TestPgConnStrYugabyte(t *testing.T) {
	for _, c := range []struct {
		cfg  adapters.ConnectionConfig
		want string
	}{
		{adapters.ConnectionConfig{Type: "yugabyte", DSN: "yugabyte://u:p@h:5433/db"}, "postgres://u:p@h:5433/db"},
		{adapters.ConnectionConfig{Type: "postgres", DSN: "postgres://u@h/db"}, "postgres://u@h/db"},
		{adapters.ConnectionConfig{Type: "yugabyte", Host: "h", Username: "u", Database: "db"}, "postgres://u@h:5433/db"},
		{adapters.ConnectionConfig{Type: "postgres", Host: "h", Username: "u", Database: "db"}, "postgres://u@h/db"},
	} {
		if got := pgConnStr(c.cfg); got != c.want {
			t.Errorf("pgConnStr(%+v) = %q, want %q", c.cfg, got, c.want)
		}
	}
}

// pg_dump never copies a YugabyteDB source's heartbeat table.
func TestPgDumpSchemaArgsYugabyte(t *testing.T) {
	src := adapters.ConnectionConfig{Type: "yugabyte"}
	args, ok := pgDumpSchemaArgs(src, adapters.ConnectionConfig{Type: "yugabyte"})
	if !ok || len(args) != 1 || args[0] != "--exclude-table=*.xferdb_heartbeat" {
		t.Fatalf("args = %v, %v", args, ok)
	}
	src.Schema = "s"
	args, ok = pgDumpSchemaArgs(src, adapters.ConnectionConfig{Type: "postgres", Schema: "s"})
	if !ok || len(args) != 2 || args[0] != `--schema="s"` {
		t.Fatalf("args = %v, %v", args, ok)
	}
}

// Only the primary keys of a post-data dump go in before a YugabyteDB
// copy: indexes, unique and foreign keys wait for their stage.
func TestIsPrimaryKey(t *testing.T) {
	dump := []byte(`--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: commerce; Owner: -
--

ALTER TABLE ONLY commerce.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (order_id);

--
-- Name: customers customers_email_key; Type: CONSTRAINT; Schema: commerce; Owner: -
--

ALTER TABLE ONLY commerce.customers
    ADD CONSTRAINT customers_email_key UNIQUE (email);

--
-- Name: orders_customer_idx; Type: INDEX; Schema: commerce; Owner: -
--

CREATE INDEX orders_customer_idx ON commerce.orders USING lsm (customer_id HASH);

--
-- Name: orders orders_customer_fk; Type: FK CONSTRAINT; Schema: commerce; Owner: -
--

ALTER TABLE ONLY commerce.orders
    ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES commerce.customers(customer_id);
`)
	var keys []string
	for _, b := range parsePgDumpBlocks(dump) {
		if isPrimaryKey(b) {
			keys = append(keys, b.Name)
		}
	}
	if len(keys) != 1 || keys[0] != "orders orders_pkey" {
		t.Fatalf("primary keys = %v", keys)
	}
}
