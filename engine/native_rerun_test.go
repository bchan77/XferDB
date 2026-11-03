package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestPgDumpRerunLive migrates a PostgreSQL database to PostgreSQL twice
// without recreating the schema. The second run's post-data finds every key,
// index, constraint and trigger already on the target, and must skip them
// rather than fail with "multiple primary keys" (42P16), as it did when the
// UI tests left a migrated table in the shared target. Needs
// XFERDB_TEST_PAIRS_PG and pg_dump/psql on PATH.
func TestPgDumpRerunLive(t *testing.T) {
	pgAddr := os.Getenv("XFERDB_TEST_PAIRS_PG")
	if pgAddr == "" {
		t.Skip("XFERDB_TEST_PAIRS_PG not set; skipping pg_dump rerun test")
	}
	if !hasPgTools() {
		t.Skip("pg_dump/psql not on PATH")
	}
	ctx := context.Background()
	dir := t.TempDir()
	host, port := hostPort(t, pgAddr)
	dsn := func(db string) string {
		return fmt.Sprintf("host=%s port=%d user=postgres dbname=%s sslmode=disable", host, port, db)
	}
	name := fmt.Sprintf("rerun_%d", time.Now().UnixNano()%1e6)
	cfgs := map[string]adapters.ConnectionConfig{}
	for _, db := range []string{name + "_s", name + "_t"} {
		admin := openDB(t, "postgres", dsn("postgres"))
		execDB(t, admin, "CREATE DATABASE "+db)
		admin.Close()
		t.Cleanup(func() {
			a := openDB(t, "postgres", dsn("postgres"))
			a.Exec("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)")
			a.Close()
		})
		cfgs[db] = adapters.ConnectionConfig{Type: "postgres", Host: host, Port: port, Username: "postgres", Password: "x", Database: db, SSLMode: "disable"}
	}

	src := openDB(t, "postgres", dsn(name+"_s"))
	defer src.Close()
	for _, stmt := range []string{
		`CREATE TABLE users (id serial PRIMARY KEY, email text UNIQUE, age int CHECK (age >= 0))`,
		`CREATE INDEX users_age_idx ON users (age)`,
		`CREATE TABLE orders (id serial PRIMARY KEY, user_id int REFERENCES users (id))`,
		`CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`,
		`CREATE TRIGGER orders_touch BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION touch()`,
		`INSERT INTO users (email, age) SELECT 'u' || g || '@x', g FROM generate_series(1, 50) g`,
		`INSERT INTO orders (user_id) SELECT g FROM generate_series(1, 50) g`,
	} {
		execDB(t, src, stmt)
	}

	meta, err := state.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	p := &adapters.Project{ID: name, Name: name, SourceConfig: cfgs[name+"_s"], TargetConfig: cfgs[name+"_t"], CreatedAt: time.Now()}
	if err := meta.CreateProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	for run := 1; run <= 2; run++ {
		mig := &adapters.Migration{ID: fmt.Sprintf("%s_%d", name, run), ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
		if err := meta.CreateMigration(ctx, mig); err != nil {
			t.Fatal(err)
		}
		if err := New(mig.ID, p, meta).Run(ctx); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}

	tgt := openDB(t, "postgres", dsn(name+"_t"))
	defer tgt.Close()
	var users, orders, cons, trig int
	if err := tgt.QueryRow(`SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM orders),
		(SELECT count(*) FROM pg_constraint WHERE conrelid IN ('users'::regclass, 'orders'::regclass)),
		(SELECT count(*) FROM pg_trigger WHERE tgname = 'orders_touch')`).Scan(&users, &orders, &cons, &trig); err != nil {
		t.Fatal(err)
	}
	// Two primary keys, the unique email, the check and the foreign key.
	if users != 50 || orders != 50 || cons != 5 || trig != 1 {
		t.Errorf("target: %d users, %d orders, %d constraints, %d triggers; want 50, 50, 5, 1", users, orders, cons, trig)
	}
}
