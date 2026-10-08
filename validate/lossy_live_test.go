package validate

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/engine"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestLossyByPlanLive migrates a table with narrowing type overrides between
// every pair of PostgreSQL, MySQL and SQLite through the real engine, then
// validates it: the plan's conversions must come out as expected_lossy, and
// a real defect planted on top must still fail.
//
// Opt-in: XFERDB_TEST_LOSSY_PG and XFERDB_TEST_LOSSY_MYSQL hold host:port of
// servers that accept user postgres (trust auth) and root (no password),
// with SSL off. Each run creates and drops its own databases.
func TestLossyByPlanLive(t *testing.T) {
	pgAddr, myAddr := os.Getenv("XFERDB_TEST_LOSSY_PG"), os.Getenv("XFERDB_TEST_LOSSY_MYSQL")
	if pgAddr == "" || myAddr == "" {
		t.Skip("XFERDB_TEST_LOSSY_PG / XFERDB_TEST_LOSSY_MYSQL not set; skipping live lossy-by-plan test")
	}
	dir := t.TempDir()

	ddl := map[string]string{
		"postgres": `CREATE TABLE probe (id int primary key, f double precision, d numeric(10,4), ts timestamp(6))`,
		"mysql":    `CREATE TABLE probe (id int primary key, f double, d decimal(10,4), ts datetime(6))`,
		"sqlite":   `CREATE TABLE probe (id INTEGER PRIMARY KEY, f REAL, d NUMERIC(10,4), ts TIMESTAMP)`,
	}
	const rows = `INSERT INTO probe VALUES
		(1, 1.5, 1.2345, '2024-03-01 10:30:45.123456'),
		(2, 2.5, 1.2355, '2024-03-01 23:59:59.999999'),
		(3, -2.5, -1.2350, '2024-03-02 00:00:00'),
		(4, 3, 9.9999, '2024-03-02 12:00:00')`
	// PostgreSQL refuses 1.5 in an integer column, so f is only narrowed
	// for a MySQL target.
	overrides := map[string]map[string]string{
		"postgres": {"d": "numeric(10,2)", "ts": "date"},
		"mysql":    {"d": "numeric(10,2)", "ts": "date", "f": "integer"},
		"sqlite":   {"d": "numeric(10,2)", "ts": "date", "f": "integer"},
	}
	// SQLite stores every value as given, so nothing is lossy there.
	wantResult := map[string]string{
		"postgres": state.ValidationExpectedLossy,
		"mysql":    state.ValidationExpectedLossy,
		"sqlite":   state.ValidationPass,
	}

	open := func(typ, db string) (*sql.DB, adapters.ConnectionConfig) {
		switch typ {
		case "postgres":
			host, port := splitAddr(t, pgAddr)
			admin := mustOpen(t, "postgres", fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", host, port))
			admin.Exec("DROP DATABASE IF EXISTS " + db)
			mustExec(t, admin, "CREATE DATABASE "+db)
			admin.Close()
			t.Cleanup(func() {
				a := mustOpen(t, "postgres", fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", host, port))
				a.Exec("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)")
				a.Close()
			})
			return mustOpen(t, "postgres", fmt.Sprintf("host=%s port=%d user=postgres dbname=%s sslmode=disable", host, port, db)),
				adapters.ConnectionConfig{Type: "postgres", Host: host, Port: port, Username: "postgres", Password: "x", Database: db, SSLMode: "disable"}
		case "mysql":
			host, port := splitAddr(t, myAddr)
			admin := mustOpen(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
			admin.Exec("DROP DATABASE IF EXISTS " + db)
			mustExec(t, admin, "CREATE DATABASE "+db)
			admin.Close()
			t.Cleanup(func() {
				a := mustOpen(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
				a.Exec("DROP DATABASE IF EXISTS " + db)
				a.Close()
			})
			return mustOpen(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/%s", host, port, db)),
				adapters.ConnectionConfig{Type: "mysql", Host: host, Port: port, Username: "root", Database: db}
		}
		path := filepath.Join(dir, db+".db")
		return mustOpen(t, "sqlite3", path), adapters.ConnectionConfig{Type: "sqlite", DSN: path}
	}

	types := []string{"postgres", "mysql", "sqlite"}
	for _, sn := range types {
		for _, tn := range types {
			t.Run(sn+"_to_"+tn, func(t *testing.T) {
				ctx := context.Background()
				name := fmt.Sprintf("lossy_%s_%s_%d", sn[:2], tn[:2], time.Now().UnixNano()%1e6)
				src, scfg := open(sn, name+"_s")
				defer src.Close()
				mustExec(t, src, ddl[sn])
				mustExec(t, src, rows)
				tgt, tcfg := open(tn, name+"_t")
				defer tgt.Close()

				meta, err := state.Open(filepath.Join(dir, name+".state"))
				if err != nil {
					t.Fatal(err)
				}
				defer meta.Close()
				p := &adapters.Project{ID: name, Name: name, SourceConfig: scfg, TargetConfig: tcfg, CreatedAt: time.Now()}
				if err := meta.CreateProject(ctx, p); err != nil {
					t.Fatal(err)
				}
				no, yes := false, true
				for col, typ := range overrides[tn] {
					typ := typ
					if err := meta.OverridePlan(ctx, p.ID, "probe", col, state.SchemaPlanOverride{PgType: &typ, IsPK: &no, Nullable: &yes}); err != nil {
						t.Fatal(err)
					}
				}
				mig := &adapters.Migration{ID: name, ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
				if err := meta.CreateMigration(ctx, mig); err != nil {
					t.Fatal(err)
				}
				if err := engine.New(mig.ID, p, meta).Run(ctx); err != nil {
					t.Fatalf("migration: %v", err)
				}
				plan, err := meta.GetPlanForProject(ctx, p.ID)
				if err != nil {
					t.Fatal(err)
				}
				validateProbe := func(level string) state.ValidationTable {
					var got state.ValidationTable
					err := Run(ctx, Request{Project: p, Plan: plan, Tables: []string{"probe"}, Level: level, BatchSize: 100},
						func(r state.ValidationTable) { got = r })
					if err != nil {
						t.Fatalf("validate: %v", err)
					}
					return got
				}

				for _, level := range []string{LevelL2, LevelL3} {
					r := validateProbe(level)
					if r.Result != wantResult[tn] {
						t.Fatalf("%s: result %s (%s; note %q), want %s", level, r.Result, r.Detail, r.LevelNote, wantResult[tn])
					}
					if wantResult[tn] == state.ValidationExpectedLossy && (r.LossyRows != 4 || len(r.Diffs) != 4) {
						t.Errorf("%s: %d lossy rows, %d examples; want 4 and 4 (%s)", level, r.LossyRows, len(r.Diffs), r.Detail)
					}
				}

				// A real defect on top of the plan's conversions still fails,
				// and L3 tells the two apart.
				mustExec(t, tgt, `UPDATE probe SET d = 5 WHERE id = 3`)
				r := validateProbe(LevelL3)
				if r.Result != state.ValidationFail || r.DiffMismatch != 1 {
					t.Fatalf("after planting a defect: result %s with %d mismatches (%s), want fail with 1", r.Result, r.DiffMismatch, r.Detail)
				}
				if wantResult[tn] == state.ValidationExpectedLossy && r.LossyRows != 3 {
					t.Errorf("after planting a defect: %d lossy rows, want the other 3 (%s)", r.LossyRows, r.Detail)
				}
			})
		}
	}
}

func splitAddr(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

func mustOpen(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustExec(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("%v\n%s", err, strings.TrimSpace(stmt))
	}
}
