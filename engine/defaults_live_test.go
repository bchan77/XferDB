package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// TestColumnDefaultsLive migrates a table of typical column defaults
// between every pair of PostgreSQL, MySQL and SQLite, then inserts a row on
// the target that leaves those columns out and checks what they got. A
// default the target can't express (a sequence, a UUID function on SQLite)
// is dropped rather than failing CREATE TABLE, which is what a PostgreSQL
// now() did on SQLite before. Needs XFERDB_TEST_PAIRS_PG and
// XFERDB_TEST_PAIRS_MYSQL (host:port of servers whose postgres / root login
// needs no password).
func TestColumnDefaultsLive(t *testing.T) {
	pgAddr, myAddr := os.Getenv("XFERDB_TEST_PAIRS_PG"), os.Getenv("XFERDB_TEST_PAIRS_MYSQL")
	if pgAddr == "" || myAddr == "" {
		t.Skip("XFERDB_TEST_PAIRS_PG / XFERDB_TEST_PAIRS_MYSQL not set; skipping live column defaults test")
	}
	dir := t.TempDir()

	// The same defaults in each dialect's own words. seq is a key the
	// source generates (serial, which only a PostgreSQL target keeps).
	ddl := map[string]string{
		"postgres": `CREATE TABLE probe (
			id int PRIMARY KEY,
			status varchar(20) DEFAULT 'new',
			note text DEFAULT 'it''s',
			qty int DEFAULT 0,
			neg int DEFAULT -1,
			price numeric(10,2) DEFAULT 9.99,
			active boolean DEFAULT true,
			flag boolean DEFAULT false,
			created_at timestamp DEFAULT now(),
			created_on date DEFAULT CURRENT_DATE,
			uid uuid DEFAULT gen_random_uuid(),
			seq serial)`,
		"mysql": `CREATE TABLE probe (
			id int PRIMARY KEY,
			status varchar(20) DEFAULT 'new',
			note text DEFAULT ('it''s'),
			qty int DEFAULT 0,
			neg int DEFAULT -1,
			price decimal(10,2) DEFAULT 9.99,
			active tinyint(1) DEFAULT 1,
			flag tinyint(1) DEFAULT 0,
			created_at datetime(6) DEFAULT CURRENT_TIMESTAMP(6),
			created_on date DEFAULT (CURRENT_DATE),
			uid char(36) DEFAULT (uuid()),
			seq int NOT NULL)`,
		"sqlite": `CREATE TABLE probe (
			id INTEGER PRIMARY KEY,
			status TEXT DEFAULT 'new',
			note TEXT DEFAULT 'it''s',
			qty INTEGER DEFAULT 0,
			neg INTEGER DEFAULT -1,
			price NUMERIC DEFAULT 9.99,
			active BOOLEAN DEFAULT TRUE,
			flag BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			created_on DATE DEFAULT CURRENT_DATE,
			uid TEXT,
			seq INTEGER NOT NULL)`,
	}

	open := func(typ, db string) (*sql.DB, adapters.ConnectionConfig) {
		switch typ {
		case "postgres":
			host, port := hostPort(t, pgAddr)
			dsn := func(db string) string {
				return fmt.Sprintf("host=%s port=%d user=postgres dbname=%s sslmode=disable", host, port, db)
			}
			admin := openDB(t, "postgres", dsn("postgres"))
			admin.Exec("DROP DATABASE IF EXISTS " + db)
			execDB(t, admin, "CREATE DATABASE "+db)
			admin.Close()
			t.Cleanup(func() {
				a := openDB(t, "postgres", dsn("postgres"))
				a.Exec("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)")
				a.Close()
			})
			return openDB(t, "postgres", dsn(db)),
				adapters.ConnectionConfig{Type: "postgres", Host: host, Port: port, Username: "postgres", Password: "x", Database: db, SSLMode: "disable"}
		case "mysql":
			host, port := hostPort(t, myAddr)
			admin := openDB(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
			admin.Exec("DROP DATABASE IF EXISTS " + db)
			execDB(t, admin, "CREATE DATABASE "+db)
			admin.Close()
			t.Cleanup(func() {
				a := openDB(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
				a.Exec("DROP DATABASE IF EXISTS " + db)
				a.Close()
			})
			return openDB(t, "mysql", fmt.Sprintf("root@tcp(%s:%d)/%s", host, port, db)),
				adapters.ConnectionConfig{Type: "mysql", Host: host, Port: port, Username: "root", Database: db}
		}
		path := filepath.Join(dir, db+".db")
		return openDB(t, "sqlite3", path), adapters.ConnectionConfig{Type: "sqlite", DSN: path}
	}

	types := []string{"postgres", "mysql", "sqlite"}
	for _, sn := range types {
		for _, tn := range types {
			t.Run(sn+"_to_"+tn, func(t *testing.T) {
				ctx := context.Background()
				name := fmt.Sprintf("dflt_%s_%s_%d", sn[:2], tn[:2], time.Now().UnixNano()%1e6)
				src, scfg := open(sn, name+"_s")
				defer src.Close()
				execDB(t, src, ddl[sn])
				execDB(t, src, `INSERT INTO probe (id, seq) VALUES (1, 1)`)
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
				mig := &adapters.Migration{ID: name, ProjectID: p.ID, Status: adapters.StatusPending, CreatedAt: time.Now()}
				if err := meta.CreateMigration(ctx, mig); err != nil {
					t.Fatal(err)
				}
				if err := New(mig.ID, p, meta).Run(ctx); err != nil {
					t.Fatalf("migration: %v", err)
				}

				// The copied row came over with the source's values.
				var copied string
				if err := tgt.QueryRow(`SELECT status FROM probe WHERE id = 1`).Scan(&copied); err != nil || copied != "new" {
					t.Fatalf("copied row: status %q, %v", copied, err)
				}
				// A new row gets the defaults.
				execDB(t, tgt, `INSERT INTO probe (id, seq) VALUES (100, 100)`)
				var status, note, qty, neg, price, active, flag, createdAt, createdOn, uid sql.NullString
				if err := tgt.QueryRow(`SELECT status, note, qty, neg, price, active, flag, created_at, created_on, uid FROM probe WHERE id = 100`).
					Scan(&status, &note, &qty, &neg, &price, &active, &flag, &createdAt, &createdOn, &uid); err != nil {
					t.Fatal(err)
				}
				check := func(col string, got sql.NullString, ok bool) {
					t.Helper()
					if !ok {
						t.Errorf("%s: got %q (valid %v)", col, got.String, got.Valid)
					}
				}
				check("status", status, status.String == "new")
				check("note", note, note.String == "it's")
				check("qty", qty, qty.String == "0")
				check("neg", neg, neg.String == "-1")
				p99, _ := strconv.ParseFloat(price.String, 64)
				check("price", price, p99 == 9.99)
				check("active", active, active.String == "true" || active.String == "1" || active.String == "t")
				check("flag", flag, flag.String == "false" || flag.String == "0" || flag.String == "f")
				check("created_at", createdAt, createdAt.Valid)
				check("created_on", createdOn, createdOn.Valid)
				// gen_random_uuid() and uuid() carry over between PostgreSQL
				// and MySQL; SQLite has no such function.
				wantUUID := sn != "sqlite" && tn != "sqlite"
				check("uid", uid, uid.Valid == wantUUID && (!wantUUID || len(uid.String) == 36))
			})
		}
	}
}

func hostPort(t *testing.T, addr string) (string, int) {
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

func openDB(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func execDB(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("%v\n%s", err, stmt)
	}
}
