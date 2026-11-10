package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestYugabyteVersion(t *testing.T) {
	yb := "PostgreSQL 15.12-YB-2025.2.5.2-b0 on x86_64-pc-linux-gnu, compiled by clang version 19.1.0, 64-bit"
	pg := "PostgreSQL 16.4 (Debian 16.4-1.pgdg120+2) on x86_64-pc-linux-gnu, compiled by gcc, 64-bit"
	if !isYugabyte(yb) || isYugabyte(pg) {
		t.Fatalf("isYugabyte: yb %v, pg %v", isYugabyte(yb), isYugabyte(pg))
	}
	if got := yugabyteRelease(yb); got != "2025.2.5.2-b0" {
		t.Fatalf("yugabyteRelease = %q", got)
	}
	if got := yugabyteRelease(pg); got != "" {
		t.Fatalf("yugabyteRelease(pg) = %q", got)
	}
}

func TestReplicaIdentityOK(t *testing.T) {
	for identity, want := range map[string]bool{"d": true, "f": true, "i": true, "n": false, "c": false} {
		if got := replicaIdentityOK(identity); got != want {
			t.Errorf("replicaIdentityOK(%q) = %v, want %v", identity, got, want)
		}
	}
}

func TestYugabyteDSN(t *testing.T) {
	for in, want := range map[string]string{
		"yugabyte://u:p@h:5433/db?sslmode=require": "postgres://u:p@h:5433/db?sslmode=require",
		"YugabyteDB://u@h/db":                      "postgres://u@h/db",
		"postgres://u@h/db":                        "postgres://u@h/db",
		"host=h port=5433":                         "host=h port=5433",
	} {
		if got := postgresURL(in); got != want {
			t.Errorf("postgresURL(%q) = %q, want %q", in, got, want)
		}
	}
	got := buildDSN(adapters.ConnectionConfig{Type: adapters.TypeYugabyte, Host: "h", Database: "db", Username: "u"})
	if !strings.Contains(got, "port=5433") {
		t.Errorf("a yugabyte config without a port: %s", got)
	}
	got = buildDSN(adapters.ConnectionConfig{Type: adapters.TypePostgres, Host: "h", Database: "db", Username: "u"})
	if !strings.Contains(got, "port=5432") {
		t.Errorf("a postgres config without a port: %s", got)
	}
	got = buildDSN(adapters.ConnectionConfig{Type: adapters.TypeYugabyte, DSN: "yugabyte://u@h:5433/db", Schema: "s"})
	if got != "postgres://u@h:5433/db?search_path=%22s%22" {
		t.Errorf("a yugabyte:// DSN with a schema: %s", got)
	}
}

// checkNames returns the checks by name.
func checkNames(checks []adapters.LiveCheck) map[string]adapters.LiveCheck {
	m := make(map[string]adapters.LiveCheck, len(checks))
	for _, c := range checks {
		m[c.Name] = c
	}
	return m
}

// TestLiveChecksStuckServer: a server that accepts connections and never
// answers fails each check on its own timeout, and every check is still
// reported, instead of the readiness check hanging (#158).
func TestLiveChecksStuckServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var mu sync.Mutex
	var held []net.Conn
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			c.Close()
		}
	}()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c) // never answered
			mu.Unlock()
		}
	}()

	old := liveCheckTimeout
	liveCheckTimeout = 200 * time.Millisecond
	defer func() { liveCheckTimeout = old }()

	port := ln.Addr().(*net.TCPAddr).Port
	src := &Source{}
	cfg := adapters.ConnectionConfig{Type: "postgres", DSN: fmt.Sprintf("host=127.0.0.1 port=%d user=u dbname=d sslmode=disable", port)}
	db, err := openForTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	src.db, src.config = db, cfg

	start := time.Now()
	checks, err := src.LiveChecks(context.Background(), []string{"t"})
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("LiveChecks took %s against a stuck server", took)
	}
	byName := checkNames(checks)
	for _, name := range []string{
		"PostgreSQL 10 or later", "wal_level = logical", "replication privilege",
		"a free replication slot", "a free WAL sender", "a primary key on every table",
		"permission to create the publication",
	} {
		c, ok := byName[name]
		if !ok {
			t.Errorf("no %q check in %+v", name, checks)
			continue
		}
		if c.OK {
			t.Errorf("%q passed against a stuck server", name)
		}
		if !strings.Contains(c.Detail, "no answer within") {
			t.Errorf("%q: detail %q doesn't say it timed out", name, c.Detail)
		}
	}
}

// TestLiveChecksPostgres runs the checks on XFERDB_TEST_CDC_PG: a table
// without a key, one with REPLICA IDENTITY NOTHING and one with a DEFERRABLE
// primary key (#176) each fail their own check, and a yugabyte-typed project
// there is told it's PostgreSQL.
func TestLiveChecksPostgres(t *testing.T) {
	cfg, db := cdcSource(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	keyed, nothing, nokey := "xferdb_lc_keyed_"+suffix, "xferdb_lc_nothing_"+suffix, "xferdb_lc_nokey_"+suffix
	deferred := "xferdb_lc_deferred_" + suffix
	mustExec(t, db, `CREATE TABLE `+keyed+` (id int PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE `+nothing+` (id int PRIMARY KEY)`)
	mustExec(t, db, `ALTER TABLE `+nothing+` REPLICA IDENTITY NOTHING`)
	mustExec(t, db, `CREATE TABLE `+nokey+` (id int)`)
	// Its replica identity is DEFAULT, which the identity check alone passes.
	mustExec(t, db, `CREATE TABLE `+deferred+` (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)`)
	defer mustExec(t, db, `DROP TABLE IF EXISTS `+keyed+`, `+nothing+`, `+nokey+`, `+deferred)

	src := &Source{db: db, config: cfg}
	checks, err := src.LiveChecks(context.Background(), []string{keyed, nothing, nokey, deferred})
	if err != nil {
		t.Fatal(err)
	}
	byName := checkNames(checks)
	if c := byName["a primary key on every table"]; c.OK || !strings.Contains(c.Detail, nokey) || strings.Contains(c.Detail, nothing) {
		t.Errorf("primary key check: %+v", c)
	}
	// The deferrable key fails its own check, naming the constraint; the
	// table without a key isn't counted, and the others pass.
	if c := byName["a primary key that isn't DEFERRABLE"]; c.OK || c.Detail != "1 of 3 tables with a DEFERRABLE key: "+deferred+" ("+deferred+"_pkey)" ||
		!strings.Contains(c.Fix, "DROP CONSTRAINT") || strings.Contains(c.Fix, "REPLICA IDENTITY FULL") {
		t.Errorf("deferrable key check: %+v", c)
	}
	c := byName["a replica identity logical replication can use"]
	if c.OK || !strings.Contains(c.Detail, nothing+" (NOTHING)") || strings.Contains(c.Detail, keyed) || !strings.Contains(c.Fix, "REPLICA IDENTITY DEFAULT") {
		t.Errorf("replica identity check: %+v", c)
	}
	if _, ok := byName["a free WAL sender"]; !ok {
		t.Error("no WAL sender check on PostgreSQL")
	}
	if _, ok := byName["source type matches the server"]; ok {
		t.Errorf("a postgres project on PostgreSQL got a type mismatch: %+v", checks)
	}

	ybCfg := cfg
	ybCfg.Type = adapters.TypeYugabyte
	checks, _ = (&Source{db: db, config: ybCfg}).LiveChecks(context.Background(), []string{keyed})
	if c, ok := checkNames(checks)["source type matches the server"]; !ok || c.OK {
		t.Errorf("a yugabyte project on PostgreSQL: %+v", checks)
	}
}

// TestLiveChecksYugabyte runs the checks on XFERDB_TEST_YB, a DSN for a
// YugabyteDB YSQL server: they finish promptly (no pg_stat_replication),
// recognise the server, and report YugabyteDB's retention. Read-only.
func TestLiveChecksYugabyte(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_YB")
	if dsn == "" {
		t.Skip("XFERDB_TEST_YB not set; skipping YugabyteDB readiness test")
	}
	cfg := adapters.ConnectionConfig{Type: adapters.TypeYugabyte, DSN: dsn}
	db, err := openForTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	checks, err := (&Source{db: db, config: cfg}).LiveChecks(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("took %s", time.Since(start))
	for _, c := range checks {
		t.Logf("ok=%v warning=%v %s: %s", c.OK, c.Warning, c.Name, c.Detail)
	}
	byName := checkNames(checks)
	if c := byName["YugabyteDB"]; !c.OK {
		t.Errorf("server not recognised as YugabyteDB: %+v", checks)
	}
	for _, name := range []string{"a free WAL sender", "WAL a slot may hold", "source type matches the server"} {
		if _, ok := byName[name]; ok {
			t.Errorf("unexpected %q check on YugabyteDB", name)
		}
	}
	for _, name := range []string{"replication commands enabled", "hybrid-time replication slots", "permission to create the heartbeat table",
		"a replica identity logical replication can use", "how long the source keeps changes"} {
		if _, ok := byName[name]; !ok {
			t.Errorf("no %q check on YugabyteDB", name)
		}
	}

	pgCfg := cfg
	pgCfg.Type = adapters.TypePostgres
	checks, _ = (&Source{db: db, config: pgCfg}).LiveChecks(context.Background(), nil)
	if c, ok := checkNames(checks)["source type matches the server"]; !ok || c.OK {
		t.Errorf("a postgres project on YugabyteDB: %+v", checks)
	}
}

func openForTest(cfg adapters.ConnectionConfig) (*sql.DB, error) {
	return sql.Open("postgres", buildDSN(cfg))
}
