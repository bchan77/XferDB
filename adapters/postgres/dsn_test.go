package postgres

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

func TestBuildDSNQuotesValues(t *testing.T) {
	got := buildDSN(adapters.ConnectionConfig{
		Host: "db.local", Port: 5433, Database: "app", Username: "o'brien", Password: "", SSLMode: "disable",
	})
	want := `host='db.local' port=5433 dbname='app' user='o\'brien' password='' sslmode='disable'`
	if got != want {
		t.Errorf("buildDSN = %s\nwant      %s", got, want)
	}
	if got := dsnValue(`a b\c`); got != `'a b\\c'` {
		t.Errorf("dsnValue = %s", got)
	}
}

// TestConnectWithEmptyPassword connects with no password and SSL off to a
// live server that trusts the connection. Before #130 the empty password
// swallowed `sslmode=disable`, so lib/pq demanded SSL. Opt-in: set
// XFERDB_TEST_PG_TRUST to the "host:port" of such a server.
func TestConnectWithEmptyPassword(t *testing.T) {
	addr := os.Getenv("XFERDB_TEST_PG_TRUST")
	if addr == "" {
		t.Skip("XFERDB_TEST_PG_TRUST not set; skipping live PostgreSQL test")
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portStr)
	src := NewSource()
	err = src.Connect(context.Background(), adapters.ConnectionConfig{
		Type: "postgres", Host: host, Port: port, Database: "postgres", Username: "postgres", SSLMode: "disable",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	src.Close()
}
