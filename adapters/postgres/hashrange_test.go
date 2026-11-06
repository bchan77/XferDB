package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// On YugabyteDB: a hash-sharded key's hash columns are found (only those,
// for a key with range columns too), and the hash ranges together read
// every row exactly once. A range-sharded key has none. Needs XFERDB_TEST_YB.
func TestHashRanges(t *testing.T) {
	dsn := os.Getenv("XFERDB_TEST_YB")
	if dsn == "" {
		t.Skip("XFERDB_TEST_YB not set")
	}
	ctx := context.Background()
	src := &Source{}
	if err := src.Connect(ctx, adapters.ConnectionConfig{Type: adapters.TypeYugabyte, DSN: dsn}); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	hashed, ranged := "xferdb_hash_"+suffix, "xferdb_range_"+suffix
	mustExec(t, src.db, `CREATE TABLE `+hashed+` (grp text, id int, v text, PRIMARY KEY ((grp, id) HASH, v ASC))`)
	mustExec(t, src.db, `CREATE TABLE `+ranged+` (id int, PRIMARY KEY (id ASC))`)
	defer mustExec(t, src.db, `DROP TABLE IF EXISTS `+hashed+`, `+ranged)
	mustExec(t, src.db, `INSERT INTO `+hashed+` SELECT 'g' || (i % 7), i, 'v' || i FROM generate_series(1, 5000) i`)

	cols, err := src.HashColumns(ctx, hashed)
	if err != nil || len(cols) != 2 || cols[0] != "grp" || cols[1] != "id" {
		t.Fatalf("hash columns = %v, %v", cols, err)
	}
	if cols, err := src.HashColumns(ctx, ranged); err != nil || cols != nil {
		t.Fatalf("range-sharded key: %v, %v", cols, err)
	}

	seen := map[string]bool{}
	const chunks = 13
	for i := 0; i < chunks; i++ {
		b, err := src.ReadHashRange(ctx, hashed, cols, i*adapters.HashSpace/chunks, (i+1)*adapters.HashSpace/chunks)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range b.Records {
			k := fmt.Sprint(r["grp"], "/", r["id"])
			if seen[k] {
				t.Fatalf("row %s read twice", k)
			}
			seen[k] = true
		}
	}
	if len(seen) != 5000 {
		t.Fatalf("read %d rows of 5000", len(seen))
	}

	// PostgreSQL has no hash sharding.
	pg := &Source{db: src.db, config: adapters.ConnectionConfig{Type: adapters.TypePostgres}}
	if cols, _ := pg.HashColumns(ctx, hashed); cols != nil {
		t.Fatalf("hash columns on a postgres source: %v", cols)
	}
}
