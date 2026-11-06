package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
)

// TestLiveStreams leaves a stream behind the way a crashed teardown would
// (slot and publication, plus a heartbeat row on YugabyteDB), finds it and
// drops it. On XFERDB_TEST_CDC_PG and XFERDB_TEST_YB, whichever are set.
func TestLiveStreams(t *testing.T) {
	ran := false
	for _, c := range []struct{ env, typ string }{
		{"XFERDB_TEST_CDC_PG", adapters.TypePostgres},
		{"XFERDB_TEST_YB", adapters.TypeYugabyte},
	} {
		dsn := os.Getenv(c.env)
		if dsn == "" {
			continue
		}
		ran = true
		t.Run(c.typ, func(t *testing.T) { testLiveStreams(t, adapters.ConnectionConfig{Type: c.typ, DSN: dsn}) })
	}
	if !ran {
		t.Skip("neither XFERDB_TEST_CDC_PG nor XFERDB_TEST_YB set")
	}
}

func testLiveStreams(t *testing.T, cfg adapters.ConnectionConfig) {
	ctx := context.Background()
	src := &Source{}
	if err := src.Connect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	db := src.db
	yb := cfg.Type == adapters.TypeYugabyte
	name := fmt.Sprintf("xferdb_leftover%d", time.Now().UnixNano())
	table := name + "_t"
	mustExec(t, db, `CREATE TABLE `+table+` (id int PRIMARY KEY)`)
	defer mustExec(t, db, `DROP TABLE IF EXISTS `+table)
	mustExec(t, db, `ALTER TABLE `+table+` REPLICA IDENTITY DEFAULT`)
	cs := &ChangeSource{cfg: cfg, slot: name, pub: name, db: db, yb: yb}
	if yb {
		if err := cs.prepareHeartbeat(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pubTables := table
	if yb {
		pubTables += ", " + heartbeatTable
	}
	mustExec(t, db, `CREATE PUBLICATION `+name+` FOR TABLE `+pubTables)
	if yb {
		if _, err := cs.createHybridTimeSlot(ctx); err != nil {
			t.Fatal(err)
		}
	} else {
		mustExec(t, db, `SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, name)
	}
	defer func() { // whatever happens
		db.Exec(`SELECT pg_drop_replication_slot($1)`, name)
		db.Exec(`DROP PUBLICATION IF EXISTS ` + name)
	}()

	find := func() *adapters.LiveStream {
		streams, err := src.LiveStreams(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for i := range streams {
			if streams[i].Name == name {
				return &streams[i]
			}
		}
		return nil
	}
	st := find()
	if st == nil || !st.Slot || !st.Publication || st.Heartbeat != yb || (st.RetainedBytes == nil) == !yb {
		t.Fatalf("stream = %+v", st)
	}
	if err := src.DropLiveStream(ctx, "not_ours"); err == nil {
		t.Fatal("dropped a name that isn't a stream's")
	}
	// A new YugabyteDB slot counts as active for a minute.
	deadline := time.Now().Add(100 * time.Second)
	for {
		err := src.DropLiveStream(ctx, name)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "in use") || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}
	if st := find(); st != nil {
		t.Fatalf("still there after the drop: %+v", st)
	}
}
