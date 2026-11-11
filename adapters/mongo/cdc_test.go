package mongo

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/state"
)

// A resume token from a MongoDB 8.0 insert event at cluster time
// Timestamp(1794415306, 2).
const testToken = "826AF49ACA000000022B042C0100296E5A100470CDE88A997B47BF9ACF9A8849B81074463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B657900461E5F6964002B02000004"

func TestTokenPosition(t *testing.T) {
	raw, _ := bson.Marshal(bson.D{{Key: "_data", Value: testToken}})
	p, err := tokenPosition(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.T != 1794415306 || p.I != 2 || p.Token != testToken {
		t.Fatalf("position = %+v", p)
	}
	for _, bad := range []bson.D{{{Key: "_data", Value: "00"}}, {{Key: "_data", Value: "zz"}}, {{Key: "x", Value: 1}}} {
		raw, _ := bson.Marshal(bad)
		if _, err := tokenPosition(raw); err == nil {
			t.Errorf("tokenPosition(%v) took it", bad)
		}
	}
}

func TestPositionRoundTrip(t *testing.T) {
	s := &ChangeSource{}
	for _, text := range []string{"1794415306.2", "1794415306.2/" + testToken} {
		p, err := s.ParsePosition(text)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.FormatPosition(p); got != text {
			t.Errorf("round trip %q = %q", text, got)
		}
	}
	for _, bad := range []string{"", "x", "1.", "1.2.3", "-1.2", "99999999999.1"} {
		if p, err := s.ParsePosition(bad); err == nil && len(p) > 0 {
			t.Errorf("ParsePosition(%q) = %q", bad, p)
		}
	}
}

func TestBehind(t *testing.T) {
	s := &ChangeSource{}
	pos := func(t, i uint32) cdc.Position { return position{T: t, I: i}.encode() }
	for _, c := range []struct {
		p, target cdc.Position
		want      int64
	}{
		{pos(100, 1), pos(100, 1), 0},
		{pos(100, 5), pos(100, 1), 0},
		{pos(101, 1), pos(100, 9), 0},
		{pos(100, 1), pos(100, 2), 1},
		{pos(100, 9), pos(103, 1), 3e6},
	} {
		if got := s.Behind(c.p, c.target); got != c.want {
			t.Errorf("Behind(%s, %s) = %d, want %d", c.p, c.target, got, c.want)
		}
	}
	if !cdc.PositionsAreTimes(s) {
		t.Error("a MongoDB source's positions are times")
	}
}

// Confirmed moves to the stream's high-water mark only while everything
// sent is acked: a change queued but not yet stored holds it back.
func TestConfirmed(t *testing.T) {
	s := &ChangeSource{}
	if s.Confirmed() != nil {
		t.Fatal("nothing confirmed yet")
	}
	s.sent = position{T: 10, I: 1, Token: "a"}
	s.noteIdle(position{T: 20, I: 1, Token: "b"})
	if err := s.Ack(context.Background(), position{T: 5, I: 1, Token: "c"}.encode()); err != nil {
		t.Fatal(err)
	}
	if got := string(s.Confirmed()); got != "5.1/c" {
		t.Fatalf("with a change in flight: confirmed %s, want 5.1/c", got)
	}
	s.Ack(context.Background(), position{T: 10, I: 1, Token: "a"}.encode()) //nolint:errcheck
	if got := string(s.Confirmed()); got != "20.1/b" {
		t.Fatalf("all acked: confirmed %s, want the high-water mark 20.1/b", got)
	}
	// An older ack doesn't move it back.
	s.Ack(context.Background(), position{T: 1, I: 1}.encode()) //nolint:errcheck
	if got := string(s.Confirmed()); got != "20.1/b" {
		t.Fatalf("after an older ack: confirmed %s", got)
	}
}

func TestChangeSourceSetPlan(t *testing.T) {
	s := &ChangeSource{}
	err := s.SetPlan([]state.SchemaPlanRow{
		{Collection: "users", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
		{Collection: "users", FieldName: "address", Strategy: "flatten"},
		{Collection: "users", FieldName: "address.city", PgColumn: "address_city", PgType: "text", Strategy: "direct"},
		{Collection: "users", FieldName: "secret", Strategy: "skip"},
		{Collection: "users", FieldName: "tags", PgColumn: "tags", PgType: "jsonb", Strategy: "as_jsonb"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.columns["users"]; len(got) != 4 || got[0] != "_id" || got[1] != "address_city" || got[2] != "tags" || got[3] != "_extra" {
		t.Errorf("columns = %v", got)
	}
	if got := s.keyCols["users"]; len(got) != 1 || got[0] != "_id" {
		t.Errorf("key = %v", got)
	}
	if err := s.SetPlan([]state.SchemaPlanRow{{Collection: "nokey", FieldName: "a", PgColumn: "a", Strategy: "direct"}}); err == nil {
		t.Error("a plan without a key was taken")
	}
}

// A document that lost a field still sets that column, to NULL: the row
// replaces the target's whole row.
func TestChangeFillsMissingColumns(t *testing.T) {
	s := &ChangeSource{}
	if err := s.SetPlan([]state.SchemaPlanRow{
		{Collection: "users", FieldName: "_id", PgColumn: "_id", PgType: "text", Strategy: "direct", IsPK: true},
		{Collection: "users", FieldName: "nick", PgColumn: "nick", PgType: "text", Strategy: "direct"},
	}); err != nil {
		t.Fatal(err)
	}
	oid := bson.NewObjectID()
	ev := &changeEvent{OperationType: "update", FullDocument: bson.D{{Key: "_id", Value: oid}, {Key: "new", Value: "x"}}}
	ev.NS.Coll = "users"
	c, ok, err := s.change(context.Background(), ev)
	if err != nil || !ok {
		t.Fatalf("change: %v %v", ok, err)
	}
	if c.Op != cdc.OpUpdate || c.Key["_id"] != oid.Hex() || c.Partial() {
		t.Fatalf("change = %+v", c)
	}
	if v, ok := c.After["nick"]; !ok || v != nil {
		t.Errorf("nick = %v (present %v), want NULL", v, ok)
	}
	if c.After["_extra"] != `{"new":"x"}` {
		t.Errorf("_extra = %v", c.After["_extra"])
	}

	del := &changeEvent{OperationType: "delete", DocumentKey: bson.D{{Key: "_id", Value: oid}}}
	del.NS.Coll = "users"
	c, ok, err = s.change(context.Background(), del)
	if err != nil || !ok || c.Op != cdc.OpDelete || len(c.Key) != 1 || c.Key["_id"] != oid.Hex() {
		t.Fatalf("delete = %+v %v %v", c, ok, err)
	}

	drop := &changeEvent{OperationType: "drop"}
	drop.NS.Coll = "users"
	if _, _, err := s.change(context.Background(), drop); err == nil {
		t.Error("a dropped collection didn't end the stream")
	}
}

func TestTypesBetween(t *testing.T) {
	// numbers (2) .. objectId (6): string, object, binData.
	got := typesBetween(2, 6)
	want := []string{"string", "symbol", "object", "binData"}
	if len(got) != len(want) {
		t.Fatalf("typesBetween = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("typesBetween = %v", got)
		}
	}
	if typesBetween(3, 4) != nil {
		t.Error("adjacent groups have nothing between them")
	}
}
