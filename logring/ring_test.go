package logring

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestRingKeepsLastLinesRedacted(t *testing.T) {
	r := New(3)
	var out bytes.Buffer
	log := slog.New(NewHandler(slog.NewTextHandler(&out, nil), r, slog.LevelDebug, SkipOKRequests))
	for i := 0; i < 5; i++ {
		log.Info("step", "n", i)
		log.Info("http.request", "path", "/api/v1/health", "status", 200)
	}
	log.With("migration_id", "m1").Error("table_failed",
		"dsn", "postgres://bob:hunter2@db:5432/x", "detail", "password=hunter2 sslmode=require")
	lines := r.Lines()
	if len(lines) != 3 {
		t.Fatalf("%d lines, want 3", len(lines))
	}
	var first map[string]any
	if err := json.Unmarshal(lines[0], &first); err != nil || first["n"] != float64(3) {
		t.Errorf("oldest kept line %s (%v), want n=3", lines[0], err)
	}
	last := string(lines[2])
	if strings.Contains(last, "hunter2") || !strings.Contains(last, `"migration_id":"m1"`) {
		t.Errorf("last line %s: want credentials masked and attrs kept", last)
	}
	if !strings.Contains(out.String(), "step") {
		t.Error("records not passed on to the wrapped handler")
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		`oracle://system:p@ss@h:1521/FREE`:    `oracle://***:***@h:1521/FREE`,
		`{"password":"s3cret","x":1}`:         `{"password":"***","x":1}`,
		`"msg":"connect password=abc host=h"`: `"msg":"connect password=*** host=h"`,
		`no secrets here`:                     `no secrets here`,
	} {
		if got := string(Redact([]byte(in))); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSkipOKRequestsKeepsErrors(t *testing.T) {
	r := New(10)
	log := slog.New(NewHandler(slog.DiscardHandler, r, slog.LevelInfo, SkipOKRequests))
	log.Info("http.request", "path", "/a", "status", 200)
	log.Warn("http.request", "path", "/b", "status", 500)
	if lines := r.Lines(); len(lines) != 1 || !strings.Contains(string(lines[0]), `"/b"`) {
		t.Errorf("kept %q, want only the failed request", lines)
	}
}
