// Package logring keeps the server's most recent log records in memory, as
// JSON lines, so a support bundle has the log even when the server was started
// without --log-file.
package logring

import (
	"context"
	"log/slog"
	"regexp"
	"sync"
)

// Ring holds the last N log lines (JSON, one record each), credentials
// redacted. Safe for concurrent use.
type Ring struct {
	mu    sync.Mutex
	lines [][]byte
	next  int
	full  bool
}

// New returns a ring keeping the last n lines.
func New(n int) *Ring {
	if n < 1 {
		n = 1
	}
	return &Ring{lines: make([][]byte, n)}
}

// Write stores p as one line. slog's JSON handler writes each record in a
// single call, so a line is a whole record.
func (r *Ring) Write(p []byte) (int, error) {
	line := Redact(append([]byte(nil), p...))
	r.mu.Lock()
	r.lines[r.next] = line
	r.next = (r.next + 1) % len(r.lines)
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
	return len(p), nil
}

// Lines returns the stored lines, oldest first.
func (r *Ring) Lines() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]byte
	if r.full {
		out = append(out, r.lines[r.next:]...)
	}
	return append(out, r.lines[:r.next]...)
}

// Cap is how many lines the ring keeps.
func (r *Ring) Cap() int { return len(r.lines) }

var (
	// scheme://user:secret@host
	urlCreds = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s"@:]*:[^/\s"]*@`)
	// password=secret, pwd: secret, "password":"secret" (JSON-escaped quotes too)
	keyValue = regexp.MustCompile(`(?i)((?:password|passwd|pwd|secret|token)\\?"?\s*[=:]\s*\\?"?)[^\s"&;\\]+`)
)

// Redact masks credentials in a log line: the user and password of a URL,
// and the value of password/secret/token keys.
func Redact(b []byte) []byte {
	b = urlCreds.ReplaceAll(b, []byte("${1}***:***@"))
	return keyValue.ReplaceAll(b, []byte("${1}***"))
}

// Handler sends each record to next and also, as JSON, to the ring.
type Handler struct {
	next slog.Handler
	ring slog.Handler
	skip func(slog.Record) bool
}

// NewHandler wraps next so every record it handles is also kept in r. The
// ring keeps records at level and above, except those skip (if set) reports.
func NewHandler(next slog.Handler, r *Ring, level slog.Leveler, skip func(slog.Record) bool) *Handler {
	return &Handler{next: next, ring: slog.NewJSONHandler(r, &slog.HandlerOptions{Level: level}), skip: skip}
}

// SkipOKRequests is a skip function for NewHandler: it leaves out the request
// log's lines for requests that succeeded (the web UI polls every second or
// two, which would push everything else out of the ring), keeping 4xx/5xx.
func SkipOKRequests(rec slog.Record) bool {
	if rec.Message != "http.request" {
		return false
	}
	ok := true
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "status" {
			ok = a.Value.Resolve().Int64() < 400
			return false
		}
		return true
	})
	return ok
}

func (h *Handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l) || h.ring.Enabled(ctx, l)
}

func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	if h.ring.Enabled(ctx, rec.Level) && (h.skip == nil || !h.skip(rec)) {
		_ = h.ring.Handle(ctx, rec.Clone())
	}
	if h.next.Enabled(ctx, rec.Level) {
		return h.next.Handle(ctx, rec)
	}
	return nil
}

func (h *Handler) WithAttrs(as []slog.Attr) slog.Handler {
	return &Handler{next: h.next.WithAttrs(as), ring: h.ring.WithAttrs(as), skip: h.skip}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{next: h.next.WithGroup(name), ring: h.ring.WithGroup(name), skip: h.skip}
}
