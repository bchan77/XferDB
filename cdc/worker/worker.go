// Package worker runs a live migration's capture and apply loops as their
// own processes (`xferdb cdc capture|apply --dir <dir>`), started by the
// server, one pair per live migration (#102). A worker that crashes or runs
// out of memory takes down only its own migration, and a server restart
// leaves running workers running.
//
// A worker's directory holds everything it needs and everything it reports:
//
//	spec.json          what to run (written by the server, mode 0600: it holds credentials)
//	queue/             the disk queue between capture and apply
//	<role>.addr        the status endpoint: "127.0.0.1:port token"
//	<role>.error       why the last run stopped, if it failed
//	<role>.lock        held while a worker of that role runs
//
// Workers never write the server's metadb. The status endpoint is plain
// HTTP on loopback with a random token rather than a Unix socket, whose path
// is limited to about 104 bytes on macOS and would not fit under a data dir.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/adapters"
	"gitea.homelab.local/nextdevops/XferDB/cdc"
	"gitea.homelab.local/nextdevops/XferDB/registry"
)

// Roles.
const (
	RoleCapture = "capture"
	RoleApply   = "apply"
)

// Spec is what a live migration's workers run.
type Spec struct {
	MigrationID string                    `json:"migration_id"`
	Source      adapters.ConnectionConfig `json:"source"`
	Target      adapters.ConnectionConfig `json:"target"`
	Tables      []string                  `json:"tables"`
	// StartPos is where capture streams from when its queue is empty: the
	// position the slot was created at, before the snapshot copy.
	StartPos cdc.Position `json:"start_pos"`
	// SegmentBytes is the queue's segment size; 0 means the default.
	SegmentBytes int64            `json:"segment_bytes,omitempty"`
	Apply        cdc.ApplyOptions `json:"apply"`
}

// QueueDir is the queue inside a worker directory.
func QueueDir(dir string) string { return filepath.Join(dir, "queue") }

// WriteSpec saves spec in dir, readable only by its owner.
func WriteSpec(dir string, spec *Spec) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, "spec.json"), b)
}

// ReadSpec loads the spec in dir.
func ReadSpec(dir string) (*Spec, error) {
	b, err := os.ReadFile(filepath.Join(dir, "spec.json"))
	if err != nil {
		return nil, err
	}
	var s Spec
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("spec.json: %w", err)
	}
	return &s, nil
}

// Status is what a worker reports on its status endpoint.
type Status struct {
	Role      string             `json:"role"`
	PID       int                `json:"pid"`
	StartedAt time.Time          `json:"started_at"`
	Capture   *cdc.CaptureStatus `json:"capture,omitempty"`
	Apply     *cdc.ApplyStatus   `json:"apply,omitempty"`
}

// Run runs a worker of role on dir until ctx is done or the loop fails. A
// failure is also written to <role>.error, for the server to show after the
// process has gone.
func Run(ctx context.Context, dir, role string) (err error) {
	if role != RoleCapture && role != RoleApply {
		return fmt.Errorf("unknown worker role %q", role)
	}
	release, err := lockRole(dir, role)
	if err != nil {
		// Not this run's failure: leave the running worker's files alone.
		return err
	}
	defer release()
	defer func() {
		if err != nil && ctx.Err() == nil {
			_ = writeFileAtomic(filepath.Join(dir, role+".error"), []byte(err.Error()))
		}
	}()
	_ = os.Remove(filepath.Join(dir, role+".error"))
	spec, err := ReadSpec(dir)
	if err != nil {
		return err
	}
	st := Status{Role: role, PID: os.Getpid(), StartedAt: time.Now().UTC()}
	switch role {
	case RoleCapture:
		return runCapture(ctx, dir, spec, st)
	case RoleApply:
		return runApply(ctx, dir, spec, st)
	}
	return fmt.Errorf("unknown worker role %q", role)
}

func runCapture(ctx context.Context, dir string, spec *Spec, st Status) error {
	src, err := registry.NewChangeSource(ctx, spec.Source, spec.MigrationID)
	if err != nil {
		return err
	}
	defer src.Close()
	q, err := cdc.OpenWriter(QueueDir(dir), spec.SegmentBytes)
	if err != nil {
		return err
	}
	defer q.Close()
	c := &cdc.Capturer{Source: src, Queue: q, Start: spec.StartPos}
	stop, err := serveStatus(dir, RoleCapture, func() Status {
		s := c.Status()
		st.Capture = &s
		return st
	})
	if err != nil {
		return err
	}
	defer stop()
	return c.Run(ctx)
}

func runApply(ctx context.Context, dir string, spec *Spec, st Status) error {
	tgt, err := registry.NewTarget(spec.Target.Type)
	if err != nil {
		return err
	}
	ap, ok := tgt.(adapters.ChangeApplier)
	if !ok {
		return fmt.Errorf("a %s target can't apply live changes", spec.Target.Type)
	}
	if err := tgt.Connect(ctx, spec.Target); err != nil {
		return err
	}
	defer tgt.Close()
	if err := os.MkdirAll(QueueDir(dir), 0o700); err != nil {
		return err
	}
	opts := spec.Apply
	opts.RemoveApplied = true
	a := &cdc.Applier{Stream: spec.MigrationID, Dir: QueueDir(dir), Target: ap, Opts: opts}
	refetch, closeSrc, err := sourceRefetch(ctx, spec.Source)
	if err != nil {
		return err
	}
	defer closeSrc()
	a.Refetch = refetch
	stop, err := serveStatus(dir, RoleApply, func() Status {
		s := a.Status()
		st.Apply = &s
		return st
	})
	if err != nil {
		return err
	}
	defer stop()
	return a.Run(ctx)
}

// sourceRefetch connects to the source for Applier.Refetch.
func sourceRefetch(ctx context.Context, cfg adapters.ConnectionConfig) (func(context.Context, string, map[string]interface{}) (map[string]interface{}, bool, error), func(), error) {
	src, err := registry.NewSource(cfg.Type)
	if err != nil {
		return nil, nil, err
	}
	lookup, ok := src.(adapters.RowLookup)
	if !ok {
		return nil, func() {}, nil
	}
	if err := src.Connect(ctx, cfg); err != nil {
		return nil, nil, fmt.Errorf("connect to source: %w", err)
	}
	refetch := func(ctx context.Context, table string, key map[string]interface{}) (map[string]interface{}, bool, error) {
		cols := make([]string, 0, len(key))
		for k := range key {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		vals := make([]interface{}, len(cols))
		for i, c := range cols {
			vals[i] = key[c]
		}
		rows, err := lookup.LookupRows(ctx, table, cols, [][]interface{}{vals})
		if err != nil || len(rows) == 0 {
			return nil, false, err
		}
		return rows[0], true, nil
	}
	return refetch, func() { src.Close() }, nil
}

// serveStatus serves status on a loopback port and writes its address and
// token to <role>.addr.
func serveStatus(dir, role string, status func() Status) (func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tok := make([]byte, 16)
	if _, err := rand.Read(tok); err != nil {
		ln.Close()
		return nil, err
	}
	token := hex.EncodeToString(tok)
	var mu sync.Mutex
	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			mu.Lock()
			s := status()
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(s)
		}),
	}
	go srv.Serve(ln) //nolint:errcheck
	addrFile := filepath.Join(dir, role+".addr")
	if err := writeFileAtomic(addrFile, []byte(ln.Addr().String()+" "+token+"\n")); err != nil {
		srv.Close()
		return nil, err
	}
	return func() {
		os.Remove(addrFile)
		srv.Close()
	}, nil
}

// ErrAlreadyRunning means another worker of the same role holds the
// directory.
var ErrAlreadyRunning = errors.New("worker already running")

// ErrNotRunning means a worker has no status endpoint: it isn't running.
var ErrNotRunning = errors.New("worker not running")

// ReadStatus asks the worker of role on dir for its status. If it isn't
// running, the error says why it stopped when it left a reason.
func ReadStatus(ctx context.Context, dir, role string) (*Status, error) {
	b, err := os.ReadFile(filepath.Join(dir, role+".addr"))
	if os.IsNotExist(err) {
		if reason, err := os.ReadFile(filepath.Join(dir, role+".error")); err == nil {
			return nil, fmt.Errorf("%w: %s", ErrNotRunning, strings.TrimSpace(string(reason)))
		}
		return nil, ErrNotRunning
	}
	if err != nil {
		return nil, err
	}
	addr, token, ok := strings.Cut(strings.TrimSpace(string(b)), " ")
	if !ok {
		return nil, fmt.Errorf("%s.addr is malformed", role)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/status", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		// Killed without removing its address file.
		return nil, fmt.Errorf("%w: %v", ErrNotRunning, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker status: %s", resp.Status)
	}
	var s Status
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
