package live

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"gitea.homelab.local/nextdevops/XferDB/cdc/worker"
)

// Launcher starts and stops a live migration's worker processes.
type Launcher interface {
	// Start starts the worker of role on dir and returns without waiting
	// for it.
	Start(dir, role string) error
	// Stop stops the worker of role on dir, if it runs, and waits until it
	// has.
	Stop(ctx context.Context, dir, role string) error
}

// ExecLauncher runs workers as `<Path> cdc <role> --dir <dir>` processes in
// their own process group, so they outlive the server: a server restart
// finds them still running and carries on supervising them.
type ExecLauncher struct {
	// Path is the xferdb binary; empty means this process's own.
	Path string
}

// Start implements Launcher. The worker's output goes to <role>.log in dir.
func (l ExecLauncher) Start(dir, role string) error {
	path := l.Path
	if path == "" {
		p, err := os.Executable()
		if err != nil {
			return err
		}
		path = p
	}
	logf, err := os.OpenFile(filepath.Join(dir, role+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	cmd := exec.Command(path, "cdc", role, "--dir", dir)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s worker: %w", role, err)
	}
	go cmd.Wait() //nolint:errcheck // reap it if it exits while the server runs
	return nil
}

// Stop implements Launcher: SIGTERM, then SIGKILL if the worker hasn't gone
// after 30s. The worker's PID comes from its status endpoint, so a worker
// started by an earlier server process is stopped too.
func (l ExecLauncher) Stop(ctx context.Context, dir, role string) error {
	st, err := worker.ReadStatus(ctx, dir, role)
	if errors.Is(err, worker.ErrNotRunning) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := syscall.Kill(st.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(st.PID, 0) != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	syscall.Kill(st.PID, syscall.SIGKILL) //nolint:errcheck
	return nil
}

// InProcessLauncher runs workers as goroutines of this process, for tests,
// where os.Executable is the test binary rather than xferdb.
type InProcessLauncher struct {
	mu      sync.Mutex
	running map[string]*inProcWorker
}

type inProcWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Start implements Launcher.
func (l *InProcessLauncher) Start(dir, role string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.running == nil {
		l.running = map[string]*inProcWorker{}
	}
	key := dir + "\x00" + role
	if w, ok := l.running[key]; ok {
		select {
		case <-w.done:
		default:
			return nil // still running
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &inProcWorker{cancel: cancel, done: make(chan struct{})}
	l.running[key] = w
	go func() {
		defer close(w.done)
		worker.Run(ctx, dir, role) //nolint:errcheck // reported through <role>.error
	}()
	return nil
}

// Stop implements Launcher.
func (l *InProcessLauncher) Stop(ctx context.Context, dir, role string) error {
	l.mu.Lock()
	w, ok := l.running[dir+"\x00"+role]
	l.mu.Unlock()
	if !ok {
		return nil
	}
	w.cancel()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StopAll stops every worker it started, as a server crash in a test.
func (l *InProcessLauncher) StopAll() {
	l.mu.Lock()
	ws := make([]*inProcWorker, 0, len(l.running))
	for _, w := range l.running {
		ws = append(ws, w)
	}
	l.mu.Unlock()
	for _, w := range ws {
		w.cancel()
		<-w.done
	}
}
