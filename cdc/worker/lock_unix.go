//go:build unix

package worker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockRole takes <role>.lock in dir for as long as the worker runs, so a
// second worker of the same role, started while one is still coming up,
// exits instead of streaming or applying alongside it. The lock goes with
// the process, however it ends.
func lockRole(dir, role string) (release func(), err error) {
	f, err := os.OpenFile(filepath.Join(dir, role+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("lock %s worker: %w", role, err)
	}
	return func() { f.Close() }, nil
}
