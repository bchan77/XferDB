//go:build unix

package live

import "golang.org/x/sys/unix"

// diskFree returns the bytes free to an unprivileged user, and the size,
// of the filesystem holding path.
func diskFree(path string) (free, size int64, ok bool) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, false
	}
	return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), true
}
