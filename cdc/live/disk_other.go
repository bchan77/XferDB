//go:build !unix

package live

func diskFree(string) (free, size int64, ok bool) { return 0, 0, false }
