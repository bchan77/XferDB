//go:build !unix

package worker

func lockRole(dir, role string) (func(), error) { return func() {}, nil }
