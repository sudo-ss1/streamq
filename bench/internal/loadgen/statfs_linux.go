//go:build linux

package loadgen

import (
	"fmt"
	"syscall"
)

// freeBytes returns the space available to unprivileged users on dir's filesystem.
func freeBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * st.Bsize, nil
}
