//go:build !linux

package loadgen

import "errors"

// freeBytes is unsupported on this platform; the free-space check is skipped.
func freeBytes(string) (int64, error) {
	return 0, errors.New("free space unknown on this platform")
}
