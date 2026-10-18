package storage

import (
	"errors"
	"sync"
	"sync/atomic"
)

var errNotImplemented = errors.New("storage: not implemented")

// offsetIndex is the sparse, memory-mapped offset index of one segment (SPEC §3.1). Entries are
// 8 bytes, big-endian: (relativeOffset uint32, position uint32).
//
// The single writer (holding Log.mu) appends; the entry bytes are written before the entry count
// is published, and readers binary-search only the published entries. mu is held for reading by
// lookups and for writing by close, truncateTo and reset, so lookups never touch unmapped memory.
type offsetIndex struct {
	mu         sync.RWMutex
	path       string
	baseOffset int64
	n          atomic.Int64 // published entry count
}

// openOffsetIndex opens or creates the index at path, extends the file to maxBytes rounded down
// to a multiple of 8 (sparse) and maps it. The valid entry count is the leading run of entries
// that are not all-zero and strictly increase in both relative offset and position.
func openOffsetIndex(path string, baseOffset int64, maxBytes int) (*offsetIndex, error) {
	_, _, _ = path, baseOffset, maxBytes
	return nil, errNotImplemented
}

// append adds an entry. offset must be greater than the last entry's offset and fit in uint32
// relative to the base; ErrIndexFull if there is no room.
func (i *offsetIndex) append(offset int64, pos uint32) error {
	_, _ = offset, pos
	return errNotImplemented
}

// lookup returns the largest entry with offset <= target, or (baseOffset, 0) if none.
func (i *offsetIndex) lookup(target int64) (int64, uint32) {
	_ = target
	return i.baseOffset, 0
}

// truncateTo drops every entry with offset >= offset.
func (i *offsetIndex) truncateTo(offset int64) { _ = offset }

// reset drops every entry.
func (i *offsetIndex) reset() {}

// entries returns the published entry count.
func (i *offsetIndex) entries() int { return int(i.n.Load()) }

// lastOffset returns the offset of the last entry, or baseOffset if empty.
func (i *offsetIndex) lastOffset() int64 { return i.baseOffset }

// isFull reports whether another entry would not fit.
func (i *offsetIndex) isFull() bool { return false }

// sanityCheck verifies entries are strictly increasing and every position is < logSize.
func (i *offsetIndex) sanityCheck(logSize int64) error {
	_ = logSize
	return errNotImplemented
}

// trim shrinks the file to exactly the used entries. The mapping is kept; nothing past the
// published entries is ever accessed, so concurrent lookups stay safe.
func (i *offsetIndex) trim() error { return errNotImplemented }

// flush fsyncs the index file (on Linux this also writes back MAP_SHARED dirty pages).
func (i *offsetIndex) flush() error { return errNotImplemented }

// rename renames the index file to newPath; the mapping stays valid.
func (i *offsetIndex) rename(newPath string) error {
	_ = newPath
	return errNotImplemented
}

// close unmaps and closes the file. Later lookups return (baseOffset, 0); later appends fail
// with ErrClosed.
func (i *offsetIndex) close() error { return errNotImplemented }
