package storage

import (
	"sync"
	"sync/atomic"
)

// timeIndex is the sparse, memory-mapped time index of one segment (SPEC §3.1). Entries are 12
// bytes, big-endian: (timestamp int64, relativeOffset uint32). Concurrency is as for offsetIndex.
type timeIndex struct {
	mu         sync.RWMutex
	path       string
	baseOffset int64
	n          atomic.Int64 // published entry count
}

// openTimeIndex opens or creates the time index at path (see openOffsetIndex; maxBytes is rounded
// down to a multiple of 12). The valid entry count is the leading run of entries that are not
// all-zero, with strictly increasing timestamps and non-decreasing offsets.
//
// An entry is (largest batch maxTimestamp seen so far in the segment, baseOffset of the batch
// where that maximum was first seen).
func openTimeIndex(path string, baseOffset int64, maxBytes int) (*timeIndex, error) {
	_, _, _ = path, baseOffset, maxBytes
	return nil, errNotImplemented
}

// maybeAppend adds (ts, offset) if ts is greater than the last entry's timestamp and the entry
// would not be all-zero; otherwise it is a no-op. Unless force is set, an append that would take
// the last free slot returns ErrIndexFull (the last slot is reserved for the forced final entry
// written at roll); with force it may use that slot.
func (t *timeIndex) maybeAppend(ts int64, offset int64, force bool) error {
	_, _, _ = ts, offset, force
	return errNotImplemented
}

// lookup returns the largest entry with timestamp <= target, or (-1, baseOffset) if none.
func (t *timeIndex) lookup(target int64) (int64, int64) {
	_ = target
	return -1, t.baseOffset
}

// lastEntry returns the last entry, or (-1, baseOffset) if empty.
func (t *timeIndex) lastEntry() (int64, int64) { return -1, t.baseOffset }

// truncateTo drops every entry with offset >= offset.
func (t *timeIndex) truncateTo(offset int64) { _ = offset }

// reset drops every entry.
func (t *timeIndex) reset() {}

// entries returns the published entry count.
func (t *timeIndex) entries() int { return int(t.n.Load()) }

// isFull reports whether another non-forced entry would not fit.
func (t *timeIndex) isFull() bool { return false }

// sanityCheck verifies timestamps strictly increase and offsets are within [base, nextOffset).
func (t *timeIndex) sanityCheck(nextOffset int64) error {
	_ = nextOffset
	return errNotImplemented
}

// trim shrinks the file to exactly the used entries (see offsetIndex.trim).
func (t *timeIndex) trim() error { return errNotImplemented }

// flush fsyncs the index file.
func (t *timeIndex) flush() error { return errNotImplemented }

// rename renames the index file to newPath.
func (t *timeIndex) rename(newPath string) error {
	_ = newPath
	return errNotImplemented
}

// close unmaps and closes the file.
func (t *timeIndex) close() error { return errNotImplemented }
