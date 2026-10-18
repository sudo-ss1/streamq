package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
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

	mm         *mmapFile   // nil once closed; guarded by mu
	maxEntries int         // capacity of the mapping
	trimmed    atomic.Bool // file was shrunk by trim and must be re-extended before appending
}

// openTimeIndex opens or creates the time index at path (see openOffsetIndex; maxBytes is rounded
// down to a multiple of 12). The valid entry count is the leading run of entries that are not
// all-zero, with strictly increasing timestamps and non-decreasing offsets.
//
// An entry is (largest batch maxTimestamp seen so far in the segment, baseOffset of the batch
// where that maximum was first seen).
func openTimeIndex(path string, baseOffset int64, maxBytes int) (*timeIndex, error) {
	maxEntries := maxBytes / timeIndexEntrySize
	if maxEntries < 1 {
		return nil, fmt.Errorf("storage: time index %s: max bytes %d below entry size", path, maxBytes)
	}
	mm, origSize, err := openMmapFile(path, maxEntries*timeIndexEntrySize)
	if err != nil {
		return nil, err
	}
	t := &timeIndex{path: path, baseOffset: baseOffset, mm: mm, maxEntries: maxEntries}
	n := 0
	var prevTs int64
	var prevRel uint32
	for ; n < maxEntries; n++ {
		ts, rel := t.entryAt(n)
		if ts == 0 && rel == 0 {
			break
		}
		if n > 0 && (ts <= prevTs || rel < prevRel) {
			break
		}
		prevTs, prevRel = ts, rel
	}
	t.n.Store(int64(n))
	if err := zeroTail(mm, origSize, int64(n*timeIndexEntrySize)); err != nil {
		return nil, errors.Join(err, mm.close())
	}
	return t, nil
}

// entryAt decodes entry k. The caller must hold mu (read or write) with the index open.
func (t *timeIndex) entryAt(k int) (ts int64, rel uint32) {
	b := t.mm.data[k*timeIndexEntrySize:]
	return int64(binary.BigEndian.Uint64(b)), binary.BigEndian.Uint32(b[8:])
}

// maybeAppend adds (ts, offset) if ts is greater than the last entry's timestamp and the entry
// would not be all-zero; otherwise it is a no-op. Unless force is set, an append that would take
// the last free slot returns ErrIndexFull (the last slot is reserved for the forced final entry
// written at roll); with force it may use that slot.
func (t *timeIndex) maybeAppend(ts int64, offset int64, force bool) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mm == nil {
		return fmt.Errorf("storage: time index %s: %w", t.path, ErrClosed)
	}
	relative := offset - t.baseOffset
	if relative < 0 || relative > math.MaxUint32 {
		return fmt.Errorf("storage: time index %s: offset %d not representable relative to base %d", t.path, offset, t.baseOffset)
	}
	rel := uint32(relative)
	n := int(t.n.Load())
	if n > 0 {
		lastTs, lastRel := t.entryAt(n - 1)
		if ts <= lastTs {
			return nil
		}
		if rel < lastRel {
			return fmt.Errorf("storage: time index %s: offset %d before last entry offset %d",
				t.path, offset, t.baseOffset+int64(lastRel))
		}
	}
	if ts == 0 && rel == 0 {
		return nil
	}
	limit := t.maxEntries - 1
	if force {
		limit = t.maxEntries
	}
	if n >= limit {
		return fmt.Errorf("storage: time index %s: %w", t.path, ErrIndexFull)
	}
	if t.trimmed.Load() {
		if err := t.mm.truncate(int64(len(t.mm.data))); err != nil {
			return err
		}
		t.trimmed.Store(false)
	}
	b := t.mm.data[n*timeIndexEntrySize:]
	binary.BigEndian.PutUint64(b, uint64(ts))
	binary.BigEndian.PutUint32(b[8:], rel)
	t.n.Store(int64(n + 1))
	return nil
}

// lookup returns the largest entry with timestamp <= target, or (-1, baseOffset) if none.
func (t *timeIndex) lookup(target int64) (int64, int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mm == nil {
		return -1, t.baseOffset
	}
	n := int(t.n.Load())
	k := sort.Search(n, func(k int) bool {
		ts, _ := t.entryAt(k)
		return ts > target
	})
	if k == 0 {
		return -1, t.baseOffset
	}
	ts, rel := t.entryAt(k - 1)
	return ts, t.baseOffset + int64(rel)
}

// lastEntry returns the last entry, or (-1, baseOffset) if empty.
func (t *timeIndex) lastEntry() (int64, int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := int(t.n.Load())
	if t.mm == nil || n == 0 {
		return -1, t.baseOffset
	}
	ts, rel := t.entryAt(n - 1)
	return ts, t.baseOffset + int64(rel)
}

// truncateTo drops every entry with offset >= offset.
func (t *timeIndex) truncateTo(offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mm == nil {
		return
	}
	n := int(t.n.Load())
	k := sort.Search(n, func(k int) bool {
		_, rel := t.entryAt(k)
		return t.baseOffset+int64(rel) >= offset
	})
	t.dropFromLocked(k, n)
}

// reset drops every entry.
func (t *timeIndex) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mm == nil {
		return
	}
	t.dropFromLocked(0, int(t.n.Load()))
}

// dropFromLocked keeps entries [0, k), zeroes [k, n) and re-extends a trimmed file. The caller
// holds mu for writing.
func (t *timeIndex) dropFromLocked(k, n int) {
	clear(t.mm.data[k*timeIndexEntrySize : n*timeIndexEntrySize])
	t.n.Store(int64(k))
	if t.trimmed.Load() {
		// Best effort: if re-extension fails, the next append retries it.
		if err := t.mm.truncate(int64(len(t.mm.data))); err == nil {
			t.trimmed.Store(false)
		}
	}
}

// entries returns the published entry count.
func (t *timeIndex) entries() int { return int(t.n.Load()) }

// isFull reports whether another non-forced entry would not fit.
func (t *timeIndex) isFull() bool { return int(t.n.Load()) >= t.maxEntries-1 }

// sanityCheck verifies timestamps strictly increase and offsets are within [base, nextOffset).
func (t *timeIndex) sanityCheck(nextOffset int64) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mm == nil {
		return fmt.Errorf("storage: time index %s: %w", t.path, ErrClosed)
	}
	n := int(t.n.Load())
	var prevTs int64
	var prevOff int64
	for k := range n {
		ts, rel := t.entryAt(k)
		off := t.baseOffset + int64(rel)
		if k > 0 && (ts <= prevTs || off < prevOff) {
			return fmt.Errorf("storage: time index %s: entry %d (%d, %d) not after (%d, %d): %w",
				t.path, k, ts, off, prevTs, prevOff, ErrCorruptIndex)
		}
		if off >= nextOffset {
			return fmt.Errorf("storage: time index %s: entry %d offset %d >= next offset %d: %w",
				t.path, k, off, nextOffset, ErrCorruptIndex)
		}
		prevTs, prevOff = ts, off
	}
	return nil
}

// trim shrinks the file to exactly the used entries (see offsetIndex.trim).
func (t *timeIndex) trim() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mm == nil {
		return fmt.Errorf("storage: time index %s: %w", t.path, ErrClosed)
	}
	if err := t.mm.truncate(t.n.Load() * timeIndexEntrySize); err != nil {
		return err
	}
	t.trimmed.Store(true)
	return nil
}

// flush fsyncs the index file.
func (t *timeIndex) flush() error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mm == nil {
		return fmt.Errorf("storage: time index %s: %w", t.path, ErrClosed)
	}
	return t.mm.sync()
}

// rename renames the index file to newPath.
func (t *timeIndex) rename(newPath string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := os.Rename(t.path, newPath); err != nil {
		return fmt.Errorf("storage: rename time index: %w", err)
	}
	t.path = newPath
	return nil
}

// close unmaps and closes the file.
func (t *timeIndex) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mm == nil {
		return nil
	}
	err := t.mm.close()
	t.mm = nil
	return err
}
