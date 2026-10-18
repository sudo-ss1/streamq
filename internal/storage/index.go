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

	mm         *mmapFile   // nil once closed; guarded by mu
	maxEntries int         // capacity of the mapping
	trimmed    atomic.Bool // file was shrunk by trim and must be re-extended before appending
}

// openOffsetIndex opens or creates the index at path, extends the file to maxBytes rounded down
// to a multiple of 8 (sparse) and maps it. The valid entry count is the leading run of entries
// that are not all-zero and strictly increase in both relative offset and position.
func openOffsetIndex(path string, baseOffset int64, maxBytes int) (*offsetIndex, error) {
	maxEntries := maxBytes / offsetIndexEntrySize
	if maxEntries < 1 {
		return nil, fmt.Errorf("storage: offset index %s: max bytes %d below entry size", path, maxBytes)
	}
	mm, origSize, err := openMmapFile(path, maxEntries*offsetIndexEntrySize)
	if err != nil {
		return nil, err
	}
	idx := &offsetIndex{path: path, baseOffset: baseOffset, mm: mm, maxEntries: maxEntries}
	n := 0
	var prevRel, prevPos uint32
	for ; n < maxEntries; n++ {
		rel, pos := idx.entryAt(n)
		if rel == 0 && pos == 0 {
			break
		}
		if n > 0 && (rel <= prevRel || pos <= prevPos) {
			break
		}
		prevRel, prevPos = rel, pos
	}
	idx.n.Store(int64(n))
	// Zero everything past the valid run (garbage or a stale tail) so it can never be
	// mistaken for valid entries after later appends and an unclean shutdown.
	if err := zeroTail(mm, origSize, int64(n*offsetIndexEntrySize)); err != nil {
		return nil, errors.Join(err, mm.close())
	}
	return idx, nil
}

// zeroTail makes the file exactly len(mm.data) bytes long with every byte at or after used
// zero, by cutting the file to used and extending it again (a sparse hole reads as zero).
func zeroTail(mm *mmapFile, origSize, used int64) error {
	if origSize <= used {
		return nil
	}
	if err := mm.truncate(used); err != nil {
		return err
	}
	return mm.truncate(int64(len(mm.data)))
}

// entryAt decodes entry k. The caller must hold mu (read or write) with the index open.
func (i *offsetIndex) entryAt(k int) (rel, pos uint32) {
	b := i.mm.data[k*offsetIndexEntrySize:]
	return binary.BigEndian.Uint32(b), binary.BigEndian.Uint32(b[4:])
}

// append adds an entry. offset must be greater than the last entry's offset and fit in uint32
// relative to the base; ErrIndexFull if there is no room.
func (i *offsetIndex) append(offset int64, pos uint32) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.mm == nil {
		return fmt.Errorf("storage: offset index %s: %w", i.path, ErrClosed)
	}
	relative := offset - i.baseOffset
	if relative < 0 || relative > math.MaxUint32 {
		return fmt.Errorf("storage: offset index %s: offset %d not representable relative to base %d", i.path, offset, i.baseOffset)
	}
	rel := uint32(relative)
	n := int(i.n.Load())
	if n > 0 {
		lastRel, lastPos := i.entryAt(n - 1)
		if rel <= lastRel || pos <= lastPos {
			return fmt.Errorf("storage: offset index %s: entry (%d, %d) not after last (%d, %d)",
				i.path, offset, pos, i.baseOffset+int64(lastRel), lastPos)
		}
	} else if rel == 0 && pos == 0 {
		return fmt.Errorf("storage: offset index %s: all-zero entry", i.path)
	}
	if n >= i.maxEntries {
		return fmt.Errorf("storage: offset index %s: %w", i.path, ErrIndexFull)
	}
	if i.trimmed.Load() {
		if err := i.mm.truncate(int64(len(i.mm.data))); err != nil {
			return err
		}
		i.trimmed.Store(false)
	}
	b := i.mm.data[n*offsetIndexEntrySize:]
	binary.BigEndian.PutUint32(b, rel)
	binary.BigEndian.PutUint32(b[4:], pos)
	i.n.Store(int64(n + 1))
	return nil
}

// lookup returns the largest entry with offset <= target, or (baseOffset, 0) if none.
func (i *offsetIndex) lookup(target int64) (int64, uint32) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.mm == nil || target < i.baseOffset {
		return i.baseOffset, 0
	}
	n := int(i.n.Load())
	// First entry with offset > target; the answer is the one before it.
	k := sort.Search(n, func(k int) bool {
		rel, _ := i.entryAt(k)
		return i.baseOffset+int64(rel) > target
	})
	if k == 0 {
		return i.baseOffset, 0
	}
	rel, pos := i.entryAt(k - 1)
	return i.baseOffset + int64(rel), pos
}

// truncateTo drops every entry with offset >= offset.
func (i *offsetIndex) truncateTo(offset int64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.mm == nil {
		return
	}
	n := int(i.n.Load())
	k := sort.Search(n, func(k int) bool {
		rel, _ := i.entryAt(k)
		return i.baseOffset+int64(rel) >= offset
	})
	i.dropFromLocked(k, n)
}

// reset drops every entry.
func (i *offsetIndex) reset() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.mm == nil {
		return
	}
	i.dropFromLocked(0, int(i.n.Load()))
}

// dropFromLocked keeps entries [0, k), zeroes [k, n) and re-extends a trimmed file so the
// segment can take appends again. The caller holds mu for writing.
func (i *offsetIndex) dropFromLocked(k, n int) {
	clear(i.mm.data[k*offsetIndexEntrySize : n*offsetIndexEntrySize])
	i.n.Store(int64(k))
	if i.trimmed.Load() {
		// Best effort: if re-extension fails, the next append retries it.
		if err := i.mm.truncate(int64(len(i.mm.data))); err == nil {
			i.trimmed.Store(false)
		}
	}
}

// entries returns the published entry count.
func (i *offsetIndex) entries() int { return int(i.n.Load()) }

// lastOffset returns the offset of the last entry, or baseOffset if empty.
func (i *offsetIndex) lastOffset() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	n := int(i.n.Load())
	if i.mm == nil || n == 0 {
		return i.baseOffset
	}
	rel, _ := i.entryAt(n - 1)
	return i.baseOffset + int64(rel)
}

// isFull reports whether another entry would not fit.
func (i *offsetIndex) isFull() bool { return int(i.n.Load()) >= i.maxEntries }

// sanityCheck verifies entries are strictly increasing and every position is < logSize.
func (i *offsetIndex) sanityCheck(logSize int64) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.mm == nil {
		return fmt.Errorf("storage: offset index %s: %w", i.path, ErrClosed)
	}
	n := int(i.n.Load())
	var prevRel, prevPos uint32
	for k := range n {
		rel, pos := i.entryAt(k)
		if k > 0 && (rel <= prevRel || pos <= prevPos) {
			return fmt.Errorf("storage: offset index %s: entry %d (%d, %d) not after (%d, %d): %w",
				i.path, k, rel, pos, prevRel, prevPos, ErrCorruptIndex)
		}
		prevRel, prevPos = rel, pos
	}
	if n > 0 && int64(prevPos) >= logSize {
		return fmt.Errorf("storage: offset index %s: last position %d >= log size %d: %w",
			i.path, prevPos, logSize, ErrCorruptIndex)
	}
	return nil
}

// trim shrinks the file to exactly the used entries. The mapping is kept; nothing past the
// published entries is ever accessed, so concurrent lookups stay safe.
func (i *offsetIndex) trim() error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.mm == nil {
		return fmt.Errorf("storage: offset index %s: %w", i.path, ErrClosed)
	}
	if err := i.mm.truncate(i.n.Load() * offsetIndexEntrySize); err != nil {
		return err
	}
	i.trimmed.Store(true)
	return nil
}

// flush fsyncs the index file (on Linux this also writes back MAP_SHARED dirty pages).
func (i *offsetIndex) flush() error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.mm == nil {
		return fmt.Errorf("storage: offset index %s: %w", i.path, ErrClosed)
	}
	return i.mm.sync()
}

// rename renames the index file to newPath; the mapping stays valid.
func (i *offsetIndex) rename(newPath string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := os.Rename(i.path, newPath); err != nil {
		return fmt.Errorf("storage: rename offset index: %w", err)
	}
	i.path = newPath
	return nil
}

// close unmaps and closes the file. Later lookups return (baseOffset, 0); later appends fail
// with ErrClosed.
func (i *offsetIndex) close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.mm == nil {
		return nil
	}
	err := i.mm.close()
	i.mm = nil
	return err
}
