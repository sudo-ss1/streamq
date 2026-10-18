package storage

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const testIndexBase = int64(1000)

func openTestOffsetIndex(t *testing.T, path string, maxBytes int) *offsetIndex {
	t.Helper()
	idx, err := openOffsetIndex(path, testIndexBase, maxBytes)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, idx.close()) })
	return idx
}

// fillOffsetIndex appends entries (base+10k, 100k) for k in 1..count.
func fillOffsetIndex(t *testing.T, idx *offsetIndex, count int) {
	t.Helper()
	for k := 1; k <= count; k++ {
		require.NoError(t, idx.append(testIndexBase+int64(10*k), uint32(100*k)))
	}
}

func TestOffsetIndexLookup(t *testing.T) {
	idx := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "0.index"), 1024)
	fillOffsetIndex(t, idx, 3) // (1010,100) (1020,200) (1030,300)

	tests := []struct {
		name    string
		target  int64
		wantOff int64
		wantPos uint32
	}{
		{"below base", 5, testIndexBase, 0},
		{"base", testIndexBase, testIndexBase, 0},
		{"before first entry", 1009, testIndexBase, 0},
		{"exact first", 1010, 1010, 100},
		{"between entries", 1015, 1010, 100},
		{"exact middle", 1020, 1020, 200},
		{"just before last", 1029, 1020, 200},
		{"exact last", 1030, 1030, 300},
		{"after last", 99999, 1030, 300},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			off, pos := idx.lookup(tc.target)
			require.Equal(t, tc.wantOff, off)
			require.Equal(t, tc.wantPos, pos)
		})
	}
	require.Equal(t, 3, idx.entries())
	require.Equal(t, int64(1030), idx.lastOffset())
}

func TestOffsetIndexEmptyLookup(t *testing.T) {
	idx := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "0.index"), 1024)
	off, pos := idx.lookup(5000)
	require.Equal(t, testIndexBase, off)
	require.Equal(t, uint32(0), pos)
	require.Equal(t, testIndexBase, idx.lastOffset())
	require.Equal(t, 0, idx.entries())
	require.NoError(t, idx.sanityCheck(0))
}

func TestOffsetIndexAppendValidation(t *testing.T) {
	idx := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "0.index"), 1024)
	require.NoError(t, idx.append(1010, 100))

	tests := []struct {
		name   string
		offset int64
		pos    uint32
	}{
		{"same offset", 1010, 200},
		{"smaller offset", 1005, 200},
		{"same position", 1020, 100},
		{"smaller position", 1020, 50},
		{"below base", 999, 200},
		{"relative offset overflows uint32", testIndexBase + math.MaxUint32 + 1, 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, idx.append(tc.offset, tc.pos))
			require.Equal(t, 1, idx.entries())
		})
	}
	require.NoError(t, idx.append(testIndexBase+math.MaxUint32, 200))

	empty := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "1.index"), 1024)
	require.Error(t, empty.append(testIndexBase, 0), "all-zero entry")
}

func TestOffsetIndexTruncateTo(t *testing.T) {
	tests := []struct {
		name      string
		truncate  int64
		wantCount int
		wantLast  int64
	}{
		{"before first entry", 1005, 0, testIndexBase},
		{"at first entry", 1010, 0, testIndexBase},
		{"between entries", 1025, 2, 1020},
		{"at middle entry", 1020, 1, 1010},
		{"at last entry", 1040, 3, 1030},
		{"after last", 5000, 4, 1040},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.index")
			idx := openTestOffsetIndex(t, path, 1024)
			fillOffsetIndex(t, idx, 4)
			idx.truncateTo(tc.truncate)
			require.Equal(t, tc.wantCount, idx.entries())
			require.Equal(t, tc.wantLast, idx.lastOffset())
			off, _ := idx.lookup(math.MaxInt64)
			require.Equal(t, tc.wantLast, off)

			// Appending after truncation works and survives a reopen without trim
			// (the dropped tail was zeroed, so stale entries never come back).
			require.NoError(t, idx.append(tc.wantLast+1, uint32(100*tc.wantCount+1)))
			require.NoError(t, idx.flush())
			re, err := openOffsetIndex(path, testIndexBase, 1024)
			require.NoError(t, err)
			defer func() { require.NoError(t, re.close()) }()
			require.Equal(t, tc.wantCount+1, re.entries())
			require.Equal(t, tc.wantLast+1, re.lastOffset())
		})
	}
}

func TestOffsetIndexReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.index")
	idx := openTestOffsetIndex(t, path, 1024)
	fillOffsetIndex(t, idx, 5)
	idx.reset()
	require.Equal(t, 0, idx.entries())
	off, pos := idx.lookup(math.MaxInt64)
	require.Equal(t, testIndexBase, off)
	require.Equal(t, uint32(0), pos)
	require.NoError(t, idx.append(1001, 7))
	require.Equal(t, int64(1001), idx.lastOffset())
}

func TestOffsetIndexFull(t *testing.T) {
	tests := []struct {
		name     string
		maxBytes int
		capacity int
	}{
		{"exact multiple", 24, 3},
		{"not a multiple of entry size", 30, 3},
		{"single entry", 15, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.index")
			idx := openTestOffsetIndex(t, path, tc.maxBytes)
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, int64(tc.capacity*offsetIndexEntrySize), fi.Size())
			for k := 1; k <= tc.capacity; k++ {
				require.False(t, idx.isFull())
				require.NoError(t, idx.append(testIndexBase+int64(k), uint32(k)))
			}
			require.True(t, idx.isFull())
			require.ErrorIs(t, idx.append(testIndexBase+100, 100), ErrIndexFull)
			require.Equal(t, tc.capacity, idx.entries())
		})
	}

	_, err := openOffsetIndex(filepath.Join(t.TempDir(), "x.index"), 0, 7)
	require.Error(t, err)
}

func TestOffsetIndexReopen(t *testing.T) {
	tests := []struct {
		name string
		trim bool
	}{
		{"after trim", true},
		{"after crash without trim", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.index")
			idx, err := openOffsetIndex(path, testIndexBase, 1024)
			require.NoError(t, err)
			fillOffsetIndex(t, idx, 5)
			if tc.trim {
				require.NoError(t, idx.trim())
				fi, err := os.Stat(path)
				require.NoError(t, err)
				require.Equal(t, int64(5*offsetIndexEntrySize), fi.Size())
			}
			require.NoError(t, idx.flush())
			require.NoError(t, idx.close())

			re := openTestOffsetIndex(t, path, 1024)
			require.Equal(t, 5, re.entries())
			require.Equal(t, int64(1050), re.lastOffset())
			off, pos := re.lookup(1035)
			require.Equal(t, int64(1030), off)
			require.Equal(t, uint32(300), pos)
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, int64(1024), fi.Size(), "reopened file is extended again")
			require.NoError(t, re.append(1060, 600))
			require.Equal(t, 6, re.entries())
		})
	}
}

func TestOffsetIndexAppendAfterTrim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.index")
	idx := openTestOffsetIndex(t, path, 1024)
	fillOffsetIndex(t, idx, 2)
	require.NoError(t, idx.trim())
	// A trimmed index that becomes active again (log truncation) must still accept appends.
	require.NoError(t, idx.append(1030, 300))
	require.NoError(t, idx.append(1040, 400))
	require.Equal(t, int64(1040), idx.lastOffset())
	require.NoError(t, idx.trim())
	idx.truncateTo(1020)
	require.NoError(t, idx.append(1025, 250))
	off, pos := idx.lookup(1029)
	require.Equal(t, int64(1025), off)
	require.Equal(t, uint32(250), pos)
}

func writeRawOffsetEntries(t *testing.T, path string, size int, entries [][2]uint32) {
	t.Helper()
	buf := make([]byte, size)
	for k, e := range entries {
		binary.BigEndian.PutUint32(buf[k*8:], e[0])
		binary.BigEndian.PutUint32(buf[k*8+4:], e[1])
	}
	require.NoError(t, os.WriteFile(path, buf, fileMode))
}

func TestOffsetIndexOpenValidRun(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		entries [][2]uint32
		want    int
	}{
		{"empty file", 0, nil, 0},
		{"zero tail", 64, [][2]uint32{{1, 10}, {2, 20}, {3, 30}}, 3},
		{"decreasing offset garbage", 64, [][2]uint32{{1, 10}, {2, 20}, {1, 30}, {9, 90}}, 2},
		{"decreasing position garbage", 64, [][2]uint32{{1, 10}, {2, 20}, {3, 5}, {9, 90}}, 2},
		{"equal entry garbage", 64, [][2]uint32{{1, 10}, {1, 10}}, 1},
		{"leading zero entry", 64, [][2]uint32{{0, 0}, {2, 20}}, 0},
		{"full file", 32, [][2]uint32{{1, 10}, {2, 20}, {3, 30}, {4, 40}}, 4},
		{"trimmed file", 16, [][2]uint32{{1, 10}, {2, 20}}, 2},
		{"larger than maxBytes", 128, [][2]uint32{{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.index")
			writeRawOffsetEntries(t, path, tc.size, tc.entries)
			idx := openTestOffsetIndex(t, path, 32)
			require.Equal(t, tc.want, idx.entries())
			require.NoError(t, idx.sanityCheck(1<<20))
			if tc.want < 4 {
				// Appending past the valid run and reopening must not resurrect garbage.
				require.NoError(t, idx.append(testIndexBase+1000, 1000))
				require.NoError(t, idx.close())
				re := openTestOffsetIndex(t, path, 32)
				require.Equal(t, tc.want+1, re.entries())
			}
		})
	}
}

func TestOffsetIndexSanityCheck(t *testing.T) {
	idx := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "0.index"), 1024)
	fillOffsetIndex(t, idx, 3) // last position 300
	tests := []struct {
		name    string
		logSize int64
		wantErr bool
	}{
		{"position below log size", 301, false},
		{"position equals log size", 300, true},
		{"position above log size", 10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := idx.sanityCheck(tc.logSize)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrCorruptIndex)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOffsetIndexClosed(t *testing.T) {
	idx, err := openOffsetIndex(filepath.Join(t.TempDir(), "0.index"), testIndexBase, 1024)
	require.NoError(t, err)
	fillOffsetIndex(t, idx, 3)
	require.NoError(t, idx.close())
	require.NoError(t, idx.close(), "close is idempotent")

	off, pos := idx.lookup(1020)
	require.Equal(t, testIndexBase, off)
	require.Equal(t, uint32(0), pos)
	require.Equal(t, testIndexBase, idx.lastOffset())
	require.ErrorIs(t, idx.append(1040, 400), ErrClosed)
	require.ErrorIs(t, idx.sanityCheck(1000), ErrClosed)
	require.ErrorIs(t, idx.flush(), ErrClosed)
	require.ErrorIs(t, idx.trim(), ErrClosed)
	idx.truncateTo(0)
	idx.reset()
}

func TestOffsetIndexRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.index")
	newPath := filepath.Join(dir, "0.index.deleted")
	idx := openTestOffsetIndex(t, path, 1024)
	fillOffsetIndex(t, idx, 2)
	require.NoError(t, idx.rename(newPath))
	require.Equal(t, newPath, idx.path)
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)

	off, pos := idx.lookup(1015)
	require.Equal(t, int64(1010), off)
	require.Equal(t, uint32(100), pos)
	require.NoError(t, idx.append(1030, 300))
	require.NoError(t, idx.trim())
	require.NoError(t, idx.flush())
	fi, err := os.Stat(newPath)
	require.NoError(t, err)
	require.Equal(t, int64(3*offsetIndexEntrySize), fi.Size())

	require.Error(t, idx.rename(filepath.Join(dir, "missing", "x.index")))
	require.Equal(t, newPath, idx.path)
}

// TestOffsetIndexConcurrentLookup checks that lookups racing a single appender only ever see
// published entries (the race detector cannot see mmapped memory, so this checks values).
func TestOffsetIndexConcurrentLookup(t *testing.T) {
	const total = 2000
	idx := openTestOffsetIndex(t, filepath.Join(t.TempDir(), "0.index"), total*offsetIndexEntrySize)

	// Entry k (1-based) is (base+3k, 7k).
	isAppended := func(off int64, pos uint32) bool {
		if off == testIndexBase && pos == 0 {
			return true
		}
		rel := off - testIndexBase
		return rel > 0 && rel%3 == 0 && rel/3 <= total && int64(pos) == 7*(rel/3)
	}

	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan string, 4)
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				var lastOff int64
				var lastPos uint32
				for target := testIndexBase - 1; target <= testIndexBase+3*total+1; target += int64(1 + g) {
					off, pos := idx.lookup(target)
					if !isAppended(off, pos) {
						errs <- "lookup returned an entry that was never appended"
						return
					}
					if off > target && off != testIndexBase {
						errs <- "lookup returned an entry above the target"
						return
					}
					if off < lastOff || pos < lastPos {
						errs <- "lookup results not monotonic in target"
						return
					}
					lastOff, lastPos = off, pos
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}
	close(start)
	for k := 1; k <= total; k++ {
		require.NoError(t, idx.append(testIndexBase+int64(3*k), uint32(7*k)))
	}
	close(done)
	wg.Wait()
	close(errs)
	for e := range errs {
		require.Fail(t, e)
	}
	require.Equal(t, total, idx.entries())
}
