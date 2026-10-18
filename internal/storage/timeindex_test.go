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

func openTestTimeIndex(t *testing.T, path string, baseOffset int64, maxBytes int) *timeIndex {
	t.Helper()
	ti, err := openTimeIndex(path, baseOffset, maxBytes)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ti.close()) })
	return ti
}

// fillTimeIndex appends (100k, base+10k) for k in 1..count.
func fillTimeIndex(t *testing.T, ti *timeIndex, count int) {
	t.Helper()
	for k := 1; k <= count; k++ {
		require.NoError(t, ti.maybeAppend(int64(100*k), testIndexBase+int64(10*k), false))
	}
}

func TestTimeIndexLookup(t *testing.T) {
	ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
	fillTimeIndex(t, ti, 3) // (100,1010) (200,1020) (300,1030)

	tests := []struct {
		name    string
		target  int64
		wantTs  int64
		wantOff int64
	}{
		{"negative target", -5, -1, testIndexBase},
		{"before first entry", 99, -1, testIndexBase},
		{"exact first", 100, 100, 1010},
		{"between entries", 150, 100, 1010},
		{"exact middle", 200, 200, 1020},
		{"just before last", 299, 200, 1020},
		{"exact last", 300, 300, 1030},
		{"after last", math.MaxInt64, 300, 1030},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, off := ti.lookup(tc.target)
			require.Equal(t, tc.wantTs, ts)
			require.Equal(t, tc.wantOff, off)
		})
	}
	ts, off := ti.lastEntry()
	require.Equal(t, int64(300), ts)
	require.Equal(t, int64(1030), off)
}

func TestTimeIndexEmpty(t *testing.T) {
	ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
	ts, off := ti.lookup(math.MaxInt64)
	require.Equal(t, int64(-1), ts)
	require.Equal(t, testIndexBase, off)
	ts, off = ti.lastEntry()
	require.Equal(t, int64(-1), ts)
	require.Equal(t, testIndexBase, off)
	require.NoError(t, ti.sanityCheck(testIndexBase))
}

func TestTimeIndexMaybeAppendSkips(t *testing.T) {
	tests := []struct {
		name    string
		base    int64
		ts      int64
		offset  int64
		wantLen int
	}{
		{"greater timestamp appended", testIndexBase, 300, 1030, 3},
		{"equal timestamp skipped", testIndexBase, 200, 1030, 2},
		{"smaller timestamp skipped", testIndexBase, 150, 1030, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), tc.base, 1200)
			fillTimeIndex(t, ti, 2)
			require.NoError(t, ti.maybeAppend(tc.ts, tc.offset, false))
			require.Equal(t, tc.wantLen, ti.entries())
		})
	}

	t.Run("all-zero entry skipped", func(t *testing.T) {
		ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), 0, 1200)
		require.NoError(t, ti.maybeAppend(0, 0, true))
		require.Equal(t, 0, ti.entries())
		require.NoError(t, ti.maybeAppend(0, 5, false), "timestamp 0 at a non-zero offset is fine")
		require.Equal(t, 1, ti.entries())
	})

	t.Run("negative timestamp", func(t *testing.T) {
		ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
		require.NoError(t, ti.maybeAppend(-10, testIndexBase, false))
		require.NoError(t, ti.maybeAppend(-5, testIndexBase+1, false))
		require.Equal(t, 2, ti.entries())
		ts, off := ti.lookup(-7)
		require.Equal(t, int64(-10), ts)
		require.Equal(t, testIndexBase, off)
	})

	t.Run("invalid offsets rejected", func(t *testing.T) {
		ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
		fillTimeIndex(t, ti, 2)
		require.Error(t, ti.maybeAppend(500, 1010, false), "offset going backwards")
		require.Error(t, ti.maybeAppend(500, testIndexBase-1, false), "below base")
		require.Error(t, ti.maybeAppend(500, testIndexBase+math.MaxUint32+1, false), "overflow")
		require.NoError(t, ti.maybeAppend(500, 1020, false), "same offset as last is allowed")
		require.Equal(t, 3, ti.entries())
	})
}

func TestTimeIndexForcedSlot(t *testing.T) {
	tests := []struct {
		name     string
		maxBytes int
		capacity int
	}{
		{"exact multiple", 36, 3},
		{"not a multiple of entry size", 47, 3},
		{"two entries (minimum config)", 24, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.timeindex")
			ti := openTestTimeIndex(t, path, testIndexBase, tc.maxBytes)
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, int64(tc.capacity*timeIndexEntrySize), fi.Size())

			for k := 1; k < tc.capacity; k++ {
				require.False(t, ti.isFull())
				require.NoError(t, ti.maybeAppend(int64(k), testIndexBase+int64(k), false))
			}
			require.True(t, ti.isFull())
			require.ErrorIs(t, ti.maybeAppend(1000, testIndexBase+1000, false), ErrIndexFull)
			require.Equal(t, tc.capacity-1, ti.entries())
			// A non-increasing forced entry is a no-op, not an error.
			require.NoError(t, ti.maybeAppend(1, testIndexBase+1000, true))
			require.Equal(t, tc.capacity-1, ti.entries())
			require.NoError(t, ti.maybeAppend(1000, testIndexBase+1000, true))
			require.Equal(t, tc.capacity, ti.entries())
			require.ErrorIs(t, ti.maybeAppend(2000, testIndexBase+2000, true), ErrIndexFull)
			ts, off := ti.lastEntry()
			require.Equal(t, int64(1000), ts)
			require.Equal(t, testIndexBase+1000, off)
		})
	}
}

func TestTimeIndexTruncateTo(t *testing.T) {
	tests := []struct {
		name     string
		truncate int64
		want     int
		wantTs   int64
	}{
		{"before first", 1000, 0, -1},
		{"at first", 1010, 0, -1},
		{"between", 1025, 2, 200},
		{"at last", 1040, 3, 300},
		{"after last", 9999, 4, 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.timeindex")
			ti := openTestTimeIndex(t, path, testIndexBase, 1200)
			fillTimeIndex(t, ti, 4)
			ti.truncateTo(tc.truncate)
			require.Equal(t, tc.want, ti.entries())
			ts, _ := ti.lastEntry()
			require.Equal(t, tc.wantTs, ts)

			require.NoError(t, ti.maybeAppend(5000, 9000, false))
			require.NoError(t, ti.close())
			re := openTestTimeIndex(t, path, testIndexBase, 1200)
			require.Equal(t, tc.want+1, re.entries())
		})
	}

	t.Run("reset", func(t *testing.T) {
		ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
		fillTimeIndex(t, ti, 4)
		ti.reset()
		require.Equal(t, 0, ti.entries())
		ts, off := ti.lookup(math.MaxInt64)
		require.Equal(t, int64(-1), ts)
		require.Equal(t, testIndexBase, off)
		require.NoError(t, ti.maybeAppend(1, testIndexBase, false))
		require.Equal(t, 1, ti.entries())
	})
}

func TestTimeIndexReopen(t *testing.T) {
	for _, trim := range []bool{true, false} {
		name := "after crash without trim"
		if trim {
			name = "after trim"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.timeindex")
			ti, err := openTimeIndex(path, testIndexBase, 1200)
			require.NoError(t, err)
			fillTimeIndex(t, ti, 5)
			if trim {
				require.NoError(t, ti.trim())
				fi, err := os.Stat(path)
				require.NoError(t, err)
				require.Equal(t, int64(5*timeIndexEntrySize), fi.Size())
			}
			require.NoError(t, ti.flush())
			require.NoError(t, ti.close())

			re := openTestTimeIndex(t, path, testIndexBase, 1200)
			require.Equal(t, 5, re.entries())
			ts, off := re.lookup(350)
			require.Equal(t, int64(300), ts)
			require.Equal(t, int64(1030), off)
			require.NoError(t, re.maybeAppend(600, 1060, false))
			require.Equal(t, 6, re.entries())
		})
	}
}

func TestTimeIndexAppendAfterTrim(t *testing.T) {
	ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
	fillTimeIndex(t, ti, 2)
	require.NoError(t, ti.trim())
	require.NoError(t, ti.maybeAppend(300, 1030, false))
	require.NoError(t, ti.maybeAppend(400, 1040, false))
	ts, off := ti.lastEntry()
	require.Equal(t, int64(400), ts)
	require.Equal(t, int64(1040), off)
}

type rawTimeEntry struct {
	ts  int64
	rel uint32
}

func TestTimeIndexOpenValidRun(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		entries []rawTimeEntry
		want    int
	}{
		{"empty file", 0, nil, 0},
		{"zero tail", 120, []rawTimeEntry{{1, 0}, {2, 1}, {3, 1}}, 3},
		{"non-increasing timestamp garbage", 120, []rawTimeEntry{{1, 0}, {5, 1}, {5, 2}, {9, 3}}, 2},
		{"decreasing offset garbage", 120, []rawTimeEntry{{1, 4}, {5, 6}, {9, 5}}, 2},
		{"leading zero entry", 120, []rawTimeEntry{{0, 0}, {5, 1}}, 0},
		{"timestamp zero at non-zero offset", 120, []rawTimeEntry{{0, 3}, {5, 4}}, 2},
		{"trimmed file", 24, []rawTimeEntry{{1, 0}, {2, 1}}, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "0.timeindex")
			buf := make([]byte, tc.size)
			for k, e := range tc.entries {
				binary.BigEndian.PutUint64(buf[k*12:], uint64(e.ts))
				binary.BigEndian.PutUint32(buf[k*12+8:], e.rel)
			}
			require.NoError(t, os.WriteFile(path, buf, fileMode))
			ti := openTestTimeIndex(t, path, testIndexBase, 120)
			require.Equal(t, tc.want, ti.entries())
			require.NoError(t, ti.sanityCheck(testIndexBase+100))

			require.NoError(t, ti.maybeAppend(1000, testIndexBase+50, false))
			require.NoError(t, ti.close())
			re := openTestTimeIndex(t, path, testIndexBase, 120)
			require.Equal(t, tc.want+1, re.entries(), "garbage past the valid run is not resurrected")
		})
	}
}

func TestTimeIndexSanityCheck(t *testing.T) {
	ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, 1200)
	fillTimeIndex(t, ti, 3) // last offset 1030
	tests := []struct {
		name       string
		nextOffset int64
		wantErr    bool
	}{
		{"offsets below next offset", 1031, false},
		{"offset equals next offset", 1030, true},
		{"offset above next offset", 1015, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ti.sanityCheck(tc.nextOffset)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrCorruptIndex)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTimeIndexClosedAndRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0.timeindex")
	newPath := filepath.Join(dir, "0.timeindex.deleted")
	ti, err := openTimeIndex(path, testIndexBase, 1200)
	require.NoError(t, err)
	fillTimeIndex(t, ti, 2)

	require.NoError(t, ti.rename(newPath))
	require.Equal(t, newPath, ti.path)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	ts, off := ti.lookup(150)
	require.Equal(t, int64(100), ts)
	require.Equal(t, int64(1010), off)
	require.NoError(t, ti.maybeAppend(300, 1030, false))
	require.NoError(t, ti.flush())

	require.NoError(t, ti.close())
	require.NoError(t, ti.close())
	ts, off = ti.lookup(math.MaxInt64)
	require.Equal(t, int64(-1), ts)
	require.Equal(t, testIndexBase, off)
	ts, off = ti.lastEntry()
	require.Equal(t, int64(-1), ts)
	require.Equal(t, testIndexBase, off)
	require.ErrorIs(t, ti.maybeAppend(400, 1040, true), ErrClosed)
	require.ErrorIs(t, ti.sanityCheck(2000), ErrClosed)
	require.ErrorIs(t, ti.flush(), ErrClosed)
	require.ErrorIs(t, ti.trim(), ErrClosed)
	ti.truncateTo(0)
	ti.reset()
}

func TestTimeIndexConcurrentLookup(t *testing.T) {
	const total = 1000
	ti := openTestTimeIndex(t, filepath.Join(t.TempDir(), "0.timeindex"), testIndexBase, (total+1)*timeIndexEntrySize)

	// Entry k (1-based) is (5k, base+2k).
	isAppended := func(ts, off int64) bool {
		if ts == -1 && off == testIndexBase {
			return true
		}
		return ts > 0 && ts%5 == 0 && ts/5 <= total && off == testIndexBase+2*(ts/5)
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
				lastTs, lastOff := int64(-1), testIndexBase
				for target := int64(-1); target <= 5*total+1; target += int64(1 + g) {
					ts, off := ti.lookup(target)
					if !isAppended(ts, off) {
						errs <- "lookup returned an entry that was never appended"
						return
					}
					if ts > target {
						errs <- "lookup returned an entry above the target"
						return
					}
					if ts < lastTs || off < lastOff {
						errs <- "lookup results not monotonic in target"
						return
					}
					lastTs, lastOff = ts, off
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
		require.NoError(t, ti.maybeAppend(int64(5*k), testIndexBase+int64(2*k), false))
	}
	close(done)
	wg.Wait()
	close(errs)
	for e := range errs {
		require.Fail(t, e)
	}
	require.Equal(t, total, ti.entries())
}
