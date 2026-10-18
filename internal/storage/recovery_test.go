package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

func segPath(dir string, base int64, suffix string) string {
	return filepath.Join(dir, segmentFileName(base, suffix))
}

// requireEveryOffsetReadable reads each offset individually, which exercises the rebuilt index
// for every position.
func requireEveryOffsetReadable(t *testing.T, l *Log, to int64) {
	t.Helper()
	for off := int64(0); off < to; off++ {
		res, err := l.Read(context.Background(), off, 1, to)
		require.NoError(t, err)
		recs := decodeAll(t, res.Data)
		require.NotEmpty(t, recs, "offset %d", off)
		require.LessOrEqual(t, recs[0].Offset, off)
		require.GreaterOrEqual(t, recs[len(recs)-1].Offset, off)
	}
}

func TestRecoveryTruncatesPartialBatch(t *testing.T) {
	// Cut points relative to the start of the last batch: inside the header, just after it,
	// and one byte short of the end.
	for _, cut := range []int64{1, 30, record.HeaderSize, record.HeaderSize + 5, -1} {
		t.Run("", func(t *testing.T) {
			dir := t.TempDir()
			cfg := smallConfig()
			l := openTestLog(t, dir, cfg, newFakeClock(), 0)
			for range 10 {
				appendRecords(t, l.Log, 3, 1000)
			}
			active := l.active()
			lastStart := active.size.Load()
			appendRecords(t, l.Log, 3, 1000)
			end := active.size.Load()
			base := active.baseOffset
			l.close(t)

			pos := lastStart + cut
			if cut < 0 {
				pos = end + cut
			}
			require.NoError(t, os.Truncate(segPath(dir, base, logSuffix), pos))

			// Even a clean recovery point does not skip the last segment.
			l = openTestLog(t, dir, cfg, newFakeClock(), 33)
			defer l.close(t)
			require.Equal(t, int64(30), l.LogEndOffset())
			require.Equal(t, int64(30), l.RecoveryPoint())
			st, err := os.Stat(segPath(dir, base, logSuffix))
			require.NoError(t, err)
			require.Equal(t, lastStart, st.Size())
			requireEveryOffsetReadable(t, l.Log, 30)
			info := appendRecords(t, l.Log, 2, 1000)
			require.Equal(t, int64(30), info.FirstOffset)
			requireContiguous(t, readAll(t, l.Log, 0, 32), 0, 32)
		})
	}
}

func TestRecoveryCorruptCRCInMiddleSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	for l.SegmentCount() < 3 {
		appendRecords(t, l.Log, 4, 1000)
	}
	appendRecords(t, l.Log, 4, 1000)
	segs := l.loadSegments()
	mid, third := segs[1], segs[2].baseOffset
	// Corrupt a CRC-covered byte of the third batch of the middle segment.
	var pos int64
	var corruptOffset int64
	for i := 0; i < 3; i++ {
		bi, err := mid.readBatchInfo(pos, mid.size.Load())
		require.NoError(t, err)
		corruptOffset = bi.baseOffset
		if i < 2 {
			pos += bi.size
		}
	}
	l.close(t)

	f, err := os.OpenFile(segPath(dir, mid.baseOffset, logSuffix), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xde, 0xad}, pos+record.HeaderSize+2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// recoveryPoint 0 simulates an unclean shutdown with no checkpoint.
	l = openTestLog(t, dir, cfg, newFakeClock(), 0)
	defer l.close(t)
	require.Equal(t, corruptOffset, l.LogEndOffset())
	require.Equal(t, 2, l.SegmentCount())
	for _, suffix := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
		_, err := os.Stat(segPath(dir, third, suffix))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	requireEveryOffsetReadable(t, l.Log, corruptOffset)
	info := appendRecords(t, l.Log, 1, 1000)
	require.Equal(t, corruptOffset, info.FirstOffset)
}

func TestRecoveryRebuildsBadIndexBelowRecoveryPoint(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	for l.SegmentCount() < 3 {
		appendRecords(t, l.Log, 4, 1000)
	}
	leo := l.LogEndOffset()
	first := l.loadSegments()[0]
	require.Positive(t, first.index.entries())
	l.close(t)

	// Overwrite the first offset-index entry with a position past the end of the log.
	var entry [8]byte
	binary.BigEndian.PutUint32(entry[0:], 1)
	binary.BigEndian.PutUint32(entry[4:], 0xFFFFFF00)
	f, err := os.OpenFile(segPath(dir, 0, indexSuffix), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(entry[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	l = openTestLog(t, dir, cfg, newFakeClock(), leo)
	defer l.close(t)
	require.Equal(t, leo, l.LogEndOffset())
	requireEveryOffsetReadable(t, l.Log, leo)
}

func TestRecoveryIgnoresLoweredMaxMessageBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	cfg.SegmentBytes = 2 << 20
	cfg.MaxMessageBytes = 1 << 20
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	_, err := l.Append(context.Background(), buildBatch(t, 1, bytes.Repeat([]byte{7}, 900<<10)), 0)
	require.NoError(t, err)
	appendRecords(t, l.Log, 1, 1)
	l.close(t)

	cfg.MaxMessageBytes = 1024
	l = openTestLog(t, dir, cfg, newFakeClock(), 0)
	defer l.close(t)
	require.Equal(t, int64(2), l.LogEndOffset())
}

func TestRecoveryCleansDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	appendRecords(t, l.Log, 2, 1)
	l.close(t)

	junk := []string{
		segmentFileName(500, logSuffix) + deletedSuffix,
		segmentFileName(500, indexSuffix) + deletedSuffix,
		segmentFileName(900, indexSuffix),     // orphan: no .log
		segmentFileName(900, timeIndexSuffix), // orphan: no .log
	}
	for _, name := range junk {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("junk"), 0o600))
	}
	l = openTestLog(t, dir, cfg, newFakeClock(), 2)
	defer l.close(t)
	for _, name := range junk {
		_, err := os.Stat(filepath.Join(dir, name))
		require.ErrorIs(t, err, os.ErrNotExist, name)
	}
	require.Equal(t, int64(2), l.LogEndOffset())
}

func TestRecoveryOffsetGapDeletesLaterSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	for l.SegmentCount() < 2 {
		appendRecords(t, l.Log, 4, 1000)
	}
	second := l.active().baseOffset
	appendRecords(t, l.Log, 4, 1000)
	l.close(t)

	// Fake a gap: rename the second segment so its base no longer follows the first.
	for _, suffix := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
		require.NoError(t, os.Rename(segPath(dir, second, suffix), segPath(dir, second+100, suffix)))
	}
	l = openTestLog(t, dir, cfg, newFakeClock(), 0)
	defer l.close(t)
	require.Equal(t, second, l.LogEndOffset())
	require.Equal(t, 1, l.SegmentCount())
}

func TestRecoveryClampsRecoveryPoint(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, smallConfig(), newFakeClock(), 0)
	appendRecords(t, l.Log, 3, 1)
	l.close(t)
	l = openTestLog(t, dir, smallConfig(), newFakeClock(), 1000)
	defer l.close(t)
	require.Equal(t, int64(3), l.RecoveryPoint())
}

func TestRecoveryEmptyActiveSegmentAfterRoll(t *testing.T) {
	// A crash right after a roll leaves an empty newest segment; it must reopen cleanly.
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	for l.SegmentCount() < 2 {
		appendRecords(t, l.Log, 4, 1000)
	}
	leo := l.LogEndOffset()
	l.close(t)
	empty := segPath(dir, leo, logSuffix)
	require.NoError(t, os.WriteFile(empty, nil, 0o600))

	l = openTestLog(t, dir, cfg, newFakeClock(), 0)
	defer l.close(t)
	require.Equal(t, leo, l.LogEndOffset())
	require.Equal(t, leo, l.active().baseOffset)
	requireContiguous(t, readAll(t, l.Log, 0, leo), 0, leo)
	appendRecords(t, l.Log, 1, 1000)
	require.Equal(t, leo+1, l.LogEndOffset())
}
