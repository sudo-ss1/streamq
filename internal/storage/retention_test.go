package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fillSegments appends batches with timestamp ts until the log has n segments; it returns the
// base offsets of the segments.
func fillSegments(t *testing.T, l *Log, n int, ts func(seg int) int64) []int64 {
	t.Helper()
	for l.SegmentCount() < n {
		appendRecords(t, l, 4, ts(l.SegmentCount()-1))
	}
	var bases []int64
	for _, s := range l.loadSegments() {
		bases = append(bases, s.baseOffset)
	}
	return bases
}

func TestRetention(t *testing.T) {
	const day = int64(24 * time.Hour / time.Millisecond)
	tests := []struct {
		name        string
		retentionMs int64
		retBytes    func(segSize int64) int64
		segTs       []int64 // maxTimestamp per segment (index = segment number)
		limit       func(bases []int64) int64
		wantDeleted int
	}{
		{
			name: "time: expired prefix deleted, active kept", retentionMs: day,
			segTs: []int64{0, 0, 0, 0}, wantDeleted: 3,
		},
		{
			name: "time: stops at first non-expired segment", retentionMs: day,
			segTs: []int64{0, 10 * day, 0, 0}, wantDeleted: 1,
		},
		{
			name: "time: segment 1 not expired keeps expired segment 2", retentionMs: day,
			segTs: []int64{10 * day, 0, 0, 0}, wantDeleted: 0,
		},
		{
			name: "time: limit offset protects data above it", retentionMs: day,
			segTs: []int64{0, 0, 0, 0}, limit: func(b []int64) int64 { return b[2] }, wantDeleted: 2,
		},
		{
			name: "time: limit inside segment protects it", retentionMs: day,
			segTs: []int64{0, 0, 0, 0}, limit: func(b []int64) int64 { return b[1] + 1 }, wantDeleted: 1,
		},
		{
			name: "size: budget of two full segments keeps two", retentionMs: -1,
			retBytes: func(s int64) int64 { return 2 * s }, segTs: []int64{10 * day, 10 * day, 10 * day, 10 * day},
			wantDeleted: 1,
		},
		{
			name: "size: budget of one full segment keeps one", retentionMs: -1,
			retBytes: func(s int64) int64 { return s }, segTs: []int64{10 * day, 10 * day, 10 * day, 10 * day},
			wantDeleted: 2,
		},
		{
			name: "size: budget just below two segments keeps one", retentionMs: -1,
			retBytes: func(s int64) int64 { return 2*s - 1 }, segTs: []int64{10 * day, 10 * day, 10 * day, 10 * day},
			wantDeleted: 1,
		},
		{
			name: "infinite retention", retentionMs: -1,
			segTs: []int64{0, 0, 0, 0}, wantDeleted: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := smallConfig()
			cfg.RetentionMs = tt.retentionMs
			cfg.SegmentMs = 1000 * day
			clock := newFakeClock()
			clock.now = time.UnixMilli(11 * day)
			l := openTestLog(t, t.TempDir(), cfg, clock, 0)
			defer l.close(t)
			bases := fillSegments(t, l.Log, len(tt.segTs), func(seg int) int64 { return tt.segTs[seg] })
			if tt.retBytes != nil {
				// Budget in units of a full segment; the active segment counts too.
				l.cfg.RetentionBytes = tt.retBytes(l.loadSegments()[0].size.Load()) + l.active().size.Load()
			}
			limit := l.LogEndOffset()
			if tt.limit != nil {
				limit = tt.limit(bases)
			}
			n, err := l.DeleteOldSegments(context.Background(), clock.Now(), limit)
			require.NoError(t, err)
			require.Equal(t, tt.wantDeleted, n)
			require.Len(t, l.deleted, tt.wantDeleted)
			require.Equal(t, len(bases)-tt.wantDeleted, l.SegmentCount())
			require.Equal(t, bases[tt.wantDeleted], l.LogStartOffset())
			if tt.wantDeleted > 0 {
				_, err := l.Read(context.Background(), bases[tt.wantDeleted]-1, 100, l.LogEndOffset())
				require.ErrorIs(t, err, ErrOffsetOutOfRange)
			}
			leo := l.LogEndOffset()
			requireContiguous(t, readAll(t, l.Log, l.LogStartOffset(), leo), l.LogStartOffset(), leo)
		})
	}
}

func TestRetentionSafeDelete(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	cfg.RetentionMs = 1000
	clock := newFakeClock()
	m, err := OpenManager(context.Background(), managerConfig(dir, time.Minute), clock, discardLogger())
	require.NoError(t, err)
	l, err := m.GetOrCreateLog(context.Background(), TopicPartition{Topic: "t", Partition: 0}, cfg)
	require.NoError(t, err)
	for l.SegmentCount() < 3 {
		appendRecords(t, l, 4, 1)
	}
	old := l.loadSegments()[0]
	oldLog, oldIndex := old.logPath, old.index.path

	require.NoError(t, m.RunRetention(context.Background(), clock.Now(), nil))
	require.Equal(t, 2, m.PendingDeletes())
	require.Equal(t, l.loadSegments()[0].baseOffset, l.LogStartOffset())

	// Files are renamed, not removed, and a reader still holding the segment can read it.
	for _, p := range []string{oldLog, oldIndex} {
		_, err := os.Stat(p)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, err = os.Stat(p + deletedSuffix)
		require.NoError(t, err)
	}
	data, err := old.read(0, 1<<20, old.nextOffset.Load())
	require.NoError(t, err)
	requireContiguous(t, decodeAll(t, data), 0, old.nextOffset.Load())

	// Not yet due.
	clock.Advance(59 * time.Second)
	require.NoError(t, m.DeleteExpired(context.Background(), clock.Now()))
	require.Equal(t, 2, m.PendingDeletes())
	_, err = os.Stat(oldLog + deletedSuffix)
	require.NoError(t, err)

	// Due: closed and unlinked; a late reader gets an error, not a crash.
	clock.Advance(time.Second)
	require.NoError(t, m.DeleteExpired(context.Background(), clock.Now()))
	require.Equal(t, 0, m.PendingDeletes())
	for _, p := range []string{oldLog, oldIndex} {
		_, err := os.Stat(p + deletedSuffix)
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	_, err = old.read(0, 1<<20, old.nextOffset.Load())
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = l.Read(context.Background(), 0, 100, l.LogEndOffset())
	require.ErrorIs(t, err, ErrOffsetOutOfRange)
	require.NoError(t, m.Close(context.Background()))
}

func TestStartupRemovesLeftoverDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	cfg.RetentionMs = 1000
	clock := newFakeClock()
	ctx := context.Background()
	tp := TopicPartition{Topic: "t", Partition: 1}
	m, err := OpenManager(ctx, managerConfig(dir, time.Hour), clock, discardLogger())
	require.NoError(t, err)
	l, err := m.GetOrCreateLog(ctx, tp, cfg)
	require.NoError(t, err)
	for l.SegmentCount() < 2 {
		appendRecords(t, l, 4, 1)
	}
	deletedLog := l.loadSegments()[0].logPath + deletedSuffix
	require.NoError(t, m.RunRetention(ctx, clock.Now(), nil))
	_, err = os.Stat(deletedLog)
	require.NoError(t, err)

	// Simulate a crash: close the files without the Manager's cleanup, leaving ".deleted" files.
	m.pendingMu.Lock()
	for _, p := range m.pending {
		require.NoError(t, p.seg.close())
	}
	m.pending = nil
	m.pendingMu.Unlock()
	require.NoError(t, l.Close())

	m2, err := OpenManager(ctx, managerConfig(dir, time.Hour), clock, discardLogger())
	require.NoError(t, err)
	_, err = os.Stat(deletedLog)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, m2.Close(ctx))
}

func managerConfig(dir string, deleteDelay time.Duration) ManagerConfig {
	cfg := DefaultManagerConfig(dir)
	cfg.FileDeleteDelay = deleteDelay
	return cfg
}

func TestAppendRejectsNegativeTimestamps(t *testing.T) {
	cfg := smallConfig()
	cfg.RetentionMs = 7 * 24 * time.Hour.Milliseconds()
	l := openTestLog(t, t.TempDir(), cfg, newFakeClock(), 0)
	defer l.close(t)
	_, err := l.Append(context.Background(), buildBatch(t, -1, []byte("x")), 0)
	require.ErrorIs(t, err, ErrInvalidTimestamp)
	require.Equal(t, int64(0), l.LogEndOffset())
	// Timestamp 0 is valid and is not deleted while within retention of a clock at epoch+1s.
	appendRecords(t, l.Log, 1, 0)
}
