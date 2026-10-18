package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

// leaderBatches appends n batches of 3 records to a leader log and returns them as replicated
// (offset-stamped) batches.
func leaderBatches(t *testing.T, n int) []record.Batch {
	t.Helper()
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	var out []record.Batch
	for i := range n {
		appendRecords(t, l.Log, 3, int64(1000+i))
	}
	res, err := l.Read(context.Background(), 0, 1<<30, l.LogEndOffset())
	require.NoError(t, err)
	for len(res.Data) > 0 {
		bs, err := record.SplitBatches(res.Data)
		require.NoError(t, err)
		out = append(out, bs...)
		next := bs[len(bs)-1].NextOffset()
		if next >= l.LogEndOffset() {
			break
		}
		res, err = l.Read(context.Background(), next, 1<<30, l.LogEndOffset())
		require.NoError(t, err)
	}
	return out
}

func TestAppendAsFollower(t *testing.T) {
	batches := leaderBatches(t, 5)
	f := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer f.close(t)
	ctx := context.Background()
	_, err := f.AppendAsFollower(ctx, batches[1])
	require.ErrorIs(t, err, ErrOffsetMismatch, "must start at the LEO")
	for _, b := range batches {
		info, err := f.AppendAsFollower(ctx, append(record.Batch(nil), b...))
		require.NoError(t, err)
		require.Equal(t, b.BaseOffset(), info.FirstOffset)
	}
	require.Equal(t, int64(15), f.LogEndOffset())
	res, err := f.Read(ctx, 0, 1<<30, 15)
	require.NoError(t, err)
	got, err := record.SplitBatches(res.Data)
	require.NoError(t, err)
	require.Equal(t, int32(7), got[0].LeaderEpoch(), "the leader's epoch is kept")
	corrupt := append(record.Batch(nil), batches[0]...)
	corrupt.SetBaseOffset(15)
	corrupt[len(corrupt)-1] ^= 0xff
	_, err = f.AppendAsFollower(ctx, corrupt)
	require.ErrorIs(t, err, ErrCorruptBatch)
}

func TestTruncateTo(t *testing.T) {
	tests := []struct {
		name    string
		offset  int64
		wantLEO int64
	}{
		{"no-op at LEO", 600, 600},
		{"inside the last segment at a batch boundary", 597, 597},
		{"inside a batch truncates the whole batch", 598, 597},
		{"across several segments", 150, 150},
		{"to the log start", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := smallConfig()
			l := openTestLog(t, dir, cfg, newFakeClock(), 0)
			for range 200 {
				appendRecords(t, l.Log, 3, 1000)
			}
			require.Greater(t, l.SegmentCount(), 5)
			require.NoError(t, l.Flush(context.Background()))
			newLEO, err := l.TruncateTo(context.Background(), tt.offset)
			require.NoError(t, err)
			require.Equal(t, tt.wantLEO, newLEO)
			require.Equal(t, tt.wantLEO, l.LogEndOffset())
			require.LessOrEqual(t, l.RecoveryPoint(), tt.wantLEO)
			requireContiguous(t, readAll(t, l.Log, 0, tt.wantLEO), 0, tt.wantLEO)
			if tt.wantLEO > 0 {
				requireEveryOffsetReadable(t, l.Log, tt.wantLEO)
			}
			// Appends continue at the new LEO, and the log reopens consistently.
			appendRecords(t, l.Log, 2, 2000)
			l.close(t)
			l = openTestLog(t, dir, cfg, newFakeClock(), 0)
			defer l.close(t)
			require.Equal(t, tt.wantLEO+2, l.LogEndOffset())
			requireContiguous(t, readAll(t, l.Log, 0, tt.wantLEO+2), 0, tt.wantLEO+2)
		})
	}
}

func TestTruncateToBelowLogStart(t *testing.T) {
	cfg := smallConfig()
	cfg.RetentionMs = 1
	clock := newFakeClock()
	l := openTestLog(t, t.TempDir(), cfg, clock, 0)
	defer l.close(t)
	for l.SegmentCount() < 3 {
		appendRecords(t, l.Log, 3, 1)
	}
	_, err := l.DeleteOldSegments(context.Background(), clock.Now(), l.LogEndOffset())
	require.NoError(t, err)
	require.Positive(t, l.LogStartOffset())
	_, err = l.TruncateTo(context.Background(), l.LogStartOffset()-1)
	require.ErrorIs(t, err, ErrOffsetOutOfRange)
}

func TestTruncateFullyAndStartAt(t *testing.T) {
	dir := t.TempDir()
	l := openTestLog(t, dir, smallConfig(), newFakeClock(), 0)
	for range 50 {
		appendRecords(t, l.Log, 3, 1000)
	}
	require.NoError(t, l.TruncateFullyAndStartAt(context.Background(), 1000))
	require.Equal(t, int64(1000), l.LogStartOffset())
	require.Equal(t, int64(1000), l.LogEndOffset())
	require.Equal(t, 1, l.SegmentCount())
	appendRecords(t, l.Log, 2, 1000)
	l.close(t)
	l = openTestLog(t, dir, smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	require.Equal(t, int64(1000), l.LogStartOffset())
	require.Equal(t, int64(1002), l.LogEndOffset())
}
