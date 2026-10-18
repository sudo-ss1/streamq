package producerstate

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

func meta(first, last int32, off int64) BatchMeta {
	return BatchMeta{FirstSeq: first, LastSeq: last, FirstOffset: off, LastOffset: off + int64(last-first), MaxTimestamp: 1000}
}

func TestCheck(t *testing.T) {
	s := New(t.TempDir(), time.Hour)
	for i := range int32(6) { // seqs 0-9, 10-19, ... 50-59 at offsets 0, 10, ...
		s.Update(7, 0, meta(i*10, i*10+9, int64(i)*10))
	}
	tests := []struct {
		name        string
		pid         int64
		first, last int32
		wantDup     bool
		wantOff     int64
		wantErr     error
	}{
		{"new producer at 0", 8, 0, 4, false, 0, nil},
		{"new producer not at 0", 8, 5, 9, false, 0, ErrUnknownProducerID},
		{"in sequence", 7, 60, 69, false, 0, nil},
		{"duplicate of newest", 7, 50, 59, true, 50, nil},
		{"duplicate of oldest kept", 7, 10, 19, true, 10, nil},
		{"duplicate older than five", 7, 0, 9, false, 0, ErrOutOfOrderSequence},
		{"gap", 7, 61, 70, false, 0, ErrOutOfOrderSequence},
		{"partial overlap", 7, 55, 64, false, 0, ErrOutOfOrderSequence},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, dup, err := s.Check(tt.pid, tt.first, tt.last)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDup, dup)
			if dup {
				require.Equal(t, tt.wantOff, m.FirstOffset)
			}
		})
	}
}

func TestSequenceWraparound(t *testing.T) {
	require.Equal(t, int32(math.MaxInt32), LastSeq(math.MaxInt32-4, 5))
	require.Equal(t, int32(2), LastSeq(math.MaxInt32-1, 5))
	s := New(t.TempDir(), time.Hour)
	s.Update(1, 0, meta(math.MaxInt32-9, math.MaxInt32, 0))
	_, dup, err := s.Check(1, 0, 9)
	require.NoError(t, err)
	require.False(t, dup, "0 follows MaxInt32")
}

func TestValidateFields(t *testing.T) {
	tests := []struct {
		pid  int64
		seq  int32
		fail bool
	}{
		{record.NoProducerID, record.NoSequence, false},
		{5, 0, false},
		{5, record.NoSequence, true},
		{record.NoProducerID, 3, true},
		{-7, record.NoSequence, true},
	}
	for _, tt := range tests {
		b := buildBatch(t, tt.pid, tt.seq, 0, 1)
		err := ValidateFields(b)
		if tt.fail {
			require.ErrorIs(t, err, ErrInvalidProducerBatch)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestExpire(t *testing.T) {
	s := New(t.TempDir(), time.Hour)
	s.Update(1, 0, BatchMeta{MaxTimestamp: 1000})
	s.Update(2, 0, BatchMeta{MaxTimestamp: 1000 + time.Hour.Milliseconds()})
	s.ExpireBefore(1001 + time.Hour.Milliseconds())
	require.Equal(t, 1, s.Len())
	_, _, err := s.Check(1, 1, 1)
	require.ErrorIs(t, err, ErrUnknownProducerID)
}

func buildBatch(t *testing.T, pid int64, seq int32, ts int64, n int) record.Batch {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: pid, BaseSequence: seq})
	for range n {
		b.Append(ts, nil, []byte("v"), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

// testLog appends idempotent batches to a real log: producer pid p gets batches of 2 records.
func testLog(t *testing.T) *storage.Log {
	t.Helper()
	ctx := context.Background()
	cfg := storage.DefaultManagerConfig(t.TempDir())
	m, err := storage.OpenManager(ctx, cfg, storage.SystemClock{}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
	l, err := m.GetOrCreateLog(ctx, storage.TopicPartition{Topic: "t", Partition: 0}, storage.DefaultLogConfig())
	require.NoError(t, err)
	return l
}

func appendTo(t *testing.T, l *storage.Log, s *State, pid int64, seq int32) {
	t.Helper()
	_, err := l.Append(context.Background(), buildBatch(t, pid, seq, time.Now().UnixMilli(), 2), 0)
	require.NoError(t, err)
	res, err := l.Read(context.Background(), l.LogEndOffset()-2, 1<<20, l.LogEndOffset())
	require.NoError(t, err)
	bs, err := record.SplitBatches(res.Data)
	require.NoError(t, err)
	s.UpdateFromBatch(bs[0])
}

func TestSnapshotAndRebuild(t *testing.T) {
	ctx := context.Background()
	l := testLog(t)
	s := New(l.Dir(), time.Hour)
	for i := range int32(4) {
		appendTo(t, l, s, 1, i*2)
		appendTo(t, l, s, 2, i*2)
	}
	require.NoError(t, s.WriteSnapshot(ctx, l.LogEndOffset())) // covers every batch so far
	for i := range int32(4) {
		appendTo(t, l, s, 1, 8+i*2)
	}
	r := New(l.Dir(), time.Hour)
	require.NoError(t, r.Rebuild(ctx, l, l.LogStartOffset(), l.LogEndOffset()))
	require.Equal(t, s.producers, r.producers, "snapshot + replay equals live state")

	// Without any snapshot the full replay gives the same state.
	require.NoError(t, r.TruncateSnapshots(ctx, -1))
	f := New(l.Dir(), time.Hour)
	require.NoError(t, f.Rebuild(ctx, l, l.LogStartOffset(), l.LogEndOffset()))
	require.Equal(t, s.producers, f.producers)
}

func TestRebuildSkipsBadAndStaleSnapshots(t *testing.T) {
	ctx := context.Background()
	l := testLog(t)
	s := New(l.Dir(), time.Hour)
	appendTo(t, l, s, 1, 0)
	require.NoError(t, s.WriteSnapshot(ctx, 2))
	appendTo(t, l, s, 1, 2)
	require.NoError(t, s.WriteSnapshot(ctx, 4))
	// Corrupt the newest snapshot, plant one above the LEO, one below the log start, and a temp
	// leftover.
	newest := filepath.Join(l.Dir(), snapshotName(4))
	data, err := os.ReadFile(newest)
	require.NoError(t, err)
	data[len(data)-2] ^= 0xff
	require.NoError(t, os.WriteFile(newest, data, 0o600))
	require.NoError(t, s.WriteSnapshot(ctx, 99))
	require.NoError(t, os.WriteFile(filepath.Join(l.Dir(), snapshotName(7)+tmpSuffix), []byte("x"), 0o600))

	r := New(l.Dir(), time.Hour)
	require.NoError(t, r.Rebuild(ctx, l, 0, l.LogEndOffset()))
	require.Equal(t, s.producers, r.producers)
	offs, err := r.snapshotOffsets()
	require.NoError(t, err)
	require.Equal(t, []int64{2}, offs, "corrupt, above-LEO snapshots and temp files removed")
	_, err = os.Stat(filepath.Join(l.Dir(), snapshotName(7)+tmpSuffix))
	require.ErrorIs(t, err, os.ErrNotExist)

	// A snapshot below the log start is ignored and deleted (plan R2).
	require.NoError(t, r.Rebuild(ctx, l, 3, l.LogEndOffset()))
	offs, err = r.snapshotOffsets()
	require.NoError(t, err)
	require.Empty(t, offs)
}

type failingReader struct{}

func (failingReader) Read(context.Context, int64, int, int64) (storage.ReadResult, error) {
	return storage.ReadResult{}, errors.New("disk on fire")
}

func (failingReader) LogStartOffset() int64 { return 0 }

// movingStartReader fails the first read as if retention removed it, then serves from the log.
type movingStartReader struct {
	*storage.Log
	failed bool
	start  int64
}

func (r *movingStartReader) Read(ctx context.Context, off int64, maxBytes int, maxOffset int64) (storage.ReadResult, error) {
	if !r.failed {
		r.failed = true
		return storage.ReadResult{}, storage.ErrOffsetOutOfRange
	}
	return r.Log.Read(ctx, off, maxBytes, maxOffset)
}

func (r *movingStartReader) LogStartOffset() int64 { return r.start }

func TestRebuildRetriesAfterRetentionRace(t *testing.T) {
	ctx := context.Background()
	l := testLog(t)
	s := New(l.Dir(), time.Hour)
	appendTo(t, l, s, 1, 0)
	appendTo(t, l, s, 2, 0)
	r := New(l.Dir(), time.Hour)
	require.NoError(t, r.Rebuild(ctx, &movingStartReader{Log: l, start: 2}, 0, l.LogEndOffset()))
	require.Equal(t, 1, r.Len(), "replayed from the new log start only")
}

func TestExpiredProducerRestartsAtZero(t *testing.T) {
	s := New(t.TempDir(), time.Hour)
	s.Update(1, 0, meta(0, 9, 0))
	s.ExpireBefore(time.Now().UnixMilli())
	_, dup, err := s.Check(1, 0, 9)
	require.NoError(t, err)
	require.False(t, dup, "an expired producer's seq 0 is accepted as new (D49)")
}

func TestRebuildFailureKeepsState(t *testing.T) {
	s := New(t.TempDir(), time.Hour)
	s.Update(1, 0, meta(0, 9, 0))
	require.Error(t, s.Rebuild(context.Background(), failingReader{}, 0, 10))
	require.Equal(t, 1, s.Len(), "state replaced only on success; the caller marks it invalid")
}

func TestClear(t *testing.T) {
	ctx := context.Background()
	s := New(t.TempDir(), time.Hour)
	s.Update(1, 0, meta(0, 9, 0))
	require.NoError(t, s.WriteSnapshot(ctx, 10))
	require.NoError(t, s.Clear(ctx))
	require.Zero(t, s.Len())
	offs, err := s.snapshotOffsets()
	require.NoError(t, err)
	require.Empty(t, offs)
}
