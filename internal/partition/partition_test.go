package partition

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

func newTestPartition(t *testing.T) *Partition {
	t.Helper()
	ctx := context.Background()
	m, err := storage.OpenManager(ctx, storage.DefaultManagerConfig(t.TempDir()), storage.SystemClock{}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
	tp := storage.TopicPartition{Topic: "t", Partition: 0}
	l, err := m.GetOrCreateLog(ctx, tp, storage.DefaultLogConfig())
	require.NoError(t, err)
	return New(tp, "id", l, 1, slog.New(slog.DiscardHandler))
}

func batch(t *testing.T, ts int64, n int) record.Batch {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for range n {
		b.Append(ts, nil, []byte("v"), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

func TestEpochChecks(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	_, err := p.Append(ctx, batch(t, 1, 1), AnyEpoch, AcksLeader)
	require.ErrorIs(t, err, ErrNotLeader, "not leader before BecomeLeader")

	p.BecomeLeader(5, []int32{1}, 1, 0)
	tests := []struct {
		name  string
		epoch int32
		want  error
	}{
		{"any", AnyEpoch, nil},
		{"current", 5, nil},
		{"older is fenced", 4, ErrFencedLeaderEpoch},
		{"newer means metadata behind", 6, ErrNotLeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Append(ctx, batch(t, 1, 1), tt.epoch, AcksLeader)
			requireErr(t, tt.want, err)
			_, _, err = p.Read(ctx, 0, 100, tt.epoch)
			requireErr(t, tt.want, err)
			_, err = p.ListOffsets(ctx, tt.epoch, LatestTimestamp)
			requireErr(t, tt.want, err)
		})
	}
	leader, epoch := p.Leader()
	require.Equal(t, int32(1), leader)
	require.Equal(t, int32(5), epoch)
}

func requireErr(t *testing.T, want, got error) {
	t.Helper()
	if want == nil {
		require.NoError(t, got)
		return
	}
	require.ErrorIs(t, got, want)
}

func TestAppendAdvancesHWAndNotifies(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	p.BecomeLeader(0, []int32{1}, 1, 0)
	ch := p.HWChanged()
	info, err := p.Append(ctx, batch(t, 1, 3), 0, AcksAll)
	require.NoError(t, err)
	require.Equal(t, int64(0), info.FirstOffset)
	require.Equal(t, int64(3), p.HighWatermark())
	select {
	case <-ch:
	default:
		t.Fatal("HWChanged not closed after the high watermark advanced")
	}
	require.NotEqual(t, ch, p.HWChanged())
}

func TestReadIsBoundedByHW(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	p.BecomeLeader(0, []int32{1}, 1, 0)
	for range 3 {
		_, err := p.Append(ctx, batch(t, 1, 2), 0, AcksLeader)
		require.NoError(t, err)
	}
	p.hw.Store(4) // as if a follower lagged (M6)
	res, hw, err := p.Read(ctx, 0, 1<<20, 0)
	require.NoError(t, err)
	require.Equal(t, int64(4), hw)
	batches, err := record.SplitBatches(res.Data)
	require.NoError(t, err)
	require.Len(t, batches, 2)
	require.Equal(t, int64(3), batches[1].LastOffset())
	res, _, err = p.Read(ctx, 4, 1<<20, 0)
	require.NoError(t, err)
	require.Empty(t, res.Data)
}

func TestAcksAllNeedsMinInsync(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	p.BecomeLeader(0, []int32{1}, 2, 0)
	_, err := p.Append(ctx, batch(t, 1, 1), 0, AcksAll)
	require.ErrorIs(t, err, ErrNotEnoughReplicas)
	require.Equal(t, int64(0), p.Log().LogEndOffset(), "nothing appended")
	_, err = p.Append(ctx, batch(t, 1, 1), 0, AcksLeader)
	require.NoError(t, err)
}

func TestBecomeLeaderHWFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	p.BecomeLeader(0, []int32{1, 2}, 1, 0) // two replicas: HW does not follow LEO
	_, err := p.Append(ctx, batch(t, 1, 5), 0, AcksLeader)
	require.NoError(t, err)
	require.Equal(t, int64(0), p.HighWatermark())
	p.BecomeLeader(1, []int32{1, 2}, 1, 100)
	require.Equal(t, int64(5), p.HighWatermark(), "clamped to LEO")
	p.BecomeLeader(2, []int32{1, 2}, 1, 3)
	require.Equal(t, int64(5), p.HighWatermark(), "never lowered")
}

func TestListOffsets(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t)
	p.BecomeLeader(0, []int32{1}, 1, 0)
	for i := range 4 {
		_, err := p.Append(ctx, batch(t, int64(100*(i+1)), 1), 0, AcksLeader)
		require.NoError(t, err)
	}
	tests := []struct {
		ts   int64
		want int64
	}{
		{EarliestTimestamp, 0},
		{LatestTimestamp, 4},
		{0, 0},
		{150, 1},
		{400, 3},
		{401, -1},
	}
	for _, tt := range tests {
		off, err := p.ListOffsets(ctx, 0, tt.ts)
		require.NoError(t, err)
		require.Equal(t, tt.want, off, "ts %d", tt.ts)
	}
	_, err := p.ListOffsets(ctx, 0, -3)
	require.Error(t, err)
}
