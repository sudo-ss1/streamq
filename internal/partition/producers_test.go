package partition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/producerstate"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// idBatch builds an idempotent batch of n records from producer pid starting at seq, stamped with
// the current time so it never expires during a test (M7 plan R15).
func idBatch(t *testing.T, pid int64, seq int32, n int) record.Batch {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: pid, BaseSequence: seq})
	for range n {
		b.Append(time.Now().UnixMilli(), nil, []byte("v"), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

// newLogPartition returns a partition on a log with cfg, plus a function that reopens a fresh
// replica (new producer state, recovered from disk) over the same log, as after a restart.
func newLogPartition(t *testing.T, cfg storage.LogConfig) (*Partition, func() *Partition) {
	t.Helper()
	ctx := context.Background()
	m, err := storage.OpenManager(ctx, storage.DefaultManagerConfig(t.TempDir()), storage.SystemClock{}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
	tp := storage.TopicPartition{Topic: "t", Partition: 0}
	l, err := m.GetOrCreateLog(ctx, tp, cfg)
	require.NoError(t, err)
	open := func() *Partition {
		epochs, err := OpenEpochCache(ctx, l.Dir(), l.LogEndOffset())
		require.NoError(t, err)
		p := New(tp, "id", l, epochs, Config{LocalID: 1, Producers: producerstate.New(l.Dir(), time.Hour)}, slog.New(slog.DiscardHandler))
		require.NoError(t, p.RecoverProducers(ctx))
		return p
	}
	return open(), open
}

func appendID(t *testing.T, p *Partition, epoch int32, pid int64, seq int32, n int) (storage.AppendInfo, error) {
	t.Helper()
	info, _, err := p.Append(context.Background(), idBatch(t, pid, seq, n), epoch, AcksLeader)
	return info, err
}

func TestIdempotentAppend(t *testing.T) {
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	info, err := appendID(t, p, 0, 5, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), info.FirstOffset)

	dup, err := appendID(t, p, 0, 5, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), dup.FirstOffset, "a retry reports the original offsets")
	require.Equal(t, int64(2), dup.LastOffset)
	require.Equal(t, int64(3), p.Log().LogEndOffset(), "nothing appended")

	info, err = appendID(t, p, 0, 5, 3, 2)
	require.NoError(t, err)
	require.Equal(t, int64(3), info.FirstOffset)

	tests := []struct {
		name string
		pid  int64
		seq  int32
		want error
	}{
		{"gap", 5, 9, producerstate.ErrOutOfOrderSequence},
		{"partial overlap", 5, 1, producerstate.ErrOutOfOrderSequence},
		{"unknown producer", 6, 4, producerstate.ErrUnknownProducerID},
	}
	for _, tt := range tests {
		_, err := appendID(t, p, 0, tt.pid, tt.seq, 2)
		require.ErrorIs(t, err, tt.want, tt.name)
	}
	require.Equal(t, int64(5), p.Log().LogEndOffset(), "rejected batches append nothing")
}

func TestIdempotentAppendValidatesFirst(t *testing.T) {
	ctx := context.Background()
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	_, err := appendID(t, p, 0, 5, 0, 3)
	require.NoError(t, err)

	short := record.Batch(make([]byte, 20))
	_, _, err = p.Append(ctx, short, 0, AcksLeader)
	require.ErrorIs(t, err, storage.ErrCorruptBatch, "a short batch is rejected, not read")

	badCRC := idBatch(t, 5, 0, 3) // matches the stored (pid, seq) exactly
	badCRC[len(badCRC)-1] ^= 0xff
	_, _, err = p.Append(ctx, badCRC, 0, AcksLeader)
	require.ErrorIs(t, err, storage.ErrCorruptBatch, "never answered as a duplicate")

	mb := record.NewBuilder(record.BuilderOptions{ProducerID: 5, BaseSequence: record.NoSequence})
	mb.Append(time.Now().UnixMilli(), nil, []byte("v"), nil)
	mixed, err := mb.Build()
	require.NoError(t, err)
	_, _, err = p.Append(ctx, mixed, 0, AcksLeader)
	require.ErrorIs(t, err, producerstate.ErrInvalidProducerBatch)
	require.Equal(t, int64(3), p.Log().LogEndOffset())
}

// followerAppendID appends an idempotent batch as the leader would have stamped it.
func followerAppendID(t *testing.T, p *Partition, fetchEpoch int32, base int64, pid int64, seq int32, n int) {
	t.Helper()
	b := idBatch(t, pid, seq, n)
	b.SetBaseOffset(base)
	b.SetLeaderEpoch(fetchEpoch)
	require.NoError(t, p.AppendAsFollower(context.Background(), fetchEpoch, b))
}

func TestPromotedFollowerDetectsDuplicates(t *testing.T) {
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	followerAppendID(t, p, 0, 0, 5, 0, 3)
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1}, 1, 0))
	dup, err := appendID(t, p, 1, 5, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), dup.FirstOffset)
	require.Equal(t, int64(3), p.Log().LogEndOffset())
}

func TestTruncationRebuildsProducerState(t *testing.T) {
	ctx := context.Background()
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	followerAppendID(t, p, 0, 0, 5, 0, 3)
	followerAppendID(t, p, 0, 3, 5, 3, 3) // uncommitted, truncated below
	_, err := p.TruncateTo(ctx, 0, 3)
	require.NoError(t, err)
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1}, 1, 0))
	info, err := appendID(t, p, 1, 5, 3, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), info.FirstOffset, "the truncated batch is no longer a duplicate: appended again")
	dup, err := appendID(t, p, 1, 5, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), dup.FirstOffset)
}

type failingReplay struct{}

func (failingReplay) Read(context.Context, int64, int, int64) (storage.ReadResult, error) {
	return storage.ReadResult{}, errors.New("disk on fire")
}

func (failingReplay) LogStartOffset() int64 { return 0 }

// TestFailedRebuildRefusesIdempotentAppends is plan R1: a truncation whose rebuild fails must not
// leave the old state answering for a truncated batch, even after a promotion.
func TestFailedRebuildRefusesIdempotentAppends(t *testing.T) {
	ctx := context.Background()
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	followerAppendID(t, p, 0, 0, 5, 0, 3)
	followerAppendID(t, p, 0, 3, 5, 3, 3)
	p.replay = failingReplay{}
	leo, err := p.TruncateTo(ctx, 0, 3)
	require.NoError(t, err, "replication goes on; only the producer state is invalid")
	require.Equal(t, int64(3), leo)
	require.False(t, p.prodValid)
	followerAppendID(t, p, 0, 3, 6, 0, 1) // the follower keeps appending
	_, err = p.TruncateTo(ctx, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), p.Log().LogEndOffset())

	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1}, 1, 0)) // rebuild fails again: logged
	_, err = appendID(t, p, 1, 5, 3, 3)
	require.ErrorIs(t, err, ErrNotLeader, "no duplicate answer from stale state")
	require.Equal(t, int64(3), p.Log().LogEndOffset())
	_, _, err = p.Append(ctx, batch(t, 1, 1), 1, AcksLeader)
	require.NoError(t, err, "non-idempotent appends are unaffected")

	p.replay = p.log
	p.MaintainProducers(ctx, time.Now())
	info, err := appendID(t, p, 1, 5, 3, 3)
	require.NoError(t, err)
	require.Equal(t, int64(4), info.FirstOffset, "appended as new after the rebuild")
}

func snapshotFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".snapshot") {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestSnapshotOnRollAndRecovery(t *testing.T) {
	ctx := context.Background()
	cfg := storage.DefaultLogConfig()
	cfg.SegmentBytes, cfg.MaxMessageBytes = 1024, 512
	p, reopen := newLogPartition(t, cfg)
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	rolledAt, rollPID, rollSeq := int64(-1), int64(-1), int32(-1)
	for i := range int32(40) {
		info, err := appendID(t, p, 0, int64(i%3), (i/3)*2, 2)
		require.NoError(t, err)
		if info.RolledAt >= 0 && rolledAt < 0 && i >= 3 {
			rolledAt, rollPID, rollSeq = info.RolledAt, int64(i%3), (i/3)*2
		}
	}
	require.Greater(t, p.Log().SegmentCount(), 2)
	require.NotEmpty(t, snapshotFiles(t, p.Log().Dir()), "snapshots written on roll")
	require.GreaterOrEqual(t, rolledAt, int64(0))

	// The snapshot at a roll point holds the state before the rolling batch: loaded on its own (no
	// replay), the rolling batch continues its producer's sequence and is not a duplicate.
	only := t.TempDir()
	name := fmt.Sprintf("%020d.snapshot", rolledAt)
	data, err := os.ReadFile(filepath.Join(p.Log().Dir(), name)) //nolint:gosec // test file under t.TempDir()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(only, name), data, 0o600)) //nolint:gosec // test file under t.TempDir()
	snap := producerstate.New(only, time.Hour)
	require.NoError(t, snap.Rebuild(ctx, p.Log(), 0, rolledAt))
	_, dup, err := snap.Check(rollPID, rollSeq, rollSeq+1)
	require.NoError(t, err, "the rolling batch is the next in sequence")
	require.False(t, dup, "the snapshot excludes the rolling batch")
	_, dup, err = snap.Check(rollPID, rollSeq-2, rollSeq-1)
	require.NoError(t, err)
	require.True(t, dup, "the batch before it is included")

	r := reopen() // recovery: newest snapshot + replay
	becomeLeader(t, r, leaderState(1, 1, []int32{1}, []int32{1}, 1, 0))
	leo := r.Log().LogEndOffset()
	for pid := range int64(3) {
		last := int32(39) - int32((39-pid)%3) // last i with i%3 == pid
		dup, err := appendID(t, r, 1, pid, (last/3)*2, 2)
		require.NoError(t, err, "pid %d", pid)
		require.Less(t, dup.FirstOffset, leo)
	}
	require.Equal(t, leo, r.Log().LogEndOffset(), "every retry is a duplicate after recovery")

	// A clean-close snapshot at the LEO lets the next recovery replay nothing.
	require.NoError(t, r.SnapshotProducers(ctx))
	require.Contains(t, snapshotFiles(t, r.Log().Dir()), fmt.Sprintf("%020d.snapshot", leo))
}

func TestTruncateFullyClearsProducerState(t *testing.T) {
	ctx := context.Background()
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	followerAppendID(t, p, 0, 0, 5, 0, 3)
	require.NoError(t, p.TruncateFullyAndStartAt(ctx, 0, 100))
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1}, 1, 0))
	_, err := appendID(t, p, 1, 5, 3, 3)
	require.ErrorIs(t, err, producerstate.ErrUnknownProducerID, "state cleared with the log (D49)")
}

// TestFailedTruncationInvalidatesProducerState: if the log is truncated but a later step fails,
// the producer state must not stay valid over the shorter log (review finding: a retry with an
// unchanged LEO would otherwise skip the rebuild).
func TestFailedTruncationInvalidatesProducerState(t *testing.T) {
	ctx := context.Background()
	p, _ := newLogPartition(t, storage.DefaultLogConfig())
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 1, Replicas: []int32{1, 2}})
	b := idBatch(t, 5, 0, 3)
	b.SetLeaderEpoch(0)
	require.NoError(t, p.AppendAsFollower(ctx, 1, b))
	followerAppendID(t, p, 1, 3, 5, 3, 3) // starts epoch 1 at offset 3

	dir := p.Log().Dir()
	require.NoError(t, os.Chmod(dir, 0o500)) //nolint:gosec // make the epoch checkpoint write fail
	_, err := p.TruncateTo(ctx, 1, 3)
	require.NoError(t, os.Chmod(dir, 0o750)) //nolint:gosec // restore
	require.Error(t, err, "the epoch-cache persist fails after the log was cut")
	require.Equal(t, int64(3), p.Log().LogEndOffset())
	require.False(t, p.prodValid, "state invalid until a rebuild succeeds")

	_, err = p.TruncateTo(ctx, 1, 3) // the fetcher's retry: the LEO no longer changes
	require.NoError(t, err)
	require.True(t, p.prodValid)
	becomeLeader(t, p, leaderState(2, 2, []int32{1, 2}, []int32{1}, 1, 0))
	info, err := appendID(t, p, 2, 5, 3, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), info.FirstOffset)
	require.Equal(t, int64(6), p.Log().LogEndOffset(), "the truncated batch is appended again, not a false duplicate")
}

// rollSegments appends non-idempotent batches until the log has at least n segments.
func rollSegments(t *testing.T, p *Partition, epoch int32, n int) {
	t.Helper()
	for p.Log().SegmentCount() < n {
		_, _, err := p.Append(context.Background(), batch(t, time.Now().UnixMilli(), 4), epoch, AcksLeader)
		require.NoError(t, err)
	}
}

// TestInvalidStateIsNeverSnapshotted (failure review): while the state is invalid, rolls must not
// persist it, or the next successful rebuild would trust a snapshot that still lists a truncated
// batch and answer its retry as a duplicate of data that no longer exists.
func TestInvalidStateIsNeverSnapshotted(t *testing.T) {
	ctx := context.Background()
	cfg := storage.DefaultLogConfig()
	cfg.SegmentBytes, cfg.MaxMessageBytes = 1024, 512
	p, _ := newLogPartition(t, cfg)
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	followerAppendID(t, p, 0, 0, 5, 0, 3)
	followerAppendID(t, p, 0, 3, 5, 3, 3) // truncated below
	p.replay = failingReplay{}
	_, err := p.TruncateTo(ctx, 0, 3)
	require.NoError(t, err)
	require.False(t, p.prodValid)
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1}, 1, 0)) // rebuild fails again
	rollSegments(t, p, 1, 3)

	p.replay = p.log
	p.MaintainProducers(ctx, time.Now())
	require.True(t, p.prodValid)
	leo := p.Log().LogEndOffset()
	info, err := appendID(t, p, 1, 5, 3, 3)
	require.NoError(t, err)
	require.Equal(t, leo, info.FirstOffset, "appended as new, not a false duplicate")
}

// TestFailedRecoveryIsNeverSnapshotted: an open whose recovery fails must not snapshot the empty
// state on a roll, or producers logged before it would be forgotten.
func TestFailedRecoveryIsNeverSnapshotted(t *testing.T) {
	ctx := context.Background()
	cfg := storage.DefaultLogConfig()
	cfg.SegmentBytes, cfg.MaxMessageBytes = 1024, 512
	p, _ := newLogPartition(t, cfg)
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	_, err := appendID(t, p, 0, 5, 0, 3)
	require.NoError(t, err)

	epochs, err := OpenEpochCache(ctx, p.Log().Dir(), p.Log().LogEndOffset())
	require.NoError(t, err)
	r := New(p.tp, "id", p.log, epochs, Config{LocalID: 1, Producers: producerstate.New(p.Log().Dir(), time.Hour)}, slog.New(slog.DiscardHandler))
	r.replay = failingReplay{}
	require.Error(t, r.RecoverProducers(ctx))
	becomeLeader(t, r, leaderState(1, 1, []int32{1}, []int32{1}, 1, 0))
	rollSegments(t, r, 1, 3)

	r.replay = r.log
	r.MaintainProducers(ctx, time.Now())
	leo := r.Log().LogEndOffset()
	dup, err := appendID(t, r, 1, 5, 0, 3)
	require.NoError(t, err)
	require.Equal(t, int64(0), dup.FirstOffset, "the logged batch is still recognised")
	require.Equal(t, leo, r.Log().LogEndOffset())
}
