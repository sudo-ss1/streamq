package partition

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// fakeClock is a manual clock; After channels fire when Advance passes their deadline.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, fakeTimer{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	keep := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.at.After(c.now) {
			w.ch <- c.now
			continue
		}
		keep = append(keep, w)
	}
	c.waiters = keep
}

func (c *fakeClock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

func newTestPartition(t *testing.T, cfg Config) *Partition {
	t.Helper()
	ctx := context.Background()
	m, err := storage.OpenManager(ctx, storage.DefaultManagerConfig(t.TempDir()), storage.SystemClock{}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
	tp := storage.TopicPartition{Topic: "t", Partition: 0}
	l, err := m.GetOrCreateLog(ctx, tp, storage.DefaultLogConfig())
	require.NoError(t, err)
	epochs, err := OpenEpochCache(ctx, l.Dir(), l.LogEndOffset())
	require.NoError(t, err)
	if cfg.LocalID == 0 {
		cfg.LocalID = 1
	}
	return New(tp, "id", l, epochs, cfg, slog.New(slog.DiscardHandler))
}

func leaderState(epoch, partitionEpoch int32, replicas, isr []int32, minInsync int32, hw int64) State {
	return State{Leader: 1, LeaderEpoch: epoch, PartitionEpoch: partitionEpoch, Replicas: replicas, ISR: isr, MinInsync: minInsync, HWCheckpoint: hw}
}

func becomeLeader(t *testing.T, p *Partition, st State) {
	t.Helper()
	require.NoError(t, p.BecomeLeader(context.Background(), st))
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

// replicated returns a batch as a leader would have stamped it.
func replicated(t *testing.T, base int64, epoch int32, n int) record.Batch {
	t.Helper()
	b := batch(t, 1, n)
	b.SetBaseOffset(base)
	b.SetLeaderEpoch(epoch)
	return b
}

func appendN(t *testing.T, p *Partition, epoch int32, n int) {
	t.Helper()
	_, _, err := p.Append(context.Background(), batch(t, 1, n), epoch, AcksLeader)
	require.NoError(t, err)
}

func fetch(t *testing.T, p *Partition, replica int32, epoch int32, offset int64) ReplicaFetchResult {
	t.Helper()
	res, err := p.HandleReplicaFetch(context.Background(), ReplicaFetchRequest{ReplicaID: replica, LeaderEpoch: epoch, FetchOffset: offset, MaxBytes: 1 << 20})
	require.NoError(t, err)
	return res
}

func requireErr(t *testing.T, want, got error) {
	t.Helper()
	if want == nil {
		require.NoError(t, got)
		return
	}
	require.ErrorIs(t, got, want)
}

func TestEpochChecks(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	_, _, err := p.Append(ctx, batch(t, 1, 1), AnyEpoch, AcksLeader)
	require.ErrorIs(t, err, ErrNotLeader, "not leader before BecomeLeader")

	becomeLeader(t, p, leaderState(5, 0, []int32{1}, []int32{1}, 1, 0))
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
			_, _, err := p.Append(ctx, batch(t, 1, 1), tt.epoch, AcksLeader)
			requireErr(t, tt.want, err)
			_, _, err = p.Read(ctx, 0, 100, tt.epoch)
			requireErr(t, tt.want, err)
			_, _, err = p.ListOffsets(ctx, tt.epoch, LatestTimestamp)
			requireErr(t, tt.want, err)
			_, err = p.HandleReplicaFetch(ctx, ReplicaFetchRequest{ReplicaID: 1, LeaderEpoch: tt.epoch, FetchOffset: 0})
			if tt.want != nil {
				require.ErrorIs(t, err, tt.want)
			}
		})
	}
	leader, epoch := p.Leader()
	require.Equal(t, int32(1), leader)
	require.Equal(t, int32(5), epoch)
}

func TestAppendAdvancesHWAndNotifies(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	ch := p.HWChanged()
	info, epoch, err := p.Append(ctx, batch(t, 1, 3), 0, AcksAll)
	require.NoError(t, err)
	require.Equal(t, int64(0), info.FirstOffset)
	require.Equal(t, int32(0), epoch)
	require.Equal(t, int64(3), p.HighWatermark())
	select {
	case <-ch:
	default:
		t.Fatal("HWChanged not closed after the high watermark advanced")
	}
	require.NotEqual(t, ch, p.HWChanged())
	require.NoError(t, p.WaitForCommit(ctx, info.LastOffset, 0, time.Minute), "sole ISR member: committed at once")
}

func TestReadIsBoundedByHW(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0))
	for range 3 {
		appendN(t, p, 0, 2)
	}
	fetch(t, p, 2, 0, 4) // the follower has 4 records
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

	// Replica fetches read up to the LEO instead.
	rf := fetch(t, p, 2, 0, 4)
	batches, err = record.SplitBatches(rf.Records)
	require.NoError(t, err)
	require.Equal(t, int64(5), batches[len(batches)-1].LastOffset())
	require.Equal(t, int64(6), rf.LogEndOffset)
}

func TestAcksAllNeedsMinInsync(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1}, 2, 0))
	_, _, err := p.Append(ctx, batch(t, 1, 1), 0, AcksAll)
	require.ErrorIs(t, err, ErrNotEnoughReplicas)
	require.Equal(t, int64(0), p.Log().LogEndOffset(), "nothing appended")
	_, _, err = p.Append(ctx, batch(t, 1, 1), 0, AcksLeader)
	require.NoError(t, err)
}

// reopen returns a fresh replica over p's log and epoch cache, as after a broker restart.
func reopen(p *Partition) *Partition {
	return New(p.tp, p.topicID, p.log, p.epochs, p.cfg, slog.New(slog.DiscardHandler))
}

func TestBecomeLeaderHWFromCheckpoint(t *testing.T) {
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0))
	appendN(t, p, 0, 5)
	require.Equal(t, int64(0), p.HighWatermark(), "the follower's LEO is unknown")

	r := reopen(p)
	becomeLeader(t, r, leaderState(1, 1, []int32{1, 2}, []int32{1, 2}, 1, 100))
	require.Equal(t, int64(5), r.HighWatermark(), "clamped to LEO")
	r = reopen(p)
	becomeLeader(t, r, leaderState(1, 1, []int32{1, 2}, []int32{1, 2}, 1, 3))
	require.Equal(t, int64(3), r.HighWatermark(), "resumes from the checkpoint")
	becomeLeader(t, r, leaderState(2, 2, []int32{1, 2}, []int32{1, 2}, 1, 5))
	require.Equal(t, int64(3), r.HighWatermark(), "only the first transition uses the checkpoint")
}

func TestListOffsets(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1}, []int32{1}, 1, 0))
	for i := range 4 {
		_, _, err := p.Append(ctx, batch(t, int64(100*(i+1)), 1), 0, AcksLeader)
		require.NoError(t, err)
	}
	tests := []struct {
		ts     int64
		want   int64
		wantTs int64
	}{
		{EarliestTimestamp, 0, -1},
		{LatestTimestamp, 4, -1},
		{0, 0, 100},
		{150, 1, 200},
		{400, 3, 400},
		{401, -1, -1},
	}
	for _, tt := range tests {
		off, ts, err := p.ListOffsets(ctx, 0, tt.ts)
		require.NoError(t, err)
		require.Equal(t, tt.want, off, "ts %d", tt.ts)
		require.Equal(t, tt.wantTs, ts, "ts %d", tt.ts)
	}
	_, _, err := p.ListOffsets(ctx, 0, -3)
	require.Error(t, err)
}

func TestClassifyIsMonotonic(t *testing.T) {
	p := newTestPartition(t, Config{})
	st := leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0)
	require.Equal(t, TransitionLeader, p.Classify(st))
	becomeLeader(t, p, st)
	require.Equal(t, TransitionNone, p.Classify(st), "identical")
	require.Equal(t, TransitionNone, p.Classify(leaderState(-1, 9, nil, nil, 1, 0)), "older epoch")
	next := leaderState(0, 1, []int32{1, 2}, []int32{1}, 1, 0)
	require.Equal(t, TransitionISR, p.Classify(next))
	require.True(t, p.ApplyISR(next))
	require.False(t, p.ApplyISR(next), "already applied")
	follow := State{Leader: 2, LeaderEpoch: 1, PartitionEpoch: 2, Replicas: []int32{1, 2}, ISR: []int32{2}}
	require.Equal(t, TransitionFollower, p.Classify(follow))
	p.BecomeFollower(follow)
	require.False(t, p.IsLeader())
	_, _, err := p.Append(context.Background(), batch(t, 1, 1), AnyEpoch, AcksLeader)
	require.ErrorIs(t, err, ErrNotLeader)
	require.Equal(t, TransitionISR, p.Classify(State{Leader: 2, LeaderEpoch: 1, PartitionEpoch: 3, Replicas: []int32{1, 2}, ISR: []int32{1, 2}}))
	require.Equal(t, TransitionLeader, p.Classify(leaderState(2, 4, []int32{1, 2}, []int32{1}, 1, 0)))
}

func TestHWOverMaximalISR(t *testing.T) {
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2, 3}, []int32{1, 2}, 1, 0))
	appendN(t, p, 0, 5)
	require.Equal(t, int64(0), p.HighWatermark())
	fetch(t, p, 2, 0, 5)
	require.Equal(t, int64(5), p.HighWatermark(), "out-of-sync replica 3 does not hold the HW")
	appendN(t, p, 0, 5)
	fetch(t, p, 3, 0, 0)
	_, ok := p.NextAlterISR()
	require.False(t, ok, "3 is behind the HW: no expansion")
	fetch(t, p, 2, 0, 10)
	require.Equal(t, int64(10), p.HighWatermark())

	fetch(t, p, 3, 0, 10)
	req, ok := p.NextAlterISR()
	require.True(t, ok, "3 caught up: expand")
	require.Equal(t, AlterISRRequest{LeaderEpoch: 0, PartitionEpoch: 0, NewISR: []int32{1, 2, 3}}, req)
	_, ok = p.NextAlterISR()
	require.False(t, ok, "one request in flight")

	appendN(t, p, 0, 2)
	fetch(t, p, 2, 0, 12)
	require.Equal(t, int64(10), p.HighWatermark(), "pending member 3 holds the HW (maximal ISR)")
	fetch(t, p, 3, 0, 12)
	require.Equal(t, int64(12), p.HighWatermark())
}

func TestAlterISRResults(t *testing.T) {
	tests := []struct {
		name        string
		errs        []error
		wantPending bool // NextAlterISR offers it again
		wantHeld    bool // the pending member still counts for the HW
	}{
		{"accepted waits for metadata", []error{nil}, false, true},
		{"lost response then fenced keeps pending", []error{errors.New("unknown outcome"), metadata.ErrFencedLeaderEpoch}, false, true},
		{"transport error retries", []error{errors.New("unavailable")}, true, true},
		{"invalid isr drops it", []error{metadata.ErrInvalidISR}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newFakeClock()
			p := newTestPartition(t, Config{Clock: clk})
			becomeLeader(t, p, leaderState(0, 0, []int32{1, 2, 3}, []int32{1, 2}, 1, 0))
			fetch(t, p, 2, 0, 0)
			fetch(t, p, 3, 0, 0) // expand
			var req AlterISRRequest
			for _, err := range tt.errs {
				var ok bool
				req, ok = p.NextAlterISR()
				require.True(t, ok)
				p.AlterISRResult(req, err)
				if err != nil && tt.wantPending {
					_, ok = p.NextAlterISR()
					require.False(t, ok, "a retriable failure backs off")
				}
				clk.Advance(time.Second)
			}
			_, ok := p.NextAlterISR()
			require.Equal(t, tt.wantPending, ok)
			appendN(t, p, 0, 3)
			fetch(t, p, 2, 0, 3)
			if tt.wantHeld {
				require.Equal(t, int64(0), p.HighWatermark())
			} else {
				require.Equal(t, int64(3), p.HighWatermark())
			}
			// Committed metadata resolves the pending change.
			require.True(t, p.ApplyISR(leaderState(0, 1, []int32{1, 2, 3}, []int32{1, 2, 3}, 1, 0)))
			_, ok = p.NextAlterISR()
			require.False(t, ok)
			fetch(t, p, 3, 0, 3)
			require.Equal(t, int64(3), p.HighWatermark())
			p.AlterISRResult(req, nil) // a late answer for a resolved request is ignored
		})
	}
}

func TestCaughtUpRuleAndShrink(t *testing.T) {
	clk := newFakeClock()
	p := newTestPartition(t, Config{Clock: clk, ReplicaLagMax: 10 * time.Second})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0))
	// The leader keeps appending; the follower always fetches what the leader had at its previous
	// fetch, so it is never at the LEO but stays caught up (Kafka's rule).
	var off int64
	for range 6 {
		appendN(t, p, 0, 5)
		fetch(t, p, 2, 0, off)
		off = p.Log().LogEndOffset() // next time, fetch what the leader had now
		clk.Advance(4 * time.Second)
		p.MaybeShrinkISR(clk.Now())
		_, ok := p.NextAlterISR()
		require.False(t, ok, "trailing follower must not be shrunk")
	}
	// The follower stops fetching.
	clk.Advance(7 * time.Second)
	p.MaybeShrinkISR(clk.Now())
	req, ok := p.NextAlterISR()
	require.True(t, ok)
	require.Equal(t, []int32{1}, req.NewISR)
}

func TestShrinkAfterLagWhenNeverFetched(t *testing.T) {
	clk := newFakeClock()
	p := newTestPartition(t, Config{Clock: clk, ReplicaLagMax: 10 * time.Second})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2, 3}, []int32{1, 2, 3}, 1, 0))
	clk.Advance(5 * time.Second)
	fetch(t, p, 2, 0, 0)
	clk.Advance(6 * time.Second)
	p.MaybeShrinkISR(clk.Now())
	req, ok := p.NextAlterISR()
	require.True(t, ok)
	require.Equal(t, []int32{1, 2}, req.NewISR, "only 3 lagged for longer than replica.lag.max.ms")
}

func TestExpandSkipsFencedAndOldEpochData(t *testing.T) {
	fenced := true
	p := newTestPartition(t, Config{IsFenced: func(id int32) bool { return id == 2 && fenced }})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2, 3}, []int32{1, 3}, 1, 0))
	appendN(t, p, 0, 4)
	// Epoch 1 starts at 4; replica 3's unknown LEO holds the HW at 0.
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2, 3}, []int32{1, 3}, 1, 0))
	require.Equal(t, int64(0), p.HighWatermark())
	fenced = false
	fetch(t, p, 2, 1, 2)
	_, ok := p.NextAlterISR()
	require.False(t, ok, "LEO >= HW but below the current epoch's start (KIP-841)")
	fenced = true
	fetch(t, p, 2, 1, 4)
	_, ok = p.NextAlterISR()
	require.False(t, ok, "fenced broker is not added")
	fenced = false
	fetch(t, p, 2, 1, 4)
	req, ok := p.NextAlterISR()
	require.True(t, ok)
	require.Equal(t, []int32{1, 2, 3}, req.NewISR, "replica order")
}

func TestWaitForCommit(t *testing.T) {
	ctx := context.Background()
	type step func(t *testing.T, p *Partition, clk *fakeClock)
	tests := []struct {
		name string
		act  step
		want error
	}{
		{"committed by follower fetch", func(t *testing.T, p *Partition, _ *fakeClock) {
			t.Helper()
			fetch(t, p, 2, 0, 3)
		}, nil},
		{"leadership lost, then truncation and a follower HW past the offset", func(t *testing.T, p *Partition, _ *fakeClock) {
			t.Helper()
			p.BecomeFollower(State{Leader: 2, LeaderEpoch: 1, PartitionEpoch: 1, Replicas: []int32{1, 2}})
			_, err := p.TruncateTo(context.Background(), 1, 0)
			require.NoError(t, err)
			require.NoError(t, p.AppendAsFollower(context.Background(), 1, replicated(t, 0, 1, 5)))
			p.UpdateFollowerHW(1, 5)
			require.Equal(t, int64(5), p.HighWatermark())
		}, ErrNotLeader},
		{"not enough replicas after append", func(t *testing.T, p *Partition, _ *fakeClock) {
			t.Helper()
			require.True(t, p.ApplyISR(leaderState(0, 1, []int32{1, 2}, []int32{1}, 2, 0)))
		}, ErrNotEnoughReplicas},
		{"timeout", func(_ *testing.T, _ *Partition, clk *fakeClock) { clk.Advance(time.Minute) }, ErrRequestTimedOut},
		{"partition removed", func(_ *testing.T, p *Partition, _ *fakeClock) { p.Remove() }, ErrNotLeader},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newFakeClock()
			p := newTestPartition(t, Config{Clock: clk})
			becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 2, 0))
			fetch(t, p, 2, 0, 0)
			info, epoch, err := p.Append(ctx, batch(t, 1, 3), 0, AcksAll)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- p.WaitForCommit(ctx, info.LastOffset, epoch, 30*time.Second) }()
			require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
			tt.act(t, p, clk)
			select {
			case err := <-done:
				requireErr(t, tt.want, err)
			case <-time.After(5 * time.Second):
				t.Fatal("waiter not released")
			}
		})
	}
}

func TestReplicaFetchLongPoll(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	p := newTestPartition(t, Config{Clock: clk})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0))
	type result struct {
		res ReplicaFetchResult
		err error
	}
	poll := func() chan result {
		out := make(chan result, 1)
		go func() {
			res, err := p.HandleReplicaFetch(ctx, ReplicaFetchRequest{ReplicaID: 2, LeaderEpoch: 0, FetchOffset: p.Log().LogEndOffset(), MaxBytes: 1 << 20, MaxWait: time.Minute})
			out <- result{res, err}
		}()
		require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
		return out
	}
	out := poll()
	appendN(t, p, 0, 2)
	r := <-out
	require.NoError(t, r.err)
	require.NotEmpty(t, r.res.Records, "woken by the append")

	clk.Advance(time.Minute) // drop the old timer
	out = poll()
	clk.Advance(time.Minute)
	r = <-out
	require.NoError(t, r.err)
	require.Empty(t, r.res.Records, "timed out empty")

	out = poll()
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 1, PartitionEpoch: 1, Replicas: []int32{1, 2}})
	r = <-out
	require.ErrorIs(t, r.err, ErrFencedLeaderEpoch, "woken by the leadership change")
}

func TestReplicaFetchErrors(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(0, 0, []int32{1, 2}, []int32{1, 2}, 1, 0))
	appendN(t, p, 0, 2)
	_, err := p.HandleReplicaFetch(ctx, ReplicaFetchRequest{ReplicaID: 9, FetchOffset: 0})
	require.ErrorIs(t, err, ErrNotLeader, "not a replica")
	res, err := p.HandleReplicaFetch(ctx, ReplicaFetchRequest{ReplicaID: 2, FetchOffset: 3})
	require.ErrorIs(t, err, storage.ErrOffsetOutOfRange, "beyond the LEO")
	require.Equal(t, int64(2), res.LogEndOffset)
	require.Equal(t, int64(0), p.HighWatermark(), "an invalid offset does not move the follower")
}

func TestOffsetForLeaderEpoch(t *testing.T) {
	p := newTestPartition(t, Config{})
	becomeLeader(t, p, leaderState(1, 0, []int32{1}, []int32{1}, 1, 0))
	appendN(t, p, 1, 5)
	becomeLeader(t, p, leaderState(3, 1, []int32{1}, []int32{1}, 1, 0))
	appendN(t, p, 3, 3)
	tests := []struct {
		current, requested int32
		wantEpoch          int32
		wantEnd            int64
		wantErr            error
	}{
		{3, 1, 1, 5, nil},
		{3, 2, 1, 5, nil},
		{3, 3, 3, 8, nil},
		{3, 0, -1, -1, nil},
		{2, 1, 0, 0, ErrFencedLeaderEpoch},
		{4, 1, 0, 0, ErrNotLeader},
	}
	for _, tt := range tests {
		e, end, logStart, err := p.OffsetForLeaderEpoch(tt.current, tt.requested)
		if tt.wantErr != nil {
			require.ErrorIs(t, err, tt.wantErr)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tt.wantEpoch, e, "requested %d", tt.requested)
		require.Equal(t, tt.wantEnd, end, "requested %d", tt.requested)
		require.Equal(t, int64(0), logStart)
	}
}

func TestFollowerAppendAndTruncate(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 3, PartitionEpoch: 0, Replicas: []int32{1, 2}, ISR: []int32{2}})
	require.ErrorIs(t, p.AppendAsFollower(ctx, 2, replicated(t, 0, 1, 2)), ErrNotLeader, "stale fetcher epoch")
	require.NoError(t, p.AppendAsFollower(ctx, 3, replicated(t, 0, 1, 2)))
	require.NoError(t, p.AppendAsFollower(ctx, 3, replicated(t, 2, 3, 2)))
	require.ErrorIs(t, p.AppendAsFollower(ctx, 3, replicated(t, 9, 3, 1)), storage.ErrOffsetMismatch)
	require.Equal(t, []storage.EpochEntry{{Epoch: 1, StartOffset: 0}, {Epoch: 3, StartOffset: 2}}, p.epochs.Entries())
	latest, ok := p.LatestEpoch()
	require.True(t, ok)
	require.Equal(t, int32(3), latest)
	e, end := p.LocalEndOffsetFor(1)
	require.Equal(t, int32(1), e)
	require.Equal(t, int64(2), end)

	p.UpdateFollowerHW(3, 100)
	require.Equal(t, int64(4), p.HighWatermark(), "bounded by the local LEO")
	p.UpdateFollowerHW(2, 1)
	require.Equal(t, int64(4), p.HighWatermark(), "stale epoch ignored")

	leo, err := p.TruncateTo(ctx, 3, 2)
	require.NoError(t, err)
	require.Equal(t, int64(2), leo)
	require.Equal(t, []storage.EpochEntry{{Epoch: 1, StartOffset: 0}}, p.epochs.Entries())
	require.Equal(t, int64(2), p.HighWatermark(), "lowered with the log")

	require.NoError(t, p.TruncateFullyAndStartAt(ctx, 3, 50))
	require.Equal(t, int64(50), p.Log().LogEndOffset())
	require.Equal(t, int64(50), p.HighWatermark())
	_, ok = p.LatestEpoch()
	require.False(t, ok)

	becomeLeader(t, p, leaderState(4, 1, []int32{1, 2}, []int32{1}, 1, 0))
	_, err = p.TruncateTo(ctx, 4, 0)
	require.ErrorIs(t, err, ErrNotLeader, "a leader never truncates through the follower path")
}

func TestBecomeFollowerHWFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	require.NoError(t, p.AppendAsFollower(ctx, 0, replicated(t, 0, 0, 5)))
	r := reopen(p)
	r.BecomeFollower(State{Leader: 2, LeaderEpoch: 1, Replicas: []int32{1, 2}, HWCheckpoint: 3})
	require.Equal(t, int64(3), r.HighWatermark(), "a restarted follower resumes from its checkpoint")
	r = reopen(p)
	r.BecomeFollower(State{Leader: 2, LeaderEpoch: 1, Replicas: []int32{1, 2}, HWCheckpoint: 50})
	require.Equal(t, int64(5), r.HighWatermark(), "clamped to the LEO")
}

func TestStaleHWCheckpointIgnoredAfterFirstTransition(t *testing.T) {
	ctx := context.Background()
	p := newTestPartition(t, Config{})
	p.BecomeFollower(State{Leader: 2, LeaderEpoch: 0, Replicas: []int32{1, 2}})
	require.NoError(t, p.AppendAsFollower(ctx, 0, replicated(t, 0, 0, 4)))
	require.NoError(t, p.AppendAsFollower(ctx, 0, replicated(t, 4, 0, 4)))
	p.UpdateFollowerHW(0, 8)
	_, err := p.TruncateTo(ctx, 0, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), p.HighWatermark())
	require.NoError(t, p.AppendAsFollower(ctx, 0, replicated(t, 4, 0, 4))) // uncommitted refetch
	becomeLeader(t, p, leaderState(1, 1, []int32{1, 2}, []int32{1, 2}, 1, 8))
	require.Equal(t, int64(4), p.HighWatermark(), "the pre-truncation checkpoint must not expose new data")
}
