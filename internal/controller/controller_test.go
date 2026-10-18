package controller

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeStore applies commands to a real metadata.State with Raft-like term checks.
type fakeStore struct {
	mu        sync.Mutex
	state     *metadata.State
	index     uint64
	leader    bool
	term      uint64
	commit    uint64
	applied   []*streamqv1.MetadataCommand
	listeners []func(bool)
	// afterApply runs after each ApplyForwarded (outside the lock), e.g. to change the term.
	afterApply func()
}

func newFakeStore() *fakeStore {
	return &fakeStore{state: metadata.NewState(nil, 0), leader: true, term: 1}
}

func (f *fakeStore) Snapshot() metadata.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.state.Snapshot()
	s.Index = f.index
	return s
}

func (f *fakeStore) ApplyForwarded(_ context.Context, cmd *streamqv1.MetadataCommand) (uint64, error) {
	if f.afterApply != nil {
		defer f.afterApply()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leader {
		return 0, metadata.ErrNotController
	}
	f.index++
	f.commit = f.index
	_, err := f.state.ApplyInTerm(cmd, f.term)
	if err == nil {
		f.applied = append(f.applied, cmd)
	}
	return f.index, err
}

func (f *fakeStore) CommitIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commit
}

func (f *fakeStore) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}

func (f *fakeStore) Term() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.term
}

// Barrier mimics Raft: the new leader's no-op and the barrier entry advance the commit index but
// are never applied to the FSM, so the applied command index stays behind it.
func (f *fakeStore) Barrier(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commit += 2
	return nil
}

func (f *fakeStore) OnLeadershipChange(fn func(bool)) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listeners = append(f.listeners, fn)
	return func() {}
}

func (f *fakeStore) setLeader(leader bool, term uint64) {
	f.mu.Lock()
	f.leader, f.term = leader, term
	f.mu.Unlock()
}

func (f *fakeStore) commands(kind string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.applied {
		switch kind {
		case "fence":
			if c.GetFenceBroker() != nil {
				n++
			}
		case "unfence":
			if c.GetUnfenceBroker() != nil {
				n++
			}
		}
	}
	return n
}

// fakeClock is advanced manually; the ticker is driven by step calls in the tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) NewTicker(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }

func newTestController(t *testing.T) (*Controller, *fakeStore, *fakeClock) {
	t.Helper()
	st := newFakeStore()
	clk := &fakeClock{now: time.UnixMilli(1_700_000_000_000)}
	c := New(Config{BrokerID: 1, SessionTimeout: 9 * time.Second}, st, clk, slog.New(slog.DiscardHandler))
	c.step(context.Background()) // activate
	return c, st, clk
}

func register(t *testing.T, c *Controller, id int32) RegisterResult {
	t.Helper()
	res, err := c.Register(context.Background(), RegisterRequest{BrokerID: id, Host: "h", Port: 9000 + id, InternalAddr: "i"})
	require.NoError(t, err)
	return res
}

func TestRegisterThenUnfenceOnceCaughtUp(t *testing.T) {
	c, st, _ := newTestController(t)
	ctx := context.Background()
	res := register(t, c, 2)
	require.Equal(t, int64(1), res.BrokerEpoch)
	b, _ := st.Snapshot().Broker(2)
	require.True(t, b.Fenced, "registered fenced")

	fenced, err := c.Heartbeat(ctx, 2, 1, res.CatchUpIndex-1)
	require.NoError(t, err)
	require.True(t, fenced, "not caught up yet")
	fenced, err = c.Heartbeat(ctx, 2, 1, res.CatchUpIndex)
	require.NoError(t, err)
	require.False(t, fenced)
	b, _ = st.Snapshot().Broker(2)
	require.False(t, b.Fenced)
	// Further heartbeats do not propose more unfences.
	_, err = c.Heartbeat(ctx, 2, 1, res.CatchUpIndex+5)
	require.NoError(t, err)
	require.Equal(t, 1, st.commands("unfence"))

	_, err = c.Heartbeat(ctx, 2, 7, 100)
	require.ErrorIs(t, err, ErrStaleBrokerEpoch)
	_, err = c.Heartbeat(ctx, 9, 1, 100)
	require.ErrorIs(t, err, ErrStaleBrokerEpoch)
}

func TestMissedHeartbeatsFenceExactlyOnce(t *testing.T) {
	c, st, clk := newTestController(t)
	ctx := context.Background()
	for _, id := range []int32{2, 3} {
		res := register(t, c, id)
		_, err := c.Heartbeat(ctx, id, res.BrokerEpoch, res.CatchUpIndex)
		require.NoError(t, err)
	}
	for range 5 {
		clk.Advance(3 * time.Second)
		_, err := c.Heartbeat(ctx, 3, 1, 1000) // broker 3 keeps heartbeating
		require.NoError(t, err)
		c.step(ctx)
	}
	b2, _ := st.Snapshot().Broker(2)
	b3, _ := st.Snapshot().Broker(3)
	require.True(t, b2.Fenced, "broker 2 missed its session timeout")
	require.False(t, b3.Fenced)
	require.Equal(t, 1, st.commands("fence"), "fenced exactly once")
}

func TestGracePeriodAfterGainingLeadership(t *testing.T) {
	c, st, clk := newTestController(t)
	ctx := context.Background()
	res := register(t, c, 2)
	_, err := c.Heartbeat(ctx, 2, res.BrokerEpoch, res.CatchUpIndex)
	require.NoError(t, err)

	// Leadership moves away and comes back in a new term long after the last heartbeat.
	st.setLeader(false, 2)
	c.step(ctx)
	_, err = c.Heartbeat(ctx, 2, 1, 100)
	require.ErrorIs(t, err, metadata.ErrNotController)
	clk.Advance(time.Minute)
	st.setLeader(true, 3)
	c.step(ctx)
	b, _ := st.Snapshot().Broker(2)
	require.False(t, b.Fenced, "a new controller grants a grace period instead of fencing immediately")
	clk.Advance(10 * time.Second)
	c.step(ctx)
	b, _ = st.Snapshot().Broker(2)
	require.True(t, b.Fenced, "fenced once the grace period expires without heartbeats")
}

func TestDecisionsAreStampedWithActivationTerm(t *testing.T) {
	c, st, clk := newTestController(t)
	ctx := context.Background()
	res := register(t, c, 2)
	_, err := c.Heartbeat(ctx, 2, res.BrokerEpoch, res.CatchUpIndex)
	require.NoError(t, err)
	clk.Advance(time.Minute)
	// The node lost and regained leadership (term 1 → 5) but has not stepped yet: its next fence
	// decision must not be committed under the new term.
	st.setLeader(true, 5)
	c.checkLiveness(ctx)
	b, _ := st.Snapshot().Broker(2)
	require.False(t, b.Fenced, "a decision stamped with term 1 is rejected in term 5")
	require.Equal(t, 0, st.commands("fence"))
	c.step(ctx) // re-activates in term 5 with a fresh grace period
	clk.Advance(10 * time.Second)
	c.step(ctx)
	b, _ = st.Snapshot().Broker(2)
	require.True(t, b.Fenced)
}

func TestHeartbeatAfterControllerChangeSetsCatchUp(t *testing.T) {
	c, st, _ := newTestController(t)
	ctx := context.Background()
	res := register(t, c, 2)
	// A new controller (fresh in-memory state) receives the fenced broker's heartbeat.
	c2 := New(Config{BrokerID: 3, SessionTimeout: 9 * time.Second}, st, &fakeClock{now: time.UnixMilli(1)}, slog.New(slog.DiscardHandler))
	c2.step(ctx)
	fenced, err := c2.Heartbeat(ctx, 2, res.BrokerEpoch, 0)
	require.NoError(t, err)
	require.True(t, fenced)
	// The broker reports the applied command index (all it can ever observe), which is below the
	// commit index after the barrier: it must still be unfenced (no further metadata writes).
	require.Less(t, st.Snapshot().Index, st.CommitIndex())
	fenced, err = c2.Heartbeat(ctx, 2, res.BrokerEpoch, st.Snapshot().Index)
	require.NoError(t, err)
	require.False(t, fenced)
}

func TestNotLeaderRejectsRequests(t *testing.T) {
	st := newFakeStore()
	st.setLeader(false, 1)
	c := New(Config{BrokerID: 1, SessionTimeout: time.Second}, st, &fakeClock{}, slog.New(slog.DiscardHandler))
	c.step(context.Background())
	_, err := c.Register(context.Background(), RegisterRequest{BrokerID: 2, Host: "h", Port: 1})
	require.ErrorIs(t, err, metadata.ErrNotController)
}

func TestRunStopsOnCancel(t *testing.T) {
	st := newFakeStore()
	c := New(Config{BrokerID: 1, SessionTimeout: time.Second}, st, SystemClock{}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.Run(ctx) }()
	require.Eventually(t, func() bool { _, err := c.activeTerm(); return err == nil }, 5*time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestAllocateProducerIDs(t *testing.T) {
	ctx := context.Background()
	c, st, _ := newTestController(t)
	r1, r2 := register(t, c, 1), register(t, c, 2)
	a, err := c.AllocateProducerIDs(ctx, 1, r1.BrokerEpoch)
	require.NoError(t, err)
	b, err := c.AllocateProducerIDs(ctx, 2, r2.BrokerEpoch)
	require.NoError(t, err)
	a2, err := c.AllocateProducerIDs(ctx, 1, r1.BrokerEpoch)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1000, 2000}, []int64{a, b, a2}, "distinct blocks")

	_, err = c.AllocateProducerIDs(ctx, 1, r1.BrokerEpoch+5)
	require.ErrorIs(t, err, ErrStaleBrokerEpoch)

	// The term moves while the allocation is applied: the result is not trusted (plan R13).
	st.afterApply = func() { st.setLeader(true, 2) }
	_, err = c.AllocateProducerIDs(ctx, 1, r1.BrokerEpoch)
	require.ErrorIs(t, err, metadata.ErrNotController)
}
