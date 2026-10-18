package group

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeClock is a manual clock. Advance runs due callbacks synchronously, in deadline order, on
// the calling goroutine, without holding the clock's lock.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at   time.Time
	f    func()
	done bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), f: f}
	c.timers = append(c.timers, t)
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if t.done {
			return false
		}
		t.done = true
		return true
	}
}

// Advance moves the clock forward by d, firing every timer due by then (including timers
// scheduled by fired callbacks).
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now.Add(d)
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var next *fakeTimer
		live := c.timers[:0]
		for _, t := range c.timers {
			if t.done {
				continue
			}
			live = append(live, t)
			if !t.at.After(target) && (next == nil || t.at.Before(next.at)) {
				next = t
			}
		}
		c.timers = live
		if next == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		if next.at.After(c.now) {
			c.now = next.at
		}
		next.done = true
		c.mu.Unlock()
		next.f()
	}
}

const (
	testSession   = time.Second
	testRebalance = 5 * time.Second
	testDelay     = 100 * time.Millisecond
)

type env struct {
	t     *testing.T
	c     *Coordinator
	clock *fakeClock
	store *local.Store
	dir   string
}

func testConfig() Config {
	return Config{
		BrokerID:                1,
		MaxSessionTimeout:       10 * time.Second,
		DefaultRebalanceTimeout: testRebalance,
		InitialRebalanceDelay:   testDelay,
	}
}

func openStore(t *testing.T, dir string) *local.Store {
	t.Helper()
	s, err := local.Open(context.Background(), dir, metadata.Broker{ID: 1, Host: "localhost", Port: 9092}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	dir := t.TempDir()
	e := &env{t: t, clock: newFakeClock(), dir: dir}
	e.store = openStore(t, dir)
	e.c = New(cfg, e.store, e.clock, nil)
	t.Cleanup(e.c.Close)
	return e
}

func createTopic(t *testing.T, s metadata.Store, name string, partitions int) {
	t.Helper()
	parts := make([]*streamqv1.PartitionAssignment, partitions)
	for i := range parts {
		parts[i] = &streamqv1.PartitionAssignment{Partition: int32(i), Replicas: []int32{1}}
	}
	require.NoError(t, s.Propose(context.Background(), &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd: &streamqv1.MetadataCommand_CreateTopic{CreateTopic: &streamqv1.CreateTopicCommand{
			Name: name, TopicId: "id-" + name, Partitions: parts,
		}},
	}))
}

func deleteTopic(t *testing.T, s metadata.Store, name string) {
	t.Helper()
	require.NoError(t, s.Propose(context.Background(), &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd:     &streamqv1.MetadataCommand_DeleteTopic{DeleteTopic: &streamqv1.DeleteTopicCommand{Name: name}},
	}))
}

type joinOut struct {
	res JoinResult
	err error
}

// pendingJoin is a Join running in its own goroutine.
type pendingJoin struct {
	ch     chan joinOut
	cancel context.CancelFunc
}

func (e *env) startJoin(req JoinRequest) *pendingJoin {
	ctx, cancel := context.WithCancel(context.Background())
	p := &pendingJoin{ch: make(chan joinOut, 1), cancel: cancel}
	e.t.Cleanup(func() {
		cancel()
		<-p.ch // joined: no goroutine outlives the test
		p.ch <- joinOut{}
	})
	go func() {
		res, err := e.c.Join(ctx, req)
		p.ch <- joinOut{res, err}
	}()
	return p
}

// wait returns the Join's outcome; the Join must already have finished or finish promptly.
func (p *pendingJoin) wait(t *testing.T) joinOut {
	t.Helper()
	select {
	case out := <-p.ch:
		p.ch <- out // keep it for cleanup and repeated waits
		return out
	case <-time.After(5 * time.Second):
		require.FailNow(t, "join did not finish")
		return joinOut{}
	}
}

// done reports whether the Join finished.
func (p *pendingJoin) done() bool {
	select {
	case out := <-p.ch:
		p.ch <- out
		return true
	default:
		return false
	}
}

// waitJoined waits until the Join returns successfully and returns its result.
func (p *pendingJoin) result(t *testing.T) JoinResult {
	t.Helper()
	out := p.wait(t)
	require.NoError(t, out.err)
	return out.res
}

// parked returns the number of parked members and the number of members of a group.
func (e *env) parked(groupID string) (parked, members int) {
	e.c.mu.Lock()
	defer e.c.mu.Unlock()
	g := e.c.groups[groupID]
	if g == nil {
		return 0, 0
	}
	for _, m := range g.members {
		if m.waiter != nil {
			parked++
		}
	}
	return parked, len(g.members)
}

// waitParked waits until n members of the group are parked in Join.
func (e *env) waitParked(groupID string, n int) {
	e.t.Helper()
	require.Eventually(e.t, func() bool {
		p, _ := e.parked(groupID)
		return p == n
	}, 5*time.Second, time.Millisecond, "waiting for %d parked members", n)
}

func (e *env) state(groupID string) string {
	d, err := e.c.Describe(groupID)
	require.NoError(e.t, err)
	return d.State
}

// joinAll starts a new-member Join per subscription, waits until all are parked, advances past
// the initial delay and returns the results in order.
func (e *env) joinAll(groupID string, reqs ...JoinRequest) []JoinResult {
	e.t.Helper()
	p0, _ := e.parked(groupID)
	var ps []*pendingJoin
	for i, req := range reqs {
		req.GroupID = groupID
		ps = append(ps, e.startJoin(req))
		e.waitParked(groupID, p0+i+1)
	}
	e.clock.Advance(testDelay)
	out := make([]JoinResult, len(ps))
	for i, p := range ps {
		out[i] = p.result(e.t)
	}
	return out
}

func sub(topics ...string) JoinRequest {
	return JoinRequest{Topics: topics, SessionTimeout: testSession}
}

// requireCover checks that the results share one generation and assign every partition of the
// topics exactly once.
func requireCover(t *testing.T, results []JoinResult, partitions map[string]int) {
	t.Helper()
	seen := map[TopicPartition]string{}
	for _, r := range results {
		require.Equal(t, results[0].GenerationID, r.GenerationID)
		for topic, ps := range r.Assignment {
			for _, p := range ps {
				tp := TopicPartition{topic, p}
				prev, dup := seen[tp]
				require.False(t, dup, "%v assigned to %s and %s", tp, prev, r.MemberID)
				seen[tp] = r.MemberID
			}
		}
	}
	total := 0
	for _, n := range partitions {
		total += n
	}
	require.Len(t, seen, total)
}
