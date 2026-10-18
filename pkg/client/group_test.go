package client

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// groupFake is a fakeBroker that also acts as a single-member-at-a-time group coordinator with
// scriptable hooks. Group state is guarded by gmu (never held together with fakeBroker.mu).
type groupFake struct {
	*fakeBroker

	gmu         sync.Mutex
	coordinator *streamqv1.Broker // FindCoordinator answer; nil = this broker
	generation  int32
	seq         int
	members     map[string]bool
	committed   map[TopicPartition]int64
	partErr     map[TopicPartition]streamqv1.ErrorCode // per-partition commit results
	events      []string
	joins       []*streamqv1.JoinGroupRequest
	joinTTL     []time.Duration // remaining RPC deadline seen by each JoinGroup (0 = none)
	commits     []*streamqv1.CommitOffsetRequest
	fetches     []*streamqv1.FetchOffsetRequest
	leaves      []*streamqv1.LeaveGroupRequest
	heartbeats  int
	finds       int

	// Optional overrides; return (nil, nil) to fall through to the default behaviour.
	onJoin      func(*streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error)
	onHeartbeat func(*streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error)
	onCommit    func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error)
}

func newGroupFake(id int32, host string, topics map[string]int32) *groupFake {
	fb := newFakeBroker(id, host, 9092)
	for n, p := range topics {
		fb.topics[n] = p
	}
	return &groupFake{fakeBroker: fb, members: map[string]bool{}, committed: map[TopicPartition]int64{},
		partErr: map[TopicPartition]streamqv1.ErrorCode{}}
}

func (f *groupFake) self() *streamqv1.Broker {
	return &streamqv1.Broker{Id: f.id, Host: f.host, Port: f.port}
}

func (f *groupFake) event(name string) {
	f.events = append(f.events, name)
}

func (f *groupFake) setCoordinator(b *streamqv1.Broker) {
	f.gmu.Lock()
	f.coordinator = b
	f.gmu.Unlock()
}

func (f *groupFake) setHeartbeat(h func(*streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error)) {
	f.gmu.Lock()
	f.onHeartbeat = h
	f.gmu.Unlock()
}

func (f *groupFake) setJoin(h func(*streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error)) {
	f.gmu.Lock()
	f.onJoin = h
	f.gmu.Unlock()
}

func (f *groupFake) setCommit(h func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error)) {
	f.gmu.Lock()
	f.onCommit = h
	f.gmu.Unlock()
}

func (f *groupFake) snapshot() (events []string, joins []*streamqv1.JoinGroupRequest, commits []*streamqv1.CommitOffsetRequest) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	return slices.Clone(f.events), slices.Clone(f.joins), slices.Clone(f.commits)
}

func (f *groupFake) counts() (finds, heartbeats int) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	return f.finds, f.heartbeats
}

func (f *groupFake) joinMembers() []string {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	var out []string
	for _, j := range f.joins {
		out = append(out, j.GetMemberId())
	}
	return out
}

func (f *groupFake) FindCoordinator(context.Context, *streamqv1.FindCoordinatorRequest) (*streamqv1.FindCoordinatorResponse, error) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	f.finds++
	f.event("find")
	b := f.coordinator
	if b == nil {
		b = f.self()
	}
	return &streamqv1.FindCoordinatorResponse{Coordinator: b}, nil
}

func (f *groupFake) JoinGroup(ctx context.Context, req *streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error) {
	var ttl time.Duration
	if d, ok := ctx.Deadline(); ok {
		ttl = time.Until(d)
	}
	f.gmu.Lock()
	f.event("join")
	f.joins = append(f.joins, req)
	f.joinTTL = append(f.joinTTL, ttl)
	hook := f.onJoin
	f.gmu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.mu.Lock()
	var assignment []*streamqv1.TopicPartitions
	for _, t := range req.GetTopics() {
		if n, ok := f.topics[t]; ok {
			tp := &streamqv1.TopicPartitions{Topic: t}
			for p := range n {
				tp.Partitions = append(tp.Partitions, p)
			}
			assignment = append(assignment, tp)
		}
	}
	f.mu.Unlock()

	f.gmu.Lock()
	defer f.gmu.Unlock()
	member := req.GetMemberId()
	switch {
	case member == "":
		f.seq++
		member = fmt.Sprintf("m-%d", f.seq)
	case !f.members[member]:
		return &streamqv1.JoinGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID}, nil
	}
	f.members[member] = true
	f.generation++
	return &streamqv1.JoinGroupResponse{GenerationId: f.generation, MemberId: member, Assignor: "range", Assignment: assignment}, nil
}

func (f *groupFake) SyncGroup(_ context.Context, req *streamqv1.SyncGroupRequest) (*streamqv1.SyncGroupResponse, error) {
	f.gmu.Lock()
	f.event("sync")
	switch {
	case !f.members[req.GetMemberId()]:
		f.gmu.Unlock()
		return &streamqv1.SyncGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID}, nil
	case req.GetGenerationId() != f.generation:
		f.gmu.Unlock()
		return &streamqv1.SyncGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION}, nil
	}
	f.gmu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &streamqv1.SyncGroupResponse{}
	// Reverse topic/partition order on purpose: the client must sort.
	names := make([]string, 0, len(f.topics))
	for n := range f.topics {
		names = append(names, n)
	}
	slices.Sort(names)
	slices.Reverse(names)
	for _, n := range names {
		tp := &streamqv1.TopicPartitions{Topic: n}
		for p := f.topics[n] - 1; p >= 0; p-- {
			tp.Partitions = append(tp.Partitions, p)
		}
		resp.Assignment = append(resp.Assignment, tp)
	}
	return resp, nil
}

func (f *groupFake) Heartbeat(_ context.Context, req *streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error) {
	f.gmu.Lock()
	f.heartbeats++
	f.event("heartbeat")
	hook := f.onHeartbeat
	f.gmu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	return &streamqv1.HeartbeatResponse{}, nil
}

func (f *groupFake) LeaveGroup(_ context.Context, req *streamqv1.LeaveGroupRequest) (*streamqv1.LeaveGroupResponse, error) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	f.event("leave")
	f.leaves = append(f.leaves, req)
	delete(f.members, req.GetMemberId())
	return &streamqv1.LeaveGroupResponse{}, nil
}

func (f *groupFake) CommitOffset(_ context.Context, req *streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
	f.gmu.Lock()
	f.event("commit")
	f.commits = append(f.commits, req)
	hook := f.onCommit
	f.gmu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.gmu.Lock()
	defer f.gmu.Unlock()
	switch {
	case !f.members[req.GetMemberId()]:
		return &streamqv1.CommitOffsetResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID}, nil
	case req.GetGenerationId() != f.generation:
		return &streamqv1.CommitOffsetResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION}, nil
	}
	resp := &streamqv1.CommitOffsetResponse{}
	for _, o := range req.GetOffsets() {
		tp := TopicPartition{Topic: o.GetTopic(), Partition: o.GetPartition()}
		code := f.partErr[tp]
		if code == streamqv1.ErrorCode_ERROR_CODE_NONE {
			f.committed[tp] = o.GetOffset()
		}
		resp.Results = append(resp.Results, &streamqv1.OffsetCommitResult{Topic: tp.Topic, Partition: tp.Partition, ErrorCode: code})
	}
	return resp, nil
}

func (f *groupFake) FetchOffset(_ context.Context, req *streamqv1.FetchOffsetRequest) (*streamqv1.FetchOffsetResponse, error) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	f.event("fetchOffset")
	f.fetches = append(f.fetches, req)
	resp := &streamqv1.FetchOffsetResponse{}
	for _, t := range req.GetPartitions() {
		for _, p := range t.GetPartitions() {
			off, ok := f.committed[TopicPartition{Topic: t.GetTopic(), Partition: p}]
			if !ok {
				off = -1
			}
			resp.Offsets = append(resp.Offsets, &streamqv1.OffsetCommit{Topic: t.GetTopic(), Partition: p, Offset: off})
		}
	}
	return resp, nil
}

func (f *groupFake) committedOffset(tp TopicPartition) (int64, bool) {
	f.gmu.Lock()
	defer f.gmu.Unlock()
	off, ok := f.committed[tp]
	return off, ok
}

// groupHeartbeatOnce answers the next heartbeat with code (or err), then NONE.
func groupHeartbeatOnce(code streamqv1.ErrorCode, err error) func(*streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error) {
	var once sync.Once
	return func(*streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error) {
		var resp *streamqv1.HeartbeatResponse
		var rerr error
		once.Do(func() {
			if err != nil {
				rerr = err
				return
			}
			resp = &streamqv1.HeartbeatResponse{ErrorCode: code}
		})
		return resp, rerr
	}
}

// groupTestHeartbeat is the heartbeat interval tests use unless they set their own.
const groupTestHeartbeat = time.Second

// groupEnv is one fake coordinator/broker "b1:9092" plus a client.
type groupEnv struct {
	fc  *fakeCluster
	gf  *groupFake
	c   *Client
	clk *fakeClock
}

func newGroupEnv(t *testing.T, topics map[string]int32) *groupEnv {
	t.Helper()
	fc := newFakeCluster(t)
	gf := newGroupFake(1, "b1", topics)
	fc.add("b1:9092", gf)
	return &groupEnv{fc: fc, gf: gf, c: fc.newClient("b1:9092"), clk: newFakeClock()}
}

// consumer returns a GroupConsumer on the env's fake clock, closed at test cleanup.
func (e *groupEnv) consumer(t *testing.T, cfg GroupConsumerConfig) *GroupConsumer {
	t.Helper()
	if cfg.GroupID == "" {
		cfg.GroupID = "g"
	}
	if len(cfg.Topics) == 0 {
		cfg.Topics = []string{"t"}
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = groupTestHeartbeat
	}
	if cfg.AutoCommitInterval == 0 {
		cfg.AutoCommitInterval = time.Hour
	}
	g, err := newGroupConsumer(e.c, cfg, e.clk)
	require.NoError(t, err)
	t.Cleanup(func() { _ = g.Close(context.Background()) })
	return g
}

// groupPollN polls until n records arrive.
func groupPollN(t *testing.T, g *GroupConsumer, n int) []Record {
	t.Helper()
	var out []Record
	for i := 0; i < 200 && len(out) < n; i++ {
		recs, err := g.Poll(context.Background())
		require.NoError(t, err)
		out = append(out, recs...)
	}
	require.Len(t, out, n)
	return out
}

// groupOffsets returns the offsets of recs for tp.
func groupOffsets(recs []Record, tp TopicPartition) []int64 {
	var out []int64
	for _, r := range recs {
		if r.Topic == tp.Topic && r.Partition == tp.Partition {
			out = append(out, r.Offset)
		}
	}
	return out
}

func groupRange(from, to int64) []int64 {
	var out []int64
	for o := from; o < to; o++ {
		out = append(out, o)
	}
	return out
}

// groupFireHeartbeat waits for the heartbeat timer to be armed and fires it.
func groupFireHeartbeat(t *testing.T, clk *fakeClock) {
	t.Helper()
	require.Eventually(t, func() bool { return clk.pendingTimers() == 1 }, 5*time.Second, time.Millisecond)
	clk.Advance(groupTestHeartbeat)
}

func TestGroupConsumerConfig(t *testing.T) {
	_, c := singleBroker(t, nil)
	for _, tc := range []struct {
		name string
		cfg  GroupConsumerConfig
	}{
		{"no group", GroupConsumerConfig{Topics: []string{"t"}}},
		{"no topics", GroupConsumerConfig{GroupID: "g"}},
		{"empty topic", GroupConsumerConfig{GroupID: "g", Topics: []string{"t", ""}}},
		{"heartbeat not below session", GroupConsumerConfig{GroupID: "g", Topics: []string{"t"},
			SessionTimeout: time.Second, HeartbeatInterval: time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewGroupConsumer(c, tc.cfg)
			require.Error(t, err)
		})
	}

	topics := []string{"b", "a", "b"}
	g, err := NewGroupConsumer(c, GroupConsumerConfig{GroupID: "g", Topics: topics})
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a", "b"}, topics, "caller's slice untouched")
	require.Equal(t, []string{"a", "b"}, g.cfg.Topics)
	require.Equal(t, 10*time.Second, g.cfg.SessionTimeout)
	require.Equal(t, 30*time.Second, g.cfg.RebalanceTimeout)
	require.Equal(t, 3*time.Second, g.cfg.HeartbeatInterval)
	require.Equal(t, 5*time.Second, g.cfg.AutoCommitInterval)
	require.Equal(t, []string{"range"}, g.cfg.Assignors)
	require.Equal(t, int32(-1), g.Generation())
	require.Empty(t, g.MemberID())
	require.Empty(t, g.Assignment())
	require.ErrorIs(t, g.Commit(context.Background()), errGroupNotMember)
	require.NoError(t, g.Close(context.Background()))
}

func TestGroupConsumerJoinConsumesFromCommitted(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 2})
	tp0, tp1 := TopicPartition{"t", 0}, TopicPartition{"t", 1}
	consumerSeed(e.gf.fakeBroker, tp0, 10)
	consumerSeed(e.gf.fakeBroker, tp1, 10)
	e.gf.committed[tp0] = 4
	g := e.consumer(t, GroupConsumerConfig{
		SessionTimeout: 7 * time.Second, RebalanceTimeout: 9 * time.Second,
		Assignors: []string{"roundrobin", "range"},
		Consumer:  ConsumerConfig{AutoOffsetReset: ResetEarliest},
	})

	recs := groupPollN(t, g, 16)
	require.Equal(t, groupRange(4, 10), groupOffsets(recs, tp0), "resumes at the committed offset")
	require.Equal(t, groupRange(0, 10), groupOffsets(recs, tp1), "no committed offset: earliest")

	require.Equal(t, []TopicPartition{tp0, tp1}, g.Assignment())
	require.Equal(t, int32(1), g.Generation())
	require.Equal(t, "m-1", g.MemberID())

	events, joins, _ := e.gf.snapshot()
	require.Equal(t, []string{"find", "join", "sync", "fetchOffset"}, events[:4])
	require.Len(t, joins, 1)
	j := joins[0]
	require.Equal(t, "g", j.GetGroupId())
	require.Empty(t, j.GetMemberId())
	require.Equal(t, int32(7000), j.GetSessionTimeoutMs())
	require.Equal(t, int32(9000), j.GetRebalanceTimeoutMs())
	require.Equal(t, []string{"t"}, j.GetTopics())
	require.Equal(t, []string{"roundrobin", "range"}, j.GetAssignors())
	e.gf.gmu.Lock()
	fetch := e.gf.fetches[0]
	e.gf.gmu.Unlock()
	require.Equal(t, "g", fetch.GetGroupId())
	require.Len(t, fetch.GetPartitions(), 1)
	require.Equal(t, []int32{0, 1}, fetch.GetPartitions()[0].GetPartitions())
}

func TestGroupConsumerOffsetReset(t *testing.T) {
	tp := TopicPartition{"t", 0}
	for _, tc := range []struct {
		name      string
		reset     OffsetReset
		committed int64 // -1: none
		want      []int64
	}{
		{"earliest without commit", ResetEarliest, -1, groupRange(0, 13)},
		{"latest without commit", ResetLatest, -1, groupRange(10, 13)},
		{"committed out of range, earliest", ResetEarliest, 50, groupRange(0, 13)},
		{"committed out of range, latest", ResetLatest, 50, groupRange(10, 13)},
		{"committed in range", ResetLatest, 7, groupRange(7, 13)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newGroupEnv(t, map[string]int32{"t": 1})
			consumerSeed(e.gf.fakeBroker, tp, 10)
			if tc.committed >= 0 {
				e.gf.committed[tp] = tc.committed
			}
			g := e.consumer(t, GroupConsumerConfig{Consumer: ConsumerConfig{AutoOffsetReset: tc.reset}})

			// Poll until the position is settled (and any available records consumed).
			var recs []Record
			for i := 0; i < 50; i++ {
				got, err := g.Poll(context.Background())
				require.NoError(t, err)
				recs = append(recs, got...)
				if pos, ok := g.inner.Position(tp); ok && pos >= 10 {
					break
				}
			}
			pos, ok := g.inner.Position(tp)
			require.True(t, ok)
			require.Equal(t, int64(10), pos)

			consumerSeed(e.gf.fakeBroker, tp, 3)
			recs = append(recs, groupPollN(t, g, len(tc.want)-len(recs))...)
			require.Equal(t, tc.want, groupOffsets(recs, tp))
		})
	}
}

func TestGroupConsumerHeartbeatRebalanceCommitsThenRejoins(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 1})
	tp := TopicPartition{"t", 0}
	consumerSeed(e.gf.fakeBroker, tp, 10)
	g := e.consumer(t, GroupConsumerConfig{Consumer: ConsumerConfig{AutoOffsetReset: ResetEarliest}})
	groupPollN(t, g, 10)
	require.Equal(t, int32(1), g.Generation())

	e.gf.setHeartbeat(groupHeartbeatOnce(streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS, nil))
	groupFireHeartbeat(t, e.clk)
	require.Eventually(t, g.mustJoin, 5*time.Second, time.Millisecond)
	require.Equal(t, int32(1), g.Generation(), "still a member until the rejoin")
	require.Equal(t, 0, e.clk.pendingTimers(), "the old generation's heartbeats stopped")

	before, _, _ := e.gf.snapshot()
	consumerSeed(e.gf.fakeBroker, tp, 2)
	recs := groupPollN(t, g, 2)
	require.Equal(t, []int64{10, 11}, groupOffsets(recs, tp), "resumes at the commit made before rejoining")

	events, joins, commits := e.gf.snapshot()
	require.Equal(t, []string{"commit", "join", "sync", "fetchOffset"}, events[len(before):len(before)+4])
	require.Len(t, commits, 1)
	require.Equal(t, int32(1), commits[0].GetGenerationId())
	require.Equal(t, "m-1", commits[0].GetMemberId())
	require.Equal(t, int64(10), commits[0].GetOffsets()[0].GetOffset())
	require.Equal(t, "m-1", joins[1].GetMemberId(), "rejoins with its member id")
	require.Equal(t, int32(2), g.Generation())
	require.Eventually(t, func() bool { return e.clk.pendingTimers() == 1 }, 5*time.Second, time.Millisecond,
		"one heartbeat goroutine for the new generation")
}

func TestGroupConsumerHeartbeatErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		code        streamqv1.ErrorCode
		err         error
		clearMember bool
		rediscover  bool
	}{
		{"rebalance in progress", streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS, nil, false, false},
		{"illegal generation", streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION, nil, false, false},
		{"unknown member", streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID, nil, true, false},
		{"not coordinator", streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR, nil, false, true},
		{"transport", streamqv1.ErrorCode_ERROR_CODE_NONE, status.Error(codes.Unavailable, "down"), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newGroupEnv(t, map[string]int32{"t": 1})
			g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true})
			_, err := g.Poll(context.Background())
			require.NoError(t, err)
			require.Equal(t, "m-1", g.MemberID())

			e.gf.setHeartbeat(groupHeartbeatOnce(tc.code, tc.err))
			groupFireHeartbeat(t, e.clk)
			require.Eventually(t, g.mustJoin, 5*time.Second, time.Millisecond)
			if tc.clearMember {
				require.Empty(t, g.MemberID())
				require.Equal(t, int32(-1), g.Generation())
			} else {
				require.Equal(t, "m-1", g.MemberID())
			}

			_, err = g.Poll(context.Background())
			require.NoError(t, err)
			wantMember := "m-1"
			if tc.clearMember {
				wantMember = ""
			}
			require.Equal(t, []string{"", wantMember}, e.gf.joinMembers())
			finds, _ := e.gf.counts()
			if tc.rediscover {
				require.Equal(t, 2, finds)
			} else {
				require.Equal(t, 1, finds)
			}
			require.Equal(t, int32(2), g.Generation())
			require.False(t, g.mustJoin())
		})
	}
}

func TestGroupConsumerAutoCommitInterval(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 1})
	tp := TopicPartition{"t", 0}
	consumerSeed(e.gf.fakeBroker, tp, 10)
	g := e.consumer(t, GroupConsumerConfig{
		SessionTimeout: 2 * time.Hour, HeartbeatInterval: time.Hour, AutoCommitInterval: 5 * time.Second,
		Consumer: ConsumerConfig{AutoOffsetReset: ResetEarliest},
	})
	groupPollN(t, g, 10)
	numCommits := func() int { _, _, c := e.gf.snapshot(); return len(c) }
	require.Zero(t, numCommits())

	e.clk.Advance(4 * time.Second)
	_, err := g.Poll(context.Background())
	require.NoError(t, err)
	require.Zero(t, numCommits(), "interval not elapsed")

	e.clk.Advance(time.Second)
	_, err = g.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, numCommits())
	off, ok := e.gf.committedOffset(tp)
	require.True(t, ok)
	require.Equal(t, int64(10), off)

	consumerSeed(e.gf.fakeBroker, tp, 2)
	groupPollN(t, g, 2)
	require.Equal(t, 1, numCommits(), "polls within the interval do not commit")
	e.clk.Advance(5 * time.Second)
	_, err = g.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, numCommits())
	off, _ = e.gf.committedOffset(tp)
	require.Equal(t, int64(12), off)
}

func TestGroupConsumerClose(t *testing.T) {
	for _, tc := range []struct {
		name       string
		autoCommit bool
	}{{"auto commit", true}, {"manual commit", false}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newGroupEnv(t, map[string]int32{"t": 1})
			tp := TopicPartition{"t", 0}
			consumerSeed(e.gf.fakeBroker, tp, 10)
			g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: !tc.autoCommit,
				Consumer: ConsumerConfig{AutoOffsetReset: ResetEarliest}})
			groupPollN(t, g, 10)
			require.Eventually(t, func() bool { return e.clk.pendingTimers() == 1 }, 5*time.Second, time.Millisecond)

			require.NoError(t, g.Close(context.Background()))
			require.Zero(t, e.clk.pendingTimers(), "heartbeats stopped")
			events, _, commits := e.gf.snapshot()
			if tc.autoCommit {
				require.Len(t, commits, 1)
				require.Equal(t, int64(10), commits[0].GetOffsets()[0].GetOffset())
				require.Equal(t, []string{"commit", "leave"}, events[len(events)-2:])
			} else {
				require.Empty(t, commits)
				require.Equal(t, "leave", events[len(events)-1])
			}
			e.gf.gmu.Lock()
			require.Len(t, e.gf.leaves, 1)
			require.Equal(t, "m-1", e.gf.leaves[0].GetMemberId())
			e.gf.gmu.Unlock()

			require.Equal(t, int32(-1), g.Generation())
			require.Empty(t, g.MemberID())
			require.Empty(t, g.Assignment())
			require.NoError(t, g.Close(context.Background()), "idempotent")
			_, err := g.Poll(context.Background())
			require.ErrorIs(t, err, ErrClosed)
			require.ErrorIs(t, g.Commit(context.Background()), ErrClosed)
			e.gf.gmu.Lock()
			require.Len(t, e.gf.leaves, 1)
			e.gf.gmu.Unlock()
		})
	}
}

func TestGroupConsumerCloseBeforeJoin(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 1})
	g := e.consumer(t, GroupConsumerConfig{})
	require.NoError(t, g.Close(context.Background()))
	events, _, _ := e.gf.snapshot()
	require.Empty(t, events, "no RPCs for a consumer that never joined")
}

func TestGroupConsumerRediscovery(t *testing.T) {
	t.Run("heartbeat not coordinator", func(t *testing.T) {
		e := newGroupEnv(t, map[string]int32{"t": 1})
		b2 := newGroupFake(2, "b2", map[string]int32{"t": 1})
		e.fc.add("b2:9092", b2)
		g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true})
		_, err := g.Poll(context.Background())
		require.NoError(t, err)
		require.Equal(t, "m-1", g.MemberID())

		// The coordinator moves to b2; b1 answers NOT_COORDINATOR.
		e.gf.setCoordinator(b2.self())
		e.gf.setHeartbeat(func(*streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error) {
			return &streamqv1.HeartbeatResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR}, nil
		})
		groupFireHeartbeat(t, e.clk)
		require.Eventually(t, g.mustJoin, 5*time.Second, time.Millisecond)

		_, err = g.Poll(context.Background())
		require.NoError(t, err)
		finds, _ := e.gf.counts()
		require.Equal(t, 2, finds)
		// b2 does not know m-1: UNKNOWN_MEMBER_ID clears the id and the client joins afresh.
		require.Equal(t, []string{"m-1", ""}, b2.joinMembers())
		require.Equal(t, "m-1", g.MemberID())
		require.Equal(t, int32(1), g.Generation())

		groupFireHeartbeat(t, e.clk)
		require.Eventually(t, func() bool { _, hb := b2.counts(); return hb == 1 }, 5*time.Second, time.Millisecond,
			"heartbeats go to the new coordinator")
	})

	t.Run("join not coordinator", func(t *testing.T) {
		e := newGroupEnv(t, map[string]int32{"t": 1})
		b2 := newGroupFake(2, "b2", map[string]int32{"t": 1})
		e.fc.add("b2:9092", b2)
		e.gf.setJoin(func(*streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error) {
			e.gf.setCoordinator(b2.self())
			return &streamqv1.JoinGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR}, nil
		})
		g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true})
		_, err := g.Poll(context.Background())
		require.NoError(t, err)
		finds, _ := e.gf.counts()
		require.Equal(t, 2, finds)
		require.Len(t, e.gf.joinMembers(), 1)
		require.Equal(t, []string{""}, b2.joinMembers())
		require.Equal(t, int32(1), g.Generation())
	})

	t.Run("join transport error", func(t *testing.T) {
		e := newGroupEnv(t, map[string]int32{"t": 1})
		var once sync.Once
		e.gf.setJoin(func(*streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error) {
			var err error
			once.Do(func() { err = status.Error(codes.Unavailable, "down") })
			return nil, err
		})
		g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true})
		_, err := g.Poll(context.Background())
		require.NoError(t, err)
		finds, _ := e.gf.counts()
		require.Equal(t, 2, finds)
		require.Equal(t, int32(1), g.Generation())
	})
}

func TestGroupConsumerJoinUnknownMemberClearsID(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 1})
	g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true})
	_, err := g.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, "m-1", g.MemberID())

	// The coordinator forgot m-1 (e.g. it expired during a long pause).
	e.gf.gmu.Lock()
	delete(e.gf.members, "m-1")
	e.gf.gmu.Unlock()
	e.gf.setHeartbeat(groupHeartbeatOnce(streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS, nil))
	groupFireHeartbeat(t, e.clk)
	require.Eventually(t, g.mustJoin, 5*time.Second, time.Millisecond)

	_, err = g.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"", "m-1", ""}, e.gf.joinMembers())
	require.Equal(t, "m-2", g.MemberID())
	require.Equal(t, int32(2), g.Generation())
}

func TestGroupConsumerJoinDeadline(t *testing.T) {
	for _, tc := range []struct {
		name             string
		rebalanceTimeout time.Duration
		atLeast, atMost  time.Duration
	}{
		{"rebalance timeout above request timeout", 60 * time.Second, 60 * time.Second, 65 * time.Second},
		{"default rebalance timeout", 0, 30 * time.Second, 35 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newGroupEnv(t, map[string]int32{"t": 1})
			g := e.consumer(t, GroupConsumerConfig{RebalanceTimeout: tc.rebalanceTimeout, DisableAutoCommit: true})
			_, err := g.Poll(context.Background())
			require.NoError(t, err)
			e.gf.gmu.Lock()
			ttl := e.gf.joinTTL[0]
			e.gf.gmu.Unlock()
			require.GreaterOrEqual(t, ttl, tc.atLeast-time.Second, "server sees a deadline covering the rebalance timeout")
			require.LessOrEqual(t, ttl, tc.atMost)
		})
	}
}

func TestGroupConsumerCommit(t *testing.T) {
	tp0, tp1 := TopicPartition{"t", 0}, TopicPartition{"t", 1}
	reqErr := func(code streamqv1.ErrorCode) func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
		return func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
			return &streamqv1.CommitOffsetResponse{ErrorCode: code}, nil
		}
	}
	for _, tc := range []struct {
		name        string
		hook        func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error)
		partErr     map[TopicPartition]streamqv1.ErrorCode
		wantErr     error
		rejoin      bool
		clearMember bool
		committed   []TopicPartition
		finds       int
	}{
		{name: "ok", committed: []TopicPartition{tp0, tp1}, finds: 1},
		{name: "illegal generation", hook: reqErr(streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION),
			wantErr: ErrIllegalGeneration, rejoin: true, finds: 1},
		{name: "rebalance in progress", hook: reqErr(streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS),
			wantErr: ErrRebalanceInProgress, rejoin: true, finds: 1},
		{name: "unknown member", hook: reqErr(streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID),
			wantErr: ErrUnknownMemberID, rejoin: true, clearMember: true, finds: 1},
		{name: "request-level unknown topic is retriable without rejoin",
			hook:    reqErr(streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION),
			wantErr: ErrUnknownTopicOrPartition, finds: 1},
		{name: "per-partition error keeps the others",
			partErr: map[TopicPartition]streamqv1.ErrorCode{tp1: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
			wantErr: ErrUnknownTopicOrPartition, committed: []TopicPartition{tp0}, finds: 1},
		{name: "not coordinator rediscovers and retries", hook: func() func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
			var once sync.Once
			return func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
				var resp *streamqv1.CommitOffsetResponse
				once.Do(func() {
					resp = &streamqv1.CommitOffsetResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR}
				})
				return resp, nil
			}
		}(), committed: []TopicPartition{tp0, tp1}, finds: 2},
		{name: "transport error rediscovers and retries", hook: func() func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
			var once sync.Once
			return func(*streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
				var err error
				once.Do(func() { err = status.Error(codes.Unavailable, "down") })
				return nil, err
			}
		}(), committed: []TopicPartition{tp0, tp1}, finds: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newGroupEnv(t, map[string]int32{"t": 2})
			consumerSeed(e.gf.fakeBroker, tp0, 4)
			consumerSeed(e.gf.fakeBroker, tp1, 5)
			for tp, code := range tc.partErr {
				e.gf.partErr[tp] = code
			}
			g := e.consumer(t, GroupConsumerConfig{DisableAutoCommit: true,
				Consumer: ConsumerConfig{AutoOffsetReset: ResetEarliest}})
			groupPollN(t, g, 9)

			e.gf.setCommit(tc.hook)
			err := g.Commit(context.Background())
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.rejoin, g.mustJoin())
			if tc.clearMember {
				require.Empty(t, g.MemberID())
			} else {
				require.Equal(t, "m-1", g.MemberID())
			}
			finds, _ := e.gf.counts()
			require.Equal(t, tc.finds, finds)
			want := map[TopicPartition]int64{tp0: 4, tp1: 5}
			for _, tp := range []TopicPartition{tp0, tp1} {
				off, ok := e.gf.committedOffset(tp)
				if slices.Contains(tc.committed, tp) {
					require.True(t, ok, "%v committed", tp)
					require.Equal(t, want[tp], off)
				} else {
					require.False(t, ok, "%v not committed", tp)
				}
			}
		})
	}
}

func TestGroupConsumerConcurrentAccessors(t *testing.T) {
	e := newGroupEnv(t, map[string]int32{"t": 3})
	for p := range int32(3) {
		consumerSeed(e.gf.fakeBroker, TopicPartition{"t", p}, 30)
	}
	g := e.consumer(t, GroupConsumerConfig{Consumer: ConsumerConfig{AutoOffsetReset: ResetEarliest}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			a := g.Assignment()
			if len(a) > 0 {
				a[0] = TopicPartition{} // the caller owns the returned slice
			}
			_ = g.Generation()
			_ = g.MemberID()
		}
	}()

	seen := map[TopicPartition][]int64{}
	consume := func() {
		recs, err := g.Poll(context.Background())
		require.NoError(t, err)
		for _, r := range recs {
			tp := TopicPartition{r.Topic, r.Partition}
			seen[tp] = append(seen[tp], r.Offset)
		}
	}
	for round := range 3 {
		consume()
		require.Equal(t, int32(round+1), g.Generation())
		require.Equal(t, []TopicPartition{{"t", 0}, {"t", 1}, {"t", 2}}, g.Assignment())
		e.gf.setHeartbeat(groupHeartbeatOnce(streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS, nil))
		groupFireHeartbeat(t, e.clk)
		require.Eventually(t, g.mustJoin, 5*time.Second, time.Millisecond)
	}
	for i := 0; i < 200 && len(seen[TopicPartition{"t", 0}])+len(seen[TopicPartition{"t", 1}])+len(seen[TopicPartition{"t", 2}]) < 90; i++ {
		consume()
	}
	close(stop)
	wg.Wait()
	for p := range int32(3) {
		require.Equal(t, groupRange(0, 30), seen[TopicPartition{"t", p}], "every record once, in order, across rebalances")
	}
}
