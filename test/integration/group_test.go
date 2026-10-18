//go:build integration

package integration

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
	"github.com/sudo-ss1/streamq/internal/testnode"
	"github.com/sudo-ss1/streamq/pkg/client"
)

const (
	groupSession   = 2 * time.Second
	groupHeartbeat = 200 * time.Millisecond
)

func startGroupNode(t *testing.T, dir string) *node.Node {
	t.Helper()
	return testnode.StartWith(t, dir, func(c *config.Config) {
		c.GroupInitialRebalanceDelayMs = 300
		c.GroupRebalanceTimeoutMs = 5000
		c.GroupSessionTimeoutMs = groupSession.Milliseconds()
	})
}

func newGroupClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.New(client.Config{Bootstrap: []string{addr}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func groupCfg(group, topic string) client.GroupConsumerConfig {
	return client.GroupConsumerConfig{
		GroupID: group, Topics: []string{topic},
		SessionTimeout: groupSession, RebalanceTimeout: 5 * time.Second, HeartbeatInterval: groupHeartbeat,
		AutoCommitInterval: 100 * time.Millisecond,
		Consumer:           client.ConsumerConfig{MaxWait: 100 * time.Millisecond, AutoOffsetReset: client.ResetEarliest},
	}
}

// runner polls a GroupConsumer in its own goroutine and collects record values.
type runner struct {
	gc     *client.GroupConsumer
	cancel context.CancelFunc
	done   chan error
	mu     sync.Mutex
	values []string
}

func startRunner(t *testing.T, c *client.Client, cfg client.GroupConsumerConfig) *runner {
	t.Helper()
	gc, err := client.NewGroupConsumer(c, cfg)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{gc: gc, cancel: cancel, done: make(chan error, 1)}
	go func() {
		for ctx.Err() == nil {
			recs, err := gc.Poll(ctx)
			if err != nil && ctx.Err() == nil && !client.IsRetriable(err) {
				r.done <- err
				return
			}
			r.mu.Lock()
			for _, rec := range recs {
				r.values = append(r.values, string(rec.Value))
			}
			r.mu.Unlock()
		}
		r.done <- nil
	}()
	return r
}

func (r *runner) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	require.NoError(t, <-r.done)
	require.NoError(t, r.gc.Close(context.Background()))
}

func (r *runner) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.values)
}

// stableCover reports whether all consumers share one generation and their assignments are
// disjoint and together cover partitions 0..n-1 of topic.
func stableCover(gcs []*client.GroupConsumer, topic string, n int32) bool {
	gen := gcs[0].Generation()
	if gen < 0 {
		return false
	}
	seen := map[int32]bool{}
	for _, gc := range gcs {
		if gc.Generation() != gen {
			return false
		}
		for _, tp := range gc.Assignment() {
			if tp.Topic != topic || seen[tp.Partition] {
				return false
			}
			seen[tp.Partition] = true
		}
	}
	return int32(len(seen)) == n
}

// rawMember is a group member driven by raw RPCs, so a test can kill it (stop heartbeating
// without leaving) or replay a stale generation.
type rawMember struct {
	api      streamqv1.StreamQServiceClient
	group    string
	topic    string
	memberID string
	gen      int32
}

func (m *rawMember) join(t *testing.T, ctx context.Context) {
	t.Helper()
	require.NoError(t, m.tryJoin(ctx))
}

// tryJoin joins (or rejoins) and syncs, returning an error instead of failing the test so it can
// run off the test goroutine.
func (m *rawMember) tryJoin(ctx context.Context) error {
	resp, err := m.api.JoinGroup(ctx, &streamqv1.JoinGroupRequest{
		GroupId: m.group, MemberId: m.memberID, SessionTimeoutMs: int32(groupSession.Milliseconds()),
		RebalanceTimeoutMs: 5000, Topics: []string{m.topic}, Assignors: []string{"range"},
	})
	if err != nil {
		return err
	}
	if resp.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
		return fmt.Errorf("join: %s: %s", resp.GetErrorCode(), resp.GetErrorMessage())
	}
	m.memberID, m.gen = resp.GetMemberId(), resp.GetGenerationId()
	sync, err := m.api.SyncGroup(ctx, &streamqv1.SyncGroupRequest{GroupId: m.group, MemberId: m.memberID, GenerationId: m.gen})
	if err != nil {
		return err
	}
	if sync.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
		return fmt.Errorf("sync: %s", sync.GetErrorCode())
	}
	return nil
}

// heartbeatUntil heartbeats (rejoining on REBALANCE_IN_PROGRESS) until stop is closed.
func (m *rawMember) heartbeatUntil(ctx context.Context, stop <-chan struct{}) error {
	tick := time.NewTicker(groupHeartbeat)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		resp, err := m.api.Heartbeat(ctx, &streamqv1.HeartbeatRequest{GroupId: m.group, MemberId: m.memberID, GenerationId: m.gen})
		if err != nil {
			return err
		}
		if resp.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS {
			jr, err := m.api.JoinGroup(ctx, &streamqv1.JoinGroupRequest{
				GroupId: m.group, MemberId: m.memberID, SessionTimeoutMs: int32(groupSession.Milliseconds()),
				RebalanceTimeoutMs: 5000, Topics: []string{m.topic}, Assignors: []string{"range"},
			})
			if err != nil {
				return err
			}
			if jr.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return fmt.Errorf("rejoin: %s", jr.GetErrorCode())
			}
			m.gen = jr.GetGenerationId()
		}
	}
}

func createGroupTopic(t *testing.T, c *client.Client, topic string, partitions int32) {
	t.Helper()
	require.NoError(t, client.NewAdmin(c).CreateTopic(testCtx(t), topic, partitions, 1, map[string]string{"min_insync_replicas": "1"}))
}

// TestGroupsNoDoubleAssignmentAndKill: 3 members on 6 partitions get a disjoint full cover; when
// one dies (stops heartbeating, no LeaveGroup) the other two own all 6 within the session timeout.
func TestGroupsNoDoubleAssignmentAndKill(t *testing.T) {
	const topic, groupID = "events", "g1"
	n := startGroupNode(t, t.TempDir())
	defer func() { require.NoError(t, n.Close(context.Background())) }()
	c := newGroupClient(t, n.Addr())
	createGroupTopic(t, c, topic, 6)
	ctx := testCtx(t)

	r1 := startRunner(t, c, groupCfg(groupID, topic))
	r2 := startRunner(t, c, groupCfg(groupID, topic))
	victim := &rawMember{api: dial(t, n.Addr()), group: groupID, topic: topic}
	victim.join(t, ctx)
	kill := make(chan struct{})
	hbDone := make(chan error, 1)
	go func() { hbDone <- victim.heartbeatUntil(ctx, kill) }()

	gcs := []*client.GroupConsumer{r1.gc, r2.gc}
	// Stable with three members: the two consumers share 4 partitions, the victim has the other 2.
	require.Eventually(t, func() bool {
		if r1.gc.Generation() != r2.gc.Generation() || r1.gc.Generation() < 0 {
			return false
		}
		n1, n2 := len(r1.gc.Assignment()), len(r2.gc.Assignment())
		return n1 == 2 && n2 == 2
	}, 30*time.Second, 20*time.Millisecond)
	require.False(t, stableCover(gcs, topic, 6), "the victim still owns two partitions")
	desc, err := dial(t, n.Addr()).DescribeGroup(ctx, &streamqv1.DescribeGroupRequest{GroupId: groupID})
	require.NoError(t, err)
	require.Len(t, desc.GetMembers(), 3)
	owned := map[int32]string{}
	for _, m := range desc.GetMembers() {
		for _, tp := range m.GetAssignment() {
			for _, p := range tp.GetPartitions() {
				require.NotContains(t, owned, p, "partition %d assigned twice", p)
				owned[p] = m.GetMemberId()
			}
		}
	}
	require.Len(t, owned, 6)

	// Kill the victim: no LeaveGroup, heartbeats just stop.
	close(kill)
	require.NoError(t, <-hbDone)
	killed := time.Now()
	require.Eventually(t, func() bool { return stableCover(gcs, topic, 6) }, groupSession+5*time.Second, 20*time.Millisecond)
	require.Less(t, time.Since(killed), groupSession+5*time.Second)

	r1.stop(t)
	r2.stop(t)
}

// TestGroupsStaleGenerationCommitRejected: a member that moved to generation N+1 cannot commit
// with generation N.
func TestGroupsStaleGenerationCommitRejected(t *testing.T) {
	const topic, groupID = "events", "g2"
	n := startGroupNode(t, t.TempDir())
	defer func() { require.NoError(t, n.Close(context.Background())) }()
	c := newGroupClient(t, n.Addr())
	createGroupTopic(t, c, topic, 2)
	ctx := testCtx(t)
	api := dial(t, n.Addr())

	a := &rawMember{api: api, group: groupID, topic: topic}
	a.join(t, ctx)
	oldGen := a.gen
	// A second member joins: a rebalance; a must rejoin into the next generation.
	b := &rawMember{api: api, group: groupID, topic: topic}
	bJoined := make(chan error, 1)
	go func() { bJoined <- b.tryJoin(ctx) }()
	require.Eventually(t, func() bool {
		hb, err := api.Heartbeat(ctx, &streamqv1.HeartbeatRequest{GroupId: groupID, MemberId: a.memberID, GenerationId: a.gen})
		return err == nil && hb.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS
	}, 10*time.Second, 20*time.Millisecond)
	a.join(t, ctx)
	require.NoError(t, <-bJoined)
	require.Greater(t, a.gen, oldGen)

	commit := func(gen int32) streamqv1.ErrorCode {
		resp, err := api.CommitOffset(ctx, &streamqv1.CommitOffsetRequest{GroupId: groupID, MemberId: a.memberID, GenerationId: gen,
			Offsets: []*streamqv1.OffsetCommit{{Topic: topic, Partition: 0, Offset: 1}}})
		require.NoError(t, err)
		return resp.GetErrorCode()
	}
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION, commit(oldGen))
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, commit(a.gen))
	_, err := api.LeaveGroup(ctx, &streamqv1.LeaveGroupRequest{GroupId: groupID, MemberId: a.memberID})
	require.NoError(t, err)
	_, err = api.LeaveGroup(ctx, &streamqv1.LeaveGroupRequest{GroupId: groupID, MemberId: b.memberID})
	require.NoError(t, err)
}

// TestGroupsResumeFromCommittedOffsets: a group that consumed and committed resumes where it left
// off — with a fresh consumer, and after a broker restart.
func TestGroupsResumeFromCommittedOffsets(t *testing.T) {
	const topic, groupID = "orders", "g3"
	dir := t.TempDir()
	n := startGroupNode(t, dir)
	c := newGroupClient(t, n.Addr())
	createGroupTopic(t, c, topic, 6)
	ctx := testCtx(t)
	produceN := func(c *client.Client, from, to int) {
		t.Helper()
		p, err := client.NewProducer(c, client.ProducerConfig{})
		require.NoError(t, err)
		for i := from; i < to; i++ {
			require.NoError(t, p.Produce(ctx, client.Record{Topic: topic, Key: []byte(fmt.Sprint("k", i%12)), Value: []byte(fmt.Sprint(i))}, nil))
		}
		require.NoError(t, p.Close(ctx))
	}
	produceN(c, 0, 600)

	// Three consumers share the work, then close one after another (each commits on close).
	rs := []*runner{startRunner(t, c, groupCfg(groupID, topic)), startRunner(t, c, groupCfg(groupID, topic)), startRunner(t, c, groupCfg(groupID, topic))}
	require.Eventually(t, func() bool {
		total := 0
		for _, r := range rs {
			total += len(r.seen())
		}
		return total >= 600
	}, 60*time.Second, 50*time.Millisecond)
	all := map[string]bool{}
	for _, r := range rs {
		r.stop(t)
		for _, v := range r.seen() {
			all[v] = true
		}
	}
	require.Len(t, all, 600)

	// A fresh consumer in the same group sees only new records.
	produceN(c, 600, 610)
	r := startRunner(t, c, groupCfg(groupID, topic))
	require.Eventually(t, func() bool { return len(r.seen()) >= 10 }, 30*time.Second, 50*time.Millisecond)
	r.stop(t)
	got := r.seen()
	slices.Sort(got)
	want := []string{"600", "601", "602", "603", "604", "605", "606", "607", "608", "609"}
	slices.Sort(want)
	require.Equal(t, want, got, "resumed from committed offsets without re-reading")

	// After a broker restart, committed offsets survive.
	require.NoError(t, c.Close())
	require.NoError(t, n.Close(context.Background()))
	n = startGroupNode(t, dir)
	defer func() { require.NoError(t, n.Close(context.Background())) }()
	c = newGroupClient(t, n.Addr())
	produceN(c, 610, 615)
	r = startRunner(t, c, groupCfg(groupID, topic))
	require.Eventually(t, func() bool { return len(r.seen()) >= 5 }, 30*time.Second, 50*time.Millisecond)
	r.stop(t)
	got = r.seen()
	slices.Sort(got)
	require.Equal(t, []string{"610", "611", "612", "613", "614"}, got)
}
