package raftstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// testCluster runs n raftstore nodes on pre-bound loopback listeners. The forwarder calls the
// target node's ApplyForwarded directly (the gRPC path is tested in internal/server).
type testCluster struct {
	t      *testing.T
	dirs   []string
	peers  []Peer
	mu     sync.Mutex
	stores map[int32]*Store
}

type directForwarder struct{ c *testCluster }

func (f directForwarder) Forward(ctx context.Context, addr string, cmd *streamqv1.MetadataCommand) (uint64, error) {
	f.c.mu.Lock()
	var target *Store
	for _, p := range f.c.peers {
		if p.InternalAddr == addr {
			target = f.c.stores[p.ID]
		}
	}
	f.c.mu.Unlock()
	if target == nil {
		return 0, fmt.Errorf("node %s down: %w", addr, ErrNotAppended)
	}
	return target.ApplyForwarded(ctx, cmd)
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, stores: map[int32]*Store{}}
	listeners := make([]net.Listener, n)
	for i := range n {
		lis, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx // test setup
		require.NoError(t, err)
		listeners[i] = lis
		c.peers = append(c.peers, Peer{ID: int32(i + 1), RaftAddr: lis.Addr().String(), InternalAddr: fmt.Sprintf("internal-%d", i+1)})
		c.dirs = append(c.dirs, t.TempDir())
	}
	for i := range n {
		c.start(int32(i+1), listeners[i])
	}
	t.Cleanup(c.closeAll)
	return c
}

func (c *testCluster) start(id int32, lis net.Listener) {
	c.t.Helper()
	if lis == nil {
		var err error
		lis, err = listenRetry(c.peers[id-1].RaftAddr)
		require.NoError(c.t, err)
	}
	s, err := Open(context.Background(), Config{
		NodeID: id, Peers: c.peers, DataDir: c.dirs[id-1], Listener: lis, Forwarder: directForwarder{c},
		HeartbeatTimeout: 300 * time.Millisecond, ElectionTimeout: 300 * time.Millisecond,
		SnapshotThreshold: 1 << 20, TrailingLogs: 5, RetryBackoff: 20 * time.Millisecond,
	})
	require.NoError(c.t, err)
	c.mu.Lock()
	c.stores[id] = s
	c.mu.Unlock()
}

// listenRetry rebinds a port just released by a stopped node.
func listenRetry(addr string) (net.Listener, error) {
	var err error
	for range 100 {
		var lis net.Listener
		lis, err = net.Listen("tcp", addr) //nolint:noctx // test setup
		if err == nil {
			return lis, nil
		}
		<-time.After(20 * time.Millisecond)
	}
	return nil, err
}

func (c *testCluster) stop(id int32) {
	c.t.Helper()
	c.mu.Lock()
	s := c.stores[id]
	delete(c.stores, id)
	c.mu.Unlock()
	require.NoError(c.t, s.Close())
}

func (c *testCluster) closeAll() {
	c.mu.Lock()
	stores := c.stores
	c.stores = map[int32]*Store{}
	c.mu.Unlock()
	for _, s := range stores {
		_ = s.Close()
	}
}

func (c *testCluster) store(id int32) *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stores[id]
}

func (c *testCluster) leader() *Store {
	c.t.Helper()
	var leader *Store
	require.Eventually(c.t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	}, 15*time.Second, 10*time.Millisecond)
	return leader
}

func (c *testCluster) follower() *Store {
	c.t.Helper()
	leader := c.leader()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.stores {
		if s != leader {
			return s
		}
	}
	c.t.Fatal("no follower")
	return nil
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func registerCmd(id int32) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_RegisterBroker{
		RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: id, Host: "h", Port: 9000 + id},
	}}
}

func unfenceCmd(id int32, epoch int64) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_UnfenceBroker{
		UnfenceBroker: &streamqv1.UnfenceBrokerCommand{BrokerId: id, BrokerEpoch: epoch},
	}}
}

func createCmd(name string) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: name, TopicId: "id-" + name, Partitions: []*streamqv1.PartitionAssignment{
			{Partition: 0, Replicas: []int32{1, 2, 3}},
		}},
	}}
}

// registerAll registers and unfences brokers 1..3 through store s.
func registerAll(t *testing.T, s *Store) {
	t.Helper()
	ctx := ctxT(t)
	for id := int32(1); id <= 3; id++ {
		require.NoError(t, s.Propose(ctx, registerCmd(id)))
		require.NoError(t, s.Propose(ctx, unfenceCmd(id, 1)))
	}
}

func requireConverged(t *testing.T, c *testCluster, topic string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range c.stores {
			if _, ok := s.Snapshot().Topic(topic); !ok {
				return false
			}
		}
		return true
	}, 15*time.Second, 10*time.Millisecond)
}

func TestProposeOnFollowerIsForwardedAndConverges(t *testing.T) {
	c := newTestCluster(t, 3)
	f := c.follower()
	registerAll(t, f)

	// Every node's subscriber sees the create exactly once.
	var mu sync.Mutex
	seen := map[int32]int{}
	for id := int32(1); id <= 3; id++ {
		_, unsub := c.store(id).Subscribe(func(_ context.Context, ch metadata.Change) {
			if ch.Kind == metadata.TopicCreated {
				mu.Lock()
				seen[id]++
				mu.Unlock()
			}
		})
		defer unsub()
	}
	ctx := ctxT(t)
	require.NoError(t, f.Propose(ctx, createCmd("a")))
	_, ok := f.Snapshot().Topic("a")
	require.True(t, ok, "Propose returns only once the proposer applied the command")
	requireConverged(t, c, "a")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seen[1] == 1 && seen[2] == 1 && seen[3] == 1
	}, 10*time.Second, 10*time.Millisecond)

	// Deterministic command errors come back on followers too and leave the state unchanged, and
	// still advance subscribers' applied index (Applied marker, D35).
	applied := make(chan metadata.Change, 16)
	_, unsubApplied := f.Subscribe(func(_ context.Context, ch metadata.Change) {
		if ch.Kind == metadata.Applied {
			applied <- ch
		}
	})
	defer unsubApplied()
	require.ErrorIs(t, f.Propose(ctx, createCmd("a")), metadata.ErrTopicExists)
	select {
	case ch := <-applied:
		require.Equal(t, f.AppliedIndex(), ch.Index)
	case <-time.After(10 * time.Second):
		t.Fatal("no Applied marker for a rejected command")
	}
	// Leader-only proposals fail on a follower.
	require.ErrorIs(t, f.ProposeAsLeader(ctx, createCmd("b")), metadata.ErrNotController)
	require.NoError(t, c.leader().ProposeAsLeader(ctx, createCmd("b")))
	snap := c.leader().Snapshot()
	require.Equal(t, c.leader().cfg.NodeID, snap.ControllerID)
	require.Positive(t, snap.ControllerEpoch)
	require.Positive(t, snap.Index)
}

func TestTermBoundCommandRejectedAfterLeadershipChange(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	registerAll(t, leader)
	ctx := ctxT(t)
	cmd := createCmd("x")
	cmd.ControllerEpoch = leader.Term()
	staleTerm := cmd.ControllerEpoch
	c.stop(leader.cfg.NodeID)
	newLeader := c.leader()
	require.Greater(t, newLeader.Term(), staleTerm)
	err := newLeader.ProposeAsLeader(ctx, cmd)
	require.ErrorIs(t, err, metadata.ErrNotController, "a decision made in an older term never applies")
	_, ok := newLeader.Snapshot().Topic("x")
	require.False(t, ok)
}

func TestLeaderFailoverKeepsMetadata(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	registerAll(t, leader)
	ctx := ctxT(t)
	require.NoError(t, leader.Propose(ctx, createCmd("before")))
	requireConverged(t, c, "before")

	old := leader.cfg.NodeID
	changes := make(chan bool, 10)
	cancel := c.follower().OnLeadershipChange(func(is bool) { changes <- is })
	defer cancel()
	c.stop(old)
	newLeader := c.leader()
	require.NotEqual(t, old, newLeader.cfg.NodeID)
	_, ok := newLeader.Snapshot().Topic("before")
	require.True(t, ok, "metadata intact after failover")
	require.NoError(t, c.follower().Propose(ctx, createCmd("after")))

	// The stopped node restarts from its data dir and catches up.
	c.start(old, nil)
	requireConverged(t, c, "after")
	require.Eventually(t, func() bool { return c.store(old).LeaderID() >= 0 }, 10*time.Second, 10*time.Millisecond)
}

func TestInstallSnapshotRestoresLaggingFollower(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	registerAll(t, leader)
	ctx := ctxT(t)
	lagging := c.follower()
	lagID := lagging.cfg.NodeID
	c.stop(lagID)

	for i := range 20 {
		require.NoError(t, leader.Propose(ctx, createCmd(fmt.Sprint("t", i))))
	}
	require.NoError(t, leader.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion,
		Cmd: &streamqv1.MetadataCommand_DeleteTopic{DeleteTopic: &streamqv1.DeleteTopicCommand{Name: "t3"}}}))
	require.NoError(t, leader.TakeSnapshot())
	// More entries after the snapshot, so the log before it can be truncated (TrailingLogs 5).
	for i := range 10 {
		require.NoError(t, leader.Propose(ctx, createCmd(fmt.Sprint("u", i))))
	}
	require.NoError(t, leader.TakeSnapshot())

	c.start(lagID, nil)
	restarted := c.store(lagID)
	requireConverged(t, c, "u9")
	// The follower was brought up to date by InstallSnapshot, not log replay (TrailingLogs 5): it
	// now holds a snapshot it never took itself. (Its Restored change fires during startup, before
	// a subscriber can attach; Restored delivery is covered by TestFSMSnapshotRoundTrip.)
	require.Eventually(t, func() bool {
		return restarted.raft.Stats()["last_snapshot_index"] != "0"
	}, 10*time.Second, 10*time.Millisecond, "InstallSnapshot was used")
	snap := restarted.Snapshot()
	_, ok := snap.Topic("t3")
	require.False(t, ok)
	require.Equal(t, leader.Snapshot().Topics, snap.Topics)
	require.GreaterOrEqual(t, restarted.AppliedIndex(), snap.Index)
}

func TestFSMSnapshotRoundTrip(t *testing.T) {
	n := newNotifier()
	defer n.close()
	f := newFSM(n)
	apply := func(i uint64, cmd *streamqv1.MetadataCommand) {
		data, err := protoMarshal(cmd)
		require.NoError(t, err)
		resp := f.Apply(rlog{index: i, data: data}.log())
		require.Nil(t, resp)
	}
	apply(1, registerCmd(1))
	apply(2, unfenceCmd(1, 1))
	apply(3, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "x", Configs: map[string]string{"k": "v"},
			Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}})
	apply(4, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: "g", Offsets: []*streamqv1.OffsetCommit{{Topic: "t", Offset: 9}}},
	}})
	apply(5, allocCmd(1))
	before := f.snapshot()
	require.Equal(t, int64(metadata.ProducerIDBlockSize), before.NextProducerID)

	snap, err := f.Snapshot()
	require.NoError(t, err)
	sink := &memSink{}
	require.NoError(t, snap.Persist(sink))

	n2 := newNotifier()
	defer n2.close()
	restored := newFSM(n2)
	got := make(chan metadata.Change, 1)
	unsub := n2.subscribe(func(_ context.Context, ch metadata.Change) { got <- ch })
	defer unsub()
	require.NoError(t, restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))
	after := restored.snapshot()
	require.Equal(t, before, after)
	require.Equal(t, uint64(5), after.Index)
	ch := <-got
	require.Equal(t, metadata.Restored, ch.Kind)
	require.Equal(t, after, *ch.Snapshot)

	// Unknown versions are rejected.
	bad, err := json.Marshal(snapshotFile{Version: 99, State: metadata.NewState(nil, 0)})
	require.NoError(t, err)
	require.Error(t, restored.Restore(io.NopCloser(bytes.NewReader(bad))))
}

func TestFSMRejectsGarbageDeterministically(t *testing.T) {
	n := newNotifier()
	defer n.close()
	f := newFSM(n)
	resp := f.Apply(rlog{index: 1, data: []byte{0xff, 0x01}}.log())
	require.ErrorIs(t, resp.(error), metadata.ErrInvalidCommand)
	require.Equal(t, uint64(1), f.appliedIndex(), "the index advances even for rejected entries")
}

func allocCmd(broker int32) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_AllocateProducerIds{
		AllocateProducerIds: &streamqv1.AllocateProducerIDsCommand{BrokerId: broker},
	}}
}

// TestFSMProducerIDBlocksSurviveOtherCommands: every apply clones the state, so a missed field
// would reset the counter and hand out a block twice (M7 plan R4).
func TestFSMProducerIDBlocksSurviveOtherCommands(t *testing.T) {
	n := newNotifier()
	defer n.close()
	f := newFSM(n)
	apply := func(i uint64, cmd *streamqv1.MetadataCommand) {
		data, err := protoMarshal(cmd)
		require.NoError(t, err)
		require.Nil(t, f.Apply(rlog{index: i, data: data}.log()))
	}
	apply(1, registerCmd(1))
	apply(2, registerCmd(2))
	apply(3, allocCmd(1))
	first := f.snapshot().ProducerIDBlocks[1]
	apply(4, unfenceCmd(1, 1))
	apply(5, allocCmd(2))
	apply(6, allocCmd(1))
	snap := f.snapshot()
	require.Equal(t, map[int32]int64{1: 2 * metadata.ProducerIDBlockSize, 2: metadata.ProducerIDBlockSize}, snap.ProducerIDBlocks)
	require.Equal(t, int64(0), first)
	require.Equal(t, int64(3*metadata.ProducerIDBlockSize), snap.NextProducerID)
}

// TestTransferLeadership: the leader hands leadership to another node while it keeps running
// (M8 plan R13).
func TestTransferLeadership(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	old := leader.cfg.NodeID
	require.NoError(t, leader.TransferLeadership(ctxT(t)))
	require.Eventually(t, func() bool {
		id := leader.LeaderID()
		return id >= 0 && id != old
	}, 10*time.Second, 10*time.Millisecond, "another node leads")
	require.False(t, leader.IsLeader())
	require.NoError(t, leader.TransferLeadership(ctxT(t)), "a follower's transfer is a no-op")
}
