//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
	"github.com/sudo-ss1/streamq/internal/storage"
	"github.com/sudo-ss1/streamq/internal/testnode"
)

// hooks lets a test set per-node BeforeBrokerClose callbacks after the cluster started.
type hooks struct {
	mu sync.Mutex
	fn map[int32]func()
}

func (h *hooks) set(id int32, fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.fn[id] = fn
}

func (h *hooks) option(id int32, o *node.Options) {
	o.BeforeBrokerClose = func() {
		h.mu.Lock()
		fn := h.fn[id]
		h.mu.Unlock()
		if fn != nil {
			fn()
		}
	}
}

// shutdownCluster starts 3 brokers whose session timeout (60 s) is far above the controlled
// shutdown budget (5 s), so only controlled shutdown can move leadership in time (plan R3).
func shutdownCluster(t *testing.T, h *hooks) *testnode.Cluster {
	t.Helper()
	c := testnode.StartClusterWith(t, 3, func(cfg *config.Config) {
		cfg.BrokerSessionTimeoutMs = 60_000
		cfg.ControlledShutdownTimeoutMs = 5_000
		cfg.TopicDefaults.MinInsyncReplicas = 2
	}, h.option)
	waitAllUnfenced(t, c, []int32{1, 2, 3}, 3)
	return c
}

func createSpread(t *testing.T, c *testnode.Cluster, topic string, partitions int32) {
	t.Helper()
	resp, err := dial(t, c.Addr(1)).CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{Name: topic, Partitions: partitions,
		ReplicationFactor: 3, Configs: map[string]string{"min_insync_replicas": "2"}})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	require.Eventually(t, func() bool {
		for _, p := range partitionsOf(metadataOf(t, c.Addr(1), topic), topic) {
			if p.GetLeaderId() < 0 || len(p.GetIsr()) != 3 {
				return false
			}
		}
		return len(partitionsOf(metadataOf(t, c.Addr(1), topic), topic)) == int(partitions)
	}, 30*time.Second, 50*time.Millisecond)
}

// requireMovedOff asserts, from broker `from`, that no partition of topic is led by `stopped` and
// every partition it led has a higher leader epoch than in `before`.
func requireMovedOff(t *testing.T, c *testnode.Cluster, from, stopped int32, topic string, before map[int32]*streamqv1.PartitionMetadata) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, p := range partitionsOf(metadataOf(t, c.Addr(from), topic), topic) {
			old := before[p.GetPartition()]
			if p.GetLeaderId() < 0 || p.GetLeaderId() == stopped {
				return false
			}
			if old.GetLeaderId() == stopped && p.GetLeaderEpoch() <= old.GetLeaderEpoch() {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond, "broker %d sees leadership moved off %d", from, stopped)
}

// TestControlledShutdownMovesLeadership (SPEC §4.6, plan R3/R16): stopping a broker moves its
// leadership before its server stops — well within the 60 s session timeout — and producing to
// every partition keeps working.
func TestControlledShutdownMovesLeadership(t *testing.T) {
	const topic = "cs"
	h := &hooks{fn: map[int32]func(){}}
	c := shutdownCluster(t, h)
	createSpread(t, c, topic, 6)
	md := metadataOf(t, c.Addr(1), topic)
	controller := md.GetControllerId()
	before := map[int32]*streamqv1.PartitionMetadata{}
	led := map[int32]int{}
	for _, p := range partitionsOf(md, topic) {
		before[p.GetPartition()] = p
		led[p.GetLeaderId()]++
	}
	victim := int32(-1)
	for id := int32(1); id <= 3; id++ {
		if id != controller && (victim < 0 || led[id] > led[victim]) {
			victim = id
		}
	}
	require.Positive(t, led[victim], "the victim leads something")
	survivors := slices.DeleteFunc([]int32{1, 2, 3}, func(id int32) bool { return id == victim })

	ran := false
	h.set(victim, func() {
		ran = true // still inside Close: the node has not closed its broker yet
		for _, id := range survivors {
			requireMovedOff(t, c, id, victim, topic, before)
		}
	})
	start := time.Now()
	c.Stop(victim)
	require.Less(t, time.Since(start), 3*time.Second, "controlled shutdown is fast")
	h.set(victim, nil) // not again when the restarted broker closes at teardown
	require.True(t, ran)

	for _, p := range partitionsOf(metadataOf(t, c.Addr(survivors[0]), topic), topic) {
		code := produceTo(t, c.Addr(p.GetLeaderId()), topic, p.GetPartition())
		require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, code, "partition %d", p.GetPartition())
	}

	c.Restart(victim)
	require.Eventually(t, func() bool {
		for _, p := range partitionsOf(metadataOf(t, c.Addr(survivors[0]), topic), topic) {
			if !slices.Contains(p.GetIsr(), victim) {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "the restarted broker rejoins every ISR")
}

// produceTo sends one acks=all record to topic/partition at addr and returns its error code.
func produceTo(t *testing.T, addr, topic string, partition int32) streamqv1.ErrorCode {
	t.Helper()
	data := rawIdempotentBatch(t, -1, -1, 1)
	resp, err := dial(t, addr).Produce(testCtx(t), &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_ALL, TimeoutMs: 10_000,
		Topics: []*streamqv1.ProduceTopic{{Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: partition, LeaderEpoch: -1, Records: data}}}}})
	require.NoError(t, err)
	return resp.GetTopics()[0].GetPartitions()[0].GetErrorCode()
}

// TestControlledShutdownOfController (plan R13): stopping the controller moves partition
// leadership and the Raft leadership; a peer sees the new controller while the stopping node is
// still inside Close.
func TestControlledShutdownOfController(t *testing.T) {
	const topic = "cs-ctrl"
	h := &hooks{fn: map[int32]func(){}}
	c := shutdownCluster(t, h)
	createSpread(t, c, topic, 3)
	md := metadataOf(t, c.Addr(1), topic)
	controller := md.GetControllerId()
	before := map[int32]*streamqv1.PartitionMetadata{}
	for _, p := range partitionsOf(md, topic) {
		before[p.GetPartition()] = p
	}
	peer := int32(1)
	if peer == controller {
		peer = 2
	}
	ran := false
	h.set(controller, func() {
		ran = true
		require.Eventually(t, func() bool {
			id := metadataOf(t, c.Addr(peer), topic).GetControllerId()
			return id >= 0 && id != controller
		}, 2*time.Second, 10*time.Millisecond, "the controller moved before the old one closed")
		requireMovedOff(t, c, peer, controller, topic, before)
	})
	c.Stop(controller)
	h.set(controller, nil)
	require.True(t, ran)
}

// TestControlledShutdownFailureWaitsForDeadline is the negative control (plan R3): when the
// controlled-shutdown request keeps failing, Close spends the whole budget, so the fast path in
// the tests above really is controlled shutdown.
func TestControlledShutdownFailureWaitsForDeadline(t *testing.T) {
	c := testnode.StartClusterWith(t, 3, func(cfg *config.Config) {
		cfg.BrokerSessionTimeoutMs = 60_000
		cfg.ControlledShutdownTimeoutMs = 1_500
	}, func(id int32, o *node.Options) {
		if id == 2 {
			o.ControlledShutdown = func(context.Context) error { return errors.New("injected failure") }
		}
	})
	waitAllUnfenced(t, c, []int32{1, 2, 3}, 3)
	start := time.Now()
	c.Stop(2)
	require.GreaterOrEqual(t, time.Since(start), 1500*time.Millisecond)
}

// term sends SIGTERM and waits for the process to exit, returning its exit code.
func (p *brokerProc) term(t *testing.T) int {
	t.Helper()
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	done := make(chan error, 1)
	go func() {
		p.drained.Wait()
		done <- p.cmd.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("broker did not exit within 30s of SIGTERM")
	}
	return p.cmd.ProcessState.ExitCode()
}

// TestSIGTERMGracefulShutdown (NFR4): SIGTERM stops the broker cleanly: exit code 0, every
// partition flushed and checkpointed (recovery point = LEO), HW checkpoint written, producer
// snapshot at the LEO; after a restart everything is readable.
func TestSIGTERMGracefulShutdown(t *testing.T) {
	const (
		topic      = "graceful"
		partitions = 3
	)
	bin := buildBroker(t)
	dir := t.TempDir()
	p := startProc(t, bin, dir)
	c := dial(t, p.addr)
	createTopic(t, c, topic, partitions)
	produceKeyed(t, c, topic, partitions, generate(3000, 30), 50, nil)
	ends := map[int32]int64{}
	for part := range int32(partitions) {
		resp, err := c.ListOffsets(testCtx(t), &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{{Topic: topic,
			Partitions: []*streamqv1.ListOffsetsPartition{{Partition: part, LeaderEpoch: -1, Timestamp: -1}}}}})
		require.NoError(t, err)
		ends[part] = resp.GetTopics()[0].GetPartitions()[0].GetOffset()
	}

	require.Equal(t, 0, p.term(t), "clean exit")

	rps, err := storage.ReadOffsetCheckpoint(testCtx(t), filepath.Join(dir, storage.RecoveryPointCheckpointFile))
	require.NoError(t, err)
	hws, err := storage.ReadOffsetCheckpoint(testCtx(t), filepath.Join(dir, "replication-offset-checkpoint"))
	require.NoError(t, err)
	for part, end := range ends {
		tp := storage.TopicPartition{Topic: topic, Partition: part}
		require.Equal(t, end, rps[tp], "partition %d flushed to its LEO", part)
		require.Equal(t, end, hws[tp], "partition %d HW checkpointed", part)
		_, err := os.Stat(filepath.Join(dir, tp.DirName(), fmt.Sprintf("%020d.snapshot", end)))
		require.NoError(t, err, "producer snapshot at the LEO")
	}

	p = startProc(t, bin, dir)
	got := consumeAll(t, dial(t, p.addr), topic, partitions)
	require.Len(t, got, 3000)
	verifyPerKeyOrder(t, got, partitions)
}
