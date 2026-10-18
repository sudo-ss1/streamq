//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
	"github.com/sudo-ss1/streamq/internal/testnode"
	"github.com/sudo-ss1/streamq/pkg/client"
)

// lossyNetwork fails a share of Produce RPCs: some before they reach the broker, and some after
// the broker has appended (the response is dropped). Seeded, so runs are reproducible.
type lossyNetwork struct {
	mu          sync.Mutex
	rng         *rand.Rand
	off         atomic.Bool
	lostAfter   atomic.Int64
	lostBefore  atomic.Int64
	afterShare  float64
	beforeShare float64
}

func newLossyNetwork(seed uint64) *lossyNetwork {
	return &lossyNetwork{rng: rand.New(rand.NewPCG(seed, seed)), afterShare: 0.1, beforeShare: 0.05}
}

func (n *lossyNetwork) roll() float64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rng.Float64()
}

func (n *lossyNetwork) interceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
) error {
	if !strings.HasSuffix(method, "/Produce") || n.off.Load() {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	r := n.roll()
	if r < n.beforeShare {
		n.lostBefore.Add(1)
		return status.Error(codes.Unavailable, "injected: request lost")
	}
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err == nil && r < n.beforeShare+n.afterShare {
		n.lostAfter.Add(1)
		return status.Error(codes.Unavailable, "injected: response lost")
	}
	return err
}

// keyedValue is "<key>/<seq>".
func keyedValue(key string, seq int) string { return fmt.Sprintf("%s/%d", key, seq) }

// ackLog records acknowledged values.
type ackLog struct {
	mu    sync.Mutex
	acked map[string]bool
	n     atomic.Int64
}

func newAckLog() *ackLog { return &ackLog{acked: map[string]bool{}} }

func (a *ackLog) cb(v string) func(client.RecordMetadata, error) {
	return func(_ client.RecordMetadata, err error) {
		if err != nil {
			return
		}
		a.mu.Lock()
		a.acked[v] = true
		a.mu.Unlock()
		a.n.Add(1)
	}
}

// consumeReplicated reads every partition of topic from its current leader up to the HW, through
// the client library (which routes to leaders).
func consumeReplicated(t *testing.T, c *testnode.Cluster, cl *client.Client, topic string, partitions int32) []client.Record {
	t.Helper()
	ends := map[int32]int64{}
	for p := range partitions {
		pm := partitionMeta(t, c, 1, topic, p)
		require.NotNil(t, pm)
		bp, ok := c.Node(pm.GetLeaderId()).Broker().Partition(storage.TopicPartition{Topic: topic, Partition: p})
		require.True(t, ok)
		ends[p] = bp.HighWatermark()
	}
	cons, err := client.NewConsumer(cl, client.ConsumerConfig{AutoOffsetReset: client.ResetEarliest})
	require.NoError(t, err)
	defer func() { require.NoError(t, cons.Close()) }()
	var tps []client.TopicPartition
	for p := range partitions {
		tps = append(tps, client.TopicPartition{Topic: topic, Partition: p})
	}
	cons.Assign(tps)
	var out []client.Record
	require.Eventually(t, func() bool {
		recs, err := cons.Poll(testCtx(t))
		if err == nil {
			out = append(out, recs...)
		}
		for p, end := range ends {
			if pos, ok := cons.Position(client.TopicPartition{Topic: topic, Partition: p}); !ok || pos < end {
				return false
			}
		}
		return true
	}, 60*time.Second, time.Millisecond)
	return out
}

// verifyExactlyOnce checks every acknowledged value appears exactly once and each key's sequence
// numbers increase in offset order.
func verifyExactlyOnce(t *testing.T, recs []client.Record, acked map[string]bool) {
	t.Helper()
	seen := map[string]int{}
	last := map[string]int{}
	slices.SortFunc(recs, func(a, b client.Record) int {
		if a.Partition != b.Partition {
			return int(a.Partition - b.Partition)
		}
		return int(a.Offset - b.Offset)
	})
	for _, r := range recs {
		v := string(r.Value)
		seen[v]++
		key, seqStr, ok := strings.Cut(v, "/")
		require.True(t, ok, v)
		seq, err := strconv.Atoi(seqStr)
		require.NoError(t, err)
		if prev, ok := last[key]; ok {
			require.Greater(t, seq, prev, "key %s reordered", key)
		}
		last[key] = seq
	}
	for v, n := range seen {
		require.Equal(t, 1, n, "value %s stored %d times", v, n)
	}
	for v := range acked {
		require.Equal(t, 1, seen[v], "acknowledged value %s missing", v)
	}
}

func lossyClient(t *testing.T, c *testnode.Cluster, net *lossyNetwork) *client.Client {
	t.Helper()
	cl, err := client.New(client.Config{Bootstrap: []string{c.Addr(1), c.Addr(2), c.Addr(3)},
		RequestTimeout: 10 * time.Second, RetryBackoff: 10 * time.Millisecond, RetryBackoffMax: 200 * time.Millisecond,
		DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(net.interceptor)}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cl.Close()) })
	return cl
}

func createIdempotentTopic(t *testing.T, c *testnode.Cluster, topic string, partitions int32, configs map[string]string) {
	t.Helper()
	if configs == nil {
		configs = map[string]string{}
	}
	configs["min_insync_replicas"] = "2"
	resp, err := dial(t, c.Addr(1)).CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{Name: topic, Partitions: partitions,
		ReplicationFactor: 3, Configs: configs})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	for p := range partitions {
		require.Eventually(t, func() bool {
			pm := partitionMeta(t, c, 1, topic, p)
			return pm != nil && pm.GetLeaderId() >= 0 && len(pm.GetIsr()) == 3
		}, 30*time.Second, 50*time.Millisecond)
	}
}

// TestIdempotentProducerLossyNetwork is the M7 acceptance test (SPEC §9): with Produce requests
// and responses lost at random, the idempotent producer's retries store every acknowledged
// record exactly once and in per-key order.
func TestIdempotentProducerLossyNetwork(t *testing.T) {
	const (
		topic      = "idem"
		partitions = 3
		keys       = 30
		total      = 3000
	)
	c := replicationCluster(t)
	createIdempotentTopic(t, c, topic, partitions, nil)
	net := newLossyNetwork(7)
	cl := lossyClient(t, c, net)
	prod, err := client.NewProducer(cl, client.ProducerConfig{Linger: 2 * time.Millisecond})
	require.NoError(t, err)
	acks := newAckLog()
	for i := range total {
		key := "k" + strconv.Itoa(i%keys)
		v := keyedValue(key, i/keys)
		require.NoError(t, prod.Produce(testCtx(t), client.Record{Topic: topic, Key: []byte(key), Value: []byte(v)}, acks.cb(v)))
	}
	require.NoError(t, prod.Close(testCtx(t)))
	require.Equal(t, int64(total), acks.n.Load(), "every record acknowledged despite the losses")
	require.Positive(t, net.lostAfter.Load(), "some appended batches lost their response")
	t.Logf("lost before: %d, lost after append: %d", net.lostBefore.Load(), net.lostAfter.Load())

	net.off.Store(true)
	verifyExactlyOnce(t, consumeReplicated(t, c, cl, topic, partitions), acks.acked)
}

// rawIdempotentBatch builds a batch of n records for (pid, seq) stamped now (plan R15).
func rawIdempotentBatch(t *testing.T, pid int64, seq int32, n int) []byte {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: pid, BaseSequence: seq})
	for i := range n {
		b.Append(time.Now().UnixMilli(), nil, []byte(fmt.Sprint("p", seq, "-", i)), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

// produceRaw sends one batch with acks=all to the broker at addr and returns its base offset. It
// retries retriable codes (the broker may not have opened a just-created partition yet); the
// batches are idempotent, so a retry can never duplicate.
func produceRaw(t *testing.T, addr, topic string, data []byte) int64 {
	t.Helper()
	api := dial(t, addr)
	var pr *streamqv1.ProducePartitionResponse
	require.Eventually(t, func() bool {
		resp, err := api.Produce(testCtx(t), &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_ALL, TimeoutMs: 10_000,
			Topics: []*streamqv1.ProduceTopic{{Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: 0, LeaderEpoch: -1, Records: slices.Clone(data)}}}}})
		if err != nil {
			return false // transport error: retry (the batch is idempotent)
		}
		pr = resp.GetTopics()[0].GetPartitions()[0]
		switch pr.GetErrorCode() {
		case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER,
			streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS:
			return false
		}
		return true
	}, 30*time.Second, 20*time.Millisecond)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, pr.GetErrorCode(), pr.GetErrorMessage())
	return pr.GetBaseOffset()
}

func initProducer(t *testing.T, addr string) int64 {
	t.Helper()
	resp, err := dial(t, addr).InitProducer(testCtx(t), &streamqv1.InitProducerRequest{})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	return resp.GetProducerId()
}

// TestIdempotentStateSurvivesLeaderFailover: producer state replicated to followers lets a new
// leader recognise a retry of a batch the old leader appended (deterministic probe, plan R5/R14),
// and a lossy idempotent stream across the failover stays exactly-once.
func TestIdempotentStateSurvivesLeaderFailover(t *testing.T) {
	const (
		topic = "idem-failover"
		keys  = 10
	)
	c := replicationCluster(t)
	createIdempotentTopic(t, c, topic, 1, nil)
	// The probe topic must share the stream topic's leader, so stopping it fails both over.
	streamLeader := partitionMeta(t, c, 1, topic, 0).GetLeaderId()
	probe := ""
	for i := range 10 {
		name := fmt.Sprintf("idem-probe-%d", i)
		createIdempotentTopic(t, c, name, 1, nil)
		if partitionMeta(t, c, 1, name, 0).GetLeaderId() == streamLeader {
			probe = name
			break
		}
	}
	require.NotEmpty(t, probe, "a probe topic led by the stream topic's leader")

	// Deterministic probe on its own topic.
	pid := initProducer(t, c.Addr(1))
	probeBatch := rawIdempotentBatch(t, pid, 0, 3)
	probeLeader := partitionMeta(t, c, 1, probe, 0).GetLeaderId()
	base := produceRaw(t, c.Addr(probeLeader), probe, slices.Clone(probeBatch))

	// A lossy stream on the main topic, running across the failover.
	net := newLossyNetwork(11)
	cl := lossyClient(t, c, net)
	prod, err := client.NewProducer(cl, client.ProducerConfig{Linger: time.Millisecond})
	require.NoError(t, err)
	acks := newAckLog()
	var stop atomic.Bool
	done := make(chan error, 1)
	go func() {
		for i := 0; !stop.Load(); i++ {
			key := "k" + strconv.Itoa(i%keys)
			v := keyedValue(key, i/keys)
			if err := prod.Produce(context.Background(), client.Record{Topic: topic, Key: []byte(key), Value: []byte(v)}, acks.cb(v)); err != nil {
				done <- err
				return
			}
			if i%20 == 19 {
				_ = prod.Flush(context.Background())
			}
		}
		done <- nil
	}()
	require.Eventually(t, func() bool { return acks.n.Load() >= 200 }, 60*time.Second, 10*time.Millisecond)

	// Fail over the leader of both topics.
	require.Equal(t, streamLeader, probeLeader)
	c.Stop(probeLeader)
	survivors := slices.DeleteFunc([]int32{1, 2, 3}, func(id int32) bool { return id == probeLeader })
	var newLeader int32
	require.Eventually(t, func() bool {
		pm := partitionMeta(t, c, survivors[0], probe, 0)
		if pm == nil || pm.GetLeaderId() < 0 || pm.GetLeaderId() == probeLeader {
			return false
		}
		newLeader = pm.GetLeaderId()
		return true
	}, 30*time.Second, 50*time.Millisecond)
	tp := storage.TopicPartition{Topic: probe, Partition: 0}
	bp, ok := c.Node(newLeader).Broker().Partition(tp)
	require.True(t, ok)
	leo := bp.Log().LogEndOffset()
	require.Equal(t, base, produceRaw(t, c.Addr(newLeader), probe, slices.Clone(probeBatch)), "the new leader reports the original offset")
	require.Equal(t, leo, bp.Log().LogEndOffset(), "and appends nothing")

	after := acks.n.Load()
	// Keep producing until 100 more acks and at least one lost-after-append retry happened.
	require.Eventually(t, func() bool { return acks.n.Load() >= after+100 && net.lostAfter.Load() > 0 },
		60*time.Second, 10*time.Millisecond)
	stop.Store(true)
	require.NoError(t, <-done)
	require.NoError(t, prod.Close(testCtx(t)))
	require.Positive(t, net.lostAfter.Load())

	c.Restart(probeLeader)
	net.off.Store(true)
	verifyExactlyOnce(t, consumeReplicated(t, c, cl, topic, 1), acks.acked)
}

// TestIdempotentStateSurvivesRollingRestart: after every broker restarts (state rebuilt from
// snapshots written on segment roll and at clean shutdown), a retry of one of a producer's last
// five batches is still a duplicate (plan R10).
func TestIdempotentStateSurvivesRollingRestart(t *testing.T) {
	const topic = "idem-restart"
	c := replicationCluster(t)
	createIdempotentTopic(t, c, topic, 1, map[string]string{"segment_bytes": "4096", "max_message_bytes": "2048"})
	pid := initProducer(t, c.Addr(1))
	leader := partitionMeta(t, c, 1, topic, 0).GetLeaderId()
	var batches [][]byte
	var bases []int64
	for i := range 60 {
		b := rawIdempotentBatch(t, pid, int32(i*5), 5)
		batches = append(batches, b)
		bases = append(bases, produceRaw(t, c.Addr(leader), topic, slices.Clone(b)))
	}
	tp := storage.TopicPartition{Topic: topic, Partition: 0}
	for id := int32(1); id <= 3; id++ {
		matches, err := filepath.Glob(filepath.Join(c.Config(id).DataDir, tp.DirName(), "*.snapshot"))
		require.NoError(t, err)
		require.NotEmpty(t, matches, "broker %d wrote roll snapshots", id)
	}
	for id := int32(1); id <= 3; id++ {
		c.Stop(id)
		c.Restart(id)
		waitISR(t, c, id, topic, 3)
	}
	leader = waitISR(t, c, 1, topic, 3).GetLeaderId()
	bp, ok := c.Node(leader).Broker().Partition(tp)
	require.True(t, ok)
	leo := bp.Log().LogEndOffset()
	for i := 55; i < 60; i++ {
		require.Equal(t, bases[i], produceRaw(t, c.Addr(leader), topic, slices.Clone(batches[i])), "batch %d is a duplicate", i)
	}
	require.Equal(t, leo, bp.Log().LogEndOffset())
}

// TestIdempotentStateSurvivesKill: after SIGKILL (no clean-shutdown snapshot) the state is rebuilt
// from the newest roll snapshot plus a replay, so retries are still duplicates (plan R16).
func TestIdempotentStateSurvivesKill(t *testing.T) {
	const topic = "idem-kill"
	bin := buildBroker(t)
	dir := t.TempDir()
	p := startProc(t, bin, dir)
	c := dial(t, p.addr)
	resp, err := c.CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{Name: topic, Partitions: 1, ReplicationFactor: 1,
		Configs: map[string]string{"min_insync_replicas": "1", "segment_bytes": "4096", "max_message_bytes": "2048"}})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	pid := initProducer(t, p.addr)
	var batches [][]byte
	var bases []int64
	for i := range 60 {
		b := rawIdempotentBatch(t, pid, int32(i*5), 5)
		batches = append(batches, b)
		bases = append(bases, produceRaw(t, p.addr, topic, slices.Clone(b)))
	}
	tp := storage.TopicPartition{Topic: topic, Partition: 0}
	matches, err := filepath.Glob(filepath.Join(dir, tp.DirName(), "*.snapshot"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
	p.kill(t)

	p = startProc(t, bin, dir)
	for i := 55; i < 60; i++ {
		require.Equal(t, bases[i], produceRaw(t, p.addr, topic, slices.Clone(batches[i])), "batch %d is a duplicate after a crash", i)
	}
	next := produceRaw(t, p.addr, topic, rawIdempotentBatch(t, pid, 300, 5))
	require.Equal(t, bases[59]+5, next, "the sequence continues")
}
