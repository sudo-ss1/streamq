//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
	"github.com/sudo-ss1/streamq/internal/testnode"
	"github.com/sudo-ss1/streamq/pkg/client"
)

func replicationCluster(t *testing.T) *testnode.Cluster {
	t.Helper()
	c := testnode.StartCluster(t, 3, func(cfg *config.Config) {
		cfg.TopicDefaults.MinInsyncReplicas = 2
		cfg.ReplicaLagMaxMs = 2000
	})
	waitAllUnfenced(t, c, []int32{1, 2, 3}, 3)
	return c
}

// partitionMeta returns partition p of topic as seen by broker id.
func partitionMeta(t *testing.T, c *testnode.Cluster, id int32, topic string, p int32) *streamqv1.PartitionMetadata {
	t.Helper()
	resp, err := dial(t, c.Addr(id)).Metadata(testCtx(t), &streamqv1.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil
	}
	for _, pm := range partitionsOf(resp, topic) {
		if pm.GetPartition() == p {
			return pm
		}
	}
	return nil
}

func createReplicated(t *testing.T, c *testnode.Cluster, topic string, minInsync int) {
	t.Helper()
	resp, err := dial(t, c.Addr(1)).CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{Name: topic, Partitions: 1, ReplicationFactor: 3,
		Configs: map[string]string{"min_insync_replicas": strconv.Itoa(minInsync)}})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	waitISR(t, c, 1, topic, 3)
}

// waitISR waits until broker id's metadata shows an ISR of size n with a leader.
func waitISR(t *testing.T, c *testnode.Cluster, id int32, topic string, n int) *streamqv1.PartitionMetadata {
	t.Helper()
	var pm *streamqv1.PartitionMetadata
	require.Eventually(t, func() bool {
		pm = partitionMeta(t, c, id, topic, 0)
		return pm != nil && pm.GetLeaderId() >= 0 && len(pm.GetIsr()) == n
	}, 30*time.Second, 50*time.Millisecond, "isr of %d", n)
	return pm
}

// localBytes returns broker id's log bytes for tp below limit.
func localBytes(t *testing.T, c *testnode.Cluster, id int32, tp storage.TopicPartition, limit int64) []byte {
	t.Helper()
	p, ok := c.Node(id).Broker().Partition(tp)
	require.True(t, ok)
	var out bytes.Buffer
	for off := p.Log().LogStartOffset(); off < limit; {
		res, err := p.Log().Read(context.Background(), off, 1<<20, limit)
		require.NoError(t, err)
		batches, err := record.SplitBatches(res.Data)
		require.NoError(t, err)
		require.NotEmpty(t, batches)
		out.Write(res.Data)
		off = batches[len(batches)-1].LastOffset() + 1
	}
	return out.Bytes()
}

// waitConverged waits until every listed broker's replica of tp has the same LEO and HW, then
// checks the logs are byte-identical.
func waitConverged(t *testing.T, c *testnode.Cluster, ids []int32, tp storage.TopicPartition) {
	t.Helper()
	require.Eventually(t, func() bool {
		var leo, hw int64 = -1, -1
		for _, id := range ids {
			p, ok := c.Node(id).Broker().Partition(tp)
			if !ok {
				return false
			}
			l, h := p.Log().LogEndOffset(), p.HighWatermark()
			if leo >= 0 && (l != leo || h != hw) {
				return false
			}
			leo, hw = l, h
		}
		return leo == hw
	}, 30*time.Second, 50*time.Millisecond, "replicas converge")
	p, _ := c.Node(ids[0]).Broker().Partition(tp)
	want := localBytes(t, c, ids[0], tp, p.HighWatermark())
	for _, id := range ids[1:] {
		require.Equal(t, want, localBytes(t, c, id, tp, p.HighWatermark()), "broker %d byte-identical", id)
	}
}

// TestReplicationNoAckedLossOnLeaderKill is the M6 acceptance test (SPEC §9): with RF 3 and
// min.insync 2, killing the partition leader during acks=all production loses no acknowledged
// record; production continues on the new leader; when the old leader returns all replicas
// converge to byte-identical logs (truncating any unacknowledged tail).
func TestReplicationNoAckedLossOnLeaderKill(t *testing.T) {
	const topic = "repl"
	c := replicationCluster(t)
	createReplicated(t, c, topic, 2)
	tp := storage.TopicPartition{Topic: topic, Partition: 0}

	cl, err := client.New(client.Config{Bootstrap: []string{c.Addr(1), c.Addr(2), c.Addr(3)}})
	require.NoError(t, err)
	defer func() { require.NoError(t, cl.Close()) }()
	prod, err := client.NewProducer(cl, client.ProducerConfig{Acks: client.AcksAll, Linger: time.Millisecond})
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		acked []string
		nAck  atomic.Int64
		stop  atomic.Bool
		done  = make(chan error, 1)
	)
	go func() {
		ctx := context.Background()
		for i := 0; !stop.Load(); i++ {
			v := "v-" + strconv.Itoa(i)
			err := prod.Produce(ctx, client.Record{Topic: topic, UsePartition: true, Partition: 0, Value: []byte(v)},
				func(_ client.RecordMetadata, err error) {
					if err == nil {
						mu.Lock()
						acked = append(acked, v)
						mu.Unlock()
						nAck.Add(1)
					}
				})
			if err != nil {
				done <- err
				return
			}
			if i%50 == 49 {
				_ = prod.Flush(ctx) // pace production so the run spans the failover
			}
		}
		done <- nil
	}()

	require.Eventually(t, func() bool { return nAck.Load() >= 200 }, 30*time.Second, 10*time.Millisecond)
	old := partitionMeta(t, c, 1, topic, 0)
	oldLeader := old.GetLeaderId()
	c.Stop(oldLeader)
	survivors := slices.DeleteFunc([]int32{1, 2, 3}, func(id int32) bool { return id == oldLeader })
	var newLeader int32
	require.Eventually(t, func() bool {
		pm := partitionMeta(t, c, survivors[0], topic, 0)
		if pm == nil || pm.GetLeaderId() < 0 || pm.GetLeaderId() == oldLeader {
			return false
		}
		newLeader = pm.GetLeaderId()
		return true
	}, 30*time.Second, 50*time.Millisecond, "new leader elected")
	afterFailover := nAck.Load()
	require.Eventually(t, func() bool { return nAck.Load() >= afterFailover+100 }, 60*time.Second, 10*time.Millisecond,
		"production continues on the new leader")
	stop.Store(true)
	require.NoError(t, <-done)
	require.NoError(t, prod.Close(context.Background()))

	got := map[string]bool{}
	for _, r := range consumeAll(t, dial(t, c.Addr(newLeader)), topic, 1) {
		got[r.value] = true
	}
	mu.Lock()
	for _, v := range acked {
		require.True(t, got[v], "acknowledged record %s lost", v)
	}
	t.Logf("acked %d records (%d after failover); %d distinct on the new leader", len(acked), int64(len(acked))-afterFailover, len(got))
	mu.Unlock()

	// The old leader returns: it truncates any unacknowledged tail and converges.
	c.Restart(oldLeader)
	waitISR(t, c, survivors[0], topic, 3)
	waitConverged(t, c, []int32{1, 2, 3}, tp)
}

// TestReplicationFollowerRejoinsISR: a stopped follower leaves the ISR; after a restart it
// catches up and is added back.
func TestReplicationFollowerRejoinsISR(t *testing.T) {
	const topic = "rejoin"
	c := replicationCluster(t)
	createReplicated(t, c, topic, 2)
	tp := storage.TopicPartition{Topic: topic, Partition: 0}
	pm := partitionMeta(t, c, 1, topic, 0)
	leader := pm.GetLeaderId()
	var follower int32
	for _, r := range pm.GetReplicas() {
		if r != leader {
			follower = r
			break
		}
	}
	produceN(t, c, leader, topic, 50)
	c.Stop(follower)
	waitISR(t, c, leader, topic, 2)
	produceN(t, c, leader, topic, 50) // min.insync 2 still satisfied
	c.Restart(follower)
	got := waitISR(t, c, leader, topic, 3)
	require.Contains(t, got.GetIsr(), follower)
	waitConverged(t, c, []int32{1, 2, 3}, tp)
	p, _ := c.Node(follower).Broker().Partition(tp)
	require.Equal(t, int64(100), p.Log().LogEndOffset())
}

// TestReplicationNotEnoughReplicas: RF 3 with min.insync 3; stopping one follower shrinks the
// ISR to 2, after which acks=all produce fails with NOT_ENOUGH_REPLICAS and appends nothing.
func TestReplicationNotEnoughReplicas(t *testing.T) {
	const topic = "nem"
	c := replicationCluster(t)
	createReplicated(t, c, topic, 3)
	pm := partitionMeta(t, c, 1, topic, 0)
	leader := pm.GetLeaderId()
	produceN(t, c, leader, topic, 10)
	var follower int32
	for _, r := range pm.GetReplicas() {
		if r != leader {
			follower = r
			break
		}
	}
	c.Stop(follower)
	waitISR(t, c, leader, topic, 2)
	p, ok := c.Node(leader).Broker().Partition(storage.TopicPartition{Topic: topic, Partition: 0})
	require.True(t, ok)
	leo := p.Log().LogEndOffset()
	code := produceOnce(t, c, leader, topic, 1)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS, code)
	require.Equal(t, leo, p.Log().LogEndOffset(), "nothing appended")
}

func produceOnce(t *testing.T, c *testnode.Cluster, leader int32, topic string, n int) streamqv1.ErrorCode {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for i := range n {
		b.Append(time.Now().UnixMilli(), nil, []byte(fmt.Sprint("v", i)), nil)
	}
	data, err := b.Build()
	require.NoError(t, err)
	resp, err := dial(t, c.Addr(leader)).Produce(testCtx(t), &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_ALL, TimeoutMs: 10_000,
		Topics: []*streamqv1.ProduceTopic{{Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: 0, LeaderEpoch: -1, Records: data}}}}})
	require.NoError(t, err)
	return resp.GetTopics()[0].GetPartitions()[0].GetErrorCode()
}

func produceN(t *testing.T, c *testnode.Cluster, leader int32, topic string, n int) {
	t.Helper()
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, produceOnce(t, c, leader, topic, n))
}
