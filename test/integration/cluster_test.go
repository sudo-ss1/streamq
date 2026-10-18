//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
	"github.com/sudo-ss1/streamq/internal/testnode"
)

func metadataOf(t *testing.T, addr string, topics ...string) *streamqv1.MetadataResponse {
	t.Helper()
	resp, err := dial(t, addr).Metadata(testCtx(t), &streamqv1.MetadataRequest{Topics: topics})
	require.NoError(t, err)
	return resp
}

// waitAllUnfenced waits until every listed broker reports all n brokers live.
func waitAllUnfenced(t *testing.T, c *testnode.Cluster, ids []int32, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, id := range ids {
			resp, err := dial(t, c.Addr(id)).Metadata(testCtx(t), &streamqv1.MetadataRequest{})
			if err != nil || len(resp.GetBrokers()) != n || resp.GetControllerId() < 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "brokers registered and unfenced")
}

func partitionsOf(resp *streamqv1.MetadataResponse, topic string) []*streamqv1.PartitionMetadata {
	for _, t := range resp.GetTopics() {
		if t.GetName() == topic {
			return t.GetPartitions()
		}
	}
	return nil
}

// TestClusterMetadataRaft is the M5 acceptance test (SPEC §9): topics created via any broker are
// visible everywhere; killing the controller elects a new one with metadata intact; a fenced
// broker's partitions move to a new leader; the broker rejoins after a restart with its data.
func TestClusterMetadataRaft(t *testing.T) {
	c := testnode.StartCluster(t, 3, nil)
	ids := []int32{1, 2, 3}
	waitAllUnfenced(t, c, ids, 3)

	// A topic created via each broker (followers forward to the controller).
	for _, id := range ids {
		resp, err := dial(t, c.Addr(id)).CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{
			Name: fmt.Sprintf("topic-%d", id), Partitions: 3, ReplicationFactor: 3,
		})
		require.NoError(t, err)
		require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	}
	topics := []string{"topic-1", "topic-2", "topic-3"}
	require.Eventually(t, func() bool {
		ref := metadataOf(t, c.Addr(1), topics...)
		for _, id := range ids {
			got := metadataOf(t, c.Addr(id), topics...)
			for _, topic := range topics {
				ps := partitionsOf(got, topic)
				if len(ps) != 3 || fmt.Sprint(ps) != fmt.Sprint(partitionsOf(ref, topic)) {
					return false
				}
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond, "identical topic metadata on every broker")

	// Produce (acks=1) to a partition led by a broker that will be restarted later, so the
	// restart must keep its partition data (plan R1: no stray deletion on a stale FSM).
	before := metadataOf(t, c.Addr(1), "topic-1")
	before3 := metadataOf(t, c.Addr(1), topics...)
	controller := before.GetControllerId()
	var victimTP storage.TopicPartition
	victimLeader := int32(-1)
	for _, p := range partitionsOf(before, "topic-1") {
		if p.GetLeaderId() == controller {
			victimTP = storage.TopicPartition{Topic: "topic-1", Partition: p.GetPartition()}
			victimLeader = p.GetLeaderId()
		}
	}
	require.NotEqual(t, int32(-1), victimLeader, "the controller leads some partition of topic-1 (round-robin over 3 brokers)")
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for i := range 10 {
		b.Append(time.Now().UnixMilli(), nil, []byte(fmt.Sprint("v", i)), nil)
	}
	batch, err := b.Build()
	require.NoError(t, err)
	pr, err := dial(t, c.Addr(victimLeader)).Produce(testCtx(t), &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_LEADER,
		Topics: []*streamqv1.ProduceTopic{{Topic: victimTP.Topic, Partitions: []*streamqv1.ProducePartition{
			{Partition: victimTP.Partition, LeaderEpoch: -1, Records: batch}}}}})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, pr.GetTopics()[0].GetPartitions()[0].GetErrorCode())

	// Kill the controller: a new one is elected among the survivors and metadata is intact.
	c.Stop(controller)
	survivors := slices.DeleteFunc(slices.Clone(ids), func(id int32) bool { return id == controller })
	var newController int32
	require.Eventually(t, func() bool {
		resp, err := dial(t, c.Addr(survivors[0])).Metadata(testCtx(t), &streamqv1.MetadataRequest{})
		if err != nil {
			return false
		}
		newController = resp.GetControllerId()
		return newController >= 0 && newController != controller
	}, 20*time.Second, 50*time.Millisecond, "a new controller is elected")
	// Metadata intact: topic configs and replica sets unchanged on every survivor (leaders and
	// epochs may change with the failover).
	identity := func(resp *streamqv1.MetadataResponse) string {
		var out []string
		for _, topic := range topics {
			for _, p := range partitionsOf(resp, topic) {
				out = append(out, fmt.Sprint(topic, p.GetPartition(), p.GetReplicas()))
			}
		}
		for _, tm := range resp.GetTopics() {
			out = append(out, fmt.Sprint(tm.GetName(), tm.GetConfigs()))
		}
		return fmt.Sprint(out)
	}
	want := identity(metadataOf(t, c.Addr(survivors[0]), topics...))
	require.Equal(t, want, identity(before3), "replica sets and configs survive the failover")
	for _, id := range survivors {
		require.Equal(t, want, identity(metadataOf(t, c.Addr(id), topics...)), "metadata intact on broker %d", id)
	}
	resp, err := dial(t, c.Addr(survivors[1])).CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{Name: "after-failover", Partitions: 2, ReplicationFactor: 2})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())

	// After the session timeout the dead broker is fenced: the partition it led has a new leader
	// with a bumped epoch (SPEC §4.4 election rule).
	var oldEpoch int32
	for _, p := range partitionsOf(before, "topic-1") {
		if p.GetPartition() == victimTP.Partition {
			oldEpoch = p.GetLeaderEpoch()
		}
	}
	require.Eventually(t, func() bool {
		for _, p := range partitionsOf(metadataOf(t, c.Addr(survivors[0]), "topic-1"), "topic-1") {
			if p.GetPartition() == victimTP.Partition {
				return p.GetLeaderId() != controller && p.GetLeaderId() >= 0 && p.GetLeaderEpoch() > oldEpoch &&
					!slices.Contains(p.GetIsr(), controller)
			}
		}
		return false
	}, 20*time.Second, 50*time.Millisecond, "fenced broker's partition re-elected")

	// The killed broker restarts on its data dir: it re-registers, gets unfenced, sees everything,
	// and its partition data survived.
	c.Restart(controller)
	waitAllUnfenced(t, c, ids, 3)
	require.Eventually(t, func() bool {
		return len(partitionsOf(metadataOf(t, c.Addr(controller), "after-failover"), "after-failover")) == 2
	}, 15*time.Second, 50*time.Millisecond)
	dir := filepath.Join(c.Config(controller).DataDir, victimTP.DirName())
	_, err = os.Stat(dir)
	require.NoError(t, err, "partition dir survived the restart")
	p, ok := c.Node(controller).Broker().Partition(victimTP)
	require.True(t, ok)
	require.Equal(t, int64(10), p.Log().LogEndOffset(), "partition data survived the restart")
}
