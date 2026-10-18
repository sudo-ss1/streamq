package broker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metrics"
)

func commitCmd(group string, offs ...*streamqv1.OffsetCommit) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: group, Offsets: offs},
	}}
}

func TestPartitionStatsSingleNode(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("b", "idb", 1)
	e.createTopic("a", "ida", 2)
	e.produce("a", 0, "x", "y", "z")
	e.produce("a", 1, "x")

	want := []metrics.PartitionStat{
		{Topic: "a", Partition: 0, Leader: true, LEO: 3, HW: 3, ISRSize: 1, Replicas: 1},
		{Topic: "a", Partition: 1, Leader: true, LEO: 1, HW: 1, ISRSize: 1, Replicas: 1},
		{Topic: "b", Partition: 0, Leader: true, LEO: 0, HW: 0, ISRSize: 1, Replicas: 1},
	}
	require.Equal(t, want, e.b.PartitionStats())

	n, reports := e.b.OfflinePartitions()
	require.True(t, reports, "single-node broker is the controller")
	require.Zero(t, n)
}

func TestConsumerLag(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("a", "ida", 2)
	e.produce("a", 0, "x", "y", "z")
	e.produce("a", 1, "x")
	require.Empty(t, e.b.ConsumerLag())

	ctx := context.Background()
	require.NoError(t, e.store.Propose(ctx, commitCmd("g",
		&streamqv1.OffsetCommit{Topic: "a", Partition: 0, Offset: 1},
		&streamqv1.OffsetCommit{Topic: "a", Partition: 1, Offset: 5}, // beyond HW: lag clamps to 0
	)))
	require.NoError(t, e.store.Propose(ctx, commitCmd("f",
		&streamqv1.OffsetCommit{Topic: "a", Partition: 0, Offset: 0},
	)))

	want := []metrics.LagStat{
		{Group: "f", Topic: "a", Partition: 0, Lag: 3},
		{Group: "g", Topic: "a", Partition: 0, Lag: 2},
		{Group: "g", Topic: "a", Partition: 1, Lag: 0},
	}
	require.Equal(t, want, e.b.ConsumerLag())
}

func TestStatsAfterClose(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("a", "ida", 1)
	e.produce("a", 0, "x", "y")
	require.NoError(t, e.store.Propose(context.Background(), commitCmd("g",
		&streamqv1.OffsetCommit{Topic: "a", Partition: 0, Offset: 1})))
	b := e.b
	e.close() // closes the broker and the store

	require.NotPanics(t, func() {
		stats := b.PartitionStats()
		require.Len(t, stats, 1)
		require.Equal(t, int64(2), stats[0].LEO)
		require.Equal(t, int64(2), stats[0].HW)
		_ = b.ConsumerLag()
		_, _ = b.OfflinePartitions()
	})
}
