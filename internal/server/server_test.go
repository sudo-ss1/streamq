package server_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
	"github.com/sudo-ss1/streamq/internal/record"
)

const (
	codeNone = streamqv1.ErrorCode_ERROR_CODE_NONE
)

func startNode(t *testing.T, dir string) streamqv1.StreamQServiceClient {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TopicDefaults.ReplicationFactor = 1
	cfg.TopicDefaults.MinInsyncReplicas = 1
	n, err := node.Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	conn, err := grpc.NewClient(n.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.NoError(t, n.Close(context.Background()))
	})
	return streamqv1.NewStreamQServiceClient(conn)
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func batch(t *testing.T, ts int64, values ...string) []byte {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, v := range values {
		b.Append(ts, nil, []byte(v), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

func produce(t *testing.T, c streamqv1.StreamQServiceClient, acks streamqv1.Acks, topic string, p, epoch int32, records []byte) *streamqv1.ProducePartitionResponse {
	t.Helper()
	resp, err := c.Produce(ctxT(t), &streamqv1.ProduceRequest{Acks: acks, Topics: []*streamqv1.ProduceTopic{{
		Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: p, LeaderEpoch: epoch, Records: records}},
	}}})
	require.NoError(t, err)
	return resp.GetTopics()[0].GetPartitions()[0]
}

func TestCreateTopicErrors(t *testing.T) {
	c := startNode(t, t.TempDir())
	ctx := ctxT(t)
	resp, err := c.CreateTopic(ctx, &streamqv1.CreateTopicRequest{Name: "orders", Partitions: 2})
	require.NoError(t, err)
	require.Equal(t, codeNone, resp.GetErrorCode(), resp.GetErrorMessage())
	tests := []struct {
		name string
		req  *streamqv1.CreateTopicRequest
		want streamqv1.ErrorCode
	}{
		{"exists", &streamqv1.CreateTopicRequest{Name: "orders"}, streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS},
		{"rf above brokers", &streamqv1.CreateTopicRequest{Name: "x", ReplicationFactor: 2}, streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR},
		{"bad name", &streamqv1.CreateTopicRequest{Name: "a/b"}, streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC},
		{"bad config key", &streamqv1.CreateTopicRequest{Name: "y", Configs: map[string]string{"nope": "1"}}, streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG},
		{"bad config value", &streamqv1.CreateTopicRequest{Name: "y", Configs: map[string]string{"segment_bytes": "5000000000"}}, streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.CreateTopic(ctx, tt.req)
			require.NoError(t, err)
			require.Equal(t, tt.want, resp.GetErrorCode(), resp.GetErrorMessage())
		})
	}
	list, err := c.ListTopics(ctx, &streamqv1.ListTopicsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, list.GetNames())
}

func TestCreateThenProduceImmediately(t *testing.T) {
	c := startNode(t, t.TempDir())
	for i := range 50 {
		name := "t" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		resp, err := c.CreateTopic(ctxT(t), &streamqv1.CreateTopicRequest{Name: name, Partitions: 3})
		require.NoError(t, err)
		require.Equal(t, codeNone, resp.GetErrorCode(), resp.GetErrorMessage())
		pr := produce(t, c, streamqv1.Acks_ACKS_ALL, name, 2, -1, batch(t, 1, "v"))
		require.Equal(t, codeNone, pr.GetErrorCode(), pr.GetErrorMessage())
	}
}

func TestProduceFetchListOffsets(t *testing.T) {
	c := startNode(t, t.TempDir())
	ctx := ctxT(t)
	_, err := c.CreateTopic(ctx, &streamqv1.CreateTopicRequest{Name: "t", Partitions: 1, Configs: map[string]string{"max_message_bytes": "1024"}})
	require.NoError(t, err)
	md, err := c.Metadata(ctx, &streamqv1.MetadataRequest{Topics: []string{"t", "missing"}})
	require.NoError(t, err)
	require.Len(t, md.GetBrokers(), 1)
	require.Equal(t, int32(1), md.GetControllerId())
	require.Equal(t, int32(1), md.GetTopics()[0].GetPartitions()[0].GetLeaderId())
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, md.GetTopics()[1].GetErrorCode())

	pr := produce(t, c, streamqv1.Acks_ACKS_LEADER, "t", 0, 0, batch(t, 100, "a", "b"))
	require.Equal(t, codeNone, pr.GetErrorCode())
	require.Equal(t, int64(0), pr.GetBaseOffset())
	pr = produce(t, c, streamqv1.Acks_ACKS_ALL, "t", 0, 0, batch(t, 200, "c"))
	require.Equal(t, int64(2), pr.GetBaseOffset())
	pr = produce(t, c, streamqv1.Acks_ACKS_NONE, "t", 0, 0, batch(t, 300, "d"))
	require.Equal(t, int64(-1), pr.GetBaseOffset(), "acks=0 returns no offset")

	errCases := []struct {
		name    string
		topic   string
		p       int32
		epoch   int32
		records []byte
		want    streamqv1.ErrorCode
	}{
		{"unknown topic", "nope", 0, -1, batch(t, 1, "x"), streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{"unknown partition", "t", 5, -1, batch(t, 1, "x"), streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION},
		{"too large", "t", 0, -1, batch(t, 1, string(make([]byte, 2000))), streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE},
		{"corrupt", "t", 0, -1, []byte("not a batch at all, definitely not one"), streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE},
		{"negative timestamp", "t", 0, -1, batch(t, -5, "x"), streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP},
		{"newer epoch", "t", 0, 4, batch(t, 1, "x"), streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER},
	}
	for _, tt := range errCases {
		t.Run(tt.name, func(t *testing.T) {
			pr := produce(t, c, streamqv1.Acks_ACKS_LEADER, tt.topic, tt.p, tt.epoch, tt.records)
			require.Equal(t, tt.want, pr.GetErrorCode(), pr.GetErrorMessage())
		})
	}

	fetch := func(offset int64) *streamqv1.FetchPartitionResponse {
		resp, err := c.Fetch(ctx, &streamqv1.FetchRequest{MaxWaitMs: 0, Topics: []*streamqv1.FetchTopic{{
			Topic: "t", Partitions: []*streamqv1.FetchPartition{{Partition: 0, LeaderEpoch: -1, FetchOffset: offset}},
		}}})
		require.NoError(t, err)
		return resp.GetTopics()[0].GetPartitions()[0]
	}
	fp := fetch(1)
	require.Equal(t, codeNone, fp.GetErrorCode())
	require.Equal(t, int64(4), fp.GetHighWatermark())
	batches, err := record.SplitBatches(fp.GetRecords())
	require.NoError(t, err)
	require.Len(t, batches, 3)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE, fetch(99).GetErrorCode())

	lo, err := c.ListOffsets(ctx, &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{{
		Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{
			{Partition: 0, LeaderEpoch: -1, Timestamp: -2},
			{Partition: 0, LeaderEpoch: -1, Timestamp: -1},
			{Partition: 0, LeaderEpoch: -1, Timestamp: 150},
			{Partition: 0, LeaderEpoch: -1, Timestamp: 999},
		},
	}}})
	require.NoError(t, err)
	parts := lo.GetTopics()[0].GetPartitions()
	require.Equal(t, []int64{0, 4, 2, -1}, []int64{parts[0].GetOffset(), parts[1].GetOffset(), parts[2].GetOffset(), parts[3].GetOffset()})
	require.Equal(t, int64(150), parts[2].GetTimestamp())

	_, err = c.ListOffsets(ctx, &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{{
		Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{{Partition: 0, Timestamp: -3}},
	}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = c.Produce(ctx, &streamqv1.ProduceRequest{Acks: 42})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestAcksAllDefaultMinInsyncOnSingleNode(t *testing.T) {
	c := startNode(t, t.TempDir())
	ctx := ctxT(t)
	resp, err := c.CreateTopic(ctx, &streamqv1.CreateTopicRequest{Name: "t", Partitions: 1, Configs: map[string]string{"min_insync_replicas": "2"}})
	require.NoError(t, err)
	require.Equal(t, codeNone, resp.GetErrorCode(), "min.insync > RF is accepted at create (D16)")
	pr := produce(t, c, streamqv1.Acks_ACKS_UNSPECIFIED, "t", 0, -1, batch(t, 1, "x"))
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS, pr.GetErrorCode())
	lo, err := c.ListOffsets(ctx, &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{{
		Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{{Partition: 0, LeaderEpoch: -1, Timestamp: -1}},
	}}})
	require.NoError(t, err)
	require.Equal(t, int64(0), lo.GetTopics()[0].GetPartitions()[0].GetOffset(), "nothing appended")
}

func TestDeleteTopicAndFetchLongPoll(t *testing.T) {
	c := startNode(t, t.TempDir())
	ctx := ctxT(t)
	_, err := c.CreateTopic(ctx, &streamqv1.CreateTopicRequest{Name: "t", Partitions: 1})
	require.NoError(t, err)

	// A long-poll fetch at the HW returns once a record arrives.
	got := make(chan *streamqv1.FetchResponse, 1)
	errs := make(chan error, 1)
	go func() {
		resp, err := c.Fetch(ctx, &streamqv1.FetchRequest{MaxWaitMs: 20_000, MinBytes: 1, Topics: []*streamqv1.FetchTopic{{
			Topic: "t", Partitions: []*streamqv1.FetchPartition{{Partition: 0, LeaderEpoch: -1}},
		}}})
		errs <- err
		got <- resp
	}()
	produce(t, c, streamqv1.Acks_ACKS_ALL, "t", 0, -1, batch(t, 1, "x"))
	require.NoError(t, <-errs)
	require.NotEmpty(t, (<-got).GetTopics()[0].GetPartitions()[0].GetRecords())

	del, err := c.DeleteTopic(ctx, &streamqv1.DeleteTopicRequest{Name: "t"})
	require.NoError(t, err)
	require.Equal(t, codeNone, del.GetErrorCode())
	del, err = c.DeleteTopic(ctx, &streamqv1.DeleteTopicRequest{Name: "t"})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, del.GetErrorCode())
	pr := produce(t, c, streamqv1.Acks_ACKS_ALL, "t", 0, -1, batch(t, 1, "x"))
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, pr.GetErrorCode())
}
