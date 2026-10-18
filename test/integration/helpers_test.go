//go:build integration

// Package integration holds end-to-end tests that run real brokers (tag "integration").
package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/partitioner"
	"github.com/sudo-ss1/streamq/internal/record"
)

func dial(t *testing.T, addr string) streamqv1.StreamQServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return streamqv1.NewStreamQServiceClient(conn)
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func createTopic(t *testing.T, c streamqv1.StreamQServiceClient, name string, partitions int32) {
	t.Helper()
	resp, err := c.CreateTopic(testCtx(t), &streamqv1.CreateTopicRequest{
		Name: name, Partitions: partitions, ReplicationFactor: 1,
		Configs: map[string]string{"min_insync_replicas": "1", "segment_bytes": "1048576"},
	})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
}

// keyedRecord is one produced record: value "key/seq".
type keyedRecord struct {
	key string
	seq int
}

func (r keyedRecord) value() string { return fmt.Sprintf("%s/%d", r.key, r.seq) }

// generate returns n records over numKeys keys, interleaved, with per-key seqs 0,1,2...
func generate(n, numKeys int) []keyedRecord {
	seqs := make([]int, numKeys)
	out := make([]keyedRecord, n)
	for i := range out {
		k := (i * 7919) % numKeys
		out[i] = keyedRecord{key: "key-" + strconv.Itoa(k), seq: seqs[k]}
		seqs[k]++
	}
	return out
}

// produceKeyed sends records partitioned by murmur2(key), in batches of batchSize per partition,
// acks=all, preserving per-key order. onAck is called with each acknowledged batch's records.
func produceKeyed(t *testing.T, c streamqv1.StreamQServiceClient, topic string, partitions int32,
	recs []keyedRecord, batchSize int, onAck func([]keyedRecord),
) {
	t.Helper()
	pending := make(map[int32][]keyedRecord)
	flush := func(p int32) {
		batch := pending[p]
		if len(batch) == 0 {
			return
		}
		b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
		now := time.Now().UnixMilli()
		for _, r := range batch {
			b.Append(now, []byte(r.key), []byte(r.value()), nil)
		}
		data, err := b.Build()
		require.NoError(t, err)
		resp, err := c.Produce(testCtx(t), &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_ALL, Topics: []*streamqv1.ProduceTopic{{
			Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: p, LeaderEpoch: -1, Records: data}},
		}}})
		require.NoError(t, err)
		pr := resp.GetTopics()[0].GetPartitions()[0]
		require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, pr.GetErrorCode(), pr.GetErrorMessage())
		if onAck != nil {
			onAck(batch)
		}
		pending[p] = nil
	}
	for _, r := range recs {
		p := partitioner.ForKey([]byte(r.key), partitions)
		pending[p] = append(pending[p], r)
		if len(pending[p]) >= batchSize {
			flush(p)
		}
	}
	for p := range partitions {
		flush(p)
	}
}

// consumed is one record read back.
type consumed struct {
	partition int32
	offset    int64
	key       string
	value     string
}

// consumeAll reads every partition from offset 0 up to its high watermark.
func consumeAll(t *testing.T, c streamqv1.StreamQServiceClient, topic string, partitions int32) []consumed {
	t.Helper()
	var out []consumed
	for p := range partitions {
		next := int64(0)
		for {
			resp, err := c.Fetch(testCtx(t), &streamqv1.FetchRequest{MaxWaitMs: 0, MaxBytes: 4 << 20, Topics: []*streamqv1.FetchTopic{{
				Topic: topic, Partitions: []*streamqv1.FetchPartition{{Partition: p, LeaderEpoch: -1, FetchOffset: next, MaxBytes: 4 << 20}},
			}}})
			require.NoError(t, err)
			fp := resp.GetTopics()[0].GetPartitions()[0]
			require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, fp.GetErrorCode())
			if next >= fp.GetHighWatermark() {
				break
			}
			batches, err := record.SplitBatches(fp.GetRecords())
			require.NoError(t, err)
			require.NotEmpty(t, batches)
			for _, b := range batches {
				_, err := record.Validate(b, len(b))
				require.NoError(t, err)
				recs, err := b.Records()
				require.NoError(t, err)
				for _, r := range recs {
					if r.Offset < next {
						continue
					}
					require.Equal(t, next, r.Offset, "offsets must be contiguous")
					out = append(out, consumed{partition: p, offset: r.Offset, key: string(r.Key), value: string(r.Value)})
					next++
				}
			}
		}
	}
	return out
}

// verifyPerKeyOrder checks every key lives in exactly its murmur2 partition and its seqs appear
// as 0..n-1 in offset order. It returns the per-key counts.
func verifyPerKeyOrder(t *testing.T, recs []consumed, partitions int32) map[string]int {
	t.Helper()
	next := map[string]int{}
	for _, r := range recs {
		key, seqStr, ok := strings.Cut(r.value, "/")
		require.True(t, ok, r.value)
		require.Equal(t, key, r.key)
		require.Equal(t, partitioner.ForKey([]byte(key), partitions), r.partition, "key %s in wrong partition", key)
		seq, err := strconv.Atoi(seqStr)
		require.NoError(t, err)
		require.Equal(t, next[key], seq, "key %s out of order at partition %d offset %d", key, r.partition, r.offset)
		next[key]++
	}
	return next
}
