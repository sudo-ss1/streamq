package main

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/partitioner"
	"github.com/sudo-ss1/streamq/internal/record"
)

// sentBatch is one decoded batch received by the fake broker.
type sentBatch struct {
	partition int32
	acks      streamqv1.Acks
	records   []record.Record
}

// produceRecorder is a fake broker that validates and records every produced batch.
type produceRecorder struct {
	t          *testing.T
	topic      string
	partitions int32
	errFor     map[int32]streamqv1.ErrorCode

	mu      sync.Mutex
	batches []sentBatch
}

func (p *produceRecorder) server() *fakeServer {
	return &fakeServer{
		metadata: func(r *streamqv1.MetadataRequest) *streamqv1.MetadataResponse {
			tm := &streamqv1.TopicMetadata{Name: p.topic}
			if len(r.GetTopics()) != 1 || r.GetTopics()[0] != p.topic {
				tm = &streamqv1.TopicMetadata{Name: strings.Join(r.GetTopics(), ","),
					ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION}
			}
			for i := range p.partitions {
				tm.Partitions = append(tm.Partitions, &streamqv1.PartitionMetadata{Partition: i, LeaderId: 1})
			}
			return &streamqv1.MetadataResponse{Topics: []*streamqv1.TopicMetadata{tm}}
		},
		produce: p.handle,
	}
}

func (p *produceRecorder) handle(r *streamqv1.ProduceRequest) *streamqv1.ProduceResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	resp := &streamqv1.ProduceResponse{}
	for _, tp := range r.GetTopics() {
		tr := &streamqv1.ProduceTopicResponse{Topic: tp.GetTopic()}
		for _, pp := range tp.GetPartitions() {
			// Assertions here run on a server goroutine; record failures with assert-style checks
			// via t.Errorf so they surface without calling FailNow off the test goroutine.
			if tp.GetTopic() != p.topic {
				p.t.Errorf("produce to topic %q, want %q", tp.GetTopic(), p.topic)
			}
			if pp.GetLeaderEpoch() != -1 {
				p.t.Errorf("leader epoch %d, want -1", pp.GetLeaderEpoch())
			}
			b, err := record.Validate(pp.GetRecords(), 1<<20)
			if err != nil {
				p.t.Errorf("validate batch: %v", err)
				continue
			}
			recs, err := b.Records()
			if err != nil {
				p.t.Errorf("decode batch: %v", err)
				continue
			}
			if b.ProducerID() != record.NoProducerID || b.BaseSequence() != record.NoSequence {
				p.t.Errorf("batch has producer fields %d/%d", b.ProducerID(), b.BaseSequence())
			}
			cp := make([]record.Record, len(recs))
			for i, rec := range recs {
				cp[i] = record.Record{Offset: rec.Offset, Timestamp: rec.Timestamp,
					Key: cloneBytes(rec.Key), Value: cloneBytes(rec.Value)}
			}
			p.batches = append(p.batches, sentBatch{partition: pp.GetPartition(), acks: r.GetAcks(), records: cp})
			tr.Partitions = append(tr.Partitions, &streamqv1.ProducePartitionResponse{
				Partition: pp.GetPartition(), ErrorCode: p.errFor[pp.GetPartition()],
			})
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp
}

func (p *produceRecorder) sent() []sentBatch {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sentBatch(nil), p.batches...)
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

func TestProduceKeyed(t *testing.T) {
	const partitions = 3
	var in strings.Builder
	var lines []string
	for i := range 40 {
		l := fmt.Sprintf("key-%d:value-%d:x", i%7, i)
		lines = append(lines, l)
		in.WriteString(l + "\n")
	}
	p := &produceRecorder{t: t, topic: "orders", partitions: partitions}
	res := runCmd(t, p.server(), in.String(),
		"produce", "--topic", "orders", "--key-separator", ":", "--acks", "leader", "--batch-size", "4")
	require.NoError(t, res.err)
	require.Equal(t, "produced 40 records\n", res.out)

	perKey := map[string][]string{}
	total := 0
	for _, b := range p.sent() {
		require.Equal(t, streamqv1.Acks_ACKS_LEADER, b.acks)
		require.NotEmpty(t, b.records)
		require.LessOrEqual(t, len(b.records), 4)
		for _, r := range b.records {
			require.Equal(t, fixedNowMs, r.Timestamp)
			require.Equal(t, partitioner.ForKey(r.Key, partitions), b.partition, "key %s", r.Key)
			perKey[string(r.Key)] = append(perKey[string(r.Key)], string(r.Value))
			total++
		}
	}
	require.Equal(t, 40, total)
	// Per-key order is preserved and the value is everything after the first separator.
	for i := range 7 {
		key := fmt.Sprintf("key-%d", i)
		var want []string
		for j, l := range lines {
			if j%7 == i {
				want = append(want, strings.TrimPrefix(l, key+":"))
			}
		}
		require.Equal(t, want, perKey[key])
	}
}

func TestProduceUnkeyedRoundRobin(t *testing.T) {
	p := &produceRecorder{t: t, topic: "logs", partitions: 3}
	// No line contains the separator, so every record is unkeyed even with --key-separator.
	in := "l0\nl1\nl2\nl3\nl4\nl5\nl6\n"
	res := runCmd(t, p.server(), in, "produce", "--topic", "logs", "--batch-size", "2", "--key-separator", "|")
	require.NoError(t, res.err)
	require.Equal(t, "produced 7 records\n", res.out)

	type got struct {
		partition int32
		values    []string
	}
	var batches []got
	for _, b := range p.sent() {
		require.Equal(t, streamqv1.Acks_ACKS_ALL, b.acks) // default --acks
		g := got{partition: b.partition}
		for _, r := range b.records {
			require.Nil(t, r.Key)
			g.values = append(g.values, string(r.Value))
		}
		batches = append(batches, g)
	}
	require.Equal(t, []got{
		{0, []string{"l0", "l1"}},
		{1, []string{"l2", "l3"}},
		{2, []string{"l4", "l5"}},
		{0, []string{"l6"}},
	}, batches)
}

func TestProduceAcks(t *testing.T) {
	tests := []struct {
		flag    string
		want    streamqv1.Acks
		wantErr string
	}{
		{flag: "none", want: streamqv1.Acks_ACKS_NONE},
		{flag: "leader", want: streamqv1.Acks_ACKS_LEADER},
		{flag: "all", want: streamqv1.Acks_ACKS_ALL},
		{flag: "0", want: streamqv1.Acks_ACKS_NONE},
		{flag: "1", want: streamqv1.Acks_ACKS_LEADER},
		{flag: "-1", want: streamqv1.Acks_ACKS_ALL},
		{flag: "some", wantErr: `invalid --acks "some"`},
	}
	for _, tt := range tests {
		t.Run(tt.flag, func(t *testing.T) {
			p := &produceRecorder{t: t, topic: "t", partitions: 1}
			res := runCmd(t, p.server(), "v\n", "produce", "--topic", "t", "--acks="+tt.flag)
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				require.Empty(t, p.sent())
				return
			}
			require.NoError(t, res.err)
			sent := p.sent()
			require.Len(t, sent, 1)
			require.Equal(t, tt.want, sent[0].acks)
		})
	}
}

func TestProducePartitionError(t *testing.T) {
	p := &produceRecorder{t: t, topic: "orders", partitions: 2,
		errFor: map[int32]streamqv1.ErrorCode{1: streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER}}
	// Unkeyed with batch size 1: l0 -> p0, l1 -> p1 (fails).
	res := runCmd(t, p.server(), "l0\nl1\nl2\n", "produce", "--topic", "orders", "--batch-size", "1")
	require.ErrorContains(t, res.err, `produce to topic "orders" partition 1: NOT_LEADER`)
	require.Contains(t, res.errOut, "NOT_LEADER")
	require.NotContains(t, res.out, "produced")
	require.Len(t, p.sent(), 2)
}

func TestProduceValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"unknown topic", []string{"produce", "--topic", "missing"}, `topic "missing": UNKNOWN_TOPIC_OR_PARTITION`},
		{"bad batch size", []string{"produce", "--topic", "t", "--batch-size", "0"}, "invalid --batch-size 0"},
		{"missing topic", []string{"produce"}, `required flag(s) "topic" not set`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &produceRecorder{t: t, topic: "t", partitions: 1}
			res := runCmd(t, p.server(), "v\n", tt.args...)
			require.ErrorContains(t, res.err, tt.wantErr)
			require.Empty(t, p.sent())
		})
	}
}

func TestProduceEmptyInput(t *testing.T) {
	p := &produceRecorder{t: t, topic: "t", partitions: 2}
	res := runCmd(t, p.server(), "", "produce", "--topic", "t")
	require.NoError(t, res.err)
	require.Equal(t, "produced 0 records\n", res.out)
	require.Empty(t, p.sent())
}
