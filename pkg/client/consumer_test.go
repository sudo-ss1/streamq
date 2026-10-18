package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// consumerSeed appends n records to tp's fake log.
func consumerSeed(fb *fakeBroker, tp TopicPartition, n int) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	for range n {
		off := int64(len(fb.logs[tp]))
		fb.logs[tp] = append(fb.logs[tp], record.Record{
			Offset:    off,
			Timestamp: 1_000 + off,
			Key:       []byte(fmt.Sprintf("k%d", off)),
			Value:     []byte(fmt.Sprintf("%s/%d/v%d", tp.Topic, tp.Partition, off)),
			Headers:   []record.Header{{Key: "h", Value: []byte("x")}},
		})
	}
}

// consumerFetchReqs returns a copy of the fetch requests seen so far.
func consumerFetchReqs(fb *fakeBroker) []*streamqv1.FetchRequest {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return append([]*streamqv1.FetchRequest(nil), fb.fetchReqs...)
}

// consumerFetched lists the (topic, partition) pairs of a fetch request in request order.
func consumerFetched(req *streamqv1.FetchRequest) []TopicPartition {
	var out []TopicPartition
	for _, t := range req.GetTopics() {
		for _, p := range t.GetPartitions() {
			out = append(out, TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()})
		}
	}
	return out
}

// consumerMetadata returns an onMetadata hook that reports fb's topics with the leader epoch
// currently in epoch.
func consumerMetadata(fb *fakeBroker, epoch *atomic.Int32) func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
	return func(req *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		resp := &streamqv1.MetadataResponse{ControllerId: fb.id, Brokers: []*streamqv1.Broker{{Id: fb.id, Host: fb.host, Port: fb.port}}}
		for _, n := range req.GetTopics() {
			parts, ok := fb.topics[n]
			if !ok {
				resp.Topics = append(resp.Topics, &streamqv1.TopicMetadata{Name: n, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION})
				continue
			}
			tm := &streamqv1.TopicMetadata{Name: n}
			for p := range parts {
				tm.Partitions = append(tm.Partitions, &streamqv1.PartitionMetadata{
					Partition: p, LeaderId: fb.id, LeaderEpoch: epoch.Load(), Replicas: []int32{fb.id}, Isr: []int32{fb.id},
				})
			}
			resp.Topics = append(resp.Topics, tm)
		}
		return resp, nil
	}
}

// consumerFailPartitions returns an onFetch hook that answers like the default fake but with code
// for the partitions in failing.
func consumerFailPartitions(fb *fakeBroker, code streamqv1.ErrorCode, failing ...TopicPartition) func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
	return func(req *streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		resp := &streamqv1.FetchResponse{}
		for _, t := range req.GetTopics() {
			tr := &streamqv1.FetchTopicResponse{Topic: t.GetTopic()}
			for _, p := range t.GetPartitions() {
				tp := TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}
				log := fb.logs[tp]
				pr := &streamqv1.FetchPartitionResponse{Partition: p.GetPartition(), HighWatermark: int64(len(log))}
				fail := false
				for _, f := range failing {
					fail = fail || f == tp
				}
				if fail {
					pr.ErrorCode = code
				} else if off := p.GetFetchOffset(); off < int64(len(log)) {
					start := off - off%3
					pr.Records = encode(log[start:min(start+3, int64(len(log)))], start)
				}
				tr.Partitions = append(tr.Partitions, pr)
			}
			resp.Topics = append(resp.Topics, tr)
		}
		return resp, nil
	}
}

func newTestConsumer(t *testing.T, c *Client, cfg ConsumerConfig) *Consumer {
	t.Helper()
	cons, err := NewConsumer(c, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cons.Close()) })
	return cons
}

// consumerPollN polls until n records arrived (bounded number of polls).
func consumerPollN(t *testing.T, cons *Consumer, n int) []Record {
	t.Helper()
	var out []Record
	for i := 0; i < 100 && len(out) < n; i++ {
		recs, err := cons.Poll(context.Background())
		require.NoError(t, err)
		out = append(out, recs...)
	}
	require.Len(t, out, n)
	return out
}

func TestConsumerDefaultsAndEmptyAssignment(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	cons := newTestConsumer(t, c, ConsumerConfig{AutoOffsetReset: ResetEarliest})

	recs, err := cons.Poll(context.Background())
	require.NoError(t, err)
	require.Nil(t, recs)
	require.Zero(t, fb.count("Fetch"))
	require.Zero(t, fb.count("Metadata"))

	tp := TopicPartition{Topic: "t", Partition: 0}
	consumerSeed(fb, tp, 1)
	cons.Assign([]TopicPartition{tp})
	consumerPollN(t, cons, 1)
	req := consumerFetchReqs(fb)[0]
	require.Equal(t, int32(500), req.GetMaxWaitMs())
	require.Equal(t, int32(1), req.GetMinBytes())
	require.Equal(t, int32(16<<20), req.GetMaxBytes())
	require.Equal(t, int32(1<<20), req.GetTopics()[0].GetPartitions()[0].GetMaxBytes())

	cons2 := newTestConsumer(t, c, ConsumerConfig{MaxWait: 20 * time.Millisecond, MinBytes: 10, MaxBytes: 1000, MaxPartitionBytes: 100})
	cons2.Assign([]TopicPartition{tp})
	require.NoError(t, cons2.Seek(tp, 0))
	consumerPollN(t, cons2, 1)
	reqs := consumerFetchReqs(fb)
	req = reqs[len(reqs)-1]
	require.Equal(t, int32(20), req.GetMaxWaitMs())
	require.Equal(t, int32(10), req.GetMinBytes())
	require.Equal(t, int32(1000), req.GetMaxBytes())
	require.Equal(t, int32(100), req.GetTopics()[0].GetPartitions()[0].GetMaxBytes())
}

func TestConsumerPollEarliestAndLatest(t *testing.T) {
	tests := []struct {
		name      string
		reset     OffsetReset
		wantFirst int64 // first offset delivered after 7 more records are appended
		wantLen   int
	}{
		{"earliest", ResetEarliest, 0, 12},
		{"latest", ResetLatest, 5, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb, c := singleBroker(t, map[string]int32{"t": 1})
			tp := TopicPartition{Topic: "t", Partition: 0}
			consumerSeed(fb, tp, 5)
			var lo []*streamqv1.ListOffsetsRequest
			fb.onListOffsets = func(req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
				lo = append(lo, req)
				return nil, nil
			}
			cons := newTestConsumer(t, c, ConsumerConfig{AutoOffsetReset: tt.reset})
			cons.Assign([]TopicPartition{tp})
			_, ok := cons.Position(tp)
			require.False(t, ok)

			if tt.reset == ResetLatest {
				recs, err := cons.Poll(context.Background())
				require.NoError(t, err)
				require.Empty(t, recs)
				pos, ok := cons.Position(tp)
				require.True(t, ok)
				require.Equal(t, int64(5), pos)
			}
			consumerSeed(fb, tp, 7)
			recs := consumerPollN(t, cons, tt.wantLen)
			for i, r := range recs {
				off := tt.wantFirst + int64(i)
				require.Equal(t, off, r.Offset)
				require.Equal(t, "t", r.Topic)
				require.Equal(t, int32(0), r.Partition)
				require.True(t, r.UsePartition)
				require.Equal(t, fmt.Sprintf("k%d", off), string(r.Key))
				require.Equal(t, fmt.Sprintf("t/0/v%d", off), string(r.Value))
				require.Equal(t, []Header{{Key: "h", Value: []byte("x")}}, r.Headers)
				require.Equal(t, time.UnixMilli(1_000+off), r.Timestamp)
			}
			pos, ok := cons.Position(tp)
			require.True(t, ok)
			require.Equal(t, int64(12), pos)

			require.Len(t, lo, 1)
			want := int64(-1)
			if tt.reset == ResetEarliest {
				want = -2
			}
			require.Equal(t, want, lo[0].GetTopics()[0].GetPartitions()[0].GetTimestamp())
		})
	}
}

func TestConsumerMidBatchPositionDropsEarlierRecords(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	tp := TopicPartition{Topic: "t", Partition: 0}
	consumerSeed(fb, tp, 6)
	cons := newTestConsumer(t, c, ConsumerConfig{})
	cons.Assign([]TopicPartition{tp})
	require.NoError(t, cons.Seek(tp, 4))

	recs, err := cons.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, recs, 2)
	require.Equal(t, int64(4), recs[0].Offset)
	require.Equal(t, int64(5), recs[1].Offset)
	require.Equal(t, int64(4), consumerFetchReqs(fb)[0].GetTopics()[0].GetPartitions()[0].GetFetchOffset())
	pos, _ := cons.Position(tp)
	require.Equal(t, int64(6), pos)
	require.Zero(t, fb.count("ListOffsets"))
}

func TestConsumerMultiPartitionExactlyOnceInOrder(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"a": 3, "b": 1})
	tps := []TopicPartition{{"a", 0}, {"a", 1}, {"a", 2}, {"b", 0}}
	sizes := []int{10, 7, 0, 5}
	total := 0
	for i, tp := range tps {
		consumerSeed(fb, tp, sizes[i])
		total += sizes[i]
	}
	cons := newTestConsumer(t, c, ConsumerConfig{AutoOffsetReset: ResetEarliest})
	cons.Assign(tps)
	require.ElementsMatch(t, tps, cons.Assignment())

	recs := consumerPollN(t, cons, total)
	next := map[TopicPartition]int64{}
	for _, r := range recs {
		tp := TopicPartition{Topic: r.Topic, Partition: r.Partition}
		require.Equal(t, next[tp], r.Offset, "%s/%d", tp.Topic, tp.Partition)
		next[tp]++
	}
	for i, tp := range tps {
		require.Equal(t, int64(sizes[i]), next[tp])
	}
	// Nothing left: another poll returns no records.
	more, err := cons.Poll(context.Background())
	require.NoError(t, err)
	require.Empty(t, more)

	// One request per poll (one leader), and the partition order rotates every poll.
	reqs := consumerFetchReqs(fb)
	require.GreaterOrEqual(t, len(reqs), 2)
	require.Equal(t, tps, consumerFetched(reqs[0]))
	// Partitions are grouped by topic in order of first appearance.
	require.Equal(t, []TopicPartition{{"a", 1}, {"a", 2}, {"a", 0}, {"b", 0}}, consumerFetched(reqs[1]))
}

func TestConsumerRefreshesMetadataOnStaleLeader(t *testing.T) {
	for _, code := range []streamqv1.ErrorCode{
		streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER,
		streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH,
	} {
		t.Run(code.String(), func(t *testing.T) {
			fb, c := singleBroker(t, map[string]int32{"t": 1})
			tp := TopicPartition{Topic: "t", Partition: 0}
			consumerSeed(fb, tp, 3)
			var epoch atomic.Int32
			epoch.Store(4)
			fb.onMetadata = consumerMetadata(fb, &epoch)
			var fetches atomic.Int32
			fb.onFetch = func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
				if fetches.Add(1) > 1 {
					return nil, nil
				}
				epoch.Store(5) // the leadership moved on
				return &streamqv1.FetchResponse{Topics: []*streamqv1.FetchTopicResponse{{
					Topic: "t", Partitions: []*streamqv1.FetchPartitionResponse{{Partition: 0, ErrorCode: code}},
				}}}, nil
			}
			cons := newTestConsumer(t, c, ConsumerConfig{})
			cons.Assign([]TopicPartition{tp})
			require.NoError(t, cons.Seek(tp, 0))

			recs, err := cons.Poll(context.Background())
			require.NoError(t, err)
			require.Empty(t, recs)
			require.Equal(t, 1, fb.count("Metadata"))

			recs = consumerPollN(t, cons, 3)
			require.Equal(t, int64(0), recs[0].Offset)
			require.Equal(t, 2, fb.count("Metadata"))
			reqs := consumerFetchReqs(fb)
			require.Len(t, reqs, 2)
			require.Equal(t, int32(4), reqs[0].GetTopics()[0].GetPartitions()[0].GetLeaderEpoch())
			require.Equal(t, int32(5), reqs[1].GetTopics()[0].GetPartitions()[0].GetLeaderEpoch())
		})
	}
}

func TestConsumerTransportErrorBacksOffAndRefreshes(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	tp := TopicPartition{Topic: "t", Partition: 0}
	consumerSeed(fb, tp, 2)
	var fetches atomic.Int32
	fb.onFetch = func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
		if fetches.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "down")
		}
		return nil, nil
	}
	cons := newTestConsumer(t, c, ConsumerConfig{AutoOffsetReset: ResetEarliest})
	cons.Assign([]TopicPartition{tp})
	recs, err := cons.Poll(context.Background())
	require.NoError(t, err)
	require.Empty(t, recs)
	metaBefore := fb.count("Metadata")
	consumerPollN(t, cons, 2)
	require.Greater(t, fb.count("Metadata"), metaBefore)
}

func TestConsumerOffsetOutOfRangeResets(t *testing.T) {
	tests := []struct {
		reset OffsetReset
		want  int64
	}{
		{ResetEarliest, 0},
		{ResetLatest, 5},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.reset), func(t *testing.T) {
			fb, c := singleBroker(t, map[string]int32{"t": 1})
			tp := TopicPartition{Topic: "t", Partition: 0}
			consumerSeed(fb, tp, 5)
			var epoch atomic.Int32
			epoch.Store(7)
			fb.onMetadata = consumerMetadata(fb, &epoch)
			var lo []*streamqv1.ListOffsetsRequest
			fb.onListOffsets = func(req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
				lo = append(lo, req)
				return nil, nil
			}
			cons := newTestConsumer(t, c, ConsumerConfig{AutoOffsetReset: tt.reset})
			cons.Assign([]TopicPartition{tp})
			require.NoError(t, cons.Seek(tp, 100))

			recs, err := cons.Poll(context.Background())
			require.NoError(t, err)
			require.Empty(t, recs)
			pos, ok := cons.Position(tp)
			require.True(t, ok)
			require.Equal(t, tt.want, pos)
			require.Len(t, lo, 1)
			p := lo[0].GetTopics()[0].GetPartitions()[0]
			require.Equal(t, int32(7), p.GetLeaderEpoch())

			if tt.reset == ResetEarliest {
				consumerPollN(t, cons, 5)
			}
		})
	}
}

func TestConsumerUnknownPartitionBacksOffAlone(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 2})
	good, bad := TopicPartition{Topic: "t", Partition: 0}, TopicPartition{Topic: "t", Partition: 1}
	consumerSeed(fb, good, 9)
	consumerSeed(fb, bad, 3)
	fb.onFetch = consumerFailPartitions(fb, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, bad)
	c.backoff = newBackoff(time.Hour, 2*time.Hour)
	c.backoff.rand = func() float64 { return 1 }
	cons := newTestConsumer(t, c, ConsumerConfig{})
	now := time.Unix(1_000_000, 0)
	cons.now = func() time.Time { return now }
	cons.Assign([]TopicPartition{good, bad})
	require.NoError(t, cons.Seek(good, 0))
	require.NoError(t, cons.Seek(bad, 0))

	recs, err := cons.Poll(context.Background())
	require.NoError(t, err)
	require.Len(t, recs, 3) // the healthy partition still delivers
	require.Equal(t, []TopicPartition{good, bad}, consumerFetched(consumerFetchReqs(fb)[0]))

	// While bad backs off it is absent from fetches, and good keeps delivering.
	recs = consumerPollN(t, cons, 6)
	for _, r := range recs {
		require.Equal(t, good.Partition, r.Partition)
	}
	reqs := consumerFetchReqs(fb)
	require.Len(t, reqs, 3)
	for _, req := range reqs[1:] {
		require.Equal(t, []TopicPartition{good}, consumerFetched(req))
	}

	// After the backoff (attempt 0: 1h) bad is fetched again, fails again, and backs off longer.
	now = now.Add(time.Hour)
	_, err = cons.Poll(context.Background())
	require.NoError(t, err)
	reqs = consumerFetchReqs(fb)
	require.ElementsMatch(t, []TopicPartition{good, bad}, consumerFetched(reqs[len(reqs)-1]))
	now = now.Add(time.Hour + time.Minute) // attempt 1: 2h, not over yet
	_, err = cons.Poll(context.Background())
	require.NoError(t, err)
	reqs = consumerFetchReqs(fb)
	require.Equal(t, []TopicPartition{good}, consumerFetched(reqs[len(reqs)-1]))

	// Once it recovers its attempts reset.
	fb.mu.Lock()
	fb.onFetch = nil
	fb.mu.Unlock()
	now = now.Add(time.Hour)
	consumerPollN(t, cons, 3)
	require.Zero(t, cons.parts[bad].attempts)
}

func TestConsumerAllPartitionsBackingOff(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	tp := TopicPartition{Topic: "t", Partition: 0}
	fb.onFetch = consumerFailPartitions(fb, streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER, tp)
	c.backoff.rand = func() float64 { return 1 }

	setup := func(t *testing.T, base time.Duration) *Consumer {
		t.Helper()
		c.backoff.base, c.backoff.max = base, base
		cons := newTestConsumer(t, c, ConsumerConfig{})
		now := time.Unix(1_000_000, 0)
		cons.now = func() time.Time { return now }
		cons.Assign([]TopicPartition{tp})
		require.NoError(t, cons.Seek(tp, 0))
		recs, err := cons.Poll(context.Background())
		require.NoError(t, err)
		require.Empty(t, recs)
		return cons
	}

	t.Run("ctx cancelled", func(t *testing.T) {
		cons := setup(t, time.Hour)
		before := fb.count("Fetch")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := cons.Poll(ctx)
			done <- err
		}()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.Equal(t, before, fb.count("Fetch"))
	})

	t.Run("close wakes poll", func(t *testing.T) {
		cons := setup(t, time.Hour)
		done := make(chan error, 1)
		go func() {
			_, err := cons.Poll(context.Background())
			done <- err
		}()
		require.NoError(t, cons.Close())
		require.ErrorIs(t, <-done, ErrClosed)
	})

	t.Run("backoff expires", func(t *testing.T) {
		cons := setup(t, time.Nanosecond) // the frozen clock keeps it backing off; the wait is 1ns
		before := fb.count("Fetch")
		recs, err := cons.Poll(context.Background())
		require.NoError(t, err)
		require.Empty(t, recs)
		require.Equal(t, before, fb.count("Fetch"))
	})
}

func TestConsumerFatalErrors(t *testing.T) {
	t.Run("returned", func(t *testing.T) {
		fb, c := singleBroker(t, map[string]int32{"t": 1})
		tp := TopicPartition{Topic: "t", Partition: 0}
		fb.onFetch = consumerFailPartitions(fb, streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE, tp)
		cons := newTestConsumer(t, c, ConsumerConfig{})
		cons.Assign([]TopicPartition{tp})
		require.NoError(t, cons.Seek(tp, 0))
		_, err := cons.Poll(context.Background())
		require.ErrorIs(t, err, ErrCorruptMessage)
		pos, _ := cons.Position(tp)
		require.Zero(t, pos)
	})

	t.Run("deferred behind records", func(t *testing.T) {
		fb, c := singleBroker(t, map[string]int32{"t": 2})
		good, bad := TopicPartition{Topic: "t", Partition: 0}, TopicPartition{Topic: "t", Partition: 1}
		consumerSeed(fb, good, 2)
		fb.onFetch = consumerFailPartitions(fb, streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP, bad)
		cons := newTestConsumer(t, c, ConsumerConfig{})
		cons.Assign([]TopicPartition{good, bad})
		require.NoError(t, cons.Seek(good, 0))
		require.NoError(t, cons.Seek(bad, 0))
		recs, err := cons.Poll(context.Background())
		require.NoError(t, err)
		require.Len(t, recs, 2)
		_, err = cons.Poll(context.Background())
		require.ErrorIs(t, err, ErrInvalidTimestamp)
	})

	t.Run("corrupt batch", func(t *testing.T) {
		fb, c := singleBroker(t, map[string]int32{"t": 1})
		tp := TopicPartition{Topic: "t", Partition: 0}
		fb.onFetch = func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
			return &streamqv1.FetchResponse{Topics: []*streamqv1.FetchTopicResponse{{
				Topic: "t", Partitions: []*streamqv1.FetchPartitionResponse{{Partition: 0, Records: make([]byte, 100)}},
			}}}, nil
		}
		cons := newTestConsumer(t, c, ConsumerConfig{})
		cons.Assign([]TopicPartition{tp})
		require.NoError(t, cons.Seek(tp, 0))
		_, err := cons.Poll(context.Background())
		require.ErrorIs(t, err, ErrCorruptMessage)
	})

	t.Run("truncated tail ignored", func(t *testing.T) {
		fb, c := singleBroker(t, map[string]int32{"t": 1})
		tp := TopicPartition{Topic: "t", Partition: 0}
		consumerSeed(fb, tp, 6)
		fb.onFetch = func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
			fb.mu.Lock()
			defer fb.mu.Unlock()
			first := encode(fb.logs[tp][:3], 0)
			second := encode(fb.logs[tp][3:], 3)
			buf := append(append([]byte{}, first...), second[:len(second)-5]...)
			return &streamqv1.FetchResponse{Topics: []*streamqv1.FetchTopicResponse{{
				Topic: "t", Partitions: []*streamqv1.FetchPartitionResponse{{Partition: 0, Records: buf}},
			}}}, nil
		}
		cons := newTestConsumer(t, c, ConsumerConfig{})
		cons.Assign([]TopicPartition{tp})
		require.NoError(t, cons.Seek(tp, 0))
		recs, err := cons.Poll(context.Background())
		require.NoError(t, err)
		require.Len(t, recs, 3)
		pos, _ := cons.Position(tp)
		require.Equal(t, int64(3), pos)
	})
}

func TestConsumerSeekAndPosition(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 2})
	p0, p1 := TopicPartition{Topic: "t", Partition: 0}, TopicPartition{Topic: "t", Partition: 1}
	consumerSeed(fb, p0, 4)
	consumerSeed(fb, p1, 8)
	var lo []*streamqv1.ListOffsetsRequest
	fb.onListOffsets = func(req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		lo = append(lo, req)
		return nil, nil
	}
	cons := newTestConsumer(t, c, ConsumerConfig{})
	ctx := context.Background()

	other := TopicPartition{Topic: "t", Partition: 5}
	require.ErrorIs(t, cons.Seek(p0, 1), errConsumerNotAssigned)
	cons.Assign([]TopicPartition{p0, p1, p0})
	require.Equal(t, []TopicPartition{p0, p1}, cons.Assignment())
	require.ErrorIs(t, cons.Seek(other, 1), errConsumerNotAssigned)
	require.ErrorIs(t, cons.SeekToEnd(ctx, other), errConsumerNotAssigned)
	require.Error(t, cons.Seek(p0, -1))
	_, ok := cons.Position(other)
	require.False(t, ok)

	require.NoError(t, cons.Seek(p0, 2))
	pos, ok := cons.Position(p0)
	require.True(t, ok)
	require.Equal(t, int64(2), pos)

	require.NoError(t, cons.SeekToEnd(ctx))
	require.Len(t, lo, 1)
	for _, p := range lo[0].GetTopics()[0].GetPartitions() {
		require.Equal(t, int64(-1), p.GetTimestamp())
	}
	pos, _ = cons.Position(p0)
	require.Equal(t, int64(4), pos)
	pos, _ = cons.Position(p1)
	require.Equal(t, int64(8), pos)

	require.NoError(t, cons.SeekToBeginning(ctx, p1))
	require.Len(t, lo, 2)
	require.Equal(t, int64(-2), lo[1].GetTopics()[0].GetPartitions()[0].GetTimestamp())
	pos, _ = cons.Position(p1)
	require.Zero(t, pos)
	pos, _ = cons.Position(p0)
	require.Equal(t, int64(4), pos)

	// Re-assigning keeps positions of partitions still assigned and drops the others.
	cons.Assign([]TopicPartition{p1})
	pos, ok = cons.Position(p1)
	require.True(t, ok)
	require.Zero(t, pos)
	_, ok = cons.Position(p0)
	require.False(t, ok)
	cons.Assign([]TopicPartition{p0, p1})
	_, ok = cons.Position(p0)
	require.False(t, ok)

	// A seek that cannot resolve reports the error and leaves positions unchanged.
	fb.onListOffsets = func(*streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		return &streamqv1.ListOffsetsResponse{Topics: []*streamqv1.ListOffsetsTopicResponse{{
			Topic: "t", Partitions: []*streamqv1.ListOffsetsPartitionResponse{{Partition: 1, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP}},
		}}}, nil
	}
	require.ErrorIs(t, cons.SeekToEnd(ctx, p1), ErrInvalidTimestamp)
	pos, _ = cons.Position(p1)
	require.Zero(t, pos)
}

func TestConsumerSeekRetriesRetriableErrors(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	tp := TopicPartition{Topic: "t", Partition: 0}
	consumerSeed(fb, tp, 3)
	var calls atomic.Int32
	fb.onListOffsets = func(*streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		if calls.Add(1) == 1 {
			return &streamqv1.ListOffsetsResponse{Topics: []*streamqv1.ListOffsetsTopicResponse{{
				Topic: "t", Partitions: []*streamqv1.ListOffsetsPartitionResponse{{Partition: 0, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER}},
			}}}, nil
		}
		return nil, nil
	}
	cons := newTestConsumer(t, c, ConsumerConfig{})
	cons.Assign([]TopicPartition{tp})
	require.NoError(t, cons.SeekToEnd(context.Background()))
	pos, _ := cons.Position(tp)
	require.Equal(t, int64(3), pos)
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 2, fb.count("Metadata"))
}

func TestConsumerClose(t *testing.T) {
	_, c := singleBroker(t, map[string]int32{"t": 1})
	cons, err := NewConsumer(c, ConsumerConfig{})
	require.NoError(t, err)
	tp := TopicPartition{Topic: "t", Partition: 0}
	cons.Assign([]TopicPartition{tp})
	require.NoError(t, cons.Close())
	require.NoError(t, cons.Close())
	_, err = cons.Poll(context.Background())
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, cons.Seek(tp, 0), ErrClosed)
	require.ErrorIs(t, cons.SeekToBeginning(context.Background()), ErrClosed)
}
