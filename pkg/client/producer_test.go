package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/partitioner"
	"github.com/sudo-ss1/streamq/internal/record"
)

// fakeClock is a manual clock for producer tests. AfterFunc callbacks and timers fire only when
// the test calls Advance (or immediately for d <= 0).
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at      time.Time
	f       func()
	ch      chan time.Time
	stopped bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.UnixMilli(1_700_000_000_000)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) add(d time.Duration, f func(), ch chan time.Time) *fakeTimer {
	c.mu.Lock()
	t := &fakeTimer{at: c.now.Add(d), f: f, ch: ch}
	if d > 0 {
		c.timers = append(c.timers, t)
		c.mu.Unlock()
		return t
	}
	c.mu.Unlock()
	fire(t)
	return t
}

func fire(t *fakeTimer) {
	if t.f != nil {
		go t.f()
	} else {
		t.ch <- time.Time{}
	}
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) func() bool {
	t := c.add(d, f, nil)
	return func() bool { return c.stop(t) }
}

func (c *fakeClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	ch := make(chan time.Time, 1)
	t := c.add(d, nil, ch)
	return ch, func() bool { return c.stop(t) }
}

func (c *fakeClock) stop(t *fakeTimer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	return was
}

// Advance moves time forward and fires due timers.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	keep := c.timers[:0]
	for _, t := range c.timers {
		switch {
		case t.stopped:
		case !t.at.After(c.now):
			t.stopped = true
			due = append(due, t)
		default:
			keep = append(keep, t)
		}
	}
	c.timers = keep
	c.mu.Unlock()
	for _, t := range due {
		fire(t)
	}
}

// pendingTimers returns the number of armed, unstopped timers.
func (c *fakeClock) pendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, t := range c.timers {
		if !t.stopped {
			n++
		}
	}
	return n
}

// newTestProducer returns a producer on the real clock (tests that need to control linger or
// delivery deadlines use newFakeClockProducer). An unset DeliveryTimeout defaults to 24h so tests
// may use long Lingers; tests that shorten DeliveryTimeout must also shorten the client's
// RequestTimeout (plan review M5).
func newTestProducer(t *testing.T, c *Client, cfg ProducerConfig) (*Producer, *fakeClock) {
	t.Helper()
	return newProducerWithClock(t, c, cfg, realClock{}), nil
}

func newFakeClockProducer(t *testing.T, c *Client, cfg ProducerConfig) (*Producer, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	return newProducerWithClock(t, c, cfg, clk), clk
}

func newProducerWithClock(t *testing.T, c *Client, cfg ProducerConfig, clk clock) *Producer {
	t.Helper()
	if cfg.DeliveryTimeout == 0 {
		cfg.DeliveryTimeout = 24 * time.Hour
	}
	p, err := newProducer(c, cfg, clk)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	return p
}

// collector gathers callback results.
type collector struct {
	mu   sync.Mutex
	mds  []RecordMetadata
	errs []error
	wg   sync.WaitGroup
}

func (cl *collector) cb() func(RecordMetadata, error) {
	cl.wg.Add(1)
	return func(md RecordMetadata, err error) {
		cl.mu.Lock()
		cl.mds = append(cl.mds, md)
		cl.errs = append(cl.errs, err)
		cl.mu.Unlock()
		cl.wg.Done()
	}
}

func (cl *collector) wait(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() { cl.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("callbacks did not complete")
	}
}

func produceOK(t *testing.T, p *Producer, rec Record, cb func(RecordMetadata, error)) {
	t.Helper()
	require.NoError(t, p.Produce(context.Background(), rec, cb))
}

func TestProducerKeyedRecordsUseMurmur2(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 5})
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour})
	var cl collector
	for i := range 50 {
		key := []byte(fmt.Sprintf("key-%d", i))
		produceOK(t, p, Record{Topic: "t", Key: key, Value: []byte("v")}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	for i, md := range cl.mds {
		require.NoError(t, cl.errs[i])
		_ = md
	}
	for part := range int32(5) {
		for _, r := range fb.records(TopicPartition{Topic: "t", Partition: part}) {
			require.Equal(t, part, partitioner.ForKey(r.Key, 5), "zero-value Partition must not force partition 0")
		}
	}
}

func TestProducerExplicitPartition(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 3})
	p, _ := newTestProducer(t, c, ProducerConfig{})
	md, err := p.ProduceSync(context.Background(), Record{Topic: "t", Partition: 2, UsePartition: true, Key: []byte("k"), Value: []byte("v")})
	require.NoError(t, err)
	require.Equal(t, int32(2), md.Partition)
	require.Len(t, fb.records(TopicPartition{Topic: "t", Partition: 2}), 1)
	err = p.Produce(context.Background(), Record{Topic: "t", Partition: 3, UsePartition: true}, nil)
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
}

// seqPartitioner makes sticky choices deterministic: 0, 1, 2, ...
func seqPartitioner() *DefaultPartitioner {
	d := NewDefaultPartitioner()
	var next int32
	d.intN = func(n int32) int32 { v := next % n; next++; return v }
	return d
}

func TestProducerStickyPartitioner(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 3})
	// Batches of ~4 records: every record of a batch shares a partition, the next batch moves.
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 300, Partitioner: seqPartitioner()})
	var cl collector
	for range 12 {
		produceOK(t, p, Record{Topic: "t", Value: []byte(strings.Repeat("v", 40))}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	fb.mu.Lock()
	reqs := fb.produceReqs
	fb.mu.Unlock()
	require.GreaterOrEqual(t, len(reqs), 3)
	var parts []int32
	for _, r := range reqs {
		pp := r.GetTopics()[0].GetPartitions()[0]
		b, err := record.Validate(pp.GetRecords(), 1<<20)
		require.NoError(t, err)
		require.Greater(t, b.RecordCount(), int32(1), "unkeyed records are batched together")
		parts = append(parts, pp.GetPartition())
	}
	for i := 1; i < len(parts); i++ {
		require.NotEqual(t, parts[i-1], parts[i], "the sticky partition moves after each batch")
	}
}

func TestProducerBatchSizeSealsAndOrder(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 1024})
	var cl collector
	for i := range 100 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte(fmt.Sprintf("%03d-%s", i, strings.Repeat("x", 30)))}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	fb.mu.Lock()
	reqs := len(fb.produceReqs)
	for _, r := range fb.produceReqs {
		require.LessOrEqual(t, len(r.GetTopics()[0].GetPartitions()[0].GetRecords()), 1024)
	}
	fb.mu.Unlock()
	require.Greater(t, reqs, 3)
	for i, md := range cl.mds {
		require.NoError(t, cl.errs[i])
		require.Equal(t, int64(i), md.Offset, "callbacks run in record order with their offsets")
	}
	recs := fb.records(TopicPartition{Topic: "t"})
	for i, r := range recs {
		require.True(t, strings.HasPrefix(string(r.Value), fmt.Sprintf("%03d-", i)))
	}
}

func TestProducerLinger(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	p, clk := newFakeClockProducer(t, c, ProducerConfig{Linger: 50 * time.Millisecond, BatchSize: 1 << 20})
	var cl collector
	for range 3 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte("v")}, cl.cb())
	}
	require.Equal(t, 1, clk.pendingTimers(), "one linger timer for the open batch")
	require.Equal(t, 0, fb.count("Produce"), "nothing sent before linger expires")
	clk.Advance(49 * time.Millisecond)
	require.Equal(t, 0, fb.count("Produce"))
	clk.Advance(time.Millisecond)
	cl.wait(t)
	require.Equal(t, 1, fb.count("Produce"), "sent as one batch without Flush")
	require.Len(t, fb.records(TopicPartition{Topic: "t"}), 3)
}

// scriptedErrors makes the fake's Produce fail with the given results before falling through.
func scriptedErrors(fb *fakeBroker, results ...func() (*streamqv1.ProduceResponse, error)) {
	var mu sync.Mutex
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if len(results) == 0 {
			return nil, nil
		}
		r := results[0]
		results = results[1:]
		return r()
	}
}

func codeResp(code streamqv1.ErrorCode) func() (*streamqv1.ProduceResponse, error) {
	return func() (*streamqv1.ProduceResponse, error) {
		return &streamqv1.ProduceResponse{Topics: []*streamqv1.ProduceTopicResponse{{Topic: "t",
			Partitions: []*streamqv1.ProducePartitionResponse{{Partition: 0, ErrorCode: code, BaseOffset: -1}}}}}, nil
	}
}

func grpcErr(code codes.Code) func() (*streamqv1.ProduceResponse, error) {
	return func() (*streamqv1.ProduceResponse, error) { return nil, status.Error(code, "injected") }
}

func TestProducerRetriesInjectedErrors(t *testing.T) {
	tests := []struct {
		name           string
		inject         []func() (*streamqv1.ProduceResponse, error)
		wantErr        error
		wantAttempts   int
		wantRefreshMin int // metadata RPCs beyond the first
	}{
		{"not leader twice", []func() (*streamqv1.ProduceResponse, error){codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER), codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER)}, nil, 3, 2},
		{"transport unavailable twice", []func() (*streamqv1.ProduceResponse, error){grpcErr(codes.Unavailable), grpcErr(codes.Unavailable)}, nil, 3, 2},
		{"not enough replicas", []func() (*streamqv1.ProduceResponse, error){codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS)}, nil, 2, 0},
		{"request timed out", []func() (*streamqv1.ProduceResponse, error){codeResp(streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT)}, nil, 2, 0},
		{"message too large is fatal", []func() (*streamqv1.ProduceResponse, error){codeResp(streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE)}, ErrMessageTooLarge, 1, 0},
		{"corrupt is fatal", []func() (*streamqv1.ProduceResponse, error){codeResp(streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE)}, ErrCorruptMessage, 1, 0},
		{"invalid argument is fatal", []func() (*streamqv1.ProduceResponse, error){grpcErr(codes.InvalidArgument)}, nil, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb, c := singleBroker(t, map[string]int32{"t": 1})
			scriptedErrors(fb, tt.inject...)
			p, _ := newTestProducer(t, c, ProducerConfig{})
			md, err := p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("v")})
			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.name == "invalid argument is fatal":
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			default:
				require.NoError(t, err)
				require.Equal(t, int64(0), md.Offset)
				require.Len(t, fb.records(TopicPartition{Topic: "t"}), 1, "acked exactly once")
			}
			require.Equal(t, tt.wantAttempts, fb.count("Produce"))
			require.GreaterOrEqual(t, fb.count("Metadata")-1, tt.wantRefreshMin)
		})
	}
}

func TestProducerFencedEpochRefreshesAndUsesNewEpoch(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	var epoch atomic.Int32
	fb.onMetadata = func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		return &streamqv1.MetadataResponse{
			Brokers: []*streamqv1.Broker{{Id: 1, Host: "b1", Port: 9092}},
			Topics: []*streamqv1.TopicMetadata{{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 0, LeaderId: 1, LeaderEpoch: epoch.Load()},
			}}},
		}, nil
	}
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		if req.GetTopics()[0].GetPartitions()[0].GetLeaderEpoch() < 7 {
			epoch.Store(7) // the "election" happened; metadata now reports epoch 7
			return codeResp(streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH)()
		}
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{})
	_, err := p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("v")})
	require.NoError(t, err)
	fb.mu.Lock()
	defer fb.mu.Unlock()
	require.Len(t, fb.produceReqs, 2)
	require.Equal(t, int32(0), fb.produceReqs[0].GetTopics()[0].GetPartitions()[0].GetLeaderEpoch())
	require.Equal(t, int32(7), fb.produceReqs[1].GetTopics()[0].GetPartitions()[0].GetLeaderEpoch())
}

func TestProducerRetriesNeverReorder(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	// The first batch fails twice; the second is queued behind it meanwhile.
	scriptedErrors(fb, codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER), codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER))
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 200})
	var cl collector
	for i := range 20 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Key: []byte("k"), Value: []byte(fmt.Sprintf("%02d%s", i, strings.Repeat("x", 30)))}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	recs := fb.records(TopicPartition{Topic: "t"})
	require.Len(t, recs, 20)
	for i, r := range recs {
		require.Equal(t, fmt.Sprintf("%02d", i), string(r.Value[:2]))
	}
}

func TestProducerDeliveryTimeout(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	c.cfg.RequestTimeout = time.Second
	fb.onProduce = func(*streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		return codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS)()
	}
	p, clk := newFakeClockProducer(t, c, ProducerConfig{DeliveryTimeout: 10 * time.Second, Linger: time.Millisecond})
	var cl collector
	produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte("v")}, cl.cb())
	clk.Advance(time.Millisecond) // linger
	require.Eventually(t, func() bool { return fb.count("Produce") >= 2 }, 10*time.Second, time.Millisecond)
	clk.Advance(10 * time.Second)
	cl.wait(t)
	require.ErrorIs(t, cl.errs[0], ErrNotEnoughReplicas, "fails with the last error")
	require.ErrorContains(t, cl.errs[0], "delivery timeout")
}

func TestProducerSplitsTooLargeBatches(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	// The broker enforces a 2 KiB limit the client does not know about.
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		if len(req.GetTopics()[0].GetPartitions()[0].GetRecords()) > 2048 {
			return codeResp(streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE)()
		}
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 1 << 20})
	var cl collector
	for i := range 10 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte(fmt.Sprintf("%02d%s", i, strings.Repeat("x", 500)))}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	for i, err := range cl.errs {
		require.NoError(t, err)
		require.Equal(t, int64(i), cl.mds[i].Offset)
	}
	recs := fb.records(TopicPartition{Topic: "t"})
	require.Len(t, recs, 10)
	for i, r := range recs {
		require.Equal(t, fmt.Sprintf("%02d", i), string(r.Value[:2]), "order kept across splits")
	}
	// The first split (5 records, ~2.6 KiB) is still too large, so splitting recursed.
	require.GreaterOrEqual(t, fb.count("Produce"), 7)

	// A single record over the limit fails.
	var big collector
	produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: make([]byte, 4096)}, big.cb())
	require.NoError(t, p.Flush(context.Background()))
	big.wait(t)
	require.ErrorIs(t, big.errs[0], ErrMessageTooLarge)
}

func TestProducerSealsAtTopicMaxMessageBytes(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	fb.onMetadata = func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		return &streamqv1.MetadataResponse{
			Brokers: []*streamqv1.Broker{{Id: 1, Host: "b1", Port: 9092}},
			Topics: []*streamqv1.TopicMetadata{{Name: "t", Configs: map[string]string{"max_message_bytes": "1500"},
				Partitions: []*streamqv1.PartitionMetadata{{Partition: 0, LeaderId: 1}}}},
		}, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 1 << 20})
	var cl collector
	for range 10 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: make([]byte, 500)}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	fb.mu.Lock()
	defer fb.mu.Unlock()
	for _, r := range fb.produceReqs {
		require.LessOrEqual(t, len(r.GetTopics()[0].GetPartitions()[0].GetRecords()), 1500)
	}
}

func TestProducerCloseFlushes(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 4})
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour})
	var cl collector
	for i := range 40 {
		produceOK(t, p, Record{Topic: "t", Key: []byte(fmt.Sprint(i)), Value: []byte("v")}, cl.cb())
	}
	require.Equal(t, 0, fb.count("Produce"), "everything is still lingering")
	require.NoError(t, p.Close(context.Background()))
	cl.mu.Lock()
	require.Len(t, cl.errs, 40, "every callback ran before Close returned")
	for _, err := range cl.errs {
		require.NoError(t, err)
	}
	cl.mu.Unlock()
	total := 0
	for part := range int32(4) {
		total += len(fb.records(TopicPartition{Topic: "t", Partition: part}))
	}
	require.Equal(t, 40, total)
	require.ErrorIs(t, p.Produce(context.Background(), Record{Topic: "t", Value: []byte("late")}, nil), ErrProducerClosed)
	require.NoError(t, p.Close(context.Background()), "idempotent")
}

func TestProducerCloseWithExpiredContextFailsPending(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	release := make(chan struct{})
	fb.onProduce = func(*streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		<-release
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 100})
	var cl collector
	for range 5 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: make([]byte, 80)}, cl.cb())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.Close(ctx), context.Canceled)
	close(release)
	cl.mu.Lock()
	defer cl.mu.Unlock()
	require.Len(t, cl.errs, 5)
	for _, err := range cl.errs {
		require.ErrorIs(t, err, ErrProducerClosed)
	}
}

func TestProducerBufferMemoryBlocks(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	release := make(chan struct{})
	fb.onProduce = func(*streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		<-release
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 100, BufferMemory: 1000, MaxRequestSize: 900})
	rec := Record{Topic: "t", UsePartition: true, Value: make([]byte, 400)}
	produceOK(t, p, rec, nil)
	produceOK(t, p, rec, nil)
	// The buffer is full: Produce blocks until ctx ends.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, p.Produce(ctx, rec, nil), context.DeadlineExceeded)
	// Close unblocks a blocked Produce with ErrProducerClosed.
	blocked := make(chan error, 1)
	go func() { blocked <- p.Produce(context.Background(), rec, nil) }()
	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close(context.Background()) }()
	require.ErrorIs(t, <-blocked, ErrProducerClosed)
	close(release)
	require.NoError(t, <-closeDone)
}

func TestProducerUnknownTopic(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{})
	p, _ := newTestProducer(t, c, ProducerConfig{MaxBlock: 100 * time.Millisecond})
	err := p.Produce(context.Background(), Record{Topic: "later", Value: []byte("v")}, nil)
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)

	// The topic appears while Produce waits for it.
	p2, _ := newTestProducer(t, c, ProducerConfig{MaxBlock: time.Minute})
	go func() {
		require.Eventually(t, func() bool { return fb.count("Metadata") > 3 }, 10*time.Second, time.Millisecond)
		fb.mu.Lock()
		fb.topics["later"] = 1
		fb.mu.Unlock()
	}()
	_, err = p2.ProduceSync(context.Background(), Record{Topic: "later", Value: []byte("v")})
	require.NoError(t, err)
}

func TestProducerRejectsInvalidRecords(t *testing.T) {
	_, c := singleBroker(t, map[string]int32{"t": 1})
	p, _ := newTestProducer(t, c, ProducerConfig{MaxRequestSize: 1000})
	ctx := context.Background()
	require.Error(t, p.Produce(ctx, Record{Value: []byte("no topic")}, nil))
	require.ErrorIs(t, p.Produce(ctx, Record{Topic: "t", Timestamp: time.UnixMilli(-5)}, nil), ErrInvalidTimestamp)
	require.ErrorIs(t, p.Produce(ctx, Record{Topic: "t", Value: make([]byte, 2000)}, nil), ErrMessageTooLarge)
	_, err := NewProducer(c, ProducerConfig{DeliveryTimeout: time.Second})
	require.Error(t, err, "delivery timeout below linger + request timeout")
	_, err = NewProducer(c, ProducerConfig{Linger: time.Hour})
	require.Error(t, err, "default delivery timeout below linger + request timeout")
}

func TestProducerAcksNone(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		require.Equal(t, streamqv1.Acks_ACKS_NONE, req.GetAcks())
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Acks: AcksNone})
	md, err := p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("v")})
	require.NoError(t, err)
	require.Equal(t, int64(-1), md.Offset)
}

func TestProducerConcurrentProduceAndClose(t *testing.T) {
	_, c := singleBroker(t, map[string]int32{"t": 4})
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Millisecond, BatchSize: 500})
	var accepted, called atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range 200 {
				err := p.Produce(context.Background(), Record{Topic: "t", Key: []byte(fmt.Sprint(g, i)), Value: []byte("v")},
					func(RecordMetadata, error) { called.Add(1) })
				if err == nil {
					accepted.Add(1)
				}
			}
		}()
	}
	close(start)
	require.NoError(t, p.Close(context.Background()))
	wg.Wait()
	require.Equal(t, accepted.Load(), called.Load(), "every accepted record's callback ran exactly once")
}
