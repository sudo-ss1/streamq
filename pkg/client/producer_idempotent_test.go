package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// sentBatch is the idempotence header of one Produce request's batch.
type sentBatch struct {
	pid int64
	seq int32
	n   int32
}

func sentBatches(t *testing.T, fb *fakeBroker) []sentBatch {
	t.Helper()
	fb.mu.Lock()
	defer fb.mu.Unlock()
	var out []sentBatch
	for _, req := range fb.produceReqs {
		b, err := record.Validate(req.GetTopics()[0].GetPartitions()[0].GetRecords(), 1<<30)
		require.NoError(t, err)
		out = append(out, sentBatch{b.ProducerID(), b.BaseSequence(), b.RecordCount()})
	}
	return out
}

// TestIdempotentRetryAfterLostResponse: the broker appends but the response is lost; the retry
// carries the same producer ID and sequence, so the broker answers with the original offset and
// each record is stored and acknowledged exactly once.
func TestIdempotentRetryAfterLostResponse(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	var mu sync.Mutex
	lost := 0
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if lost < 2 {
			lost++
			fb.produce(req)                                              // appended...
			return nil, status.Error(codes.Unavailable, "response lost") // ...but never acknowledged
		}
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Millisecond})
	var cl collector
	for i := range 3 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte(fmt.Sprint(i))}, cl.cb())
		require.NoError(t, p.Flush(context.Background()))
	}
	cl.wait(t)
	for i, err := range cl.errs {
		require.NoError(t, err)
		require.Equal(t, int64(i), cl.mds[i].Offset, "the original offset is reported")
	}
	require.Len(t, fb.records(TopicPartition{Topic: "t"}), 3, "no duplicates")
	sent := sentBatches(t, fb)
	require.Equal(t, sentBatch{101, 0, 1}, sent[0])
	require.Equal(t, sent[0], sent[1], "the retry resends the same identity")
	require.Equal(t, 1, fb.count("InitProducer"))
}

func TestIdempotentFailureReplacesProducerID(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	scriptedErrors(fb, codeResp(streamqv1.ErrorCode_ERROR_CODE_OUT_OF_ORDER_SEQUENCE))
	p, _ := newTestProducer(t, c, ProducerConfig{})
	_, err := p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("a")})
	require.ErrorIs(t, err, ErrOutOfOrderSequence, "not retried")
	_, err = p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("b")})
	require.NoError(t, err)
	sent := sentBatches(t, fb)
	require.Equal(t, []sentBatch{{101, 0, 1}, {102, 0, 1}}, sent, "a new producer ID restarts at sequence 0")
	require.Equal(t, 2, fb.count("InitProducer"))
}

func TestIdempotentSplitReusesSequence(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	fb.onProduce = func(req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		if len(req.GetTopics()[0].GetPartitions()[0].GetRecords()) > 2048 {
			return codeResp(streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE)()
		}
		return nil, nil
	}
	p, _ := newTestProducer(t, c, ProducerConfig{Linger: time.Hour, BatchSize: 1 << 20})
	var cl collector
	for i := range 6 {
		produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte(fmt.Sprintf("%02d%s", i, strings.Repeat("x", 500)))}, cl.cb())
	}
	require.NoError(t, p.Flush(context.Background()))
	cl.wait(t)
	var accepted []sentBatch
	for _, b := range sentBatches(t, fb) {
		if b.n <= 3 {
			accepted = append(accepted, b)
		}
	}
	require.Equal(t, []sentBatch{{101, 0, 3}, {101, 3, 3}}, accepted, "halves continue from the rejected batch's sequence")
	require.Len(t, fb.records(TopicPartition{Topic: "t"}), 6)
}

func TestIdempotenceOnlyWithAcksAll(t *testing.T) {
	tests := []struct {
		name string
		cfg  ProducerConfig
	}{
		{"disabled", ProducerConfig{DisableIdempotence: true}},
		{"acks=1", ProducerConfig{Acks: AcksLeader}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fb, c := singleBroker(t, map[string]int32{"t": 1})
			p, _ := newTestProducer(t, c, tt.cfg)
			_, err := p.ProduceSync(context.Background(), Record{Topic: "t", UsePartition: true, Value: []byte("a")})
			require.NoError(t, err)
			require.Equal(t, []sentBatch{{record.NoProducerID, record.NoSequence, 1}}, sentBatches(t, fb))
			require.Zero(t, fb.count("InitProducer"))
		})
	}
}

// TestIdempotentDeliveryTimeoutReplacesProducerID: a batch that timed out may or may not have been
// appended, so the next batch starts under a new producer ID at sequence 0 (D48).
func TestIdempotentDeliveryTimeoutReplacesProducerID(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	c.cfg.RequestTimeout = time.Second
	var mu sync.Mutex
	failing := true
	fb.onProduce = func(*streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return codeResp(streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS)()
		}
		return nil, nil
	}
	p, clk := newFakeClockProducer(t, c, ProducerConfig{DeliveryTimeout: 10 * time.Second, Linger: time.Millisecond})
	var first collector
	produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte("a")}, first.cb())
	clk.Advance(time.Millisecond)
	require.Eventually(t, func() bool { return fb.count("Produce") >= 2 }, 10*time.Second, time.Millisecond)
	clk.Advance(10 * time.Second)
	first.wait(t)
	require.ErrorContains(t, first.errs[0], "delivery timeout")

	mu.Lock()
	failing = false
	mu.Unlock()
	var second collector
	produceOK(t, p, Record{Topic: "t", UsePartition: true, Value: []byte("b")}, second.cb())
	clk.Advance(time.Millisecond)
	second.wait(t)
	require.NoError(t, second.errs[0])
	sent := sentBatches(t, fb)
	require.Equal(t, sentBatch{101, 0, 1}, sent[0])
	require.Equal(t, sentBatch{102, 0, 1}, sent[len(sent)-1], "new producer ID, sequence 0")
}

// TestReplacePIDIgnoresStaleGeneration: a failure of a batch sequenced under an older producer ID
// does not force yet another InitProducer (plan R12).
func TestReplacePIDIgnoresStaleGeneration(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	p, _ := newTestProducer(t, c, ProducerConfig{})
	pid1, gen1, err := p.producerID()
	require.NoError(t, err)
	p.replacePID(gen1)
	pid2, gen2, err := p.producerID()
	require.NoError(t, err)
	require.NotEqual(t, pid1, pid2)
	p.replacePID(gen1) // late failure under the old producer ID
	pid3, gen3, err := p.producerID()
	require.NoError(t, err)
	require.Equal(t, pid2, pid3)
	require.Equal(t, gen2, gen3)
	require.Equal(t, 2, fb.count("InitProducer"))
}
