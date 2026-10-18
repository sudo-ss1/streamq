package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/bench/internal/hist"
	"github.com/sudo-ss1/streamq/pkg/client"
)

var errFake = errors.New("fake")

func TestRecorderWindowing(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	at := func(s float64) time.Time { return base.Add(time.Duration(s * float64(time.Second))) }
	win := Window{WarmupEnd: at(100), End: at(200)}
	type event struct {
		intended, ack float64
		acked         bool // false: never completed before Freeze
		err           error
	}
	tests := []struct {
		name          string
		events        []event
		wantMeasured  uint64
		wantSent      int64
		wantInWindow  int64 // throughput acks
		wantTimeouts  int64
		wantErrors    map[string]int64
		wantMaxLatSec float64
	}{
		{
			name:         "in window, acked in window",
			events:       []event{{intended: 150, ack: 151, acked: true}},
			wantMeasured: 1, wantSent: 1, wantInWindow: 1, wantMaxLatSec: 1,
		},
		{
			name:         "sent during warmup, acked in window: throughput only",
			events:       []event{{intended: 99.5, ack: 100.5, acked: true}},
			wantMeasured: 0, wantSent: 0, wantInWindow: 1,
		},
		{
			name: "stall starting in the window and ending after it: latency counts, throughput does not",
			events: []event{
				{intended: 190, ack: 260, acked: true},
				{intended: 195, ack: 260, acked: true},
				{intended: 199.999, ack: 260, acked: true},
			},
			wantMeasured: 3, wantSent: 3, wantInWindow: 0, wantMaxLatSec: 70,
		},
		{
			name:         "intended at window end is excluded",
			events:       []event{{intended: 200, ack: 200.1, acked: true}},
			wantMeasured: 0, wantSent: 0, wantInWindow: 0,
		},
		{
			name:         "intended at warmup end is included",
			events:       []event{{intended: 100, ack: 100.25, acked: true}},
			wantMeasured: 1, wantSent: 1, wantInWindow: 1, wantMaxLatSec: 0.25,
		},
		{
			name:         "never acked: timeout",
			events:       []event{{intended: 150}, {intended: 160, ack: 161, acked: true}},
			wantMeasured: 1, wantSent: 2, wantInWindow: 1, wantTimeouts: 1, wantMaxLatSec: 1,
		},
		{
			name: "errors are counted by kind, not as latency",
			events: []event{
				{intended: 150, ack: 151, acked: true, err: fmt.Errorf("x: %w", client.ErrNotLeader)},
				{intended: 150, ack: 151, acked: true, err: client.ErrNotEnoughReplicas},
				{intended: 150, ack: 151, acked: true, err: errFake},
				{intended: 50, ack: 51, acked: true, err: errFake}, // warmup error: not reported
			},
			wantSent:   3,
			wantErrors: map[string]int64{"not_leader": 1, "not_enough_replicas": 1, "other": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRecorder(win)
			for _, e := range tc.events {
				tok := r.Sent(at(e.intended), at(e.intended), 100)
				if e.acked {
					r.Done(tok, at(e.ack), e.err)
				}
			}
			tot := Merge([]*Recorder{r})
			require.Equal(t, tc.wantMeasured, tot.Latency.Count())
			require.Equal(t, tc.wantSent, tot.Sent)
			require.Equal(t, tc.wantInWindow, tot.AckedInWindow)
			require.Equal(t, tc.wantInWindow*100, tot.BytesInWindow)
			require.Equal(t, tc.wantTimeouts, tot.Timeouts)
			if tc.wantErrors == nil {
				tc.wantErrors = map[string]int64{}
			}
			require.Equal(t, tc.wantErrors, tot.Errors)
			maxLat := time.Duration(tc.wantMaxLatSec * float64(time.Second))
			require.Equal(t, int64(maxLat), tot.Latency.Max())
		})
	}
}

func TestRecorderFreezeIgnoresLateAcks(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	win := Window{WarmupEnd: base, End: base.Add(time.Minute)}
	r := NewRecorder(win)
	tok := r.Sent(base.Add(time.Second), base.Add(time.Second), 10)
	r.Freeze()
	r.Done(tok, base.Add(2*time.Second), nil) // after the drain: ignored
	tot := Merge([]*Recorder{r})
	require.Equal(t, int64(1), tot.Timeouts)
	require.Equal(t, uint64(0), tot.Latency.Count())
	require.Equal(t, int64(0), tot.AckedInWindow)
}

func TestRecorderBehindSchedule(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	r := NewRecorder(Window{WarmupEnd: base, End: base.Add(time.Minute)})
	r.Sent(base, base.Add(behindTolerance), 1)                       // on tolerance: not behind
	r.Sent(base, base.Add(behindTolerance+time.Microsecond), 1)      // behind
	r.Sent(base.Add(-time.Second), base.Add(10*time.Millisecond), 1) // warmup: not counted
	require.Equal(t, int64(1), Merge([]*Recorder{r}).Behind)
}

func TestRecorderConcurrent(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	r := NewRecorder(Window{WarmupEnd: base, End: base.Add(time.Hour)})
	const goroutines, per = 8, 5000
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range per {
				sent := base.Add(time.Duration(g*per+i) * time.Microsecond)
				tok := r.Sent(sent, sent, 1)
				var err error
				if i%100 == 0 {
					err = client.ErrRequestTimedOut
				}
				r.Done(tok, sent.Add(time.Millisecond), err)
			}
		})
	}
	wg.Wait()
	tot := Merge([]*Recorder{r})
	require.Equal(t, int64(goroutines*per), tot.Sent)
	require.Equal(t, uint64(goroutines*per*99/100), tot.Latency.Count())
	require.Equal(t, int64(goroutines*per/100), tot.Errors["request_timed_out"])
	require.Equal(t, int64(0), tot.Timeouts)
}

func TestErrorKind(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{client.ErrNotLeader, "not_leader"},
		{fmt.Errorf("wrapped: %w", client.ErrProducerClosed), "producer_closed"},
		{context.Canceled, "canceled"},
		{context.DeadlineExceeded, "deadline_exceeded"},
		{client.ErrOutOfOrderSequence, "out_of_order_sequence"},
		{errFake, "other"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, errorKind(tc.err), "%v", tc.err)
	}
}

// stallSender is a fake producer: its first Produce blocks until released, every call acks
// synchronously at the clock's current time.
type stallSender struct {
	clk      Clock
	calls    atomic.Int64
	entered  chan struct{}
	release  chan struct{}
	stallOne sync.Once
}

func (s *stallSender) Produce(_ context.Context, _ client.Record, cb func(client.RecordMetadata, error)) error {
	s.calls.Add(1)
	s.stallOne.Do(func() {
		close(s.entered)
		<-s.release
	})
	cb(client.RecordMetadata{}, nil)
	return nil
}

// TestStalledProduceShowsInIntendedLatency is the coordinated-omission check (M10 plan R3): a
// Produce call that stalls for 10 ms delays the records scheduled behind it, and their latency
// from the intended send time shows it while their service time does not.
func TestStalledProduceShowsInIntendedLatency(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	win := Window{WarmupEnd: t0, End: t0.Add(time.Hour)}
	rec := NewRecorder(win)
	s := &stallSender{clk: clk, entered: make(chan struct{}), release: make(chan struct{})}
	w := &worker{topic: "t", p: s, clk: clk, bucket: NewTokenBucket(clk, 1000, t0), rec: rec,
		pay: newPayloads(0, 16, 0, 1, false), end: win.End}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run(ctx)
	}()
	<-s.entered
	clk.Advance(10 * time.Millisecond)
	close(s.release)
	// Records 1..10 were due during the stall and go out at once; record 11 waits for its slot.
	require.Eventually(t, func() bool { return s.calls.Load() == 11 && clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	<-done

	tot := Merge([]*Recorder{rec})
	require.Equal(t, uint64(11), tot.Latency.Count())
	require.Equal(t, int64(10*time.Millisecond), tot.Latency.Max())
	require.InEpsilon(t, int64(5*time.Millisecond), tot.Latency.Quantile(0.5), hist.MaxRelativeError, "latencies are 10, 9, ..., 0 ms")
	require.Equal(t, int64(0), tot.Service.Quantile(0.5), "only the stalled call itself was slow")
	require.Equal(t, int64(10*time.Millisecond), tot.Service.Max())
	require.Equal(t, int64(8), tot.Behind, "records 1..8 started more than 1 ms late")
}

func TestWorkerUnpacedStopsAtWindowEnd(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	win := Window{WarmupEnd: t0, End: t0.Add(time.Second)}
	rec := NewRecorder(win)
	var calls atomic.Int64
	p := senderFunc(func(_ context.Context, _ client.Record, cb func(client.RecordMetadata, error)) error {
		if calls.Add(1) == 5 {
			clk.Advance(time.Second) // the window ends during the fifth call
		}
		cb(client.RecordMetadata{}, nil)
		return nil
	})
	w := &worker{topic: "t", p: p, clk: clk, rec: rec, pay: newPayloads(0, 16, 0, 1, false), end: win.End}
	w.run(context.Background())
	require.Equal(t, int64(5), calls.Load())
	tot := Merge([]*Recorder{rec})
	require.Equal(t, int64(5), tot.Sent)
	require.Equal(t, int64(4), tot.AckedInWindow, "the fifth ack arrived at the window end")
}

func TestWorkerRejectedProduce(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	win := Window{WarmupEnd: t0, End: t0.Add(time.Second)}
	rec := NewRecorder(win)
	var calls atomic.Int64
	p := senderFunc(func(context.Context, client.Record, func(client.RecordMetadata, error)) error {
		if calls.Add(1) == 3 {
			clk.Advance(time.Second)
		}
		return client.ErrMessageTooLarge // not accepted: the callback never runs
	})
	w := &worker{topic: "t", p: p, clk: clk, rec: rec, pay: newPayloads(0, 16, 0, 1, false), end: win.End}
	w.run(context.Background())
	tot := Merge([]*Recorder{rec})
	require.Equal(t, map[string]int64{"message_too_large": 3}, tot.Errors)
	require.Equal(t, int64(0), tot.Timeouts)
}

type senderFunc func(context.Context, client.Record, func(client.RecordMetadata, error)) error

func (f senderFunc) Produce(ctx context.Context, rec client.Record, cb func(client.RecordMetadata, error)) error {
	return f(ctx, rec, cb)
}

func TestPayloads(t *testing.T) {
	ts := time.Unix(1_700_000_000, 123456789)
	tests := []struct {
		name  string
		card  int
		stamp bool
	}{
		{"unkeyed", 0, false},
		{"keyed", 7, false},
		{"stamped", 3, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newPayloads(1, 32, tc.card, 2, tc.stamp)
			b := newPayloads(1, 32, tc.card, 2, tc.stamp)
			keys := map[string]bool{}
			for seq := range uint64(50) {
				ka, va := a.next(1, seq, ts)
				kb, vb := b.next(1, seq, ts)
				require.Equal(t, ka, kb, "deterministic keys")
				require.Equal(t, va, vb, "deterministic values")
				require.Len(t, va, 32)
				if tc.card == 0 {
					require.Nil(t, ka)
				} else {
					keys[string(ka)] = true
				}
				got, ok := stampOf(va)
				require.True(t, ok)
				if tc.stamp {
					require.True(t, ts.Equal(got))
				}
			}
			require.Len(t, keys, tc.card)
		})
	}
	_, ok := stampOf([]byte{1, 2})
	require.False(t, ok)
}
