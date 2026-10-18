package loadgen

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sudo-ss1/streamq/bench/internal/hist"
	"github.com/sudo-ss1/streamq/pkg/client"
)

// behindTolerance is how late a send may start after its intended time before it counts as
// behind schedule.
const behindTolerance = time.Millisecond

// Window is the measurement window [WarmupEnd, End).
type Window struct {
	WarmupEnd time.Time
	End       time.Time
}

// contains reports whether t is in the window.
func (w Window) contains(t time.Time) bool { return !t.Before(w.WarmupEnd) && t.Before(w.End) }

// sendToken describes one record handed to Produce.
type sendToken struct {
	intended time.Time // scheduled send time (the call time when unpaced)
	start    time.Time // when Produce was called
	inWindow bool      // intended time inside the window
	bytes    int64
}

// Recorder is one producer's measurement shard (M10 plan R4, R12). Windowing rules:
//   - latency (from the intended send time) and service time (from the Produce call) count every
//     record whose intended send time is inside the window, whenever it is acked;
//   - throughput counts successful acks whose ack time is inside the window;
//   - records with an in-window intended time still unacked when Freeze is called are timeouts.
//
// Lock ordering: mu (freeze gate) → errMu. Histogram shards and counters are atomic.
type Recorder struct {
	win Window

	latency *hist.Shard
	service *hist.Shard

	sent      atomic.Int64 // in-window records handed to Produce
	completed atomic.Int64 // in-window records completed (ack or error)
	acked     atomic.Int64 // in-window records acked successfully
	behind    atomic.Int64 // in-window sends that started later than intended + behindTolerance

	ackedInWindow atomic.Int64 // successful acks with the ack time inside the window
	bytesInWindow atomic.Int64

	mu     sync.RWMutex // Done holds it shared; Freeze exclusively
	frozen bool
	errMu  sync.Mutex
	errs   map[string]int64
}

// NewRecorder returns a recorder for win.
func NewRecorder(win Window) *Recorder {
	return &Recorder{win: win, latency: hist.NewShard(), service: hist.NewShard(), errs: map[string]int64{}}
}

// Sent registers a record about to be produced.
func (r *Recorder) Sent(intended, start time.Time, bytes int64) sendToken {
	tok := sendToken{intended: intended, start: start, inWindow: r.win.contains(intended), bytes: bytes}
	if tok.inWindow {
		r.sent.Add(1)
		if start.Sub(intended) > behindTolerance {
			r.behind.Add(1)
		}
	}
	return tok
}

// Done records the outcome of tok at time at. Calls after Freeze are ignored.
func (r *Recorder) Done(tok sendToken, at time.Time, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.frozen {
		return
	}
	if err == nil && r.win.contains(at) {
		r.ackedInWindow.Add(1)
		r.bytesInWindow.Add(tok.bytes)
	}
	if !tok.inWindow {
		return
	}
	r.completed.Add(1)
	if err != nil {
		r.errMu.Lock()
		r.errs[errorKind(err)]++
		r.errMu.Unlock()
		return
	}
	r.acked.Add(1)
	r.latency.Record(int64(at.Sub(tok.intended)))
	r.service.Record(int64(at.Sub(tok.start)))
}

// Freeze stops recording; outcomes arriving later are ignored.
func (r *Recorder) Freeze() {
	r.mu.Lock()
	r.frozen = true
	r.mu.Unlock()
}

// Totals are the merged counts of frozen recorders.
type Totals struct {
	Latency       hist.Snapshot
	Service       hist.Snapshot
	Sent          int64
	Acked         int64
	Timeouts      int64 // in-window records never completed
	Behind        int64
	AckedInWindow int64
	BytesInWindow int64
	Errors        map[string]int64
}

// Merge freezes every recorder and merges them.
func Merge(rs []*Recorder) Totals {
	t := Totals{Errors: map[string]int64{}}
	lat := make([]*hist.Shard, 0, len(rs))
	svc := make([]*hist.Shard, 0, len(rs))
	for _, r := range rs {
		r.Freeze()
		lat, svc = append(lat, r.latency), append(svc, r.service)
		t.Sent += r.sent.Load()
		t.Acked += r.acked.Load()
		t.Timeouts += r.sent.Load() - r.completed.Load()
		t.Behind += r.behind.Load()
		t.AckedInWindow += r.ackedInWindow.Load()
		t.BytesInWindow += r.bytesInWindow.Load()
		r.errMu.Lock()
		for k, v := range r.errs {
			t.Errors[k] += v
		}
		r.errMu.Unlock()
	}
	t.Latency, t.Service = hist.Merge(lat...), hist.Merge(svc...)
	return t
}

// errorKind classifies a produce error for the errors-by-kind report.
func errorKind(err error) string {
	kinds := []struct {
		target error
		name   string
	}{
		{client.ErrNotLeader, "not_leader"},
		{client.ErrFencedLeaderEpoch, "fenced_leader_epoch"},
		{client.ErrNotEnoughReplicas, "not_enough_replicas"},
		{client.ErrRequestTimedOut, "request_timed_out"},
		{client.ErrProducerClosed, "producer_closed"},
		{client.ErrMessageTooLarge, "message_too_large"},
		{client.ErrOutOfOrderSequence, "out_of_order_sequence"},
		{client.ErrUnknownProducerID, "unknown_producer_id"},
		{client.ErrUnknownTopicOrPartition, "unknown_topic_or_partition"},
		{client.ErrClosed, "client_closed"},
		{context.Canceled, "canceled"},
		{context.DeadlineExceeded, "deadline_exceeded"},
	}
	for _, k := range kinds {
		if errors.Is(err, k.target) {
			return k.name
		}
	}
	return "other"
}
