package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sudo-ss1/streamq/pkg/client"
	"github.com/sudo-ss1/streamq/test/linearizability"
)

// bulk workload parameters (plan M9 §2, R7, R25).
const (
	bulkTopic       = "bulk"
	bulkPartitions  = 6
	bulkKeys        = 24
	bulkInterval    = 4 * time.Millisecond // one record per tick: about 250 records/s
	bulkMaxPending  = 4000                 // stop producing while this many records are in flight
	bulkSentinels   = 300                  // bounded sentinel attempts per partition
	bulkCloseWait   = 90 * time.Second     // Flush + Close budget
	bulkFinalWait   = 60 * time.Second     // bounded wait for each partition's final read
	bulkRPCTimeout  = 5 * time.Second      // client request timeout
	bulkDelivery    = 45 * time.Second     // producer delivery timeout
	bulkProduceWait = 5 * time.Second      // producer max block
)

// Bulk value outcomes.
const (
	bulkPending = iota
	bulkAcked
	bulkFailed
)

// bulkValue is one accepted record.
type bulkValue struct {
	key       string // "" for sentinels
	seq       int
	status    int
	partition int32
	offset    int64
}

// bulkWorkload produces keyed sequence values "<key>:<seq>" with the idempotent producer
// (acks=all) and tracks every outcome.
type bulkWorkload struct {
	cl       *client.Client
	producer *client.Producer
	cancel   context.CancelFunc
	done     chan struct{}

	mu       sync.Mutex
	values   map[string]*bulkValue
	rejected int
	pending  int
	errKinds map[string]int
	acks     []ackEvent
	start    time.Time
	closed   bool // set at the end of settle: later callbacks no longer change outcomes
}

func startBulk(parent context.Context) (*bulkWorkload, error) {
	cl, err := client.New(client.Config{Bootstrap: bootstrap(), RequestTimeout: bulkRPCTimeout})
	if err != nil {
		return nil, err
	}
	p, err := client.NewProducer(cl, client.ProducerConfig{Acks: client.AcksAll, DeliveryTimeout: bulkDelivery, MaxBlock: bulkProduceWait})
	if err != nil {
		_ = cl.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	w := &bulkWorkload{cl: cl, producer: p, cancel: cancel, done: make(chan struct{}), values: map[string]*bulkValue{},
		errKinds: map[string]int{}, start: time.Now()}
	go w.loop(ctx)
	return w, nil
}

// produce hands one record to the producer and tracks its outcome. It reports whether the
// record was accepted.
func (w *bulkWorkload) produce(ctx context.Context, rec client.Record, v *bulkValue) bool {
	value := string(rec.Value)
	w.mu.Lock()
	w.values[value] = v
	w.pending++
	w.mu.Unlock()
	err := w.producer.Produce(ctx, rec, func(md client.RecordMetadata, err error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.pending--
		if w.closed {
			return // too late: the outcome was already decided as failed (unsent at close)
		}
		if err != nil {
			v.status = bulkFailed
			w.errKinds[errKind(err)]++
			return
		}
		v.status, v.partition, v.offset = bulkAcked, md.Partition, md.Offset
		w.acks = append(w.acks, ackEvent{At: time.Since(w.start).Seconds(), Partition: md.Partition})
	})
	if err != nil {
		w.mu.Lock()
		delete(w.values, value)
		w.pending--
		w.rejected++
		w.errKinds["rejected: "+errKind(err)]++
		w.mu.Unlock()
		return false
	}
	return true
}

// errKind is a short, stable label for an error.
func errKind(err error) string {
	for _, e := range []error{client.ErrNotLeader, client.ErrFencedLeaderEpoch, client.ErrNotEnoughReplicas,
		client.ErrRequestTimedOut, client.ErrUnknownTopicOrPartition, client.ErrOutOfOrderSequence,
		client.ErrUnknownProducerID, client.ErrProducerClosed, client.ErrUnknownServerError,
		context.DeadlineExceeded, context.Canceled} {
		if errors.Is(err, e) {
			return e.Error()
		}
	}
	s := err.Error()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

// loop produces one keyed record per tick until ctx ends.
func (w *bulkWorkload) loop(ctx context.Context) {
	defer close(w.done)
	seqs := make([]int, bulkKeys)
	t := time.NewTicker(bulkInterval)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		w.mu.Lock()
		busy := w.pending >= bulkMaxPending
		w.mu.Unlock()
		if busy {
			continue
		}
		k := i % bulkKeys
		key := fmt.Sprintf("k%02d", k)
		seq := seqs[k]
		seqs[k]++
		value := fmt.Sprintf("%s:%d", key, seq)
		w.produce(ctx, client.Record{Topic: bulkTopic, Key: []byte(key), Value: []byte(value)}, &bulkValue{key: key, seq: seq, offset: -1})
	}
}

// stopProducing ends the produce loop (in-flight records keep being retried by the producer).
func (w *bulkWorkload) stopProducing() {
	w.cancel()
	<-w.done
}

// ackEvent is one acknowledged workload record: when (seconds since the workload started) and
// where.
type ackEvent struct {
	At        float64
	Partition int32
}

// ackEvents returns the workload's acknowledgements so far.
func (w *bulkWorkload) ackEvents() []ackEvent {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.acks)
}

// settle acknowledges a unique sentinel on every partition through the idempotent producer, then
// flushes and closes the producer, so every outcome is final before the final read (R16, R25).
// Records still unsent at close count as failed.
func (w *bulkWorkload) settle(ctx context.Context) (sentinels map[int32]int64, settleErr, closeErr string) {
	sentinels = map[int32]int64{}
	for p := range int32(bulkPartitions) {
		for k := 0; k < bulkSentinels && ctx.Err() == nil; k++ {
			value := fmt.Sprintf("sentinel-p%d-%d", p, k)
			v := &bulkValue{offset: -1}
			done := make(chan struct{})
			accepted := w.produceSync(ctx, client.Record{Topic: bulkTopic, Partition: p, UsePartition: true, Value: []byte(value)}, v, done)
			if accepted {
				<-done
			}
			w.mu.Lock()
			acked, off := v.status == bulkAcked, v.offset
			w.mu.Unlock()
			if acked {
				sentinels[p] = off
				break
			}
			wait(ctx, 200*time.Millisecond)
		}
		if _, ok := sentinels[p]; !ok {
			settleErr = fmt.Sprintf("no sentinel acknowledged on %s-%d", bulkTopic, p)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, bulkCloseWait)
	defer cancel()
	if err := w.producer.Flush(cctx); err != nil {
		closeErr = "flush: " + err.Error()
	}
	if err := w.producer.Close(cctx); err != nil {
		closeErr = strings.TrimPrefix(closeErr+"; close: "+err.Error(), "; ")
	}
	_ = w.cl.Close()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, v := range w.values {
		if v.status == bulkPending { // never completed: unsent (R25)
			v.status = bulkFailed
			w.errKinds["unsent at close"]++
		}
	}
	w.closed = true
	return sentinels, settleErr, closeErr
}

// produceSync produces one record and closes done when its callback ran.
func (w *bulkWorkload) produceSync(ctx context.Context, rec client.Record, v *bulkValue, done chan struct{}) bool {
	value := string(rec.Value)
	w.mu.Lock()
	w.values[value] = v
	w.pending++
	w.mu.Unlock()
	err := w.producer.Produce(ctx, rec, func(md client.RecordMetadata, err error) {
		defer close(done)
		w.mu.Lock()
		defer w.mu.Unlock()
		w.pending--
		if err != nil {
			v.status = bulkFailed
			return
		}
		v.status, v.partition, v.offset = bulkAcked, md.Partition, md.Offset
	})
	if err != nil {
		w.mu.Lock()
		delete(w.values, value)
		w.pending--
		w.rejected++
		w.mu.Unlock()
		return false
	}
	return true
}

// finalRead reads every bulk partition from its leader until the read covers the partition's
// sentinel and highest acknowledged offset (bounded). Anomalies are returned separately.
func (w *bulkWorkload) finalRead(ctx context.Context, cl *cluster, sentinels map[int32]int64) (map[int32][]string, []string, string) {
	need := map[int32]int64{}
	for p, off := range sentinels {
		need[p] = off
	}
	w.mu.Lock()
	for _, v := range w.values {
		if v.status == bulkAcked {
			need[v.partition] = max(need[v.partition], v.offset)
		}
	}
	w.mu.Unlock()
	out := map[int32][]string{}
	var anomalies []string
	var errs []string
	for p := range int32(bulkPartitions) {
		pctx, cancel := context.WithTimeout(ctx, bulkFinalWait)
		leader := cl.leaderFunc(bulkTopic, p)
		var last string
		for {
			api, err := leader(pctx)
			if err == nil {
				values, rerr := linearizability.ReadAll(pctx, api, bulkTopic, p)
				var anomaly *linearizability.Anomaly
				if errors.As(rerr, &anomaly) {
					anomalies = append(anomalies, fmt.Sprintf("%s-%d: %v", bulkTopic, p, anomaly))
					break
				}
				if rerr == nil {
					out[p] = values
					if int64(len(values)) > need[p] {
						break
					}
					last = fmt.Sprintf("read %d records, need offset %d", len(values), need[p])
				} else {
					last = rerr.Error()
				}
			} else {
				last = err.Error()
			}
			if !wait(pctx, 100*time.Millisecond) {
				errs = append(errs, fmt.Sprintf("%s-%d: final read incomplete within %v (%s)", bulkTopic, p, bulkFinalWait, last))
				break
			}
		}
		cancel()
	}
	return out, anomalies, strings.Join(errs, "; ")
}

// result checks the final logs against the tracked outcomes (R7).
func (w *bulkWorkload) result(final map[int32][]string) bulkResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	res := checkBulk(w.values, final)
	res.Rejected = w.rejected
	res.ErrorsByKind = map[string]int{}
	for k, n := range w.errKinds {
		res.ErrorsByKind[k] = n
	}
	return res
}

// checkBulk checks the bulk invariants (plan M9 R7, consistent with D48):
//   - every acked value appears exactly once, at its acknowledged partition and offset;
//   - acked values of a key appear in sequence order;
//   - a failed value appears at most once, anywhere (counted separately);
//   - nothing appears that was never accepted by the producer.
func checkBulk(values map[string]*bulkValue, final map[int32][]string) bulkResult {
	res := bulkResult{Produced: len(values)}
	type pos struct {
		partition int32
		offset    int64
	}
	seen := map[string][]pos{}
	for p, vs := range final {
		res.FinalRecords += len(vs)
		for o, v := range vs {
			seen[v] = append(seen[v], pos{p, int64(o)})
		}
	}
	for value, v := range values {
		switch v.status {
		case bulkAcked:
			res.Acked++
		default:
			res.Failed++
		}
		at := seen[value]
		switch {
		case len(at) > 1:
			res.Duplicates = append(res.Duplicates, value)
		case len(at) == 0 && v.status == bulkAcked:
			res.Lost = append(res.Lost, value)
		case len(at) == 1 && v.status == bulkAcked:
			res.AckedPresent++
			if at[0].partition != v.partition {
				res.WrongPartition = append(res.WrongPartition, value)
			} else if at[0].offset != v.offset {
				res.OffsetMismatch = append(res.OffsetMismatch, fmt.Sprintf("%s acked at %d-%d, found at %d", value, v.partition, v.offset, at[0].offset))
			}
		case len(at) == 1:
			res.FailedPresent++
		}
	}
	for value := range seen {
		if _, ok := values[value]; !ok {
			res.Phantoms = append(res.Phantoms, value)
		}
	}
	// Per-key order of acked values, in log order within each partition.
	for p, vs := range final {
		last := map[string]int{}
		for o, value := range vs {
			v, ok := values[value]
			if !ok || v.status != bulkAcked || v.key == "" {
				continue
			}
			if prev, ok := last[v.key]; ok && v.seq <= prev {
				res.OrderViolations = append(res.OrderViolations, fmt.Sprintf("%s at %d-%d after seq %d", value, p, o, prev))
			}
			last[v.key] = v.seq
		}
	}
	slices.Sort(res.Lost)
	slices.Sort(res.Duplicates)
	slices.Sort(res.Phantoms)
	return res
}
