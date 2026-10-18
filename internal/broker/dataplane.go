package broker

import (
	"context"
	"reflect"
	"time"

	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// Request limits (DECISIONS D19).
const (
	MaxFetchWait  = 30 * time.Second
	MaxFetchBytes = 16 << 20
)

// ProduceRequest is one batch for one partition.
type ProduceRequest struct {
	TP          storage.TopicPartition
	LeaderEpoch int32
	Records     []byte
}

// ProduceResult is the outcome for one ProduceRequest.
type ProduceResult struct {
	TP             storage.TopicPartition
	Err            error
	BaseOffset     int64
	LogStartOffset int64
	LeaderID       int32
	LeaderEpoch    int32
}

// DefaultProduceTimeout bounds an acks=all wait when the request gives no timeout_ms (D43).
const DefaultProduceTimeout = 30 * time.Second

// produceDeadlineMargin keeps the acks=all wait below the RPC deadline, so the client gets
// REQUEST_TIMED_OUT rather than a transport deadline error (plan R13).
const produceDeadlineMargin = 100 * time.Millisecond

// Produce appends each batch to its partition as leader. Each request is independent; results are
// in request order. With acks=none the appends still run synchronously (DECISIONS: M2 plan R7).
// With acks=all every batch is appended first, then each waits until committed (the purgatory,
// SPEC §4.5) or until timeout (0 = DefaultProduceTimeout) bounded by ctx's deadline.
func (b *Broker) Produce(ctx context.Context, acks partition.Acks, timeout time.Duration, reqs []ProduceRequest) []ProduceResult {
	out := make([]ProduceResult, len(reqs))
	type waiter struct {
		p          *partition.Partition
		lastOffset int64
		epoch      int32
	}
	waits := make([]*waiter, len(reqs))
	for i, r := range reqs {
		res := ProduceResult{TP: r.TP, BaseOffset: -1, LeaderID: -1, LeaderEpoch: -1}
		p, ok := b.Partition(r.TP)
		if !ok {
			res.Err = ErrUnknownPartition
			out[i] = res
			continue
		}
		res.LeaderID, res.LeaderEpoch = p.Leader()
		info, epoch, err := p.Append(ctx, r.Records, r.LeaderEpoch, acks)
		if err != nil {
			res.Err = err
		} else {
			res.BaseOffset = info.FirstOffset
			if acks == partition.AcksAll {
				waits[i] = &waiter{p: p, lastOffset: info.LastOffset, epoch: epoch}
			}
		}
		res.LogStartOffset = p.Log().LogStartOffset()
		out[i] = res
	}
	if acks != partition.AcksAll {
		return out
	}
	if timeout <= 0 {
		timeout = DefaultProduceTimeout
	}
	deadline := b.clock.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Add(-produceDeadlineMargin).Before(deadline) {
		deadline = d.Add(-produceDeadlineMargin)
	}
	for i, w := range waits {
		if w == nil {
			continue
		}
		if err := w.p.WaitForCommit(ctx, w.lastOffset, w.epoch, max(deadline.Sub(b.clock.Now()), 0)); err != nil {
			out[i].Err = err
		}
	}
	return out
}

// FetchRequest is one partition of a consumer fetch.
type FetchRequest struct {
	TP          storage.TopicPartition
	LeaderEpoch int32
	Offset      int64
	MaxBytes    int // <= 0: no per-partition limit beyond the request budget
}

// FetchResult is the outcome for one FetchRequest.
type FetchResult struct {
	TP             storage.TopicPartition
	Err            error
	HighWatermark  int64
	LogStartOffset int64
	Records        []byte
	LeaderID       int32
	LeaderEpoch    int32
}

// Fetch is the consumer long-poll (SPEC §7.1): it returns as soon as at least minBytes are
// available below the high watermarks, any partition errors, or maxWait elapses. Each round
// captures every partition's HW-change channel before reading, then waits on all of them at once
// with reflect.Select, so an append between the read and the wait is never missed and no helper
// goroutines are needed. maxBytes bounds the response (the first batch of the first non-empty
// partition is always returned whole).
func (b *Broker) Fetch(ctx context.Context, maxWait time.Duration, minBytes, maxBytes int, reqs []FetchRequest) []FetchResult {
	maxWait = min(max(maxWait, 0), MaxFetchWait)
	if maxBytes <= 0 || maxBytes > MaxFetchBytes {
		maxBytes = MaxFetchBytes
	}
	timeout := b.clock.After(maxWait)
	final := maxWait == 0
	for {
		// Capture every wake-up channel before reading: HW advances, and partition
		// creation/deletion (so a fetch on a partition deleted meanwhile returns promptly).
		b.mu.RLock()
		chans := []<-chan struct{}{b.changed}
		b.mu.RUnlock()
		for _, r := range reqs {
			if p, ok := b.Partition(r.TP); ok {
				chans = append(chans, p.HWChanged())
			}
		}
		results, total, failed := b.readAll(ctx, maxBytes, reqs)
		if final || failed || total >= max(minBytes, 1) {
			return results
		}
		if b.afterFetchRead != nil {
			b.afterFetchRead()
		}
		cases := make([]reflect.SelectCase, 0, len(chans)+3)
		for _, ch := range chans {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
		}
		cases = append(cases,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeout)},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(b.done)},
		)
		chosen, _, _ := reflect.Select(cases)
		if chosen >= len(chans) {
			final = true // timed out, cancelled or closing: one last read, then return
		}
	}
}

// readAll reads every requested partition once within the byte budget.
func (b *Broker) readAll(ctx context.Context, budget int, reqs []FetchRequest) (results []FetchResult, total int, failed bool) {
	results = make([]FetchResult, len(reqs))
	for i, r := range reqs {
		res := FetchResult{TP: r.TP, HighWatermark: -1, LogStartOffset: -1, LeaderID: -1, LeaderEpoch: -1}
		p, ok := b.Partition(r.TP)
		if !ok {
			res.Err = ErrUnknownPartition
			results[i] = res
			failed = true
			continue
		}
		res.LeaderID, res.LeaderEpoch = p.Leader()
		limit := budget - total
		if r.MaxBytes > 0 {
			limit = min(limit, r.MaxBytes)
		}
		read, hw, err := p.Read(ctx, r.Offset, max(limit, 1), r.LeaderEpoch)
		res.HighWatermark, res.LogStartOffset = hw, read.LogStartOffset
		if err != nil {
			res.Err = err
			failed = true
			results[i] = res
			continue
		}
		// Only the first non-empty partition may exceed the budget (its first batch is whole).
		if total == 0 || total+len(read.Data) <= budget {
			res.Records = read.Data
			total += len(read.Data)
		}
		results[i] = res
	}
	return results, total, failed
}

// ListOffsetsRequest is one partition of a ListOffsets call.
type ListOffsetsRequest struct {
	TP          storage.TopicPartition
	LeaderEpoch int32
	Timestamp   int64 // partition.EarliestTimestamp, partition.LatestTimestamp or >= 0
}

// ListOffsetsResult is the outcome for one ListOffsetsRequest.
type ListOffsetsResult struct {
	TP          storage.TopicPartition
	Err         error
	Offset      int64
	Timestamp   int64 // found record's timestamp for a timestamp lookup, else -1
	LeaderID    int32
	LeaderEpoch int32
}

// ListOffsets resolves offsets per partition.Partition.ListOffsets.
func (b *Broker) ListOffsets(ctx context.Context, reqs []ListOffsetsRequest) []ListOffsetsResult {
	out := make([]ListOffsetsResult, len(reqs))
	for i, r := range reqs {
		res := ListOffsetsResult{TP: r.TP, Offset: -1, Timestamp: -1, LeaderID: -1, LeaderEpoch: -1}
		p, ok := b.Partition(r.TP)
		if !ok {
			res.Err = ErrUnknownPartition
			out[i] = res
			continue
		}
		res.LeaderID, res.LeaderEpoch = p.Leader()
		res.Offset, res.Timestamp, res.Err = p.ListOffsets(ctx, r.LeaderEpoch, r.Timestamp)
		out[i] = res
	}
	return out
}
