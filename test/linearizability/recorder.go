package linearizability

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// Append timing: the broker waits up to appendCommitTimeout for the commit; the RPC deadline is
// above it.
const (
	appendCommitTimeout = 3 * time.Second
	appendRPCTimeout    = 5 * time.Second
	readRPCTimeout      = 5 * time.Second
)

// LeaderFunc returns a client for the broker currently believed to lead the partition.
type LeaderFunc func(ctx context.Context) (streamqv1.StreamQServiceClient, error)

// definiteAppendFailure reports produce error codes that guarantee nothing was appended: validation
// errors raised before any write. Every other failure, including UNKNOWN_TOPIC_OR_PARTITION (a
// closed log can follow a partial write) and every gRPC error, is an unknown outcome (D56).
func definiteAppendFailure(code streamqv1.ErrorCode) bool {
	switch code {
	case streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE, streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE,
		streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP:
		return true
	default:
		return false
	}
}

// Recorder records a concurrent history of single-attempt appends and whole-partition reads.
// It is safe for concurrent use.
type Recorder struct {
	start time.Time

	mu          sync.Mutex
	ops         []porcupine.Operation
	definite    int // appends that definitely failed (not recorded)
	failedReads int // reads that failed without effect (not recorded)
	noLeader    int // attempts skipped: no leader known
	anomalies   []Anomaly
}

// Summary counts a run's operations (D58). Any anomaly fails a run.
type Summary struct {
	Acked, Unknown, DefiniteFail, Reads, FailedReads, NoLeader int
	Anomalies                                                  []Anomaly
}

// Anomaly is a read integrity violation below the high watermark: an offset gap, an offset going
// backwards, a CRC or decode failure, or no progress (D56). It means committed data is missing or
// corrupt.
type Anomaly struct {
	Offset int64
	Err    string
}

// Error implements error.
func (a *Anomaly) Error() string {
	return fmt.Sprintf("read anomaly at offset %d: %s", a.Offset, a.Err)
}

// NewRecorder returns an empty recorder.
func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

func (r *Recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

func (r *Recorder) add(op porcupine.Operation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

// History returns the recorded operations.
func (r *Recorder) History() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]porcupine.Operation(nil), r.ops...)
}

// Summary counts the recorded and unrecorded operations.
func (r *Recorder) Summary() Summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Summary{DefiniteFail: r.definite, FailedReads: r.failedReads, NoLeader: r.noLeader,
		Anomalies: append([]Anomaly(nil), r.anomalies...)}
	for _, op := range r.ops {
		in, out := op.Input.(Input), op.Output.(Output) //nolint:forcetypeassert // recorded by this package
		switch {
		case in.Kind == ReadOp:
			s.Reads++
		case out.Unknown:
			s.Unknown++
		default:
			s.Acked++
		}
	}
	return s
}

func (r *Recorder) count(field *int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*field++
}

// Append makes one acks=all produce attempt of value to topic/partition through the current
// leader (no retries) and records it: acknowledged with its offset, omitted when it definitely
// failed, or with an unknown outcome (return time at infinity, so it may linearize anywhere after
// its call). It reports whether the attempt was acknowledged, so callers can back off on errors.
func (r *Recorder) Append(ctx context.Context, clientID int, leader LeaderFunc, topic string, partition int32, value string) bool {
	c, err := leader(ctx)
	if err != nil {
		r.count(&r.noLeader)
		return false
	}
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	b.Append(time.Now().UnixMilli(), nil, []byte(value), nil)
	data, err := b.Build()
	if err != nil {
		return false
	}
	// The broker's commit wait (timeout_ms) stays below the RPC deadline, so a slow commit is
	// answered with REQUEST_TIMED_OUT rather than a transport deadline (both are unknown).
	rctx, cancel := context.WithTimeout(ctx, appendRPCTimeout)
	defer cancel()
	call := r.now()
	resp, err := c.Produce(rctx, &streamqv1.ProduceRequest{Acks: streamqv1.Acks_ACKS_ALL, TimeoutMs: int32(appendCommitTimeout.Milliseconds()),
		Topics: []*streamqv1.ProduceTopic{{Topic: topic, Partitions: []*streamqv1.ProducePartition{{Partition: partition, LeaderEpoch: -1, Records: data}}}}})
	ret := r.now()
	in := Input{Kind: AppendOp, Value: value}
	if err == nil && len(resp.GetTopics()) == 1 && len(resp.GetTopics()[0].GetPartitions()) == 1 {
		pr := resp.GetTopics()[0].GetPartitions()[0]
		switch {
		case pr.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_NONE:
			r.add(porcupine.Operation{ClientId: clientID, Input: in, Call: call, Output: Output{Offset: pr.GetBaseOffset()}, Return: ret})
			return true
		case definiteAppendFailure(pr.GetErrorCode()):
			r.count(&r.definite)
			return false // no effect: not part of the history
		}
	}
	r.add(porcupine.Operation{ClientId: clientID, Input: in, Call: call, Output: Output{Unknown: true}, Return: math.MaxInt64})
	return false
}

// Read fetches the whole partition up to its high watermark through the current leader and
// records the values read. Failed reads have no effect and are not recorded. It reports success.
func (r *Recorder) Read(ctx context.Context, clientID int, leader LeaderFunc, topic string, partition int32) bool {
	c, err := leader(ctx)
	if err != nil {
		r.count(&r.noLeader)
		return false
	}
	rctx, cancel := context.WithTimeout(ctx, readRPCTimeout)
	defer cancel()
	call := r.now()
	values, err := ReadAll(rctx, c, topic, partition)
	var anomaly *Anomaly
	switch {
	case errors.As(err, &anomaly):
		r.mu.Lock()
		r.anomalies = append(r.anomalies, *anomaly)
		r.mu.Unlock()
		return false
	case err != nil:
		r.count(&r.failedReads)
		return false
	}
	r.add(porcupine.Operation{ClientId: clientID, Input: Input{Kind: ReadOp}, Call: call, Output: Output{Values: values}, Return: r.now()})
	return true
}

// errRead reports a failed fetch.
type errRead struct{ code streamqv1.ErrorCode }

func (e errRead) Error() string { return "fetch failed: " + e.code.String() }

// ReadAll fetches every record of topic/partition from offset 0 up to the high watermark seen by
// the first fetch, returning the values in offset order. Transport and API errors are returned
// as plain errors (no effect); integrity violations below the high watermark as *Anomaly.
func ReadAll(ctx context.Context, c streamqv1.StreamQServiceClient, topic string, partition int32) ([]string, error) {
	var values []string
	next, hw := int64(0), int64(-1)
	for hw < 0 || next < hw {
		resp, err := c.Fetch(ctx, &streamqv1.FetchRequest{MaxBytes: 4 << 20, MinBytes: 0, MaxWaitMs: 0, Topics: []*streamqv1.FetchTopic{{Topic: topic,
			Partitions: []*streamqv1.FetchPartition{{Partition: partition, LeaderEpoch: -1, FetchOffset: next, MaxBytes: 4 << 20}}}}})
		if err != nil {
			return nil, err
		}
		if len(resp.GetTopics()) != 1 || len(resp.GetTopics()[0].GetPartitions()) != 1 {
			return nil, errRead{streamqv1.ErrorCode_ERROR_CODE_UNKNOWN}
		}
		fp := resp.GetTopics()[0].GetPartitions()[0]
		if fp.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
			return nil, errRead{fp.GetErrorCode()}
		}
		if hw < 0 {
			hw = fp.GetHighWatermark()
		}
		batches, err := record.SplitBatches(fp.GetRecords())
		if err != nil {
			return nil, &Anomaly{Offset: next, Err: "split batches: " + err.Error()}
		}
		before := next
		for _, b := range batches {
			if _, err := record.Validate(b, len(b)); err != nil {
				return nil, &Anomaly{Offset: b.BaseOffset(), Err: "invalid batch: " + err.Error()}
			}
			recs, err := b.Records()
			if err != nil {
				return nil, &Anomaly{Offset: b.BaseOffset(), Err: "decode records: " + err.Error()}
			}
			for _, rec := range recs {
				if rec.Offset < next || rec.Offset >= hw {
					continue
				}
				if rec.Offset != next {
					return nil, &Anomaly{Offset: next, Err: fmt.Sprintf("offset gap: next record is %d", rec.Offset)}
				}
				values = append(values, string(rec.Value))
				next++
			}
		}
		if next == before && next < hw {
			if fp.GetHighWatermark() > next {
				// This broker's own HW says data exists here, yet it returned none.
				return nil, &Anomaly{Offset: next, Err: fmt.Sprintf("no progress below the high watermark %d", fp.GetHighWatermark())}
			}
			// The broker's HW dropped below the first response's (it lost and regained
			// leadership mid-read): not a violation, just an unfinished read (plan R27).
			return nil, errRead{streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE}
		}
	}
	return values, nil
}
