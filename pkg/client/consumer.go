package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// Consumer defaults (DECISIONS D22).
const (
	consumerDefaultMaxWait           = 500 * time.Millisecond
	consumerDefaultMinBytes          = 1
	consumerDefaultMaxBytes          = 16 << 20
	consumerDefaultMaxPartitionBytes = 1 << 20
)

// List offsets "timestamps" for the earliest and latest offsets.
const (
	consumerEarliest int64 = -2
	consumerLatest   int64 = -1
)

// errConsumerNotAssigned reports a Seek/SeekTo* on a partition outside the assignment.
var errConsumerNotAssigned = errors.New("client: partition not assigned")

// OffsetReset is the consumer's policy when it has no valid position.
type OffsetReset int

// Offset reset policies.
const (
	// ResetLatest starts at the high watermark (Kafka default).
	ResetLatest OffsetReset = iota
	// ResetEarliest starts at the log start offset.
	ResetEarliest
)

// ConsumerConfig configures a Consumer. Zero values take the documented defaults.
type ConsumerConfig struct {
	MaxWait           time.Duration // fetch long-poll wait, default 500ms
	MinBytes          int           // default 1
	MaxBytes          int           // per fetch request, default 16 MiB
	MaxPartitionBytes int           // per partition, default 1 MiB
	AutoOffsetReset   OffsetReset   // default ResetLatest
}

// consumerPartition is the fetch state of one assigned partition.
type consumerPartition struct {
	pos      int64 // next offset to read; valid if hasPos
	hasPos   bool
	attempts int       // consecutive errors, for backoff
	until    time.Time // excluded from fetches until then
}

// Consumer reads manually assigned partitions (no group membership; see GroupConsumer in M4). It
// is not safe for concurrent use, like Kafka's consumer; only Close may be called from another
// goroutine (it wakes a Poll waiting on backoff).
type Consumer struct {
	client *Client
	cfg    ConsumerConfig
	now    func() time.Time // injectable for tests

	assigned []TopicPartition // assignment order
	parts    map[TopicPartition]*consumerPartition
	rotate   int   // first partition of the next poll's order
	pending  error // fatal error held back because the failing Poll returned records

	closed    atomic.Bool
	done      chan struct{}
	closeOnce sync.Once
}

// NewConsumer returns a Consumer using c.
func NewConsumer(c *Client, cfg ConsumerConfig) (*Consumer, error) {
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = consumerDefaultMaxWait
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = consumerDefaultMinBytes
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = consumerDefaultMaxBytes
	}
	if cfg.MaxPartitionBytes <= 0 {
		cfg.MaxPartitionBytes = consumerDefaultMaxPartitionBytes
	}
	return &Consumer{
		client: c,
		cfg:    cfg,
		now:    time.Now,
		parts:  map[TopicPartition]*consumerPartition{},
		done:   make(chan struct{}),
	}, nil
}

// Assign replaces the assignment. Positions of newly assigned partitions are unset and resolved
// with AutoOffsetReset on the next Poll unless Seek is called. Partitions that stay assigned keep
// their positions.
func (c *Consumer) Assign(tps []TopicPartition) {
	parts := make(map[TopicPartition]*consumerPartition, len(tps))
	assigned := make([]TopicPartition, 0, len(tps))
	for _, tp := range tps {
		if _, dup := parts[tp]; dup {
			continue
		}
		st, ok := c.parts[tp]
		if !ok {
			st = &consumerPartition{}
		}
		parts[tp] = st
		assigned = append(assigned, tp)
	}
	c.parts, c.assigned, c.rotate = parts, assigned, 0
}

// Assignment returns the current assignment.
func (c *Consumer) Assignment() []TopicPartition { return slices.Clone(c.assigned) }

// Seek sets the next offset to read for an assigned partition.
func (c *Consumer) Seek(tp TopicPartition, offset int64) error {
	if c.closed.Load() {
		return ErrClosed
	}
	st, ok := c.parts[tp]
	if !ok {
		return fmt.Errorf("seek %s/%d: %w", tp.Topic, tp.Partition, errConsumerNotAssigned)
	}
	if offset < 0 {
		return fmt.Errorf("seek %s/%d: negative offset %d", tp.Topic, tp.Partition, offset)
	}
	st.pos, st.hasPos = offset, true
	return nil
}

// SeekToBeginning moves the given (or, if none, all) assigned partitions to their log start.
func (c *Consumer) SeekToBeginning(ctx context.Context, tps ...TopicPartition) error {
	return c.seekTo(ctx, consumerEarliest, tps)
}

// SeekToEnd moves the given (or, if none, all) assigned partitions to their high watermark.
func (c *Consumer) SeekToEnd(ctx context.Context, tps ...TopicPartition) error {
	return c.seekTo(ctx, consumerLatest, tps)
}

// seekTo resolves ts (earliest or latest) for tps, retrying retriable errors with backoff until
// ctx ends. No position changes unless every partition resolves.
func (c *Consumer) seekTo(ctx context.Context, ts int64, tps []TopicPartition) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if len(tps) == 0 {
		tps = c.assigned
	}
	for _, tp := range tps {
		if _, ok := c.parts[tp]; !ok {
			return fmt.Errorf("seek %s/%d: %w", tp.Topic, tp.Partition, errConsumerNotAssigned)
		}
	}
	resolved := make(map[TopicPartition]int64, len(tps))
	todo := slices.Clone(tps)
	for attempt := 0; ; attempt++ {
		offsets, failed, err := c.listOffsets(ctx, todo, func(TopicPartition) int64 { return ts })
		if err != nil {
			return err
		}
		for tp, off := range offsets {
			resolved[tp] = off
		}
		todo = todo[:0]
		for tp, ferr := range failed {
			if !IsRetriable(ferr) {
				return fmt.Errorf("seek %s/%d: %w", tp.Topic, tp.Partition, ferr)
			}
			if NeedsMetadataRefresh(ferr) {
				c.client.invalidate(tp.Topic)
			}
			todo = append(todo, tp)
		}
		if len(todo) == 0 {
			break
		}
		if !sleep(ctx, c.client.backoff.delay(attempt), c.done) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return ErrClosed
		}
	}
	for tp, off := range resolved {
		st := c.parts[tp]
		st.pos, st.hasPos = off, true
	}
	return nil
}

// Position returns the next offset to read for tp, if known.
func (c *Consumer) Position(tp TopicPartition) (int64, bool) {
	st, ok := c.parts[tp]
	if !ok || !st.hasPos {
		return 0, false
	}
	return st.pos, true
}

// Poll fetches the next records from the assigned partitions, waiting up to MaxWait for data.
// Returned records are in offset order per partition and their positions are advanced.
//
// A partition whose fetch fails with a retriable error (stale leadership, unknown partition,
// broker unreachable) is skipped for a backoff delay while the others keep being fetched. If
// every partition is backing off, Poll waits for the earliest backoff (or ctx) and returns no
// records. A fatal error is returned together with no records; if the same Poll already decoded
// records for other partitions, those are returned and the error is reported by the next Poll.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := c.pending; err != nil {
		c.pending = nil
		return nil, err
	}
	if len(c.assigned) == 0 {
		return nil, nil
	}

	ready, wait := c.readyPartitions()
	if len(ready) == 0 {
		if !sleep(ctx, wait, c.done) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrClosed
		}
		return nil, nil
	}

	// Resolve unset positions with the reset policy.
	var unset []TopicPartition
	for _, tp := range ready {
		if !c.parts[tp].hasPos {
			unset = append(unset, tp)
		}
	}
	if len(unset) > 0 {
		if err := c.resetPositions(ctx, unset); err != nil {
			return nil, err
		}
	}
	fetchable := ready[:0:0]
	for _, tp := range ready {
		if st := c.parts[tp]; st.hasPos && !st.until.After(c.now()) {
			fetchable = append(fetchable, tp)
		}
	}
	if len(fetchable) == 0 {
		return nil, nil
	}

	results, err := c.fetch(ctx, fetchable)
	if err != nil {
		return nil, err
	}
	var (
		out      []Record
		fatal    error
		outRange []TopicPartition
	)
	for _, res := range results {
		recs, oor, err := c.handleFetch(res)
		out = append(out, recs...)
		outRange = append(outRange, oor...)
		if err != nil && fatal == nil {
			fatal = err
		}
	}
	if len(outRange) > 0 && fatal == nil {
		fatal = c.resetPositions(ctx, outRange)
	}
	if fatal != nil {
		if len(out) == 0 {
			return nil, fatal
		}
		c.pending = fatal
	}
	return out, nil
}

// readyPartitions returns the assigned partitions not backing off, in this poll's rotated order,
// and advances the rotation. If none is ready it returns how long until the earliest is.
func (c *Consumer) readyPartitions() ([]TopicPartition, time.Duration) {
	now := c.now()
	n := len(c.assigned)
	start := c.rotate % n
	c.rotate = (start + 1) % n
	var (
		ready    []TopicPartition
		earliest time.Time
	)
	for i := range n {
		tp := c.assigned[(start+i)%n]
		st := c.parts[tp]
		if !st.until.After(now) {
			ready = append(ready, tp)
			continue
		}
		if earliest.IsZero() || st.until.Before(earliest) {
			earliest = st.until
		}
	}
	return ready, earliest.Sub(now)
}

// backOff excludes tp from fetches for the next backoff delay.
func (c *Consumer) backOff(tp TopicPartition, err error) {
	st, ok := c.parts[tp]
	if !ok {
		return
	}
	d := c.client.backoff.delay(st.attempts)
	st.attempts++
	st.until = c.now().Add(d)
	c.client.logger.Debug("consumer partition backing off",
		slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition)),
		slog.Duration("delay", d), slog.Any("error", err))
}

// retryLater handles a retriable partition error: refresh metadata if stale, then back off.
func (c *Consumer) retryLater(tp TopicPartition, err error) {
	if NeedsMetadataRefresh(err) {
		c.client.invalidate(tp.Topic)
	}
	c.backOff(tp, err)
}

// resetPositions sets positions for tps per AutoOffsetReset. Retriable failures back the
// partition off; the first fatal one is returned.
func (c *Consumer) resetPositions(ctx context.Context, tps []TopicPartition) error {
	ts := consumerLatest
	if c.cfg.AutoOffsetReset == ResetEarliest {
		ts = consumerEarliest
	}
	offsets, failed, err := c.listOffsets(ctx, tps, func(TopicPartition) int64 { return ts })
	if err != nil {
		return err
	}
	for tp, off := range offsets {
		if st, ok := c.parts[tp]; ok {
			st.pos, st.hasPos = off, true
		}
	}
	var fatal error
	for _, tp := range tps {
		ferr, ok := failed[tp]
		if !ok {
			continue
		}
		if st, ok := c.parts[tp]; ok {
			st.hasPos = false
		}
		if IsRetriable(ferr) {
			c.retryLater(tp, ferr)
		} else if fatal == nil {
			fatal = fmt.Errorf("reset offset %s/%d: %w", tp.Topic, tp.Partition, ferr)
		}
	}
	return fatal
}

// consumerRoute is the leader a group of partitions is sent to.
type consumerRoute struct {
	api    streamqv1.StreamQServiceClient
	tps    []TopicPartition
	epochs []int32
}

// route groups tps by leader, in order of first appearance. Partitions that cannot be routed are
// returned in failed. A non-nil error is fatal (ctx done, client closed).
func (c *Consumer) route(ctx context.Context, tps []TopicPartition) ([]*consumerRoute, map[TopicPartition]error, error) {
	byLeader := map[int32]*consumerRoute{}
	var routes []*consumerRoute
	failed := map[TopicPartition]error{}
	for _, tp := range tps {
		api, epoch, leaderID, err := c.client.leaderInfo(ctx, tp)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if errors.Is(err, ErrClosed) {
				return nil, nil, err
			}
			failed[tp] = err
			continue
		}
		r, ok := byLeader[leaderID]
		if !ok {
			r = &consumerRoute{api: api}
			byLeader[leaderID] = r
			routes = append(routes, r)
		}
		r.tps = append(r.tps, tp)
		r.epochs = append(r.epochs, epoch)
	}
	return routes, failed, nil
}

// rpcContext bounds one broker RPC: RequestTimeout plus the fetch long-poll wait.
func (c *Consumer) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.client.cfg.RequestTimeout+c.cfg.MaxWait)
}

// listOffsets resolves tsFor(tp) for every tp, one ListOffsets request per leader. Per-partition
// failures (routing, transport, error codes) are returned in failed; err is fatal.
func (c *Consumer) listOffsets(ctx context.Context, tps []TopicPartition, tsFor func(TopicPartition) int64) (
	offsets map[TopicPartition]int64, failed map[TopicPartition]error, err error,
) {
	routes, failed, err := c.route(ctx, tps)
	if err != nil {
		return nil, nil, err
	}
	offsets = map[TopicPartition]int64{}
	for _, r := range routes {
		req := &streamqv1.ListOffsetsRequest{}
		consumerGroupTopics(r.tps, func(topic string, idx []int) {
			lt := &streamqv1.ListOffsetsTopic{Topic: topic}
			for _, i := range idx {
				lt.Partitions = append(lt.Partitions, &streamqv1.ListOffsetsPartition{
					Partition: r.tps[i].Partition, LeaderEpoch: r.epochs[i], Timestamp: tsFor(r.tps[i]),
				})
			}
			req.Topics = append(req.Topics, lt)
		})
		rctx, cancel := c.client.withTimeout(ctx)
		resp, rerr := r.api.ListOffsets(rctx, req)
		cancel()
		if rerr != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			for _, tp := range r.tps {
				failed[tp] = fmt.Errorf("list offsets: %w", rerr)
			}
			continue
		}
		got := map[TopicPartition]bool{}
		for _, t := range resp.GetTopics() {
			for _, p := range t.GetPartitions() {
				tp := TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}
				if !slices.Contains(r.tps, tp) {
					continue
				}
				got[tp] = true
				if serr := serverError(p.GetErrorCode(), ""); serr != nil {
					failed[tp] = serr
					continue
				}
				offsets[tp] = p.GetOffset()
			}
		}
		for _, tp := range r.tps {
			if !got[tp] {
				failed[tp] = fmt.Errorf("list offsets: %s/%d missing from response: %w", tp.Topic, tp.Partition, ErrRequestTimedOut)
			}
		}
	}
	return offsets, failed, nil
}

// consumerGroupTopics calls fn once per distinct topic of tps, in order of first appearance,
// with the indexes of that topic's partitions.
func consumerGroupTopics(tps []TopicPartition, fn func(topic string, idx []int)) {
	var order []string
	byTopic := map[string][]int{}
	for i, tp := range tps {
		if _, ok := byTopic[tp.Topic]; !ok {
			order = append(order, tp.Topic)
		}
		byTopic[tp.Topic] = append(byTopic[tp.Topic], i)
	}
	for _, t := range order {
		fn(t, byTopic[t])
	}
}

// consumerFetchResult is one leader's fetch outcome.
type consumerFetchResult struct {
	route *consumerRoute
	resp  *streamqv1.FetchResponse
	err   error // transport or other RPC error; nil if resp is set
}

// fetch sends one Fetch per leader, concurrently, and waits for all of them. Unroutable
// partitions are backed off here. The returned error is fatal (ctx done, client closed).
func (c *Consumer) fetch(ctx context.Context, tps []TopicPartition) ([]consumerFetchResult, error) {
	routes, failed, err := c.route(ctx, tps)
	if err != nil {
		return nil, err
	}
	var fatal error
	for _, tp := range tps {
		if ferr, ok := failed[tp]; ok {
			if IsRetriable(ferr) {
				c.retryLater(tp, ferr)
			} else if fatal == nil {
				fatal = fmt.Errorf("fetch %s/%d: %w", tp.Topic, tp.Partition, ferr)
			}
		}
	}
	if fatal != nil {
		return nil, fatal
	}

	results := make([]consumerFetchResult, len(routes))
	var wg sync.WaitGroup
	for i, r := range routes {
		req := &streamqv1.FetchRequest{
			MaxWaitMs: int32(c.cfg.MaxWait.Milliseconds()),
			MinBytes:  int32(c.cfg.MinBytes),
			MaxBytes:  int32(c.cfg.MaxBytes),
		}
		consumerGroupTopics(r.tps, func(topic string, idx []int) {
			ft := &streamqv1.FetchTopic{Topic: topic}
			for _, j := range idx {
				tp := r.tps[j]
				ft.Partitions = append(ft.Partitions, &streamqv1.FetchPartition{
					Partition: tp.Partition, LeaderEpoch: r.epochs[j],
					FetchOffset: c.parts[tp].pos, MaxBytes: int32(c.cfg.MaxPartitionBytes),
				})
			}
			req.Topics = append(req.Topics, ft)
		})
		results[i].route = r
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := c.rpcContext(ctx)
			defer cancel()
			results[i].resp, results[i].err = r.api.Fetch(rctx, req)
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return results, nil
}

// handleFetch applies one leader's fetch result: it returns the decoded records, the partitions
// whose offsets are out of range, and the first fatal error. Retriable errors back off.
func (c *Consumer) handleFetch(res consumerFetchResult) ([]Record, []TopicPartition, error) {
	r := res.route
	if res.err != nil {
		if !isTransportError(res.err) {
			return nil, nil, fmt.Errorf("fetch: %w", res.err)
		}
		for _, tp := range r.tps {
			c.retryLater(tp, res.err)
		}
		return nil, nil, nil
	}
	var (
		out      []Record
		outRange []TopicPartition
		fatal    error
	)
	byTP := map[TopicPartition]*streamqv1.FetchPartitionResponse{}
	for _, t := range res.resp.GetTopics() {
		for _, p := range t.GetPartitions() {
			byTP[TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}] = p
		}
	}
	// Iterate in request order so records follow this poll's rotation.
	for _, tp := range r.tps {
		p, ok := byTP[tp]
		if !ok {
			continue // not answered this time; fetched again next poll
		}
		st := c.parts[tp]
		err := serverError(p.GetErrorCode(), "")
		switch {
		case err == nil:
			recs, derr := c.decode(tp, st, p.GetRecords())
			if derr != nil {
				if fatal == nil {
					fatal = derr
				}
				continue
			}
			out = append(out, recs...)
			st.attempts, st.until = 0, time.Time{}
		case errors.Is(err, ErrOffsetOutOfRange):
			c.client.logger.Info("consumer offset out of range; resetting",
				slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition)),
				slog.Int64("offset", st.pos))
			st.hasPos = false
			outRange = append(outRange, tp)
		case NeedsMetadataRefresh(err):
			c.retryLater(tp, err)
		case IsRetriable(err):
			c.backOff(tp, err)
		default:
			if fatal == nil {
				fatal = fmt.Errorf("fetch %s/%d: %w", tp.Topic, tp.Partition, err)
			}
		}
	}
	return out, outRange, fatal
}

// decode converts fetched batches into records at or after st.pos and advances st.pos. A cut-off
// final batch (the broker may stop mid-batch at max_bytes) is ignored; it is fetched again.
func (c *Consumer) decode(tp TopicPartition, st *consumerPartition, buf []byte) ([]Record, error) {
	batches, err := record.SplitBatches(buf)
	if err != nil && !errors.Is(err, record.ErrTruncated) {
		return nil, fmt.Errorf("fetch %s/%d at %d: %w: %w", tp.Topic, tp.Partition, st.pos, ErrCorruptMessage, err)
	}
	var out []Record
	pos := st.pos
	for _, b := range batches {
		if b.NextOffset() <= pos {
			continue
		}
		recs, err := b.Records()
		if err != nil {
			return nil, fmt.Errorf("fetch %s/%d at %d: %w: %w", tp.Topic, tp.Partition, b.BaseOffset(), ErrCorruptMessage, err)
		}
		for _, r := range recs {
			if r.Offset < pos {
				continue
			}
			out = append(out, Record{
				Topic:        tp.Topic,
				Partition:    tp.Partition,
				UsePartition: true,
				Key:          r.Key,
				Value:        r.Value,
				Headers:      fromRecordHeaders(r.Headers),
				Timestamp:    time.UnixMilli(r.Timestamp),
				Offset:       r.Offset,
			})
			pos = r.Offset + 1
		}
	}
	st.pos = pos
	return out, nil
}

// Close releases the consumer. Idempotent. It does not close the Client.
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.done)
	})
	return nil
}
