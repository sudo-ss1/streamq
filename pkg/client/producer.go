package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// ErrProducerClosed reports use of a closed Producer, or a record abandoned by Close.
var ErrProducerClosed = errors.New("client: producer closed")

// Acks selects when the broker acknowledges a produce (SPEC §4.5).
type Acks int

// Acks modes. The zero value is AcksAll (Kafka's default).
const (
	AcksAll Acks = iota
	AcksLeader
	AcksNone
)

func (a Acks) proto() streamqv1.Acks {
	switch a {
	case AcksLeader:
		return streamqv1.Acks_ACKS_LEADER
	case AcksNone:
		return streamqv1.Acks_ACKS_NONE
	default:
		return streamqv1.Acks_ACKS_ALL
	}
}

// ProducerConfig configures a Producer. Zero values take the defaults (DECISIONS D22).
type ProducerConfig struct {
	Acks Acks
	// Linger is how long a batch may wait for more records before it is sent (default 5ms).
	Linger time.Duration
	// BatchSize is the target encoded batch size in bytes (default 16 KiB).
	BatchSize int
	// BufferMemory bounds bytes of records accepted but not yet completed; Produce blocks when
	// it is exhausted (default 32 MiB).
	BufferMemory int
	// MaxRequestSize bounds one encoded batch (default 1 MiB).
	MaxRequestSize int
	// DeliveryTimeout bounds the time from a batch's creation to its success or final failure,
	// including retries (default 2m). It must be >= Linger + the client's RequestTimeout.
	DeliveryTimeout time.Duration
	// MaxBlock bounds how long Produce blocks waiting for topic metadata or buffer space
	// (default 60s, Kafka max.block.ms).
	MaxBlock time.Duration
	// Partitioner chooses partitions for records without UsePartition (default
	// NewDefaultPartitioner()).
	Partitioner Partitioner
}

func (c *ProducerConfig) setDefaults(requestTimeout time.Duration) error {
	if c.Linger <= 0 {
		c.Linger = 5 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 16 << 10
	}
	if c.BufferMemory <= 0 {
		c.BufferMemory = 32 << 20
	}
	if c.MaxRequestSize <= 0 {
		c.MaxRequestSize = 1 << 20
	}
	if c.DeliveryTimeout <= 0 {
		c.DeliveryTimeout = 2 * time.Minute
	}
	if c.MaxBlock <= 0 {
		c.MaxBlock = time.Minute
	}
	if c.Partitioner == nil {
		c.Partitioner = NewDefaultPartitioner()
	}
	if c.DeliveryTimeout < c.Linger+requestTimeout {
		return fmt.Errorf("client: delivery timeout %v must be >= linger %v + request timeout %v",
			c.DeliveryTimeout, c.Linger, requestTimeout)
	}
	return nil
}

// pendingRecord is one accepted record awaiting completion.
type pendingRecord struct {
	key, value []byte
	headers    []record.Header
	ts         int64 // unix ms
	bytes      int64 // buffer bytes held
	cb         func(RecordMetadata, error)
}

// batch is a group of records sent together. Records are kept (not just the encoded bytes) so a
// rejected batch can be split (DECISIONS D24).
type batch struct {
	recs    []pendingRecord
	builder *record.Builder // only while open
	created time.Time
	gen     uint64 // open-batch generation, to ignore stale linger timers
	stop    func() bool
}

// partitionState is the accumulator and send queue of one partition.
type partitionState struct {
	tp TopicPartition

	mu        sync.Mutex
	open      *batch
	gen       uint64
	queue     []*batch // sealed, oldest first; head is in flight
	closed    bool
	numParts  int32 // partition count when the open batch was started (for OnNewBatch)
	limit     int   // sealing size
	appended  uint64
	completed uint64
	progress  chan struct{} // closed and replaced when completed advances
	signal    chan struct{} // 1-buffered: wakes the sender
}

// Producer batches records per partition and sends them with retries (FR12). Each partition has
// one sender goroutine and at most one batch in flight, so records of a key are never reordered,
// even by retries (DECISIONS D21). Callbacks run on the partition's sender goroutine in record
// order; calling Flush, Close or ProduceSync from a callback deadlocks.
//
// Lock ordering: Producer.mu → partitionState.mu. No producer lock is held while calling the
// Partitioner, blocking on buffer space or metadata, or running callbacks.
type Producer struct {
	client *Client
	cfg    ProducerConfig
	clock  clock
	buf    *semaphore.Weighted

	mu      sync.Mutex
	closed  bool
	parts   map[TopicPartition]*partitionState
	senders sync.WaitGroup

	closingCtx context.Context // done when Close begins: unblocks Produce
	closing    context.CancelFunc
	abortCtx   context.Context // done when Close's deadline passes: senders give up
	abort      context.CancelFunc
}

// NewProducer returns a Producer that sends through c. Close the Producer before the Client.
func NewProducer(c *Client, cfg ProducerConfig) (*Producer, error) {
	return newProducer(c, cfg, realClock{})
}

func newProducer(c *Client, cfg ProducerConfig, clk clock) (*Producer, error) {
	if err := cfg.setDefaults(c.cfg.RequestTimeout); err != nil {
		return nil, err
	}
	p := &Producer{client: c, cfg: cfg, clock: clk, buf: semaphore.NewWeighted(int64(cfg.BufferMemory)),
		parts: map[TopicPartition]*partitionState{}}
	p.closingCtx, p.closing = context.WithCancel(context.Background())
	p.abortCtx, p.abort = context.WithCancel(context.Background())
	return p, nil
}

// recordBytes estimates a record's encoded size, used for buffer accounting and size checks.
func recordBytes(key, value []byte, headers []record.Header) int64 {
	n := int64(len(key) + len(value) + 32)
	for _, h := range headers {
		n += int64(len(h.Key) + len(h.Value) + 10)
	}
	return n
}

// blockCtx merges the caller's ctx, producer closing and MaxBlock.
func (p *Producer) blockCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	bctx, cancel := context.WithTimeout(ctx, p.cfg.MaxBlock)
	stop := context.AfterFunc(p.closingCtx, cancel)
	return bctx, func() { stop(); cancel() }
}

// blockErr explains why a blocking wait in Produce ended.
func (p *Producer) blockErr(ctx context.Context, cause error) error {
	switch {
	case p.closingCtx.Err() != nil:
		return ErrProducerClosed
	case ctx.Err() != nil:
		return ctx.Err()
	default:
		return cause
	}
}

// Produce accepts rec for asynchronous delivery; cb (may be nil) receives the result. It blocks
// while waiting for the topic's metadata (DECISIONS D23) or for buffer space, bounded by ctx and
// MaxBlock. An error return means the record was not accepted and cb will not be called.
func (p *Producer) Produce(ctx context.Context, rec Record, cb func(RecordMetadata, error)) error {
	if rec.Topic == "" {
		return errors.New("client: record has no topic")
	}
	ts := rec.Timestamp
	if ts.IsZero() {
		ts = p.clock.Now()
	}
	if ts.UnixMilli() < 0 {
		return fmt.Errorf("client: timestamp %v before the epoch: %w", ts, ErrInvalidTimestamp)
	}
	headers := toRecordHeaders(rec.Headers)
	size := recordBytes(rec.Key, rec.Value, headers)
	if size > int64(p.cfg.MaxRequestSize) || size > int64(p.cfg.BufferMemory) {
		return fmt.Errorf("client: record of ~%d bytes exceeds max request size %d or buffer %d: %w",
			size, p.cfg.MaxRequestSize, p.cfg.BufferMemory, ErrMessageTooLarge)
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrProducerClosed
	}

	bctx, cancel := p.blockCtx(ctx)
	defer cancel()
	tm, err := p.waitTopic(bctx, rec.Topic)
	if err != nil {
		return p.blockErr(ctx, err)
	}
	n := int32(len(tm.partitions))
	if err := p.buf.Acquire(bctx, size); err != nil {
		return p.blockErr(ctx, fmt.Errorf("client: waiting for buffer memory: %w", err))
	}
	pr := pendingRecord{key: rec.Key, value: rec.Value, headers: headers, ts: ts.UnixMilli(), bytes: size, cb: cb}
	// A sticky (unkeyed, partitioner-chosen) record that would start a new batch is re-partitioned
	// once after the partitioner moves on, so it joins the new sticky partition rather than
	// stranding in a batch of its own (Kafka's abortOnNewBatch).
	sticky := !rec.UsePartition && rec.Key == nil
	for {
		part := rec.Partition
		if !rec.UsePartition {
			part = p.cfg.Partitioner.Partition(rec.Topic, rec.Key, n)
		}
		if part < 0 || part >= n {
			p.buf.Release(size)
			return fmt.Errorf("client: %s/%d: %w", rec.Topic, part, ErrUnknownTopicOrPartition)
		}
		ps, err := p.partition(TopicPartition{Topic: rec.Topic, Partition: part})
		if err != nil {
			p.buf.Release(size)
			return err
		}
		sealed, err := p.append(ps, pr, n, sealLimit(p.cfg, tm), sticky)
		if sealed {
			p.cfg.Partitioner.OnNewBatch(rec.Topic, part, n)
		}
		if errors.Is(err, errAbortForNewBatch) {
			sticky = false
			continue
		}
		if err != nil {
			p.buf.Release(size)
			return err
		}
		return nil
	}
}

// errAbortForNewBatch tells Produce to re-partition a sticky record after sealing a batch.
var errAbortForNewBatch = errors.New("client: abort for new batch")

// sealLimit is min(BatchSize, MaxRequestSize, topic max_message_bytes when known).
func sealLimit(cfg ProducerConfig, tm *topicMeta) int {
	limit := min(cfg.BatchSize, cfg.MaxRequestSize)
	if v, ok := tm.configs["max_message_bytes"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = min(limit, n)
		}
	}
	return limit
}

// waitTopic returns the topic's metadata, refreshing with backoff while it is unknown.
func (p *Producer) waitTopic(ctx context.Context, topic string) (*topicMeta, error) {
	for attempt := 0; ; attempt++ {
		tm, err := p.client.topic(ctx, topic)
		if err == nil && len(tm.partitions) > 0 {
			return tm, nil
		}
		if err == nil {
			err = fmt.Errorf("topic %q has no partitions: %w", topic, ErrUnknownTopicOrPartition)
		}
		if !IsRetriable(err) && !errors.Is(err, ErrUnknownTopicOrPartition) {
			return nil, err
		}
		if !p.wait(ctx, p.client.backoff.delay(attempt)) {
			return nil, fmt.Errorf("client: topic %q not available (last error: %v): %w", topic, err, ErrUnknownTopicOrPartition)
		}
	}
}

// wait sleeps d on the producer clock unless ctx ends first; it reports whether d elapsed.
func (p *Producer) wait(ctx context.Context, d time.Duration) bool {
	ch, stop := p.clock.NewTimer(d)
	defer stop()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// partition returns tp's state, creating it and its sender on first use.
func (p *Producer) partition(tp TopicPartition) (*partitionState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrProducerClosed
	}
	if ps, ok := p.parts[tp]; ok {
		return ps, nil
	}
	ps := &partitionState{tp: tp, progress: make(chan struct{}), signal: make(chan struct{}, 1)}
	p.parts[tp] = ps
	p.senders.Add(1)
	go p.sendLoop(ps)
	return ps, nil
}

// append adds r to the partition's open batch, sealing first if it would exceed limit, and
// sealing after if the batch is full. It reports whether a batch was sealed. With
// abortOnNewBatch, a record that would have to start a new batch is not appended: the open batch
// is sealed and errAbortForNewBatch returned so the caller can re-partition it.
func (p *Producer) append(ps *partitionState, r pendingRecord, numParts int32, limit int, abortOnNewBatch bool) (bool, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return false, ErrProducerClosed
	}
	sealed := false
	if ps.open != nil && ps.open.builder.AppendedSize(r.ts, r.key, r.value, r.headers) > ps.limit {
		p.sealLocked(ps)
		sealed = true
	}
	if ps.open == nil && abortOnNewBatch {
		return sealed, errAbortForNewBatch
	}
	if ps.open == nil {
		ps.gen++
		b := &batch{
			builder: record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence}),
			created: p.clock.Now(),
			gen:     ps.gen,
		}
		gen := ps.gen
		b.stop = p.clock.AfterFunc(p.cfg.Linger, func() { p.lingerExpired(ps, gen) })
		ps.open = b
		ps.numParts = numParts
		ps.limit = limit
	}
	ps.open.builder.Append(r.ts, r.key, r.value, r.headers)
	ps.open.recs = append(ps.open.recs, r)
	ps.appended++
	if ps.open.builder.Size() >= ps.limit {
		p.sealLocked(ps)
		sealed = true
	}
	return sealed, nil
}

// sealLocked moves the open batch to the send queue and wakes the sender. Caller holds ps.mu.
func (p *Producer) sealLocked(ps *partitionState) {
	b := ps.open
	if b == nil {
		return
	}
	ps.open = nil
	b.stop()
	b.builder = nil
	ps.queue = append(ps.queue, b)
	select {
	case ps.signal <- struct{}{}:
	default:
	}
}

// lingerExpired seals the open batch if it is still the one the timer was armed for.
func (p *Producer) lingerExpired(ps *partitionState, gen uint64) {
	ps.mu.Lock()
	if ps.closed || ps.open == nil || ps.open.gen != gen {
		ps.mu.Unlock()
		return
	}
	p.sealLocked(ps)
	n := ps.numParts
	ps.mu.Unlock()
	p.cfg.Partitioner.OnNewBatch(ps.tp.Topic, ps.tp.Partition, n)
}

// sendLoop is the partition's sender: it sends sealed batches strictly in order, one at a time.
func (p *Producer) sendLoop(ps *partitionState) {
	defer p.senders.Done()
	for {
		ps.mu.Lock()
		for len(ps.queue) == 0 {
			if ps.closed && ps.open == nil {
				ps.mu.Unlock()
				return
			}
			ps.mu.Unlock()
			<-ps.signal
			ps.mu.Lock()
		}
		b := ps.queue[0]
		ps.mu.Unlock()

		halves := p.send(ps.tp, b)

		ps.mu.Lock()
		ps.queue = ps.queue[1:]
		if halves != nil {
			ps.queue = append(halves, ps.queue...)
		}
		ps.mu.Unlock()
	}
}

// send delivers one batch with retries. If the broker rejects a multi-record batch as too large
// it returns the two halves to send instead (DECISIONS D24); otherwise it completes the batch.
func (p *Producer) send(tp TopicPartition, b *batch) []*batch {
	deadline := b.created.Add(p.cfg.DeliveryTimeout)
	var lastErr error
	for attempt := 0; ; attempt++ {
		if p.abortCtx.Err() != nil {
			p.complete(tp, b, -1, ErrProducerClosed)
			return nil
		}
		if !p.clock.Now().Before(deadline) {
			if lastErr == nil {
				lastErr = ErrRequestTimedOut
			}
			p.complete(tp, b, -1, fmt.Errorf("client: delivery timeout %v exceeded: %w", p.cfg.DeliveryTimeout, lastErr))
			return nil
		}
		base, err := p.sendOnce(tp, b)
		switch {
		case err != nil && p.abortCtx.Err() != nil:
			p.complete(tp, b, -1, ErrProducerClosed) // Close gave up; the RPC was cancelled
			return nil
		case err == nil:
			p.complete(tp, b, base, nil)
			return nil
		case errors.Is(err, ErrMessageTooLarge) && len(b.recs) > 1:
			mid := len(b.recs) / 2
			return []*batch{
				{recs: b.recs[:mid], created: b.created},
				{recs: b.recs[mid:], created: b.created},
			}
		case !IsRetriable(err):
			p.complete(tp, b, -1, err)
			return nil
		}
		lastErr = err
		if NeedsMetadataRefresh(err) {
			p.client.invalidate(tp.Topic)
		}
		if !p.wait(p.abortCtx, p.client.backoff.delay(attempt)) {
			continue // aborted: the next iteration fails the batch
		}
	}
}

// sendOnce encodes b and sends it to the current leader, returning the base offset.
func (p *Producer) sendOnce(tp TopicPartition, b *batch) (int64, error) {
	api, epoch, err := p.client.leader(p.abortCtx, tp)
	if err != nil {
		return -1, err
	}
	bld := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, r := range b.recs {
		bld.Append(r.ts, r.key, r.value, r.headers)
	}
	data, err := bld.Build()
	if err != nil {
		return -1, fmt.Errorf("client: encode batch: %w", err)
	}
	timeout := p.client.cfg.RequestTimeout
	ctx, cancel := context.WithTimeout(p.abortCtx, timeout+time.Second)
	defer cancel()
	resp, err := api.Produce(ctx, &streamqv1.ProduceRequest{
		Acks:      p.cfg.Acks.proto(),
		TimeoutMs: int32(timeout.Milliseconds()),
		Topics: []*streamqv1.ProduceTopic{{Topic: tp.Topic, Partitions: []*streamqv1.ProducePartition{
			{Partition: tp.Partition, LeaderEpoch: epoch, Records: data},
		}}},
	})
	if err != nil {
		return -1, err
	}
	for _, t := range resp.GetTopics() {
		if t.GetTopic() != tp.Topic {
			continue
		}
		for _, pr := range t.GetPartitions() {
			if pr.GetPartition() == tp.Partition {
				if err := serverError(pr.GetErrorCode(), pr.GetErrorMessage()); err != nil {
					return -1, err
				}
				return pr.GetBaseOffset(), nil
			}
		}
	}
	return -1, fmt.Errorf("client: produce response missing %s/%d: %w", tp.Topic, tp.Partition, ErrRequestTimedOut)
}

// complete releases a batch's buffer bytes, runs its callbacks in order and records progress.
func (p *Producer) complete(tp TopicPartition, b *batch, base int64, err error) {
	var bytes int64
	for i, r := range b.recs {
		bytes += r.bytes
		if r.cb == nil {
			continue
		}
		md := RecordMetadata{Topic: tp.Topic, Partition: tp.Partition, Offset: -1, Timestamp: time.UnixMilli(r.ts)}
		if err == nil && base >= 0 && p.cfg.Acks != AcksNone {
			md.Offset = base + int64(i)
		}
		r.cb(md, err)
	}
	p.buf.Release(bytes)
	p.mu.Lock()
	ps := p.parts[tp]
	p.mu.Unlock()
	ps.mu.Lock()
	ps.completed += uint64(len(b.recs))
	close(ps.progress)
	ps.progress = make(chan struct{})
	ps.mu.Unlock()
}

// ProduceSync produces rec and waits for its result.
func (p *Producer) ProduceSync(ctx context.Context, rec Record) (RecordMetadata, error) {
	type result struct {
		md  RecordMetadata
		err error
	}
	done := make(chan result, 1)
	if err := p.Produce(ctx, rec, func(md RecordMetadata, err error) { done <- result{md, err} }); err != nil {
		return RecordMetadata{}, err
	}
	select {
	case r := <-done:
		return r.md, r.err
	case <-ctx.Done():
		return RecordMetadata{}, ctx.Err()
	}
}

// Flush sends every open batch now and waits until every record accepted before the call has
// completed (successfully or not), or ctx ends.
func (p *Producer) Flush(ctx context.Context) error {
	p.mu.Lock()
	parts := make([]*partitionState, 0, len(p.parts))
	for _, ps := range p.parts {
		parts = append(parts, ps)
	}
	p.mu.Unlock()
	targets := make([]uint64, len(parts))
	for i, ps := range parts {
		ps.mu.Lock()
		targets[i] = ps.appended
		p.sealLocked(ps)
		ps.mu.Unlock()
	}
	for i, ps := range parts {
		for {
			ps.mu.Lock()
			done := ps.completed >= targets[i]
			ch := ps.progress
			ps.mu.Unlock()
			if done {
				break
			}
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// Close stops accepting records, sends everything buffered (with normal retries) and stops the
// senders. If ctx ends first, records still pending fail with ErrProducerClosed and Close
// returns ctx's error once the senders have stopped. Idempotent.
func (p *Producer) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.senders.Wait()
		return nil
	}
	p.closed = true
	p.closing()
	parts := make([]*partitionState, 0, len(p.parts))
	for _, ps := range p.parts {
		parts = append(parts, ps)
	}
	p.mu.Unlock()
	for _, ps := range parts {
		ps.mu.Lock()
		p.sealLocked(ps)
		ps.closed = true
		select {
		case ps.signal <- struct{}{}:
		default:
		}
		ps.mu.Unlock()
	}
	done := make(chan struct{})
	go func() {
		p.senders.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		p.abort()
		<-done
	}
	p.abort()
	return err
}
