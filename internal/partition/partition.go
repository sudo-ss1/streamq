// Package partition holds the replica state of one partition on one broker: leadership, leader
// epoch, ISR and high watermark (SPEC §4). M2 implements the single-node leader subset: the ISR is
// only the local replica, so the high watermark follows the log end offset.
//
// Lock ordering: Partition.mu → (storage Log locks) → Partition.hwMu. Readers of the high
// watermark use an atomic; hwMu only guards the change-notification channel.
package partition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// Sentinel errors.
var (
	// ErrNotLeader reports a request to a replica that is not the leader, or whose metadata is
	// behind the request's epoch (DECISIONS D12).
	ErrNotLeader = errors.New("partition: not leader")
	// ErrFencedLeaderEpoch reports a request carrying an older leader epoch.
	ErrFencedLeaderEpoch = errors.New("partition: fenced leader epoch")
	// ErrNotEnoughReplicas reports acks=all with an ISR smaller than min.insync.replicas.
	ErrNotEnoughReplicas = errors.New("partition: not enough in-sync replicas")
)

// AnyEpoch in a request skips the leader epoch check (DECISIONS D11).
const AnyEpoch int32 = -1

// Special ListOffsets timestamps.
const (
	LatestTimestamp   int64 = -1
	EarliestTimestamp int64 = -2
)

// Acks is the produce acknowledgment mode (SPEC §4.5).
type Acks int

// Acks modes.
const (
	AcksNone Acks = iota
	AcksLeader
	AcksAll
)

// Partition is one local replica.
type Partition struct {
	tp      storage.TopicPartition
	topicID string
	log     *storage.Log
	localID int32
	logger  *slog.Logger

	mu          sync.RWMutex // leader state; held (read) across appends so the epoch can't change mid-append
	isLeader    bool
	leader      int32
	leaderEpoch int32
	isr         []int32
	minInsync   int32

	hw   atomic.Int64
	hwMu sync.Mutex
	hwCh chan struct{} // closed and replaced whenever the high watermark advances
}

// New returns a replica of tp backed by log. It is not a leader until BecomeLeader.
func New(tp storage.TopicPartition, topicID string, log *storage.Log, localID int32, logger *slog.Logger) *Partition {
	return &Partition{
		tp:      tp,
		topicID: topicID,
		log:     log,
		localID: localID,
		leader:  -1,
		logger:  logger.With(slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition))),
		hwCh:    make(chan struct{}),
	}
}

// TopicPartition returns the partition's identity.
func (p *Partition) TopicPartition() storage.TopicPartition { return p.tp }

// TopicID returns the id of the topic this replica belongs to.
func (p *Partition) TopicID() string { return p.topicID }

// Log returns the underlying log.
func (p *Partition) Log() *storage.Log { return p.log }

// BecomeLeader makes the local replica the leader at epoch with the given ISR. The high watermark
// is raised to min(hwCheckpoint, LEO) if that is higher, and then advances as the ISR allows.
func (p *Partition) BecomeLeader(epoch int32, isr []int32, minInsync int32, hwCheckpoint int64) {
	p.mu.Lock()
	p.isLeader = true
	p.leader = p.localID
	p.leaderEpoch = epoch
	p.isr = slices.Clone(isr)
	p.minInsync = minInsync
	// The high watermark is never lowered: consumers may already have read up to it.
	if start := min(max(hwCheckpoint, p.log.LogStartOffset()), p.log.LogEndOffset()); start > p.hw.Load() {
		p.setHW(start)
	}
	p.advanceHWLocked()
	p.mu.Unlock()
	p.logger.Info("became leader", slog.Int("leader_epoch", int(epoch)), slog.Int64("high_watermark", p.hw.Load()))
}

// Leader returns the leader id (-1 if unknown) and leader epoch.
func (p *Partition) Leader() (int32, int32) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.leader, p.leaderEpoch
}

// checkLeader validates a request epoch against the leader state. Caller holds mu (read).
func (p *Partition) checkLeader(requestEpoch int32) error {
	switch {
	case requestEpoch != AnyEpoch && requestEpoch < p.leaderEpoch:
		return fmt.Errorf("%w: request epoch %d < %d", ErrFencedLeaderEpoch, requestEpoch, p.leaderEpoch)
	case requestEpoch != AnyEpoch && requestEpoch > p.leaderEpoch:
		return fmt.Errorf("%w: request epoch %d > known %d", ErrNotLeader, requestEpoch, p.leaderEpoch)
	case !p.isLeader:
		return ErrNotLeader
	}
	return nil
}

// Append appends one batch as leader. With acks=all and an ISR smaller than min.insync.replicas
// it fails with ErrNotEnoughReplicas before appending.
func (p *Partition) Append(ctx context.Context, b record.Batch, requestEpoch int32, acks Acks) (storage.AppendInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkLeader(requestEpoch); err != nil {
		return storage.AppendInfo{}, err
	}
	if acks == AcksAll && int32(len(p.isr)) < p.minInsync {
		return storage.AppendInfo{}, fmt.Errorf("%w: isr %d < min.insync.replicas %d", ErrNotEnoughReplicas, len(p.isr), p.minInsync)
	}
	info, err := p.log.Append(ctx, b, p.leaderEpoch)
	if err != nil {
		return storage.AppendInfo{}, err
	}
	p.advanceHWLocked()
	return info, nil
}

// advanceHWLocked recomputes the high watermark. With ISR == {self} (M2) it is the LEO. Caller
// holds mu (read or write).
func (p *Partition) advanceHWLocked() {
	if p.isLeader && len(p.isr) == 1 && p.isr[0] == p.localID {
		p.setHW(p.log.LogEndOffset())
	}
}

// setHW raises the high watermark to hw (never lowers it) and wakes waiters.
func (p *Partition) setHW(hw int64) {
	for {
		cur := p.hw.Load()
		if hw <= cur {
			return
		}
		if p.hw.CompareAndSwap(cur, hw) {
			break
		}
	}
	p.hwMu.Lock()
	close(p.hwCh)
	p.hwCh = make(chan struct{})
	p.hwMu.Unlock()
}

// HighWatermark returns the high watermark.
func (p *Partition) HighWatermark() int64 { return p.hw.Load() }

// HWChanged returns a channel that is closed the next time the high watermark advances. Callers
// must capture it before reading, so an advance between the read and the wait is not missed.
func (p *Partition) HWChanged() <-chan struct{} {
	p.hwMu.Lock()
	defer p.hwMu.Unlock()
	return p.hwCh
}

// Read returns batches for a consumer starting at offset, bounded by the high watermark, along
// with the high watermark used.
func (p *Partition) Read(ctx context.Context, offset int64, maxBytes int, requestEpoch int32) (storage.ReadResult, int64, error) {
	p.mu.RLock()
	err := p.checkLeader(requestEpoch)
	p.mu.RUnlock()
	if err != nil {
		return storage.ReadResult{}, -1, err
	}
	hw := p.hw.Load()
	res, err := p.log.Read(ctx, offset, maxBytes, hw)
	return res, hw, err
}

// ListOffsets resolves EarliestTimestamp (log start), LatestTimestamp (high watermark) or a
// timestamp >= 0 (earliest offset below the high watermark whose record timestamp is >= it, or -1
// if none).
func (p *Partition) ListOffsets(ctx context.Context, requestEpoch int32, timestamp int64) (int64, error) {
	p.mu.RLock()
	err := p.checkLeader(requestEpoch)
	p.mu.RUnlock()
	if err != nil {
		return -1, err
	}
	switch {
	case timestamp == EarliestTimestamp:
		return p.log.LogStartOffset(), nil
	case timestamp == LatestTimestamp:
		return p.hw.Load(), nil
	case timestamp < 0:
		return -1, fmt.Errorf("partition: invalid list offsets timestamp %d", timestamp)
	}
	off, found, err := p.log.OffsetForTimestamp(ctx, timestamp, p.hw.Load())
	if err != nil {
		return -1, err
	}
	if !found {
		return -1, nil
	}
	return off, nil
}
