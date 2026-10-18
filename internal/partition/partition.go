// Package partition holds the replica state of one partition on one broker (SPEC §4): leadership
// and leader epoch, the committed ISR and any pending AlterISR (KIP-497 maximal ISR), follower
// progress on the leader, the high watermark, and the leader-epoch cache. Fetcher goroutines live
// in internal/broker; this package only provides the per-partition operations they call.
//
// Lock ordering: Partition.mu → (storage Log locks, EpochCache.mu) → Partition.hwMu. The HW is an
// atomic; hwMu guards only the wake-up channels. No partition lock is held while waiting.
package partition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sudo-ss1/streamq/internal/metadata"
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
	// ErrNotEnoughReplicas reports acks=all with an ISR smaller than min.insync.replicas, before
	// or after the append (DECISIONS D44).
	ErrNotEnoughReplicas = errors.New("partition: not enough in-sync replicas")
	// ErrRequestTimedOut reports an acks=all produce whose batch was not committed in time; it may
	// still commit (SPEC §4.5).
	ErrRequestTimedOut = errors.New("partition: request timed out")
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

// Clock abstracts time for follower lag tracking and produce timeouts.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config configures a Partition.
type Config struct {
	LocalID int32
	Clock   Clock
	// ReplicaLagMax is replica.lag.max.ms: a follower not caught up for longer leaves the ISR.
	ReplicaLagMax time.Duration
	// IsFenced reports whether a broker is fenced in local metadata (ISR expansion skips fenced
	// brokers); nil means none are.
	IsFenced func(brokerID int32) bool
}

// followerState is the leader's view of one follower (Kafka's replica state).
type followerState struct {
	leo                int64 // -1 = unknown
	lastFetchLeaderLEO int64
	lastFetchTime      time.Time
	lastCaughtUp       time.Time
}

// pendingISR is an AlterISR the leader requested but has not seen committed.
type pendingISR struct {
	isr            []int32
	leaderEpoch    int32
	partitionEpoch int32
	inFlight       bool          // a request is being sent
	retryAt        time.Time     // after a retriable failure, not resent before this
	backoff        time.Duration // next retry delay (100ms doubling to 1s)
	sent           bool          // the controller accepted it: wait for metadata
	stopped        bool          // FENCED: newer partition state exists; wait for metadata (D45)
}

// AlterISRRequest is an ISR change for the controller.
type AlterISRRequest struct {
	LeaderEpoch    int32
	PartitionEpoch int32
	NewISR         []int32
}

// Partition is one local replica.
type Partition struct {
	tp      storage.TopicPartition
	topicID string
	log     *storage.Log
	epochs  *EpochCache
	cfg     Config
	logger  *slog.Logger

	mu             sync.RWMutex // replica state; held (read) across appends so the epoch can't change mid-append
	isLeader       bool
	leader         int32
	leaderEpoch    int32
	partitionEpoch int32
	replicas       []int32
	isr            []int32 // committed ISR
	minInsync      int32
	pending        *pendingISR
	followers      map[int32]*followerState
	hwLoaded       bool // the HW checkpoint was applied (only the first transition uses it)
	removed        bool // the partition was deleted or the broker is closing: waiters give up

	hw      atomic.Int64
	hwMu    sync.Mutex
	hwCh    chan struct{} // closed and replaced when the HW advances
	stateCh chan struct{} // closed and replaced on every leadership transition
	leoCh   chan struct{} // closed and replaced when the leader's LEO advances (replica long-polls)
}

// New returns a replica of tp backed by log and its epoch cache. It is neither leader nor
// follower until a transition is applied.
func New(tp storage.TopicPartition, topicID string, log *storage.Log, epochs *EpochCache, cfg Config, logger *slog.Logger) *Partition {
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	if cfg.ReplicaLagMax <= 0 {
		cfg.ReplicaLagMax = 10 * time.Second
	}
	return &Partition{
		tp:             tp,
		topicID:        topicID,
		log:            log,
		epochs:         epochs,
		cfg:            cfg,
		leader:         -1,
		leaderEpoch:    -1,
		partitionEpoch: -1,
		logger:         logger.With(slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition))),
		hwCh:           make(chan struct{}),
		stateCh:        make(chan struct{}),
		leoCh:          make(chan struct{}),
	}
}

// TopicPartition returns the partition's identity.
func (p *Partition) TopicPartition() storage.TopicPartition { return p.tp }

// TopicID returns the id of the topic this replica belongs to.
func (p *Partition) TopicID() string { return p.topicID }

// Log returns the underlying log.
func (p *Partition) Log() *storage.Log { return p.log }

// State is a partition's replicated metadata as seen by this replica.
type State struct {
	Leader         int32
	LeaderEpoch    int32
	PartitionEpoch int32
	Replicas       []int32
	ISR            []int32
	MinInsync      int32
	HWCheckpoint   int64 // used only on the first transition after the replica opens
}

// FromMetadata converts a metadata partition state.
func FromMetadata(ps metadata.PartitionState, minInsync int32, hwCheckpoint int64) State {
	return State{Leader: ps.Leader, LeaderEpoch: ps.LeaderEpoch, PartitionEpoch: ps.PartitionEpoch,
		Replicas: ps.Replicas, ISR: ps.ISR, MinInsync: minInsync, HWCheckpoint: hwCheckpoint}
}

// Transition is what applying a State requires.
type Transition int

// Transitions.
const (
	TransitionNone     Transition = iota // stale or identical: ignore
	TransitionISR                        // same leadership, newer partition epoch: ApplyISR
	TransitionLeader                     // become leader: BecomeLeader
	TransitionFollower                   // become (or move) follower: BecomeFollower
)

// stale reports whether st is older than or equal to the current state. Caller holds mu.
func (p *Partition) stale(st State) bool {
	return st.LeaderEpoch < p.leaderEpoch || (st.LeaderEpoch == p.leaderEpoch && st.PartitionEpoch <= p.partitionEpoch)
}

// Classify returns the transition st requires, without changing anything (plan R8: the broker
// stops fetchers and then applies it). Metadata is applied monotonically (plan R6).
func (p *Partition) Classify(st State) Transition {
	p.mu.RLock()
	defer p.mu.RUnlock()
	switch {
	case p.stale(st):
		return TransitionNone
	case st.LeaderEpoch == p.leaderEpoch && st.Leader == p.leader &&
		(st.Leader == p.cfg.LocalID) == p.isLeader:
		return TransitionISR
	case st.Leader == p.cfg.LocalID:
		return TransitionLeader
	default:
		return TransitionFollower
	}
}

// ApplyISR applies a newer partition epoch at the same leadership: the committed ISR and
// replicas change, and a pending AlterISR is resolved (D45).
func (p *Partition) ApplyISR(st State) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stale(st) || st.LeaderEpoch != p.leaderEpoch {
		return false
	}
	p.partitionEpoch = st.PartitionEpoch
	p.replicas = slices.Clone(st.Replicas)
	p.minInsync = st.MinInsync
	p.pending = nil
	if p.isLeader {
		p.isr = slices.Clone(st.ISR)
		p.advanceHWLocked()
	}
	return true
}

// BecomeLeader makes this replica leader at st.LeaderEpoch. It records (epoch, LEO) in the
// leader-epoch cache before accepting appends, resets follower progress, raises the HW to
// min(checkpoint, LEO) if higher, and wakes every waiter.
func (p *Partition) BecomeLeader(ctx context.Context, st State) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stale(st) {
		return nil
	}
	leo := p.log.LogEndOffset()
	if err := p.epochs.Assign(ctx, st.LeaderEpoch, leo); err != nil {
		return fmt.Errorf("record leader epoch: %w", err)
	}
	now := p.cfg.Clock.Now()
	p.isLeader, p.leader = true, p.cfg.LocalID
	p.leaderEpoch, p.partitionEpoch = st.LeaderEpoch, st.PartitionEpoch
	p.replicas, p.isr = slices.Clone(st.Replicas), slices.Clone(st.ISR)
	p.minInsync, p.pending = st.MinInsync, nil
	p.followers = map[int32]*followerState{}
	for _, r := range st.Replicas {
		if r != p.cfg.LocalID {
			p.followers[r] = &followerState{leo: -1, lastFetchLeaderLEO: -1, lastCaughtUp: now}
		}
	}
	p.loadHWCheckpoint(st.HWCheckpoint)
	p.advanceHWLocked()
	p.wakeTransition()
	p.logger.Info("became leader", slog.Int("leader_epoch", int(st.LeaderEpoch)), slog.Int64("high_watermark", p.hw.Load()))
	return nil
}

// BecomeFollower makes this replica a follower of st.Leader (-1 = none) at st.LeaderEpoch.
func (p *Partition) BecomeFollower(st State) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stale(st) {
		return
	}
	p.isLeader, p.leader = false, st.Leader
	p.leaderEpoch, p.partitionEpoch = st.LeaderEpoch, st.PartitionEpoch
	p.replicas = slices.Clone(st.Replicas)
	p.isr, p.pending, p.followers = nil, nil, nil
	p.minInsync = st.MinInsync
	p.loadHWCheckpoint(st.HWCheckpoint)
	p.wakeTransition()
	p.logger.Info("became follower", slog.Int("leader", int(st.Leader)), slog.Int("leader_epoch", int(st.LeaderEpoch)))
}

// loadHWCheckpoint raises the HW to min(checkpoint, LEO) on the first transition after the replica
// opens, so a restarted replica resumes from its checkpointed HW (Kafka) and the checkpoint is not
// overwritten with 0. Later transitions ignore it: the in-memory HW is authoritative, and a stale
// checkpoint must never raise the HW over data a truncation removed. Caller holds mu.
func (p *Partition) loadHWCheckpoint(ckpt int64) {
	if p.hwLoaded {
		return
	}
	p.hwLoaded = true
	if start := min(max(ckpt, p.log.LogStartOffset()), p.log.LogEndOffset()); start > p.hw.Load() {
		p.setHW(start)
	}
}

// Remove marks the replica deleted (or its broker closing): acks=all waiters return ErrNotLeader
// and replica long-polls end.
func (p *Partition) Remove() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removed = true
	p.wakeTransition()
}

// wakeTransition wakes produce waiters and replica long-polls on a leadership change. Caller
// holds mu.
func (p *Partition) wakeTransition() {
	p.hwMu.Lock()
	defer p.hwMu.Unlock()
	close(p.stateCh)
	p.stateCh = make(chan struct{})
	close(p.leoCh)
	p.leoCh = make(chan struct{})
}

// Leader returns the leader id (-1 if unknown) and leader epoch.
func (p *Partition) Leader() (int32, int32) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.leader, p.leaderEpoch
}

// IsLeader reports whether this replica is the leader.
func (p *Partition) IsLeader() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.isLeader
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

// Append appends one batch as leader and returns the leader epoch it was appended in. With
// acks=all and a committed ISR smaller than min.insync.replicas it fails with
// ErrNotEnoughReplicas before appending.
func (p *Partition) Append(ctx context.Context, b record.Batch, requestEpoch int32, acks Acks) (storage.AppendInfo, int32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkLeader(requestEpoch); err != nil {
		return storage.AppendInfo{}, 0, err
	}
	if p.removed {
		return storage.AppendInfo{}, 0, fmt.Errorf("%w: partition removed or broker closing", ErrNotLeader)
	}
	if acks == AcksAll && int32(len(p.isr)) < p.minInsync {
		return storage.AppendInfo{}, 0, fmt.Errorf("%w: isr %d < min.insync.replicas %d", ErrNotEnoughReplicas, len(p.isr), p.minInsync)
	}
	info, err := p.log.Append(ctx, b, p.leaderEpoch)
	if err != nil {
		return storage.AppendInfo{}, 0, err
	}
	p.advanceHWLocked()
	p.hwMu.Lock()
	close(p.leoCh)
	p.leoCh = make(chan struct{})
	p.hwMu.Unlock()
	return info, p.leaderEpoch, nil
}

// WaitForCommit is the acks=all purgatory (SPEC §4.5, D43): it waits until the HW passes
// lastOffset while this replica is still leader in epoch, then requires the committed ISR to
// still satisfy min.insync (D44). Leadership loss returns ErrNotLeader; the timeout returns
// ErrRequestTimedOut. No polling: it waits on the HW and leadership channels.
func (p *Partition) WaitForCommit(ctx context.Context, lastOffset int64, epoch int32, timeout time.Duration) error {
	timer := p.cfg.Clock.After(timeout)
	for {
		p.hwMu.Lock()
		hwCh, stateCh := p.hwCh, p.stateCh // captured before checking
		p.hwMu.Unlock()
		p.mu.RLock()
		leading := p.isLeader && p.leaderEpoch == epoch
		committed := p.hw.Load() > lastOffset
		isr, minIsr := len(p.isr), p.minInsync
		removed := p.removed
		p.mu.RUnlock()
		switch {
		case !leading:
			return fmt.Errorf("%w: leadership changed before offset %d committed", ErrNotLeader, lastOffset)
		case !committed && removed:
			return fmt.Errorf("%w: partition removed or broker closing before offset %d committed", ErrNotLeader, lastOffset)
		case committed && int32(isr) < minIsr:
			return fmt.Errorf("%w: committed with isr %d < min.insync.replicas %d", ErrNotEnoughReplicas, isr, minIsr)
		case committed:
			return nil
		}
		select {
		case <-hwCh:
		case <-stateCh:
		case <-timer:
			return fmt.Errorf("%w: offset %d not committed within %v", ErrRequestTimedOut, lastOffset, timeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// maximalISR returns committed ISR ∪ pending additions (KIP-497, D45). Caller holds mu.
func (p *Partition) maximalISR() []int32 {
	if p.pending == nil {
		return p.isr
	}
	out := slices.Clone(p.isr)
	for _, r := range p.pending.isr {
		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}
	return out
}

// advanceHWLocked raises the HW to the min LEO over the maximal ISR (D40, D45). A member with an
// unknown LEO holds the HW where it is. Caller holds mu (read or write).
func (p *Partition) advanceHWLocked() {
	if !p.isLeader {
		return
	}
	hw := p.log.LogEndOffset()
	for _, r := range p.maximalISR() {
		if r == p.cfg.LocalID {
			continue
		}
		f := p.followers[r]
		if f == nil || f.leo < 0 {
			return
		}
		hw = min(hw, f.leo)
	}
	p.setHW(hw)
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

// Read returns batches for a consumer starting at offset, bounded by the high watermark (FR4),
// along with the high watermark used.
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
// if none). It also returns the found record's timestamp for a timestamp lookup, else -1.
func (p *Partition) ListOffsets(ctx context.Context, requestEpoch int32, timestamp int64) (offset, recordTimestamp int64, err error) {
	p.mu.RLock()
	err = p.checkLeader(requestEpoch)
	p.mu.RUnlock()
	if err != nil {
		return -1, -1, err
	}
	switch {
	case timestamp == EarliestTimestamp:
		return p.log.LogStartOffset(), -1, nil
	case timestamp == LatestTimestamp:
		return p.hw.Load(), -1, nil
	case timestamp < 0:
		return -1, -1, fmt.Errorf("partition: invalid list offsets timestamp %d", timestamp)
	}
	off, ts, found, err := p.log.OffsetForTimestamp(ctx, timestamp, p.hw.Load())
	if err != nil {
		return -1, -1, err
	}
	if !found {
		return -1, -1, nil
	}
	return off, ts, nil
}
