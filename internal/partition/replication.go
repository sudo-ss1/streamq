package partition

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// AlterISR retry backoff after a retriable failure (plan R1).
const (
	alterISRBackoffMin = 100 * time.Millisecond
	alterISRBackoffMax = time.Second
)

// ReplicaFetchRequest is a follower's pull (SPEC §4.1).
type ReplicaFetchRequest struct {
	ReplicaID   int32
	LeaderEpoch int32
	FetchOffset int64
	MaxBytes    int
	MaxWait     time.Duration
}

// ReplicaFetchResult carries batches up to the leader's LEO plus the leader's offsets.
type ReplicaFetchResult struct {
	Records        []byte
	HighWatermark  int64
	LogStartOffset int64
	LogEndOffset   int64
}

// HandleReplicaFetch serves a follower's fetch on the leader. It records the follower's progress
// (Kafka's caught-up rule, D41), advances the HW, may start an ISR expansion, and long-polls up to
// MaxWait for new data when the follower is caught up. leoCh is closed on every LEO advance and
// leadership transition, so the wait never polls.
func (p *Partition) HandleReplicaFetch(ctx context.Context, req ReplicaFetchRequest) (ReplicaFetchResult, error) {
	wait, err := p.recordFetch(req)
	if err != nil {
		return ReplicaFetchResult{HighWatermark: p.hw.Load(), LogStartOffset: p.log.LogStartOffset(), LogEndOffset: p.log.LogEndOffset()}, err
	}
	var timer <-chan time.Time
	if req.MaxWait > 0 {
		timer = p.cfg.Clock.After(req.MaxWait)
	}
	for {
		res, err := p.readForReplica(ctx, req)
		if err != nil || len(res.Records) > 0 || timer == nil {
			return res, err
		}
		select {
		case <-wait:
		case <-timer:
			return p.readForReplica(ctx, req)
		case <-ctx.Done():
			return res, nil
		}
		p.hwMu.Lock()
		wait = p.leoCh
		p.hwMu.Unlock()
	}
}

// recordFetch validates the fetch and updates follower state. It returns the LEO channel captured
// before the first read, so an append between the read and the wait is not missed.
func (p *Partition) recordFetch(req ReplicaFetchRequest) (<-chan struct{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkLeader(req.LeaderEpoch); err != nil {
		return nil, err
	}
	f, ok := p.followers[req.ReplicaID]
	if !ok {
		return nil, fmt.Errorf("%w: broker %d is not a replica", ErrNotLeader, req.ReplicaID)
	}
	leo, logStart := p.log.LogEndOffset(), p.log.LogStartOffset()
	if req.FetchOffset > leo || req.FetchOffset < logStart {
		return nil, fmt.Errorf("%w: replica fetch at %d not in [%d, %d]", storage.ErrOffsetOutOfRange, req.FetchOffset, logStart, leo)
	}
	now := p.cfg.Clock.Now()
	// Kafka's caught-up rule (plan R5): caught up now if at the LEO; otherwise caught up as of the
	// previous fetch if this fetch reached the LEO the leader had then.
	if req.FetchOffset >= leo {
		f.lastCaughtUp = now
	} else if f.lastFetchLeaderLEO >= 0 && req.FetchOffset >= f.lastFetchLeaderLEO && f.lastFetchTime.After(f.lastCaughtUp) {
		f.lastCaughtUp = f.lastFetchTime
	}
	f.lastFetchLeaderLEO, f.lastFetchTime = leo, now
	f.leo = req.FetchOffset
	p.advanceHWLocked()
	p.maybeExpandLocked(req.ReplicaID, f)
	p.hwMu.Lock()
	defer p.hwMu.Unlock()
	return p.leoCh, nil
}

// readForReplica reads from FetchOffset up to the LEO (replicas may read past the HW, SPEC §4.2).
func (p *Partition) readForReplica(ctx context.Context, req ReplicaFetchRequest) (ReplicaFetchResult, error) {
	p.mu.RLock()
	err := p.checkLeader(req.LeaderEpoch)
	p.mu.RUnlock()
	if err != nil {
		return ReplicaFetchResult{}, err
	}
	leo := p.log.LogEndOffset()
	out := ReplicaFetchResult{HighWatermark: p.hw.Load(), LogStartOffset: p.log.LogStartOffset(), LogEndOffset: leo}
	if req.FetchOffset >= leo {
		return out, nil
	}
	read, err := p.log.Read(ctx, req.FetchOffset, max(req.MaxBytes, 1), leo)
	if err != nil {
		return out, err
	}
	out.Records = read.Data
	return out, nil
}

// maybeExpandLocked starts an AlterISR adding replica if it is caught up: LEO >= HW and >= the
// start of the leader's current epoch (KIP-841), not fenced, and nothing else pending (D41).
// Caller holds mu (write).
func (p *Partition) maybeExpandLocked(replica int32, f *followerState) {
	if p.pending != nil || slices.Contains(p.isr, replica) || f.leo < p.hw.Load() {
		return
	}
	if start, ok := p.epochs.StartOffset(p.leaderEpoch); !ok || f.leo < start {
		return
	}
	if p.cfg.IsFenced != nil && p.cfg.IsFenced(replica) {
		return
	}
	var isr []int32
	for _, r := range p.replicas { // replica order, as the FSM stores it
		if r == replica || slices.Contains(p.isr, r) {
			isr = append(isr, r)
		}
	}
	p.pending = &pendingISR{isr: isr, leaderEpoch: p.leaderEpoch, partitionEpoch: p.partitionEpoch}
	p.logger.Info("expanding isr", slog.Int("replica", int(replica)), slog.Any("isr", isr))
}

// MaybeShrinkISR starts an AlterISR removing followers that have not caught up within
// replica.lag.max.ms (SPEC §4.3), unless another change is pending.
func (p *Partition) MaybeShrinkISR(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.isLeader || p.pending != nil {
		return
	}
	var keep []int32
	removed := false
	for _, r := range p.isr {
		if f, ok := p.followers[r]; ok && r != p.cfg.LocalID && now.Sub(f.lastCaughtUp) > p.cfg.ReplicaLagMax {
			removed = true
			continue
		}
		keep = append(keep, r)
	}
	if !removed {
		return
	}
	p.pending = &pendingISR{isr: keep, leaderEpoch: p.leaderEpoch, partitionEpoch: p.partitionEpoch}
	p.logger.Warn("shrinking isr", slog.Any("isr", keep))
}

// NextAlterISR returns the pending AlterISR to send, if one needs (re)sending, and marks it in
// flight. Exactly one request per partition is in flight at a time (D42).
func (p *Partition) NextAlterISR() (AlterISRRequest, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pd := p.pending
	if !p.isLeader || pd == nil || pd.inFlight || pd.sent || pd.stopped || p.cfg.Clock.Now().Before(pd.retryAt) {
		return AlterISRRequest{}, false
	}
	pd.inFlight = true
	return AlterISRRequest{LeaderEpoch: pd.leaderEpoch, PartitionEpoch: pd.partitionEpoch, NewISR: slices.Clone(pd.isr)}, true
}

// AlterISRResult records the controller's answer (D45): accepted → wait for metadata; FENCED →
// stop retrying but keep counting the maximal ISR until newer metadata arrives; invalid ISR at
// matching epochs → drop the request; anything else (transport, unknown outcome) → resend.
func (p *Partition) AlterISRResult(req AlterISRRequest, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pd := p.pending
	if pd == nil || pd.leaderEpoch != req.LeaderEpoch || pd.partitionEpoch != req.PartitionEpoch {
		return
	}
	pd.inFlight = false
	switch {
	case err == nil:
		pd.sent = true
	case errors.Is(err, metadata.ErrFencedLeaderEpoch):
		pd.stopped = true
		p.logger.Debug("alter isr fenced; holding pending isr until metadata catches up", slog.Any("error", err))
	case errors.Is(err, metadata.ErrInvalidISR):
		p.pending = nil
		p.logger.Warn("alter isr rejected", slog.Any("error", err))
	default:
		pd.backoff = min(max(pd.backoff*2, alterISRBackoffMin), alterISRBackoffMax)
		pd.retryAt = p.cfg.Clock.Now().Add(pd.backoff)
		p.logger.Debug("alter isr failed; will retry", slog.Any("error", err), slog.Duration("backoff", pd.backoff))
	}
}

// OffsetForLeaderEpoch answers a follower's truncation query on the leader (SPEC §4.4).
func (p *Partition) OffsetForLeaderEpoch(currentEpoch, requested int32) (epoch int32, endOffset, logStart int64, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkLeader(currentEpoch); err != nil {
		return -1, -1, -1, err
	}
	e, end := p.epochs.EndOffsetFor(requested, p.log.LogEndOffset())
	return e, end, p.log.LogStartOffset(), nil
}

// --- follower side ---

// checkFollower validates that this replica follows in fetchEpoch. Caller holds mu (read).
func (p *Partition) checkFollower(fetchEpoch int32) error {
	if p.isLeader || p.leaderEpoch != fetchEpoch {
		return fmt.Errorf("%w: not following in epoch %d", ErrNotLeader, fetchEpoch)
	}
	return nil
}

// AppendAsFollower appends a replicated batch if this replica still follows in fetchEpoch (a
// stale fetcher's write is refused, plan R8). A batch starting a newer epoch is recorded in the
// epoch cache before it is appended (plan R7).
func (p *Partition) AppendAsFollower(ctx context.Context, fetchEpoch int32, b record.Batch) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if err := p.checkFollower(fetchEpoch); err != nil {
		return err
	}
	if latest, ok := p.epochs.LatestEpoch(); !ok || b.LeaderEpoch() > latest {
		if err := p.epochs.Assign(ctx, b.LeaderEpoch(), b.BaseOffset()); err != nil {
			return err
		}
	}
	if _, err := p.log.AppendAsFollower(ctx, b); err != nil {
		// Drop an epoch entry assigned for a batch that did not land.
		if terr := p.epochs.TruncateFromEnd(ctx, p.log.LogEndOffset()); terr != nil {
			err = errors.Join(err, terr)
		}
		return fmt.Errorf("append replicated batch: %w", err)
	}
	return nil
}

// UpdateFollowerHW sets a follower's HW to min(leader HW, own LEO) (D40).
func (p *Partition) UpdateFollowerHW(fetchEpoch int32, leaderHW int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.checkFollower(fetchEpoch) != nil {
		return
	}
	p.setHW(min(leaderHW, p.log.LogEndOffset()))
}

// TruncateTo truncates a follower's log and epoch cache at offset and lowers the HW if needed
// (FR4: a later leader must never expose truncated data). Epoch entries starting at or after the
// new LEO are dropped even when the log itself is unchanged (an empty epoch left by an earlier
// leadership). It returns the new LEO.
func (p *Partition) TruncateTo(ctx context.Context, fetchEpoch int32, offset int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkFollower(fetchEpoch); err != nil {
		return 0, err
	}
	leo, err := p.log.TruncateTo(ctx, offset)
	if err != nil {
		return 0, err
	}
	if err := p.epochs.TruncateFromEnd(ctx, leo); err != nil {
		return 0, err
	}
	if p.hw.Load() > leo {
		p.hw.Store(leo) // the only place the HW is lowered
	}
	return leo, nil
}

// TruncateFullyAndStartAt empties a follower's log, restarting it at offset (the leader's log
// start), and resets the epoch cache and HW.
func (p *Partition) TruncateFullyAndStartAt(ctx context.Context, fetchEpoch int32, offset int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.checkFollower(fetchEpoch); err != nil {
		return err
	}
	// Clear the epoch cache first: a crash in between leaves the old log with no epochs (the HW
	// fallback), never an empty log with stale epochs.
	if err := p.epochs.TruncateFromEnd(ctx, -1); err != nil {
		return err
	}
	if err := p.log.TruncateFullyAndStartAt(ctx, offset); err != nil {
		return err
	}
	p.hw.Store(offset)
	return nil
}

// LatestEpoch returns the newest leader epoch in this replica's log.
func (p *Partition) LatestEpoch() (int32, bool) { return p.epochs.LatestEpoch() }

// LocalEndOffsetFor returns this replica's own end offset for epoch (KIP-279 iteration).
func (p *Partition) LocalEndOffsetFor(epoch int32) (int32, int64) {
	return p.epochs.EndOffsetFor(epoch, p.log.LogEndOffset())
}

// SyncLogStart drops epoch-cache entries below the log start after retention.
func (p *Partition) SyncLogStart(ctx context.Context) error {
	return p.epochs.TruncateFromStart(ctx, p.log.LogStartOffset())
}
