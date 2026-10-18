package broker

// Replication (SPEC §4): one fetcher goroutine per followed partition, the ISR loop (shrink
// checks and AlterISR requests) and the leader-side replica RPCs.
//
// Lock ordering: Broker.applyMu → Broker.fetchMu. Fetchers and AlterISR senders hold no broker or
// partition lock while blocked on RPCs; stopFetcher (called under applyMu) cancels and joins.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// ErrStaleBrokerEpoch reports a replica fetch from an older incarnation of the follower (KIP-903).
var ErrStaleBrokerEpoch = errors.New("broker: stale broker epoch")

// errDiverged makes the fetcher re-run truncation (fetch offset beyond the leader's LEO).
var errDiverged = errors.New("broker: follower log diverged from leader")

// errCheckpointPending reports a truncation whose checkpoints are not yet durable; the fetcher
// retries the checkpoint before any further append (plan R7, R20).
var errCheckpointPending = errors.New("broker: checkpoint after truncation pending")

// Replication defaults.
const (
	DefaultReplicaFetchWait     = 500 * time.Millisecond
	DefaultReplicaFetchMaxBytes = 1 << 20
	DefaultISRCheckInterval     = 100 * time.Millisecond
	fetchBackoffMin             = 50 * time.Millisecond
	fetchBackoffMax             = time.Second
)

// ReplicaFetchRequest is a follower's pull for one partition.
type ReplicaFetchRequest struct {
	TP          storage.TopicPartition
	ReplicaID   int32
	BrokerEpoch int64
	LeaderEpoch int32
	FetchOffset int64
	MaxBytes    int
	MaxWait     time.Duration
}

// EpochEndOffset is a leader's OffsetForLeaderEpoch answer.
type EpochEndOffset struct {
	Epoch          int32 // largest epoch <= the requested one, or -1
	EndOffset      int64 // its end offset, or -1
	LogStartOffset int64
}

// LeaderClient sends follower RPCs to a partition leader. Errors are this package's, partition's
// or storage's sentinels; on storage.ErrOffsetOutOfRange the result still carries the leader's
// log start and end offsets.
type LeaderClient interface {
	ReplicaFetch(ctx context.Context, leaderID int32, req ReplicaFetchRequest) (partition.ReplicaFetchResult, error)
	OffsetForLeaderEpoch(ctx context.Context, leaderID int32, tp storage.TopicPartition, currentEpoch, requestedEpoch int32) (EpochEndOffset, error)
}

// ISRSender sends an AlterISR for tp to the controller. A nil error means the controller applied
// it; metadata.ErrFencedLeaderEpoch and metadata.ErrInvalidISR are definitive rejections.
type ISRSender func(ctx context.Context, tp storage.TopicPartition, req partition.AlterISRRequest) error

// fetcher is a running replica fetcher.
type fetcher struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// SetBrokerEpoch records this broker's registration epoch; fetchers send it with each request.
func (b *Broker) SetBrokerEpoch(epoch int64) { b.brokerEpoch.Store(epoch) }

// applyState applies a metadata partition state to p, stopping and starting its fetcher around
// leadership changes (plan R8). Caller holds applyMu.
func (b *Broker) applyState(ctx context.Context, p *partition.Partition, st partition.State) {
	tp := p.TopicPartition()
	switch p.Classify(st) {
	case partition.TransitionNone:
	case partition.TransitionISR:
		p.ApplyISR(st)
	case partition.TransitionLeader:
		b.stopFetcher(tp)
		if err := p.BecomeLeader(ctx, st); err != nil {
			b.logger.Error("become leader failed", slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition)), slog.Any("error", err))
		}
	case partition.TransitionFollower:
		b.stopFetcher(tp)
		p.BecomeFollower(st)
		b.startFetcher(p, st.Leader, st.LeaderEpoch) //nolint:contextcheck // fetchers live on the broker's run ctx, not the metadata callback's
	}
}

// startFetcher starts following leader in epoch. Caller holds applyMu.
func (b *Broker) startFetcher(p *partition.Partition, leader, epoch int32) {
	if leader < 0 || b.cfg.LeaderClient == nil {
		return
	}
	b.fetchMu.Lock()
	defer b.fetchMu.Unlock()
	if b.fetchersClosed {
		return
	}
	ctx, cancel := context.WithCancel(b.runCtx)
	f := &fetcher{cancel: cancel, done: make(chan struct{})}
	b.fetchers[p.TopicPartition()] = f
	go func() {
		defer close(f.done)
		b.runFetcher(ctx, p, leader, epoch)
	}()
}

// stopFetcher cancels and joins tp's fetcher, if any. Caller holds applyMu.
func (b *Broker) stopFetcher(tp storage.TopicPartition) {
	b.fetchMu.Lock()
	f, ok := b.fetchers[tp]
	delete(b.fetchers, tp)
	b.fetchMu.Unlock()
	if ok {
		f.cancel()
		<-f.done
	}
}

// stopAllFetchers stops every fetcher and refuses new ones (Close).
func (b *Broker) stopAllFetchers() {
	b.fetchMu.Lock()
	b.fetchersClosed = true
	fs := b.fetchers
	b.fetchers = map[storage.TopicPartition]*fetcher{}
	b.fetchMu.Unlock()
	for _, f := range fs {
		f.cancel()
	}
	for _, f := range fs {
		<-f.done
	}
}

// runFetcher replicates p from leader until ctx ends (plan R3: it never exits on its own). It
// first truncates to the point where its log agrees with the leader's (KIP-279), then fetches.
func (b *Broker) runFetcher(ctx context.Context, p *partition.Partition, leader, epoch int32) {
	tp := p.TopicPartition()
	logger := b.logger.With(slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition)),
		slog.Int("leader", int(leader)), slog.Int("leader_epoch", int(epoch)))
	backoff := fetchBackoffMin
	truncated, ckptPending := false, false
	for ctx.Err() == nil {
		var err error
		switch {
		case ckptPending:
			if err = b.checkpointAfterTruncation(ctx); err == nil {
				ckptPending = false
				continue
			}
		case !truncated:
			err = b.truncateFollower(ctx, p, leader, epoch)
			truncated = err == nil
		default:
			err = b.fetchOnce(ctx, p, leader, epoch)
			if errors.Is(err, errDiverged) || errors.Is(err, storage.ErrOffsetMismatch) {
				truncated = false
			}
		}
		if errors.Is(err, errCheckpointPending) {
			ckptPending = true
		}
		if err == nil {
			backoff = fetchBackoffMin
			continue
		}
		if ctx.Err() != nil {
			return
		}
		logger.Debug("replica fetch failed; backing off", slog.Any("error", err), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return
		case <-b.clock.After(backoff):
		}
		backoff = min(backoff*2, fetchBackoffMax)
	}
}

// truncateFollower finds the offset where p's log diverges from the leader's and truncates there
// (SPEC §4.4, KIP-279, D46). It asks for the end of its latest epoch; the leader answers with the
// largest epoch E' it has at or below it and E”s end offset. If p also has E', the logs agree up
// to min(leader end, p's own end of E'): truncate there (dropping p's later, possibly empty,
// epochs) and stop. If p lacks E', truncate to p's end of the largest epoch below E' and ask
// again. Without epoch information it falls back to the local high watermark.
func (b *Broker) truncateFollower(ctx context.Context, p *partition.Partition, leader, epoch int32) error {
	for {
		leo := p.Log().LogEndOffset()
		latest, ok := p.LatestEpoch()
		if !ok {
			return b.truncate(ctx, p, epoch, min(p.HighWatermark(), leo), -1)
		}
		r, err := b.cfg.LeaderClient.OffsetForLeaderEpoch(ctx, leader, p.TopicPartition(), epoch, latest)
		if err != nil {
			return fmt.Errorf("offset for leader epoch: %w", err)
		}
		if r.Epoch < 0 || r.EndOffset < 0 {
			return b.truncate(ctx, p, epoch, min(p.HighWatermark(), leo), r.LogStartOffset)
		}
		localEpoch, localEnd := p.LocalEndOffsetFor(r.Epoch)
		switch {
		case localEpoch < 0:
			// No local epoch at or below the leader's: Kafka truncates to the leader's end offset.
			return b.truncate(ctx, p, epoch, min(leo, r.EndOffset), r.LogStartOffset)
		case localEpoch == r.Epoch:
			return b.truncate(ctx, p, epoch, min(leo, r.EndOffset, localEnd), r.LogStartOffset)
		}
		if err := b.truncate(ctx, p, epoch, min(leo, r.EndOffset, localEnd), r.LogStartOffset); err != nil {
			return err
		}
		if next, ok := p.LatestEpoch(); ok && next >= latest {
			return fmt.Errorf("truncation made no progress at epoch %d", latest) // never loop forever
		}
	}
}

// truncate truncates p to target (also dropping epoch entries at or after it), or empties it at
// leaderLogStart when target is below the local log start, then makes the recovery point durable
// before any further append (plan R7, R20).
func (b *Broker) truncate(ctx context.Context, p *partition.Partition, epoch int32, target, leaderLogStart int64) error {
	before := p.Log().LogEndOffset()
	if target < p.Log().LogStartOffset() && leaderLogStart >= 0 {
		if err := p.TruncateFullyAndStartAt(ctx, epoch, leaderLogStart); err != nil {
			return err
		}
	} else {
		leo, err := p.TruncateTo(ctx, epoch, min(max(target, p.Log().LogStartOffset()), before))
		if err != nil {
			return err
		}
		if leo == before {
			return nil // log unchanged: nothing to checkpoint
		}
	}
	return b.checkpointAfterTruncation(ctx)
}

// checkpointAfterTruncation makes the lowered recovery point and HW durable before any further
// append (plan R7, R20), so a crash cannot restore a pre-truncation HW over refetched data.
func (b *Broker) checkpointAfterTruncation(ctx context.Context) error {
	if err := b.mgr.CheckpointRecoveryPoints(ctx); err != nil {
		return fmt.Errorf("%w: %w", errCheckpointPending, err)
	}
	if err := b.writeHWCheckpoint(ctx); err != nil {
		return fmt.Errorf("%w: %w", errCheckpointPending, err)
	}
	return nil
}

// fetchOnce fetches from the leader once and appends what it got.
func (b *Broker) fetchOnce(ctx context.Context, p *partition.Partition, leader, epoch int32) error {
	tp := p.TopicPartition()
	leo := p.Log().LogEndOffset()
	res, err := b.cfg.LeaderClient.ReplicaFetch(ctx, leader, ReplicaFetchRequest{
		TP: tp, ReplicaID: b.cfg.BrokerID, BrokerEpoch: b.brokerEpoch.Load(), LeaderEpoch: epoch,
		FetchOffset: leo, MaxBytes: b.cfg.ReplicaFetchMaxBytes, MaxWait: b.cfg.ReplicaFetchWait,
	})
	if errors.Is(err, storage.ErrOffsetOutOfRange) {
		if leo < res.LogStartOffset {
			// Entirely below the leader's log start (retention ran there): start over at it.
			if err := p.TruncateFullyAndStartAt(ctx, epoch, res.LogStartOffset); err != nil {
				return err
			}
			return b.checkpointAfterTruncation(ctx)
		}
		return fmt.Errorf("%w: %w", errDiverged, err)
	}
	if err != nil {
		return err
	}
	if len(res.Records) > 0 {
		batches, err := record.SplitBatches(res.Records)
		if err != nil {
			return fmt.Errorf("replica fetch response: %w", err)
		}
		for _, batch := range batches {
			if err := p.AppendAsFollower(ctx, epoch, batch); err != nil {
				return err
			}
		}
	}
	p.UpdateFollowerHW(epoch, res.HighWatermark)
	return nil
}

// runISR is the ISR loop: on each tick every led partition checks for lagging followers and
// sends its pending AlterISR (one in flight per partition), and every partition drops epoch-cache
// entries below its log start after retention (plan R10).
func (b *Broker) runISR(ctx context.Context) error {
	var senders sync.WaitGroup
	defer senders.Wait()
	t := time.NewTicker(b.cfg.ISRCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		now := b.clock.Now()
		b.mu.RLock()
		parts := make([]*partition.Partition, 0, len(b.partitions))
		for _, p := range b.partitions {
			parts = append(parts, p)
		}
		b.mu.RUnlock()
		b.syncLogStarts(ctx)
		for _, p := range parts {
			if b.cfg.AlterISR == nil || !p.IsLeader() || b.shutdownCtx.Err() != nil {
				// Closing: followers' fetches fail now, so never shrink to the leader alone.
				continue
			}
			p.MaybeShrinkISR(now)
			req, ok := p.NextAlterISR()
			if !ok {
				continue
			}
			senders.Add(1)
			go func() {
				defer senders.Done()
				sctx, cancel := context.WithTimeout(ctx, b.cfg.ISRRequestTimeout)
				defer cancel()
				p.AlterISRResult(req, b.cfg.AlterISR(sctx, p.TopicPartition(), req))
			}()
		}
	}
}

// syncLogStarts drops epoch-cache entries below each partition's log start after retention. It
// holds applyMu so it never writes the checkpoint of a partition being deleted (plan R8).
func (b *Broker) syncLogStarts(ctx context.Context) {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	b.mu.RLock()
	parts := make([]*partition.Partition, 0, len(b.partitions))
	for _, p := range b.partitions {
		parts = append(parts, p)
	}
	b.mu.RUnlock()
	for _, p := range parts {
		if err := p.SyncLogStart(ctx); err != nil && ctx.Err() == nil {
			b.logger.Warn("epoch cache log-start sync failed", slog.String("topic", p.TopicPartition().Topic),
				slog.Int("partition", int(p.TopicPartition().Partition)), slog.Any("error", err))
		}
	}
}

// ReplicaFetch serves a follower's fetch on the leader (SPEC §4.1).
func (b *Broker) ReplicaFetch(ctx context.Context, req ReplicaFetchRequest) (partition.ReplicaFetchResult, error) {
	if b.shutdownCtx.Err() != nil {
		// Fail rather than answer empty, so remote fetchers back off instead of spinning.
		return partition.ReplicaFetchResult{}, fmt.Errorf("%w: %w", partition.ErrNotLeader, ErrShuttingDown)
	}
	p, ok := b.Partition(req.TP)
	if !ok {
		return partition.ReplicaFetchResult{}, ErrUnknownPartition
	}
	if req.BrokerEpoch > 0 {
		if br, ok := b.store.Snapshot().Broker(req.ReplicaID); ok && req.BrokerEpoch < br.Epoch {
			return partition.ReplicaFetchResult{}, fmt.Errorf("%w: broker %d epoch %d < %d", ErrStaleBrokerEpoch, req.ReplicaID, req.BrokerEpoch, br.Epoch)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(b.shutdownCtx, cancel) //nolint:contextcheck // shutdown ends long-polls promptly
	defer stop()
	return p.HandleReplicaFetch(ctx, partition.ReplicaFetchRequest{
		ReplicaID: req.ReplicaID, LeaderEpoch: req.LeaderEpoch, FetchOffset: req.FetchOffset,
		MaxBytes: min(max(req.MaxBytes, 1), MaxFetchBytes), MaxWait: min(max(req.MaxWait, 0), MaxFetchWait),
	})
}

// OffsetForLeaderEpoch answers a follower's truncation query on the leader (SPEC §4.4).
func (b *Broker) OffsetForLeaderEpoch(tp storage.TopicPartition, currentEpoch, requestedEpoch int32) (EpochEndOffset, error) {
	p, ok := b.Partition(tp)
	if !ok {
		return EpochEndOffset{Epoch: -1, EndOffset: -1, LogStartOffset: -1}, ErrUnknownPartition
	}
	e, end, logStart, err := p.OffsetForLeaderEpoch(currentEpoch, requestedEpoch)
	return EpochEndOffset{Epoch: e, EndOffset: end, LogStartOffset: logStart}, err
}

// retentionLimit bounds retention by each partition's high watermark (plan R10); logs without an
// open partition are left alone.
func (b *Broker) retentionLimit(tp storage.TopicPartition) int64 {
	if p, ok := b.Partition(tp); ok {
		return p.HighWatermark()
	}
	return 0
}

// isFenced reports whether broker id is fenced in local metadata (ISR expansion skips it).
func (b *Broker) isFenced(id int32) bool {
	br, ok := b.store.Snapshot().Broker(id)
	return !ok || br.Fenced
}
