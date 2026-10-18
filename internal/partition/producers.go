package partition

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// afterAppendLocked writes the producer snapshot at a roll point (state of every batch before
// this one) and then records the batch (SPEC §6). A failed snapshot is only logged: recovery
// replays more (D50). Caller holds prodMu.
func (p *Partition) afterAppendLocked(ctx context.Context, b record.Batch, info storage.AppendInfo) {
	// Invalid (stale or partial) state is never persisted: a later rebuild would trust it.
	if info.RolledAt >= 0 && p.prodValid {
		if err := p.prod.WriteSnapshot(ctx, info.RolledAt); err != nil {
			p.logger.Warn("producer snapshot on roll failed", slog.Int64("offset", info.RolledAt), slog.Any("error", err))
		}
	}
	p.prod.UpdateFromBatch(b)
}

// rebuildProducersLocked rebuilds producer state for the current log; on failure the state is
// marked invalid so idempotent appends are refused until a retry succeeds (M7 plan R1). Caller
// holds prodMu, which keeps the LEO stable (every append and truncation takes it, before touching
// the log).
func (p *Partition) rebuildProducersLocked(ctx context.Context) error {
	// Snapshots above a truncation point describe removed data; a failed truncation may have left
	// them behind, and later appends can bring the LEO back above them.
	if p.prodTruncatedAt >= 0 {
		if err := p.prod.TruncateSnapshots(ctx, p.prodTruncatedAt); err != nil {
			p.prodValid = false
			return fmt.Errorf("truncate producer snapshots: %w", err)
		}
	}
	if err := p.prod.Rebuild(ctx, p.replay, p.log.LogStartOffset(), p.log.LogEndOffset()); err != nil {
		p.prodValid = false
		p.logger.Warn("producer state rebuild failed; idempotent appends refused until it succeeds", slog.Any("error", err))
		return err
	}
	p.prodValid = true
	p.prodTruncatedAt = -1
	p.prodLogStart = p.log.LogStartOffset()
	return nil
}

// ensureProducersLocked retries a failed rebuild; failures are logged and left for the next
// attempt (M7 plan R17). Caller holds prodMu.
func (p *Partition) ensureProducersLocked(ctx context.Context) {
	if !p.prodValid {
		_ = p.rebuildProducersLocked(ctx)
	}
}

// RecoverProducers loads producer state from the newest snapshot and replays the log after it
// (on open). A failure leaves the state invalid and is retried by MaintainProducers.
func (p *Partition) RecoverProducers(ctx context.Context) error {
	p.prodMu.Lock()
	defer p.prodMu.Unlock()
	if p.prodTruncatedAt < 0 {
		// Snapshots above the LEO found at open (left by a crash mid-truncation) must stay
		// ignored even if this first rebuild fails and appends later pass them.
		p.prodTruncatedAt = p.log.LogEndOffset()
	}
	return p.rebuildProducersLocked(ctx)
}

// MaintainProducers retries an invalid rebuild, expires idle producers and deletes snapshots
// below the log start after retention. The broker calls it periodically.
func (p *Partition) MaintainProducers(ctx context.Context, now time.Time) {
	p.prodMu.Lock()
	defer p.prodMu.Unlock()
	p.ensureProducersLocked(ctx)
	p.prod.ExpireBefore(now.UnixMilli())
	if start := p.log.LogStartOffset(); start != p.prodLogStart {
		if err := p.prod.DeleteSnapshotsBelow(ctx, start); err != nil {
			p.logger.Warn("deleting producer snapshots below log start failed", slog.Any("error", err))
			return
		}
		p.prodLogStart = start
	}
}

// SnapshotProducers writes a producer snapshot at the LEO (clean shutdown, so a restart replays
// nothing; M7 plan R16). The log must be flushed first.
func (p *Partition) SnapshotProducers(ctx context.Context) error {
	p.prodMu.Lock()
	defer p.prodMu.Unlock()
	if !p.prodValid {
		return nil
	}
	return p.prod.WriteSnapshot(ctx, p.log.LogEndOffset())
}
