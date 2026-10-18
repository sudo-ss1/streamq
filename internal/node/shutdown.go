package node

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sudo-ss1/streamq/internal/controller"
)

// controlledShutdownPoll is how often the shutdown steps re-check controller visibility and local
// leadership.
const controlledShutdownPoll = 50 * time.Millisecond

// controlledShutdownRPCTimeout bounds one ControlledShutdown attempt, so an unreachable
// controller is retried (possibly a new one) within the overall deadline.
const controlledShutdownRPCTimeout = 2 * time.Second

// controlledShutdown moves this broker's leadership elsewhere before it stops serving (SPEC §4.6,
// DECISIONS D53). Steps 1–4 share one deadline, controlled_shutdown_timeout_ms (and ctx's); they
// never fail Close, only log:
//  1. stop the liveness loop and wait for it, so no heartbeat can unfence this broker;
//  2. ask the controller to fence it (which elects new leaders), retrying; skipped once no
//     controller has been visible for two election timeouts (no quorum: nothing can move);
//  3. wait until local metadata shows no partition led here;
//  4. hand Raft leadership to another node if this one is the controller (attempted even when
//     steps 2–3 failed).
func (n *Node) controlledShutdown(ctx context.Context) {
	if n.raft == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(n.cfg.ControlledShutdownTimeoutMs)*time.Millisecond)
	defer cancel()
	start := time.Now()
	log := n.logger.With(slog.String("phase", "controlled_shutdown"))
	if n.livenessCancel != nil {
		n.livenessCancel()
		<-n.livenessDone
	}
	if n.requestControlledShutdown(ctx, log) {
		n.waitNoLeadership(ctx, log)
	}
	// Best effort on every path: even if leadership could not be moved, a controller leaving
	// without handing off Raft leadership would cost a full election timeout.
	if err := n.raft.TransferLeadership(ctx); err != nil {
		log.Warn("raft leadership transfer failed", slog.Any("error", err))
	}
	log.Info("controlled shutdown done", slog.Duration("took", time.Since(start)))
}

// requestControlledShutdown performs step 2 and reports whether it succeeded.
func (n *Node) requestControlledShutdown(ctx context.Context, log *slog.Logger) bool {
	call := n.opts.ControlledShutdown
	if call == nil {
		call = func(ctx context.Context) error {
			rctx, cancel := context.WithTimeout(ctx, controlledShutdownRPCTimeout)
			defer cancel()
			return n.replicas.ControlledShutdown(rctx, n.cfg.BrokerID, n.epoch.Load())
		}
	}
	window := 2 * time.Duration(n.cfg.RaftElectionTimeoutMs) * time.Millisecond
	var noController time.Time
	tick := time.NewTicker(controlledShutdownPoll)
	defer tick.Stop()
	for {
		if n.raft.LeaderID() < 0 {
			if noController.IsZero() {
				noController = time.Now()
			}
			if time.Since(noController) >= window {
				log.Warn("no controller visible; stopping without moving leadership")
				return false
			}
		} else {
			noController = time.Time{}
			err := call(ctx)
			if err == nil {
				return true
			}
			if errors.Is(err, controller.ErrStaleBrokerEpoch) {
				log.Warn("controlled shutdown rejected: stale broker epoch", slog.Any("error", err))
				return false // can never succeed for this incarnation
			}
			log.Debug("controlled shutdown request failed; retrying", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			log.Warn("controlled shutdown timed out; stopping anyway")
			return false
		case <-tick.C:
		}
	}
}

// waitNoLeadership performs step 3.
func (n *Node) waitNoLeadership(ctx context.Context, log *slog.Logger) {
	tick := time.NewTicker(controlledShutdownPoll)
	defer tick.Stop()
	for n.broker.LeadsPartitions() {
		select {
		case <-ctx.Done():
			log.Warn("leadership did not move before the deadline")
			return
		case <-tick.C:
		}
	}
}
