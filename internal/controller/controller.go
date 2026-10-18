// Package controller is the cluster controller's broker-liveness logic (SPEC §2.3): it runs on
// every broker but acts only while the local Raft node is the leader. It registers brokers,
// tracks their heartbeats, fences brokers that miss the session timeout and unfences brokers
// once they have caught up on metadata (DECISIONS D34, D35).
//
// Lock ordering: Controller.mu is a leaf; Store calls (Raft applies, barriers) are never made
// while holding it.
package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// ErrStaleBrokerEpoch reports a heartbeat from an old (or unknown) broker incarnation; the broker
// must re-register.
var ErrStaleBrokerEpoch = errors.New("controller: stale broker epoch")

// Store is the subset of the Raft metadata store the controller uses.
type Store interface {
	Snapshot() metadata.Snapshot
	// ApplyForwarded applies cmd on this node if it is the leader (never forwarding) and returns
	// the entry's index.
	ApplyForwarded(ctx context.Context, cmd *streamqv1.MetadataCommand) (uint64, error)
	IsLeader() bool
	Term() uint64
	Barrier(ctx context.Context) error
	OnLeadershipChange(fn func(isLeader bool)) (cancel func())
}

// Clock abstracts time for heartbeat tracking.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) (<-chan time.Time, func())
}

// storeCallTimeout bounds each Raft call (barrier, apply) so a node shutting down — or a leader
// that lost quorum — never blocks the controller loop for long.
const storeCallTimeout = 5 * time.Second

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// NewTicker wraps time.NewTicker.
func (SystemClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

// Config configures a Controller.
type Config struct {
	BrokerID       int32
	SessionTimeout time.Duration // broker_session_timeout_ms
	CheckInterval  time.Duration // how often liveness is evaluated (default SessionTimeout/4)
}

type brokerState struct {
	epoch         int64
	lastHeartbeat time.Time
	catchUp       uint64 // index the broker must have processed before it is unfenced
	hasCatchUp    bool
	fenceSent     bool // a FenceBroker for this epoch was proposed
}

// Controller tracks broker liveness while this node is the controller.
type Controller struct {
	cfg    Config
	store  Store
	clock  Clock
	logger *slog.Logger
	wake   chan struct{}

	mu      sync.Mutex
	active  bool
	term    uint64 // Raft term captured on activation; stamps every command (D33)
	brokers map[int32]*brokerState
}

// New returns a Controller.
func New(cfg Config, store Store, clock Clock, logger *slog.Logger) *Controller {
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = max(cfg.SessionTimeout/4, 10*time.Millisecond)
	}
	return &Controller{cfg: cfg, store: store, clock: clock, logger: logger.With(slog.String("component", "controller")),
		wake: make(chan struct{}, 1), brokers: map[int32]*brokerState{}}
}

// Run evaluates leadership and liveness until ctx ends. It is the controller's only goroutine.
func (c *Controller) Run(ctx context.Context) error {
	cancel := c.store.OnLeadershipChange(func(bool) {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	})
	defer cancel()
	tick, stop := c.clock.NewTicker(c.cfg.CheckInterval)
	defer stop()
	for {
		c.step(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		case <-tick:
		}
	}
}

// step activates or deactivates the controller and runs one liveness pass while active.
func (c *Controller) step(ctx context.Context) {
	if !c.store.IsLeader() {
		c.deactivate()
		return
	}
	term := c.store.Term()
	c.mu.Lock()
	active, myTerm := c.active, c.term
	c.mu.Unlock()
	if !active || myTerm != term {
		c.deactivate()
		// Apply everything committed by earlier leaders before acting (plan R5).
		bctx, cancel := context.WithTimeout(ctx, storeCallTimeout)
		err := c.store.Barrier(bctx)
		cancel()
		if err != nil {
			c.logger.Warn("barrier before activation failed", slog.Any("error", err))
			return
		}
		if !c.store.IsLeader() || c.store.Term() != term {
			return
		}
		c.mu.Lock()
		c.active, c.term, c.brokers = true, term, map[int32]*brokerState{}
		c.mu.Unlock()
		c.logger.Info("controller activated", slog.Int("term", int(term)))
	}
	c.checkLiveness(ctx)
}

func (c *Controller) deactivate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active {
		c.logger.Info("controller deactivated", slog.Int("term", int(c.term)))
	}
	c.active, c.term, c.brokers = false, 0, map[int32]*brokerState{}
}

// track returns the state for broker b, starting a grace period for brokers not yet tracked or
// whose epoch changed. Caller holds mu.
func (c *Controller) track(b metadata.Broker, now time.Time) *brokerState {
	st, ok := c.brokers[b.ID]
	if !ok || st.epoch != b.Epoch {
		st = &brokerState{epoch: b.Epoch, lastHeartbeat: now}
		c.brokers[b.ID] = st
	}
	return st
}

// checkLiveness fences every unfenced broker whose last heartbeat is older than the session
// timeout.
func (c *Controller) checkLiveness(ctx context.Context) {
	snap := c.store.Snapshot()
	now := c.clock.Now()
	var fence []metadata.Broker
	c.mu.Lock()
	if !c.active {
		c.mu.Unlock()
		return
	}
	term := c.term
	seen := map[int32]bool{}
	for _, b := range snap.Brokers {
		seen[b.ID] = true
		st := c.track(b, now)
		if !b.Fenced && !st.fenceSent && now.Sub(st.lastHeartbeat) > c.cfg.SessionTimeout {
			st.fenceSent = true
			fence = append(fence, b)
		}
	}
	for id := range c.brokers {
		if !seen[id] {
			delete(c.brokers, id)
		}
	}
	c.mu.Unlock()
	for _, b := range fence {
		c.logger.Warn("fencing broker after missed heartbeats", slog.Int("fenced_broker", int(b.ID)), slog.Int64("broker_epoch", b.Epoch))
		cmd := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, TimestampMs: now.UnixMilli(), ControllerEpoch: term,
			Cmd: &streamqv1.MetadataCommand_FenceBroker{FenceBroker: &streamqv1.FenceBrokerCommand{BrokerId: b.ID, BrokerEpoch: b.Epoch}}}
		actx, cancel := context.WithTimeout(ctx, storeCallTimeout)
		_, err := c.store.ApplyForwarded(actx, cmd)
		cancel()
		if err != nil {
			c.logger.Warn("fence failed", slog.Int("fenced_broker", int(b.ID)), slog.Any("error", err))
			c.mu.Lock()
			if st, ok := c.brokers[b.ID]; ok && st.epoch == b.Epoch {
				st.fenceSent = false // retry on the next pass
			}
			c.mu.Unlock()
		}
	}
}

// activeTerm returns the captured term, or ErrNotController when inactive.
func (c *Controller) activeTerm() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return 0, fmt.Errorf("controller: broker %d is not the active controller: %w", c.cfg.BrokerID, metadata.ErrNotController)
	}
	return c.term, nil
}

// RegisterRequest is a RegisterBroker call.
type RegisterRequest struct {
	BrokerID     int32
	Host         string
	Port         int32
	InternalAddr string
}

// RegisterResult is the outcome of a registration.
type RegisterResult struct {
	BrokerEpoch int64
	// CatchUpIndex is the index the broker must have applied before opening its partitions and
	// before it is unfenced.
	CatchUpIndex uint64
}

// Register registers (or re-registers) a broker: it is fenced until it catches up (D35).
func (c *Controller) Register(ctx context.Context, req RegisterRequest) (RegisterResult, error) {
	term, err := c.activeTerm()
	if err != nil {
		return RegisterResult{}, err
	}
	now := c.clock.Now()
	cmd := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, TimestampMs: now.UnixMilli(), ControllerEpoch: term,
		Cmd: &streamqv1.MetadataCommand_RegisterBroker{RegisterBroker: &streamqv1.RegisterBrokerCommand{
			BrokerId: req.BrokerID, Host: req.Host, Port: req.Port, InternalAddr: req.InternalAddr,
		}}}
	index, err := c.store.ApplyForwarded(ctx, cmd)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("register broker %d: %w", req.BrokerID, err)
	}
	b, ok := c.store.Snapshot().Broker(req.BrokerID)
	if !ok {
		return RegisterResult{}, fmt.Errorf("register broker %d: not visible after apply: %w", req.BrokerID, metadata.ErrNotController)
	}
	c.mu.Lock()
	if c.active && c.term == term {
		c.brokers[req.BrokerID] = &brokerState{epoch: b.Epoch, lastHeartbeat: now, catchUp: index, hasCatchUp: true}
	}
	c.mu.Unlock()
	c.logger.Info("broker registered", slog.Int("registered_broker", int(req.BrokerID)), slog.Int64("broker_epoch", b.Epoch))
	return RegisterResult{BrokerEpoch: b.Epoch, CatchUpIndex: index}, nil
}

// Heartbeat records a broker heartbeat. A fenced broker whose processed metadata index has
// reached its catch-up index is unfenced. It reports whether the broker is still fenced.
func (c *Controller) Heartbeat(ctx context.Context, brokerID int32, brokerEpoch int64, processedIndex uint64) (bool, error) {
	term, err := c.activeTerm()
	if err != nil {
		return true, err
	}
	b, ok := c.store.Snapshot().Broker(brokerID)
	if !ok || b.Epoch != brokerEpoch {
		return true, fmt.Errorf("broker %d epoch %d: %w", brokerID, brokerEpoch, ErrStaleBrokerEpoch)
	}
	now := c.clock.Now()
	c.mu.Lock()
	st := c.track(b, now)
	st.lastHeartbeat = now
	st.fenceSent = st.fenceSent && b.Fenced
	if b.Fenced && !st.hasCatchUp {
		// Registered under a previous controller: catch up to every command applied so far (after
		// this controller's activation barrier). This is the FSM's applied command index, which
		// brokers can actually reach — never Raft's commit index, which also counts no-op and
		// barrier entries the FSM never sees.
		st.catchUp, st.hasCatchUp = c.store.Snapshot().Index, true
	}
	unfence := b.Fenced && processedIndex >= st.catchUp
	c.mu.Unlock()
	if !unfence {
		return b.Fenced, nil
	}
	cmd := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, TimestampMs: now.UnixMilli(), ControllerEpoch: term,
		Cmd: &streamqv1.MetadataCommand_UnfenceBroker{UnfenceBroker: &streamqv1.UnfenceBrokerCommand{BrokerId: brokerID, BrokerEpoch: brokerEpoch}}}
	if _, err := c.store.ApplyForwarded(ctx, cmd); err != nil {
		return true, fmt.Errorf("unfence broker %d: %w", brokerID, err)
	}
	c.logger.Info("broker unfenced", slog.Int("unfenced_broker", int(brokerID)))
	return false, nil
}

// AlterISRRequest is a partition leader's ISR change (SPEC §4.3).
type AlterISRRequest struct {
	Topic          string
	Partition      int32
	LeaderEpoch    int32
	PartitionEpoch int32
	NewISR         []int32
}

// AlterISR applies an ISR change, stamped with the controller's term. The FSM checks the epochs
// (metadata.ErrFencedLeaderEpoch) and the new ISR's content (metadata.ErrInvalidISR).
func (c *Controller) AlterISR(ctx context.Context, req AlterISRRequest) error {
	term, err := c.activeTerm()
	if err != nil {
		return err
	}
	cmd := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, TimestampMs: c.clock.Now().UnixMilli(), ControllerEpoch: term,
		Cmd: &streamqv1.MetadataCommand_AlterIsr{AlterIsr: &streamqv1.AlterISRCommand{
			Topic: req.Topic, Partition: req.Partition, LeaderEpoch: req.LeaderEpoch,
			PartitionEpoch: req.PartitionEpoch, NewIsr: req.NewISR,
		}}}
	if _, err := c.store.ApplyForwarded(ctx, cmd); err != nil {
		return fmt.Errorf("alter isr %s-%d: %w", req.Topic, req.Partition, err)
	}
	c.logger.Info("isr changed", slog.String("topic", req.Topic), slog.Int("partition", int(req.Partition)), slog.Any("isr", req.NewISR))
	return nil
}
