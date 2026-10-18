// Package raftstore is the Raft-backed metadata.Store (SPEC §2.1–2.2): every broker is a voter,
// the Raft leader is the controller, and every broker applies the deterministic metadata FSM.
//
// Lock ordering: Store.mu (leadership listeners) is a leaf; fsm.mu → notifier.mu. No lock is held
// across Raft calls or RPCs.
package raftstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// Peer is one static cluster member.
type Peer struct {
	ID           int32
	RaftAddr     string
	InternalAddr string
}

// Forwarder sends a metadata command to the controller's internal API and returns the Raft index
// it was applied at. It returns metadata.ErrNotController (wrapped) when the target is not the
// leader and nothing was appended, and errNotAppended-wrapped errors for dial failures.
type Forwarder interface {
	Forward(ctx context.Context, internalAddr string, cmd *streamqv1.MetadataCommand) (uint64, error)
}

// ErrUnknownOutcome marks a command whose fate is unknown: Raft lost leadership while committing
// it, or the apply timed out. It may still commit (DECISIONS D33).
var ErrUnknownOutcome = errors.New("raftstore: unknown outcome")

// ErrNotAppended marks a forwarding failure where the command certainly was not appended (dial
// failure), so it is safe to retry (DECISIONS D33).
var ErrNotAppended = errors.New("raftstore: command not appended")

// Config configures a Store.
type Config struct {
	NodeID int32
	Peers  []Peer // identical on every node, including this one
	// DataDir holds raft/ (bolt log and stable store, snapshots).
	DataDir string
	// Listener is the pre-bound Raft listener. Peers dial this node's Peer.RaftAddr, which is
	// normally the listener's address but may differ when traffic is proxied (DECISIONS D54); Raft
	// identifies servers by ID.
	Listener  net.Listener
	Forwarder Forwarder
	Logger    *slog.Logger

	HeartbeatTimeout  time.Duration // default 1s
	ElectionTimeout   time.Duration // default 1s
	SnapshotThreshold uint64        // default 8192
	TrailingLogs      uint64        // default 10240
	// RetryBackoff is the wait between forwarding attempts while no leader is known (default 100ms).
	RetryBackoff time.Duration
}

// Store is the Raft-backed metadata store.
type Store struct {
	cfg       Config
	logger    *slog.Logger
	raft      *raft.Raft
	fsm       *fsm
	notifier  *notifier
	transport *raft.NetworkTransport
	bolt      *raftboltdb.BoltStore
	peers     map[int32]Peer

	observer  *raft.Observer
	obsCh     chan raft.Observation
	obsDone   chan struct{}
	mu        sync.Mutex
	listeners map[int]func(bool)
	nextLis   int

	closeOnce sync.Once
	closeErr  error
}

var _ metadata.Store = (*Store)(nil)

func serverID(id int32) raft.ServerID { return raft.ServerID(strconv.FormatInt(int64(id), 10)) }

// Open opens the Raft node, bootstrapping the static cluster on first start (ErrCantBootstrap is
// ignored on restarts).
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = time.Second
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = time.Second
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 8192
	}
	if cfg.TrailingLogs == 0 {
		cfg.TrailingLogs = 10240
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	peers := map[int32]Peer{}
	servers := make([]raft.Server, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers[p.ID] = p
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: serverID(p.ID), Address: raft.ServerAddress(p.RaftAddr)})
	}
	if _, ok := peers[cfg.NodeID]; !ok {
		return nil, fmt.Errorf("raftstore: node %d is not a peer", cfg.NodeID)
	}
	dir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("raftstore: create dir: %w", err)
	}
	logger := cfg.Logger.With(slog.String("component", "raft"))
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raftstore: open bolt: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(dir, 2, hclogger(logger, "snapshot"))
	if err != nil {
		_ = bolt.Close()
		return nil, fmt.Errorf("raftstore: open snapshots: %w", err)
	}
	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream: newStreamLayer(cfg.Listener), MaxPool: 3, Timeout: 10 * time.Second, Logger: hclogger(logger, "transport"),
	})
	rcfg := raft.DefaultConfig()
	rcfg.LocalID = serverID(cfg.NodeID)
	rcfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	rcfg.ElectionTimeout = cfg.ElectionTimeout
	rcfg.LeaderLeaseTimeout = min(cfg.HeartbeatTimeout, rcfg.LeaderLeaseTimeout)
	rcfg.SnapshotThreshold = cfg.SnapshotThreshold
	rcfg.TrailingLogs = cfg.TrailingLogs
	rcfg.Logger = hclogger(logger, "raft")

	n := newNotifier()
	f := newFSM(n)
	r, err := raft.NewRaft(rcfg, f, bolt, bolt, snaps, transport)
	if err != nil {
		n.close()
		_ = transport.Close()
		_ = bolt.Close()
		return nil, fmt.Errorf("raftstore: start raft: %w", err)
	}
	s := &Store{
		cfg: cfg, logger: logger, raft: r, fsm: f, notifier: n, transport: transport, bolt: bolt, peers: peers,
		obsCh: make(chan raft.Observation, 16), obsDone: make(chan struct{}), listeners: map[int]func(bool){},
	}
	if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		_ = s.Close()
		return nil, fmt.Errorf("raftstore: bootstrap: %w", err)
	}
	s.observer = raft.NewObserver(s.obsCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(s.observer)
	go s.watchLeadership()
	return s, nil
}

// watchLeadership fans Raft leader observations out to every listener (hashicorp's LeaderCh has
// a single consumer and drops values, plan R4).
func (s *Store) watchLeadership() {
	defer close(s.obsDone)
	was := false
	for o := range s.obsCh {
		lo, ok := o.Data.(raft.LeaderObservation)
		if !ok {
			continue
		}
		is := lo.LeaderID == serverID(s.cfg.NodeID)
		if is == was {
			continue
		}
		was = is
		s.mu.Lock()
		fns := make([]func(bool), 0, len(s.listeners))
		for _, fn := range s.listeners {
			fns = append(fns, fn)
		}
		s.mu.Unlock()
		for _, fn := range fns {
			fn(is)
		}
	}
}

// OnLeadershipChange registers fn to be called (on the observer goroutine) whenever this node
// gains or loses leadership. fn must not block for long. The returned func unregisters it.
func (s *Store) OnLeadershipChange(fn func(isLeader bool)) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextLis
	s.nextLis++
	s.listeners[id] = fn
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.listeners, id)
	}
}

// TransferLeadership asks Raft to hand leadership to another voter and waits for the transfer to
// finish or ctx to end (controlled shutdown of the controller, DECISIONS D53). It is a no-op when
// this node is not the leader.
func (s *Store) TransferLeadership(ctx context.Context) error {
	if !s.IsLeader() {
		return nil
	}
	f := s.raft.LeadershipTransfer()
	done := make(chan error, 1)
	go func() { done <- f.Error() }() // ends when Raft resolves the future (bounded by its own timeout)
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("raft leadership transfer: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsLeader reports whether this node is the Raft leader (the controller).
func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }

// LeaderID returns the controller's broker id, or -1 if no leader is known.
func (s *Store) LeaderID() int32 {
	_, id := s.raft.LeaderWithID()
	if id == "" {
		return -1
	}
	n, err := strconv.ParseInt(string(id), 10, 32)
	if err != nil {
		return -1
	}
	return int32(n)
}

// Term returns the current Raft term.
func (s *Store) Term() uint64 { return s.raft.CurrentTerm() }

// AppliedIndex returns the last log index applied to the FSM.
func (s *Store) AppliedIndex() uint64 { return s.fsm.appliedIndex() }

// CommitIndex returns Raft's commit index.
func (s *Store) CommitIndex() uint64 { return s.raft.CommitIndex() }

// WaitApplied blocks until the FSM has applied index or ctx ends.
func (s *Store) WaitApplied(ctx context.Context, index uint64) error {
	return s.fsm.waitApplied(ctx, index)
}

// Barrier blocks until every entry committed before it is applied (used when gaining leadership).
func (s *Store) Barrier(ctx context.Context) error {
	if err := s.raft.Barrier(timeout(ctx)).Error(); err != nil {
		return fmt.Errorf("raftstore: barrier: %w", err)
	}
	return nil
}

// TakeSnapshot forces a Raft snapshot (tests and tooling).
func (s *Store) TakeSnapshot() error {
	if err := s.raft.Snapshot().Error(); err != nil {
		return fmt.Errorf("raftstore: snapshot: %w", err)
	}
	return nil
}

// Snapshot implements metadata.Store.
func (s *Store) Snapshot() metadata.Snapshot {
	snap := s.fsm.snapshot()
	snap.ControllerID = s.LeaderID()
	if snap.ControllerID >= 0 {
		snap.ControllerEpoch = s.raft.CurrentTerm()
	}
	return snap
}

// Subscribe implements metadata.Store.
func (s *Store) Subscribe(fn func(context.Context, metadata.Change)) (metadata.Snapshot, func()) {
	s.fsm.mu.Lock()
	snap := s.fsm.state.Snapshot()
	snap.Index = s.fsm.index
	unsub := s.notifier.subscribe(fn)
	s.fsm.mu.Unlock()
	snap.ControllerID = s.LeaderID()
	if snap.ControllerID >= 0 {
		snap.ControllerEpoch = s.raft.CurrentTerm()
	}
	return snap, unsub
}

func timeout(ctx context.Context) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		return max(time.Until(d), time.Millisecond)
	}
	return 10 * time.Second
}

// applyLocal appends cmd on this node, which must be the leader. It returns the entry's index.
// The Raft error (ErrNotLeader: not appended; ErrLeadershipLost: unknown outcome) and the FSM
// error (the command's deterministic result) are distinguished (plan R7).
func (s *Store) applyLocal(ctx context.Context, cmd *streamqv1.MetadataCommand) (uint64, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return 0, fmt.Errorf("raftstore: encode command: %w", err)
	}
	f := s.raft.Apply(data, timeout(ctx))
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, fmt.Errorf("raftstore: %w", metadata.ErrNotController)
		}
		return 0, fmt.Errorf("raftstore: apply: %w: %w", ErrUnknownOutcome, err)
	}
	if resp, ok := f.Response().(error); ok && resp != nil {
		return f.Index(), resp
	}
	return f.Index(), nil
}

// ProposeAsLeader implements metadata.Store: leader only, never forwarded.
func (s *Store) ProposeAsLeader(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	if !s.IsLeader() {
		return fmt.Errorf("raftstore: node %d: %w", s.cfg.NodeID, metadata.ErrNotController)
	}
	_, err := s.applyLocal(ctx, cmd)
	return err
}

// ApplyForwarded applies a command forwarded by a follower; leader only (no re-forwarding).
func (s *Store) ApplyForwarded(ctx context.Context, cmd *streamqv1.MetadataCommand) (uint64, error) {
	if !s.IsLeader() {
		return 0, fmt.Errorf("raftstore: node %d: %w", s.cfg.NodeID, metadata.ErrNotController)
	}
	return s.applyLocal(ctx, cmd)
}

// Propose implements metadata.Store: applied locally on the leader, or forwarded to it. It
// retries only when the command certainly was not appended, and returns once this node has
// applied the command (DECISIONS D33).
func (s *Store) Propose(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	for {
		var index uint64
		var err error
		if s.IsLeader() {
			index, err = s.applyLocal(ctx, cmd)
		} else if leader := s.LeaderID(); leader >= 0 && leader != s.cfg.NodeID {
			index, err = s.cfg.Forwarder.Forward(ctx, s.peers[leader].InternalAddr, cmd)
		} else {
			err = fmt.Errorf("raftstore: no leader known: %w", ErrNotAppended)
		}
		switch {
		case err == nil:
			return s.fsm.waitApplied(ctx, index)
		case !errors.Is(err, ErrNotAppended) && !errors.Is(err, metadata.ErrNotController):
			if index > 0 {
				// A deterministic command error: wait until it is applied here too.
				if werr := s.fsm.waitApplied(ctx, index); werr != nil {
					return werr
				}
			}
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("raftstore: propose: %w (last error: %w)", metadata.ErrNotController, err)
		case <-time.After(s.cfg.RetryBackoff):
		}
	}
}

// Close shuts Raft down, then the transport, the bolt store and the notifier (plan R16).
// Idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		var errs []error
		if err := s.raft.Shutdown().Error(); err != nil {
			errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
		}
		if s.observer != nil {
			s.raft.DeregisterObserver(s.observer)
			close(s.obsCh)
			<-s.obsDone
		}
		s.transport.CloseStreams()
		if err := s.transport.Close(); err != nil {
			errs = append(errs, fmt.Errorf("transport close: %w", err))
		}
		if err := s.bolt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("bolt close: %w", err))
		}
		s.notifier.close()
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}
