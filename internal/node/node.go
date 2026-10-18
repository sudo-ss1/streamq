// Package node wires one broker process together: metadata store (local or Raft), controller,
// broker (replica manager), group coordinator and gRPC servers. It is used by cmd/streamq-broker
// and by in-process integration tests.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/controller"
	"github.com/sudo-ss1/streamq/internal/group"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
	"github.com/sudo-ss1/streamq/internal/metadata/raftstore"
	"github.com/sudo-ss1/streamq/internal/server"
)

// gracefulStopTimeout bounds GracefulStop before falling back to Stop (M2 plan R10).
const gracefulStopTimeout = 10 * time.Second

// Options carries optional pre-bound listeners (tests bind 127.0.0.1:0 first so the cluster
// config can name the real ports; M5 plan R9). Nil listeners are bound from the config.
type Options struct {
	Client   net.Listener
	Internal net.Listener
	Raft     net.Listener
}

// Node is a running broker process.
type Node struct {
	cfg    config.Config
	logger *slog.Logger

	store     metadata.Store
	raft      *raftstore.Store // nil in single-node mode
	forwarder *server.Forwarder
	ctrl      *controller.Controller
	broker    *broker.Broker
	groups    *group.Coordinator

	grpc     *grpc.Server
	lis      net.Listener
	serveErr chan error

	internal    *grpc.Server
	internalLis net.Listener
	bg          *errgroup.Group // controller loop, internal server, liveness loop
	bgCancel    context.CancelFunc
	bgContext   context.Context
	epoch       atomic.Int64 // this broker's registration epoch (cluster mode)

	closeOnce sync.Once
	closeErr  error
}

// Start runs a broker on cfg (single-node mode if cfg.Cluster is empty, else cluster mode).
func Start(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Node, error) {
	return StartWithOptions(ctx, cfg, logger, Options{})
}

// StartWithOptions is Start with pre-bound listeners. It returns once the node serves clients;
// in cluster mode that is after it registered with the controller and caught up on metadata
// (M5 plan R1), so cluster members must be started concurrently.
func StartWithOptions(ctx context.Context, cfg config.Config, logger *slog.Logger, opts Options) (n *Node, err error) {
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	if err := checkMode(cfg); err != nil {
		return nil, err
	}
	n = &Node{cfg: cfg, logger: logger.With(slog.Int("broker_id", int(cfg.BrokerID))), serveErr: make(chan error, 1)}
	defer func() {
		if err != nil {
			n.abort(context.WithoutCancel(ctx)) // cleanup must run even if ctx is already cancelled
		}
	}()
	if n.lis, err = listen(ctx, opts.Client, cfg.ListenAddr); err != nil {
		return nil, err
	}
	port := int32(n.lis.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert // tcp listener
	if len(cfg.Cluster) == 0 {
		self := metadata.Broker{ID: cfg.BrokerID, Host: cfg.AdvertisedHost, Port: port}
		ls, err := local.Open(ctx, cfg.DataDir, self, logger)
		if err != nil {
			return nil, fmt.Errorf("open metadata: %w", err)
		}
		n.store = ls
	} else if err := n.startCluster(ctx, opts, port, logger); err != nil {
		return nil, err
	}

	if n.broker, err = broker.Open(ctx, broker.Config{
		BrokerID:        cfg.BrokerID,
		DataDir:         cfg.DataDir,
		TopicDefaults:   cfg.TopicDefaults,
		FileDeleteDelay: time.Duration(cfg.FileDeleteDelayMs) * time.Millisecond,
	}, n.store, broker.SystemClock{}, logger); err != nil {
		return nil, fmt.Errorf("open broker: %w", err)
	}
	if n.raft != nil {
		// The liveness loop outlives Start's ctx: it runs until Close (background context).
		n.bg.Go(func() error { return n.liveness(n.bgCtx(), port) }) //nolint:contextcheck // node-lifetime goroutine
	}
	n.groups = group.New(group.Config{
		BrokerID:                cfg.BrokerID,
		MaxSessionTimeout:       time.Duration(cfg.GroupSessionTimeoutMs) * time.Millisecond,
		DefaultRebalanceTimeout: time.Duration(cfg.GroupRebalanceTimeoutMs) * time.Millisecond,
		InitialRebalanceDelay:   time.Duration(cfg.GroupInitialRebalanceDelayMs) * time.Millisecond,
	}, n.store, group.SystemClock{}, n.logger)
	n.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxMessageSize), grpc.MaxSendMsgSize(server.MaxMessageSize))
	streamqv1.RegisterStreamQServiceServer(n.grpc, server.New(n.broker, n.groups, n.store, cfg.TopicDefaults, broker.SystemClock{}, n.logger))
	go func() { n.serveErr <- n.grpc.Serve(n.lis) }()
	n.logger.Info("serving", slog.String("addr", n.lis.Addr().String()))
	return n, nil
}

func listen(ctx context.Context, pre net.Listener, addr string) (net.Listener, error) {
	if pre != nil {
		return pre, nil
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return lis, nil
}

// checkMode refuses to start on a data dir holding the other mode's metadata (D30).
func checkMode(cfg config.Config) error {
	other := filepath.Join(cfg.DataDir, "raft")
	if len(cfg.Cluster) > 0 {
		other = filepath.Join(cfg.DataDir, local.FileName)
	}
	if _, err := os.Stat(other); err == nil {
		return fmt.Errorf("data dir %s holds %s from the other metadata mode (DECISIONS D30): refusing to start", cfg.DataDir, other)
	}
	return nil
}

// bgCtx returns the context of the background group.
func (n *Node) bgCtx() context.Context { return n.bgContext }

// startCluster opens the Raft store, the controller and the internal API, then registers with
// the controller and waits until the local FSM has applied the registration (plan R1).
func (n *Node) startCluster(ctx context.Context, opts Options, port int32, logger *slog.Logger) error {
	cfg := n.cfg
	var err error
	if n.internalLis, err = listen(ctx, opts.Internal, cfg.InternalAddr); err != nil {
		return err
	}
	raftLis, err := listen(ctx, opts.Raft, cfg.RaftAddr)
	if err != nil {
		return err
	}
	peers := make([]raftstore.Peer, 0, len(cfg.Cluster))
	for _, m := range cfg.Cluster {
		peers = append(peers, raftstore.Peer{ID: m.ID, RaftAddr: m.Raft, InternalAddr: m.Internal})
	}
	n.forwarder = server.NewForwarder()
	n.raft, err = raftstore.Open(ctx, raftstore.Config{
		NodeID: cfg.BrokerID, Peers: peers, DataDir: cfg.DataDir, Listener: raftLis, Forwarder: n.forwarder, Logger: logger,
		HeartbeatTimeout:  time.Duration(cfg.RaftHeartbeatTimeoutMs) * time.Millisecond,
		ElectionTimeout:   time.Duration(cfg.RaftElectionTimeoutMs) * time.Millisecond,
		SnapshotThreshold: uint64(cfg.RaftSnapshotThreshold),
	})
	if err != nil {
		_ = raftLis.Close()
		return fmt.Errorf("open raft metadata: %w", err)
	}
	n.store = n.raft
	n.ctrl = controller.New(controller.Config{
		BrokerID:       cfg.BrokerID,
		SessionTimeout: time.Duration(cfg.BrokerSessionTimeoutMs) * time.Millisecond,
	}, n.raft, controller.SystemClock{}, n.logger)
	n.internal = grpc.NewServer()
	streamqv1.RegisterStreamQInternalServiceServer(n.internal, server.NewInternal(n.ctrl, n.raft, n.logger))

	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.bgCancel = cancel
	n.bgContext = bgCtx
	n.bg = &errgroup.Group{}
	n.bg.Go(func() error { return n.ctrl.Run(bgCtx) })
	n.bg.Go(func() error {
		if err := n.internal.Serve(n.internalLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("internal server: %w", err)
		}
		return nil
	})

	reg, err := n.register(ctx, port)
	if err != nil {
		return err
	}
	n.setEpoch(reg.GetBrokerEpoch())
	if err := n.raft.WaitApplied(ctx, reg.GetCatchUpIndex()); err != nil {
		return fmt.Errorf("catch up on metadata: %w", err)
	}
	n.logger.Info("registered with controller", slog.Int64("broker_epoch", reg.GetBrokerEpoch()),
		slog.Int("catch_up_index", int(reg.GetCatchUpIndex())))
	return nil
}

// abort releases whatever a failed Start opened.
func (n *Node) abort(ctx context.Context) {
	if n.broker != nil {
		_ = n.broker.Close(ctx)
	}
	n.stopBackground()
	if n.store != nil {
		_ = n.store.Close()
	}
	if n.forwarder != nil {
		_ = n.forwarder.Close()
	}
	if n.lis != nil {
		_ = n.lis.Close()
	}
}

func (n *Node) stopBackground() {
	if n.bg == nil {
		if n.internalLis != nil && n.internal == nil {
			_ = n.internalLis.Close() // startCluster failed before the internal server took it over
		}
		return
	}
	n.bgCancel()
	if n.internal != nil {
		n.internal.Stop()
	}
	if err := n.bg.Wait(); err != nil {
		n.logger.Error("background task failed", slog.Any("error", err))
	}
	n.bg = nil
}

// Addr returns the bound client API address.
func (n *Node) Addr() string { return n.lis.Addr().String() }

// Port returns the bound client API port.
func (n *Node) Port() string {
	_, port, _ := net.SplitHostPort(n.Addr())
	return port
}

// Broker returns the replica manager (for tests).
func (n *Node) Broker() *broker.Broker { return n.broker }

// Raft returns the Raft metadata store, or nil in single-node mode (for tests).
func (n *Node) Raft() *raftstore.Store { return n.raft }

// Close shuts down in order: wake long-polls and parked joins, drain the client API (bounded),
// stop the coordinator, broker (final HW checkpoint, flushed storage), background loops and
// internal API, then the metadata store. Idempotent.
func (n *Node) Close(ctx context.Context) error {
	n.closeOnce.Do(func() {
		n.broker.BeginShutdown()
		n.groups.BeginShutdown()
		stopped := make(chan struct{})
		go func() {
			n.grpc.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(gracefulStopTimeout)
		select {
		case <-stopped:
		case <-timer.C:
			n.logger.Warn("graceful stop timed out; forcing")
			n.grpc.Stop()
			<-stopped
		}
		timer.Stop()
		serveErr := <-n.serveErr
		if errors.Is(serveErr, grpc.ErrServerStopped) {
			serveErr = nil
		}
		n.groups.Close()
		errs := []error{serveErr, n.broker.Close(ctx)}
		n.stopBackground()
		errs = append(errs, n.store.Close())
		if n.forwarder != nil {
			errs = append(errs, n.forwarder.Close())
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

// ClientAddr returns "host:port" for dialing a node bound to listenAddr on loopback (tests).
func ClientAddr(port string) string { return net.JoinHostPort("127.0.0.1", port) }
