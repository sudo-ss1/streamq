// Package node wires one broker process together: metadata store, broker (replica manager) and
// gRPC server. It is used by cmd/streamq-broker and by in-process integration tests.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
	"github.com/sudo-ss1/streamq/internal/server"
)

// gracefulStopTimeout bounds GracefulStop before falling back to Stop (M2 plan R10).
const gracefulStopTimeout = 10 * time.Second

// Node is a running broker process.
type Node struct {
	store  *local.Store
	broker *broker.Broker
	grpc   *grpc.Server
	lis    net.Listener
	logger *slog.Logger

	serveErr  chan error
	closeOnce sync.Once
	closeErr  error
}

// Start opens the metadata store and broker, then starts serving the client API on
// cfg.ListenAddr. It returns once the listener is bound and every local partition is open.
func Start(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Node, error) {
	base := logger
	logger = logger.With(slog.Int("broker_id", int(cfg.BrokerID)))
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	port := int32(lis.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert // tcp listener
	self := metadata.Broker{ID: cfg.BrokerID, Host: cfg.AdvertisedHost, Port: port}
	store, err := local.Open(ctx, cfg.DataDir, self, base)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open metadata: %w", err), lis.Close())
	}
	b, err := broker.Open(ctx, broker.Config{
		BrokerID:        cfg.BrokerID,
		DataDir:         cfg.DataDir,
		TopicDefaults:   cfg.TopicDefaults,
		FileDeleteDelay: time.Duration(cfg.FileDeleteDelayMs) * time.Millisecond,
	}, store, broker.SystemClock{}, base)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open broker: %w", err), store.Close(), lis.Close())
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(server.MaxMessageSize), grpc.MaxSendMsgSize(server.MaxMessageSize))
	streamqv1.RegisterStreamQServiceServer(gs, server.New(b, store, cfg.TopicDefaults, broker.SystemClock{}, logger))
	n := &Node{store: store, broker: b, grpc: gs, lis: lis, logger: logger, serveErr: make(chan error, 1)}
	go func() { n.serveErr <- gs.Serve(lis) }()
	logger.Info("serving", slog.String("addr", lis.Addr().String()))
	return n, nil
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

// Close shuts down in order (M2 plan R10): stop accepting and drain in-flight RPCs (bounded),
// close the broker (unsubscribe, wake long-polls, stop loops, final HW checkpoint, flush
// storage), then close the metadata store. Idempotent.
func (n *Node) Close(ctx context.Context) error {
	n.closeOnce.Do(func() {
		// Wake idle long-polls first so GracefulStop can drain promptly (NFR4).
		n.broker.BeginShutdown()
		stopped := make(chan struct{})
		go func() {
			n.grpc.GracefulStop()
			close(stopped)
		}()
		// Long-polls end when the broker closes, so close it if draining takes too long.
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
		n.closeErr = errors.Join(serveErr, n.broker.Close(ctx), n.store.Close())
	})
	return n.closeErr
}

// ClientAddr returns "host:port" for dialing a node bound to listenAddr on loopback (tests).
func ClientAddr(port string) string { return net.JoinHostPort("127.0.0.1", port) }
