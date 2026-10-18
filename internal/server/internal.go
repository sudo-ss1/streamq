package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/controller"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/raftstore"
)

// ControllerStore is what the internal API needs from the Raft metadata store.
type ControllerStore interface {
	ApplyForwarded(ctx context.Context, cmd *streamqv1.MetadataCommand) (uint64, error)
	LeaderID() int32
}

// InternalServer implements the broker-to-broker API (SPEC §7.2, DECISIONS D31).
type InternalServer struct {
	streamqv1.UnimplementedStreamQInternalServiceServer

	ctrl   *controller.Controller
	store  ControllerStore
	broker atomic.Pointer[broker.Broker]
	logger *slog.Logger
}

// NewInternal returns an InternalServer.
func NewInternal(ctrl *controller.Controller, store ControllerStore, logger *slog.Logger) *InternalServer {
	return &InternalServer{ctrl: ctrl, store: store, logger: logger}
}

func (s *InternalServer) code(err error) streamqv1.ErrorCode {
	if errors.Is(err, controller.ErrStaleBrokerEpoch) {
		return streamqv1.ErrorCode_ERROR_CODE_STALE_BROKER_EPOCH
	}
	code := errorCode(err)
	if code == streamqv1.ErrorCode_ERROR_CODE_UNKNOWN {
		s.logger.Error("unexpected internal request error", slog.Any("error", err))
	}
	return code
}

// RegisterBroker implements StreamQInternalServiceServer.
func (s *InternalServer) RegisterBroker(ctx context.Context, req *streamqv1.RegisterBrokerRequest) (*streamqv1.RegisterBrokerResponse, error) {
	res, err := s.ctrl.Register(ctx, controller.RegisterRequest{
		BrokerID: req.GetBrokerId(), Host: req.GetHost(), Port: req.GetPort(), InternalAddr: req.GetInternalAddr(),
	})
	if err != nil {
		return &streamqv1.RegisterBrokerResponse{ErrorCode: s.code(err), ErrorMessage: err.Error(), ControllerId: s.store.LeaderID()}, nil
	}
	return &streamqv1.RegisterBrokerResponse{BrokerEpoch: res.BrokerEpoch, CatchUpIndex: res.CatchUpIndex, ControllerId: s.store.LeaderID()}, nil
}

// BrokerHeartbeat implements StreamQInternalServiceServer.
func (s *InternalServer) BrokerHeartbeat(ctx context.Context, req *streamqv1.BrokerHeartbeatRequest) (*streamqv1.BrokerHeartbeatResponse, error) {
	fenced, err := s.ctrl.Heartbeat(ctx, req.GetBrokerId(), req.GetBrokerEpoch(), req.GetAppliedIndex())
	if err != nil {
		return &streamqv1.BrokerHeartbeatResponse{ErrorCode: s.code(err), ErrorMessage: err.Error(), Fenced: true,
			ControllerId: s.store.LeaderID()}, nil
	}
	return &streamqv1.BrokerHeartbeatResponse{Fenced: fenced, ControllerId: s.store.LeaderID()}, nil
}

// ForwardMetadata implements StreamQInternalServiceServer: it applies a follower's CreateTopic or
// DeleteTopic on the controller (DECISIONS D33). Other commands are controller-only.
func (s *InternalServer) ForwardMetadata(ctx context.Context, req *streamqv1.ForwardMetadataRequest) (*streamqv1.ForwardMetadataResponse, error) {
	switch req.GetCommand().GetCmd().(type) {
	case *streamqv1.MetadataCommand_CreateTopic, *streamqv1.MetadataCommand_DeleteTopic:
	default:
		return nil, status.Error(codes.InvalidArgument, "only create_topic and delete_topic may be forwarded")
	}
	index, err := s.store.ApplyForwarded(ctx, req.GetCommand())
	resp := &streamqv1.ForwardMetadataResponse{Index: index, ControllerId: s.store.LeaderID()}
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = s.code(err), err.Error()
	}
	return resp, nil
}

// Forwarder sends metadata commands to the controller's internal API (raftstore.Forwarder).
type Forwarder struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewForwarder returns a Forwarder; Close releases its connections.
func NewForwarder() *Forwarder { return &Forwarder{conns: map[string]*grpc.ClientConn{}} }

// Client returns a cached internal-API client for addr.
func (f *Forwarder) Client(addr string) (streamqv1.StreamQInternalServiceClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conns == nil {
		return nil, errors.New("forwarder closed")
	}
	conn, ok := f.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		f.conns[addr] = conn
	}
	return streamqv1.NewStreamQInternalServiceClient(conn), nil
}

// Forward implements raftstore.Forwarder. Responses map back to metadata sentinels; an
// unreachable controller is reported as raftstore.ErrNotAppended so the caller may retry.
func (f *Forwarder) Forward(ctx context.Context, addr string, cmd *streamqv1.MetadataCommand) (uint64, error) {
	c, err := f.Client(addr)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", raftstore.ErrNotAppended, err)
	}
	resp, err := c.ForwardMetadata(ctx, &streamqv1.ForwardMetadataRequest{Command: cmd})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			// The request may not have reached the controller; a retry is safe because creates
			// and deletes are idempotent at the handler level (same topic id, DECISIONS D33).
			return 0, fmt.Errorf("%w: %w", raftstore.ErrNotAppended, err)
		}
		if status.Code(err) == codes.DeadlineExceeded {
			return 0, fmt.Errorf("forward to controller: %w: %w", context.DeadlineExceeded, err)
		}
		return 0, fmt.Errorf("forward to controller: %w", err)
	}
	return resp.GetIndex(), sentinelFor(resp.GetErrorCode(), resp.GetErrorMessage())
}

// Close closes cached connections.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var errs []error
	for _, c := range f.conns {
		errs = append(errs, c.Close())
	}
	f.conns = nil
	return errors.Join(errs...)
}

// errForwardFailed reports an unexpected error code from the controller.
var errForwardFailed = errors.New("forwarded command failed")

// sentinelFor maps a forwarded command's error code back to the metadata sentinel.
func sentinelFor(code streamqv1.ErrorCode, msg string) error {
	var base error
	switch code {
	case streamqv1.ErrorCode_ERROR_CODE_NONE:
		return nil
	case streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER:
		base = metadata.ErrNotController
	case streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS:
		base = metadata.ErrTopicExists
	case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION:
		base = metadata.ErrUnknownTopic
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR:
		base = metadata.ErrInvalidReplicationFactor
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC:
		base = metadata.ErrInvalidTopic
	case streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT:
		base = raftstore.ErrUnknownOutcome
	default:
		base = errForwardFailed
	}
	return fmt.Errorf("controller: %s: %w", msg, base)
}
