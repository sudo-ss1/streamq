package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/controller"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/raftstore"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// errBrokerNotReady reports a replica RPC that arrived before the local broker opened; followers
// retry it like NOT_LEADER.
var errBrokerNotReady = fmt.Errorf("%w: broker not open yet", partition.ErrNotLeader)

// SetBroker attaches the local broker once it is open; replica RPCs are refused until then.
func (s *InternalServer) SetBroker(b *broker.Broker) { s.broker.Store(b) }

// ReplicaFetch implements StreamQInternalServiceServer.
func (s *InternalServer) ReplicaFetch(ctx context.Context, req *streamqv1.ReplicaFetchRequest) (*streamqv1.ReplicaFetchResponse, error) {
	b := s.broker.Load()
	if b == nil {
		return &streamqv1.ReplicaFetchResponse{ErrorCode: s.code(errBrokerNotReady), ErrorMessage: errBrokerNotReady.Error()}, nil
	}
	res, err := b.ReplicaFetch(ctx, broker.ReplicaFetchRequest{
		TP:          storage.TopicPartition{Topic: req.GetTopic(), Partition: req.GetPartition()},
		ReplicaID:   req.GetReplicaId(),
		BrokerEpoch: req.GetBrokerEpoch(),
		LeaderEpoch: req.GetLeaderEpoch(),
		FetchOffset: req.GetFetchOffset(),
		MaxBytes:    int(req.GetMaxBytes()),
		MaxWait:     time.Duration(req.GetMaxWaitMs()) * time.Millisecond,
	})
	resp := &streamqv1.ReplicaFetchResponse{HighWatermark: res.HighWatermark, LogStartOffset: res.LogStartOffset,
		LogEndOffset: res.LogEndOffset, Records: res.Records}
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = s.code(err), err.Error()
	}
	return resp, nil
}

// OffsetForLeaderEpoch implements StreamQInternalServiceServer.
func (s *InternalServer) OffsetForLeaderEpoch(_ context.Context, req *streamqv1.OffsetForLeaderEpochRequest) (*streamqv1.OffsetForLeaderEpochResponse, error) {
	b := s.broker.Load()
	if b == nil {
		return &streamqv1.OffsetForLeaderEpochResponse{ErrorCode: s.code(errBrokerNotReady), ErrorMessage: errBrokerNotReady.Error(),
			LeaderEpoch: -1, EndOffset: -1, LogStartOffset: -1}, nil
	}
	r, err := b.OffsetForLeaderEpoch(storage.TopicPartition{Topic: req.GetTopic(), Partition: req.GetPartition()},
		req.GetCurrentLeaderEpoch(), req.GetRequestedEpoch())
	resp := &streamqv1.OffsetForLeaderEpochResponse{LeaderEpoch: r.Epoch, EndOffset: r.EndOffset, LogStartOffset: r.LogStartOffset}
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = s.code(err), err.Error()
	}
	return resp, nil
}

// AlterISR implements StreamQInternalServiceServer.
func (s *InternalServer) AlterISR(ctx context.Context, req *streamqv1.AlterISRRequest) (*streamqv1.AlterISRResponse, error) {
	err := s.ctrl.AlterISR(ctx, controller.AlterISRRequest{
		Topic: req.GetTopic(), Partition: req.GetPartition(), LeaderEpoch: req.GetLeaderEpoch(),
		PartitionEpoch: req.GetPartitionEpoch(), NewISR: req.GetNewIsr(),
	})
	resp := &streamqv1.AlterISRResponse{ControllerId: s.store.LeaderID()}
	if err != nil {
		resp.ErrorCode, resp.ErrorMessage = s.code(err), err.Error()
	}
	return resp, nil
}

// errReplicaRPC reports an unexpected error code from a replica RPC; the fetcher backs off and
// retries.
var errReplicaRPC = errors.New("replica rpc failed")

// ReplicaClient sends follower RPCs to partition leaders and AlterISR to the controller over the
// internal API, resolving addresses from local metadata. It implements broker.LeaderClient.
type ReplicaClient struct {
	fwd   *Forwarder
	store metadata.Store
}

// NewReplicaClient returns a ReplicaClient sharing fwd's connections.
func NewReplicaClient(fwd *Forwarder, store metadata.Store) *ReplicaClient {
	return &ReplicaClient{fwd: fwd, store: store}
}

func (c *ReplicaClient) client(id int32) (streamqv1.StreamQInternalServiceClient, error) {
	b, ok := c.store.Snapshot().Broker(id)
	if !ok || b.InternalAddr == "" {
		return nil, fmt.Errorf("%w: no internal address for broker %d", errReplicaRPC, id)
	}
	return c.fwd.Client(b.InternalAddr)
}

// replicaSentinel maps a replica RPC's error code back to the sentinel the fetcher acts on.
func replicaSentinel(code streamqv1.ErrorCode, msg string) error {
	var base error
	switch code {
	case streamqv1.ErrorCode_ERROR_CODE_NONE:
		return nil
	case streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER:
		base = partition.ErrNotLeader
	case streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH:
		base = partition.ErrFencedLeaderEpoch
	case streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE:
		base = storage.ErrOffsetOutOfRange
	case streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION:
		base = broker.ErrUnknownPartition
	case streamqv1.ErrorCode_ERROR_CODE_STALE_BROKER_EPOCH:
		base = broker.ErrStaleBrokerEpoch
	default:
		base = errReplicaRPC
	}
	return fmt.Errorf("leader: %s: %w", msg, base)
}

// ReplicaFetch implements broker.LeaderClient.
func (c *ReplicaClient) ReplicaFetch(ctx context.Context, leaderID int32, req broker.ReplicaFetchRequest) (partition.ReplicaFetchResult, error) {
	cl, err := c.client(leaderID)
	if err != nil {
		return partition.ReplicaFetchResult{}, err
	}
	resp, err := cl.ReplicaFetch(ctx, &streamqv1.ReplicaFetchRequest{
		ReplicaId: req.ReplicaID, Topic: req.TP.Topic, Partition: req.TP.Partition, LeaderEpoch: req.LeaderEpoch,
		FetchOffset: req.FetchOffset, MaxBytes: int32(min(req.MaxBytes, broker.MaxFetchBytes)),
		MaxWaitMs: int32(req.MaxWait.Milliseconds()), BrokerEpoch: req.BrokerEpoch,
	})
	if err != nil {
		return partition.ReplicaFetchResult{}, fmt.Errorf("replica fetch from %d: %w", leaderID, err)
	}
	res := partition.ReplicaFetchResult{Records: resp.GetRecords(), HighWatermark: resp.GetHighWatermark(),
		LogStartOffset: resp.GetLogStartOffset(), LogEndOffset: resp.GetLogEndOffset()}
	return res, replicaSentinel(resp.GetErrorCode(), resp.GetErrorMessage())
}

// OffsetForLeaderEpoch implements broker.LeaderClient.
func (c *ReplicaClient) OffsetForLeaderEpoch(ctx context.Context, leaderID int32, tp storage.TopicPartition, currentEpoch, requestedEpoch int32) (broker.EpochEndOffset, error) {
	cl, err := c.client(leaderID)
	if err != nil {
		return broker.EpochEndOffset{}, err
	}
	resp, err := cl.OffsetForLeaderEpoch(ctx, &streamqv1.OffsetForLeaderEpochRequest{
		Topic: tp.Topic, Partition: tp.Partition, CurrentLeaderEpoch: currentEpoch, RequestedEpoch: requestedEpoch,
	})
	if err != nil {
		return broker.EpochEndOffset{}, fmt.Errorf("offset for leader epoch from %d: %w", leaderID, err)
	}
	if err := replicaSentinel(resp.GetErrorCode(), resp.GetErrorMessage()); err != nil {
		return broker.EpochEndOffset{}, err
	}
	return broker.EpochEndOffset{Epoch: resp.GetLeaderEpoch(), EndOffset: resp.GetEndOffset(), LogStartOffset: resp.GetLogStartOffset()}, nil
}

// AlterISR sends tp's ISR change to the controller (a broker.ISRSender). Only FENCED and
// INVALID_ISR are definitive; everything else (no controller, transport errors, unknown outcome)
// is retried by the partition.
func (c *ReplicaClient) AlterISR(ctx context.Context, tp storage.TopicPartition, req partition.AlterISRRequest) error {
	ctrl := c.store.Snapshot().ControllerID
	if ctrl < 0 {
		return fmt.Errorf("alter isr: %w", metadata.ErrNotController)
	}
	cl, err := c.client(ctrl)
	if err != nil {
		return err
	}
	resp, err := cl.AlterISR(ctx, &streamqv1.AlterISRRequest{
		Topic: tp.Topic, Partition: tp.Partition, LeaderEpoch: req.LeaderEpoch,
		PartitionEpoch: req.PartitionEpoch, NewIsr: req.NewISR,
	})
	if err != nil {
		return fmt.Errorf("alter isr: %w: %w", raftstore.ErrUnknownOutcome, err)
	}
	switch resp.GetErrorCode() {
	case streamqv1.ErrorCode_ERROR_CODE_NONE:
		return nil
	case streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH:
		return fmt.Errorf("controller: %s: %w", resp.GetErrorMessage(), metadata.ErrFencedLeaderEpoch)
	case streamqv1.ErrorCode_ERROR_CODE_INVALID_ISR:
		return fmt.Errorf("controller: %s: %w", resp.GetErrorMessage(), metadata.ErrInvalidISR)
	default:
		return sentinelFor(resp.GetErrorCode(), resp.GetErrorMessage())
	}
}
