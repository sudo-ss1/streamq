// Package server implements the public gRPC API (SPEC §7.1). Handlers are thin: they validate the
// request, delegate to the broker or the metadata store, and map sentinel errors to ErrorCodes —
// that mapping happens only here (errorCode).
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/broker"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/group"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/raftstore"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// MaxMessageSize is the gRPC send/receive limit on server and client (DECISIONS D19).
const MaxMessageSize = 64 << 20

// maxPartitions bounds CreateTopic requests.
const maxPartitions = 10_000

// Server implements streamqv1.StreamQServiceServer.
type Server struct {
	streamqv1.UnimplementedStreamQServiceServer

	broker   *broker.Broker
	groups   *group.Coordinator
	store    metadata.Store
	defaults config.TopicDefaults
	clock    broker.Clock
	logger   *slog.Logger
}

// New returns a Server.
func New(b *broker.Broker, groups *group.Coordinator, store metadata.Store, defaults config.TopicDefaults,
	clock broker.Clock, logger *slog.Logger,
) *Server {
	return &Server{broker: b, groups: groups, store: store, defaults: defaults, clock: clock, logger: logger}
}

// errorCode maps an error to its API ErrorCode. Unexpected errors map to UNKNOWN and are logged.
func (s *Server) errorCode(err error) streamqv1.ErrorCode {
	code := errorCode(err)
	if code == streamqv1.ErrorCode_ERROR_CODE_UNKNOWN {
		s.logger.Error("unexpected request error", slog.Any("error", err))
	}
	return code
}

func errorCode(err error) streamqv1.ErrorCode {
	switch {
	case err == nil:
		return streamqv1.ErrorCode_ERROR_CODE_NONE
	case errors.Is(err, metadata.ErrUnknownTopic), errors.Is(err, broker.ErrUnknownPartition),
		errors.Is(err, storage.ErrClosed):
		return streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
	case errors.Is(err, metadata.ErrTopicExists):
		return streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS
	case errors.Is(err, metadata.ErrInvalidReplicationFactor):
		return streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR
	case errors.Is(err, metadata.ErrInvalidTopic):
		return streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC
	case errors.Is(err, config.ErrInvalidConfig):
		return streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG
	case errors.Is(err, partition.ErrNotLeader):
		return streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER
	case errors.Is(err, partition.ErrFencedLeaderEpoch), errors.Is(err, metadata.ErrFencedLeaderEpoch):
		return streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH
	case errors.Is(err, metadata.ErrInvalidISR):
		return streamqv1.ErrorCode_ERROR_CODE_INVALID_ISR
	case errors.Is(err, broker.ErrStaleBrokerEpoch):
		return streamqv1.ErrorCode_ERROR_CODE_STALE_BROKER_EPOCH
	case errors.Is(err, partition.ErrNotEnoughReplicas):
		return streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS
	case errors.Is(err, storage.ErrOffsetOutOfRange):
		return streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE
	case errors.Is(err, storage.ErrMessageTooLarge):
		return streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE
	case errors.Is(err, storage.ErrCorruptBatch):
		return streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE
	case errors.Is(err, storage.ErrInvalidTimestamp):
		return streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP
	case errors.Is(err, metadata.ErrNotController):
		return streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER
	case errors.Is(err, group.ErrUnknownMemberID):
		return streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID
	case errors.Is(err, group.ErrIllegalGeneration):
		return streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION
	case errors.Is(err, group.ErrRebalanceInProgress):
		return streamqv1.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS
	case errors.Is(err, group.ErrNotCoordinator), errors.Is(err, group.ErrCoordinatorClosed):
		return streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, raftstore.ErrUnknownOutcome),
		errors.Is(err, partition.ErrRequestTimedOut):
		return streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT
	default:
		return streamqv1.ErrorCode_ERROR_CODE_UNKNOWN
	}
}

func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// CreateTopic implements StreamQServiceServer.
func (s *Server) CreateTopic(ctx context.Context, req *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
	fail := func(err error) (*streamqv1.CreateTopicResponse, error) {
		return &streamqv1.CreateTopicResponse{ErrorCode: s.errorCode(err), ErrorMessage: errMessage(err),
			ControllerId: s.store.Snapshot().ControllerID}, nil
	}
	if err := metadata.ValidateTopicName(req.GetName()); err != nil {
		return fail(err)
	}
	n, rf := req.GetPartitions(), req.GetReplicationFactor()
	if n <= 0 {
		n = s.defaults.Partitions
	}
	if rf <= 0 {
		rf = s.defaults.ReplicationFactor
	}
	if n > maxPartitions {
		return fail(fmt.Errorf("%w: partitions %d > %d", config.ErrInvalidConfig, n, maxPartitions))
	}
	if _, err := config.ResolveTopicConfig(s.defaults, req.GetConfigs()); err != nil {
		return fail(err)
	}
	snap := s.store.Snapshot()
	if _, exists := snap.Topic(req.GetName()); exists {
		return fail(fmt.Errorf("%w: %s", metadata.ErrTopicExists, req.GetName()))
	}
	live := snap.LiveBrokers() // RF counts unfenced brokers (D36)
	ids := make([]int32, 0, len(live))
	for _, br := range live {
		ids = append(ids, br.ID)
	}
	assignment, err := metadata.AssignReplicas(req.GetName(), ids, n, rf)
	if err != nil {
		return fail(err)
	}
	topicID, err := broker.NewTopicID()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	parts := make([]*streamqv1.PartitionAssignment, len(assignment))
	for i, replicas := range assignment {
		parts[i] = &streamqv1.PartitionAssignment{Partition: int32(i), Replicas: replicas}
	}
	cmd := &streamqv1.MetadataCommand{
		Version:     metadata.CommandVersion,
		TimestampMs: s.clock.Now().UnixMilli(),
		Cmd: &streamqv1.MetadataCommand_CreateTopic{CreateTopic: &streamqv1.CreateTopicCommand{
			Name: req.GetName(), Configs: req.GetConfigs(), Partitions: parts, TopicId: topicID,
		}},
	}
	if err := s.store.Propose(ctx, cmd); err != nil {
		// A forwarding retry may have applied our own command already (same topic id, D33).
		if t, ok := s.store.Snapshot().Topic(req.GetName()); !ok || !errors.Is(err, metadata.ErrTopicExists) || t.ID != topicID {
			return fail(err)
		}
	}
	if err := s.broker.WaitTopicReady(ctx, req.GetName(), topicID); err != nil {
		return fail(err)
	}
	return &streamqv1.CreateTopicResponse{}, nil
}

// DeleteTopic implements StreamQServiceServer.
func (s *Server) DeleteTopic(ctx context.Context, req *streamqv1.DeleteTopicRequest) (*streamqv1.DeleteTopicResponse, error) {
	fail := func(err error) (*streamqv1.DeleteTopicResponse, error) {
		return &streamqv1.DeleteTopicResponse{ErrorCode: s.errorCode(err), ErrorMessage: errMessage(err),
			ControllerId: s.store.Snapshot().ControllerID}, nil
	}
	t, ok := s.store.Snapshot().Topic(req.GetName())
	if !ok {
		return fail(fmt.Errorf("%w: %s", metadata.ErrUnknownTopic, req.GetName()))
	}
	cmd := &streamqv1.MetadataCommand{
		Version:     metadata.CommandVersion,
		TimestampMs: s.clock.Now().UnixMilli(),
		Cmd:         &streamqv1.MetadataCommand_DeleteTopic{DeleteTopic: &streamqv1.DeleteTopicCommand{Name: req.GetName(), TopicId: t.ID}},
	}
	if err := s.store.Propose(ctx, cmd); err != nil {
		// A forwarding retry may have deleted it already (D33).
		if now, ok := s.store.Snapshot().Topic(req.GetName()); !errors.Is(err, metadata.ErrUnknownTopic) || (ok && now.ID == t.ID) {
			return fail(err)
		}
	}
	if err := s.broker.WaitTopicDeleted(ctx, req.GetName(), t.ID); err != nil {
		return fail(err)
	}
	return &streamqv1.DeleteTopicResponse{}, nil
}

// ListTopics implements StreamQServiceServer.
func (s *Server) ListTopics(_ context.Context, _ *streamqv1.ListTopicsRequest) (*streamqv1.ListTopicsResponse, error) {
	snap := s.store.Snapshot()
	names := make([]string, 0, len(snap.Topics))
	for name := range snap.Topics {
		names = append(names, name)
	}
	slices.Sort(names)
	return &streamqv1.ListTopicsResponse{Names: names}, nil
}

// Metadata implements StreamQServiceServer.
func (s *Server) Metadata(_ context.Context, req *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
	snap := s.store.Snapshot()
	resp := &streamqv1.MetadataResponse{ControllerId: snap.ControllerID}
	for _, br := range snap.LiveBrokers() {
		resp.Brokers = append(resp.Brokers, &streamqv1.Broker{Id: br.ID, Host: br.Host, Port: br.Port})
	}
	names := req.GetTopics()
	if len(names) == 0 {
		for name := range snap.Topics {
			names = append(names, name)
		}
		slices.Sort(names)
	}
	for _, name := range names {
		t, ok := snap.Topic(name)
		if !ok {
			resp.Topics = append(resp.Topics, &streamqv1.TopicMetadata{
				Name: name, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
			})
			continue
		}
		tm := &streamqv1.TopicMetadata{Name: name, Configs: t.Configs}
		for _, ps := range t.Partitions {
			tm.Partitions = append(tm.Partitions, &streamqv1.PartitionMetadata{
				Partition: ps.Partition, LeaderId: ps.Leader, LeaderEpoch: ps.LeaderEpoch,
				Replicas: ps.Replicas, Isr: ps.ISR,
			})
		}
		resp.Topics = append(resp.Topics, tm)
	}
	return resp, nil
}

func toAcks(a streamqv1.Acks) (partition.Acks, error) {
	switch a {
	case streamqv1.Acks_ACKS_NONE:
		return partition.AcksNone, nil
	case streamqv1.Acks_ACKS_LEADER:
		return partition.AcksLeader, nil
	case streamqv1.Acks_ACKS_ALL, streamqv1.Acks_ACKS_UNSPECIFIED:
		return partition.AcksAll, nil
	default:
		return 0, status.Errorf(codes.InvalidArgument, "unknown acks %d", a)
	}
}

// Produce implements StreamQServiceServer.
func (s *Server) Produce(ctx context.Context, req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
	acks, err := toAcks(req.GetAcks())
	if err != nil {
		return nil, err
	}
	if req.GetTimeoutMs() < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative timeout_ms")
	}
	var reqs []broker.ProduceRequest
	for _, t := range req.GetTopics() {
		for _, p := range t.GetPartitions() {
			reqs = append(reqs, broker.ProduceRequest{
				TP:          storage.TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()},
				LeaderEpoch: p.GetLeaderEpoch(),
				Records:     p.GetRecords(),
			})
		}
	}
	results := s.broker.Produce(ctx, acks, time.Duration(req.GetTimeoutMs())*time.Millisecond, reqs)
	resp := &streamqv1.ProduceResponse{}
	byTopic := map[string]*streamqv1.ProduceTopicResponse{}
	for _, r := range results {
		pr := &streamqv1.ProducePartitionResponse{
			Partition: r.TP.Partition, ErrorCode: s.errorCode(r.Err), ErrorMessage: errMessage(r.Err),
			BaseOffset: r.BaseOffset, LogStartOffset: r.LogStartOffset,
			LeaderId: r.LeaderID, LeaderEpoch: r.LeaderEpoch,
		}
		if acks == partition.AcksNone {
			// acks=0: the client does not wait for results (Kafka); offsets and errors are dropped.
			pr = &streamqv1.ProducePartitionResponse{Partition: r.TP.Partition, BaseOffset: -1, LeaderId: -1, LeaderEpoch: -1}
		}
		tr, ok := byTopic[r.TP.Topic]
		if !ok {
			tr = &streamqv1.ProduceTopicResponse{Topic: r.TP.Topic}
			byTopic[r.TP.Topic] = tr
			resp.Topics = append(resp.Topics, tr)
		}
		tr.Partitions = append(tr.Partitions, pr)
	}
	return resp, nil
}

// Fetch implements StreamQServiceServer.
func (s *Server) Fetch(ctx context.Context, req *streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
	if req.GetMaxWaitMs() < 0 || req.GetMinBytes() < 0 || req.GetMaxBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative max_wait_ms, min_bytes or max_bytes")
	}
	var reqs []broker.FetchRequest
	for _, t := range req.GetTopics() {
		for _, p := range t.GetPartitions() {
			reqs = append(reqs, broker.FetchRequest{
				TP:          storage.TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()},
				LeaderEpoch: p.GetLeaderEpoch(),
				Offset:      p.GetFetchOffset(),
				MaxBytes:    int(p.GetMaxBytes()),
			})
		}
	}
	wait := time.Duration(req.GetMaxWaitMs()) * time.Millisecond
	results := s.broker.Fetch(ctx, wait, int(req.GetMinBytes()), int(req.GetMaxBytes()), reqs)
	resp := &streamqv1.FetchResponse{}
	byTopic := map[string]*streamqv1.FetchTopicResponse{}
	for _, r := range results {
		pr := &streamqv1.FetchPartitionResponse{
			Partition: r.TP.Partition, ErrorCode: s.errorCode(r.Err), HighWatermark: r.HighWatermark,
			LogStartOffset: r.LogStartOffset, Records: r.Records, LeaderId: r.LeaderID, LeaderEpoch: r.LeaderEpoch,
		}
		tr, ok := byTopic[r.TP.Topic]
		if !ok {
			tr = &streamqv1.FetchTopicResponse{Topic: r.TP.Topic}
			byTopic[r.TP.Topic] = tr
			resp.Topics = append(resp.Topics, tr)
		}
		tr.Partitions = append(tr.Partitions, pr)
	}
	return resp, nil
}

// ListOffsets implements StreamQServiceServer.
func (s *Server) ListOffsets(ctx context.Context, req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
	var reqs []broker.ListOffsetsRequest
	for _, t := range req.GetTopics() {
		for _, p := range t.GetPartitions() {
			if p.GetTimestamp() < partition.EarliestTimestamp {
				return nil, status.Errorf(codes.InvalidArgument, "invalid timestamp %d", p.GetTimestamp())
			}
			reqs = append(reqs, broker.ListOffsetsRequest{
				TP:          storage.TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()},
				LeaderEpoch: p.GetLeaderEpoch(),
				Timestamp:   p.GetTimestamp(),
			})
		}
	}
	results := s.broker.ListOffsets(ctx, reqs)
	resp := &streamqv1.ListOffsetsResponse{}
	byTopic := map[string]*streamqv1.ListOffsetsTopicResponse{}
	for _, r := range results {
		pr := &streamqv1.ListOffsetsPartitionResponse{
			Partition: r.TP.Partition, ErrorCode: s.errorCode(r.Err), Offset: r.Offset, Timestamp: r.Timestamp,
			LeaderId: r.LeaderID, LeaderEpoch: r.LeaderEpoch,
		}
		tr, ok := byTopic[r.TP.Topic]
		if !ok {
			tr = &streamqv1.ListOffsetsTopicResponse{Topic: r.TP.Topic}
			byTopic[r.TP.Topic] = tr
			resp.Topics = append(resp.Topics, tr)
		}
		tr.Partitions = append(tr.Partitions, pr)
	}
	return resp, nil
}
