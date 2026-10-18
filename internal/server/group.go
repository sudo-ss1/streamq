package server

import (
	"context"
	"errors"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/group"
)

// groupErr maps request-level group errors: invalid requests become gRPC InvalidArgument (D29);
// everything else is an ErrorCode.
func (s *Server) groupErr(err error) (streamqv1.ErrorCode, string, error) {
	if errors.Is(err, group.ErrInvalidGroup) {
		return 0, "", status.Error(codes.InvalidArgument, err.Error())
	}
	return s.errorCode(err), errMessage(err), nil
}

func toProtoAssignment(a group.Assignment) []*streamqv1.TopicPartitions {
	topics := make([]string, 0, len(a))
	for t := range a {
		topics = append(topics, t)
	}
	slices.Sort(topics)
	out := make([]*streamqv1.TopicPartitions, 0, len(topics))
	for _, t := range topics {
		out = append(out, &streamqv1.TopicPartitions{Topic: t, Partitions: slices.Clone(a[t])})
	}
	return out
}

// FindCoordinator implements StreamQServiceServer. The controller coordinates every group (SPEC
// §5); in a single-node cluster that is this broker.
func (s *Server) FindCoordinator(_ context.Context, req *streamqv1.FindCoordinatorRequest) (*streamqv1.FindCoordinatorResponse, error) {
	if req.GetGroupId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty group id")
	}
	snap := s.store.Snapshot()
	for _, b := range snap.Brokers {
		if b.ID == snap.ControllerID {
			return &streamqv1.FindCoordinatorResponse{Coordinator: &streamqv1.Broker{Id: b.ID, Host: b.Host, Port: b.Port}}, nil
		}
	}
	return &streamqv1.FindCoordinatorResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR,
		ErrorMessage: "no controller known"}, nil
}

// JoinGroup implements StreamQServiceServer.
func (s *Server) JoinGroup(ctx context.Context, req *streamqv1.JoinGroupRequest) (*streamqv1.JoinGroupResponse, error) {
	res, err := s.groups.Join(ctx, group.JoinRequest{
		GroupID:          req.GetGroupId(),
		MemberID:         req.GetMemberId(),
		SessionTimeout:   time.Duration(req.GetSessionTimeoutMs()) * time.Millisecond,
		RebalanceTimeout: time.Duration(req.GetRebalanceTimeoutMs()) * time.Millisecond,
		Topics:           req.GetTopics(),
		Assignors:        req.GetAssignors(),
	})
	if err != nil {
		code, msg, gerr := s.groupErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &streamqv1.JoinGroupResponse{ErrorCode: code, ErrorMessage: msg, GenerationId: -1}, nil
	}
	return &streamqv1.JoinGroupResponse{
		GenerationId: res.GenerationID, MemberId: res.MemberID, Assignor: res.Assignor,
		Assignment: toProtoAssignment(res.Assignment),
	}, nil
}

// SyncGroup implements StreamQServiceServer.
func (s *Server) SyncGroup(_ context.Context, req *streamqv1.SyncGroupRequest) (*streamqv1.SyncGroupResponse, error) {
	a, err := s.groups.Sync(req.GetGroupId(), req.GetMemberId(), req.GetGenerationId())
	if err != nil {
		code, msg, gerr := s.groupErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &streamqv1.SyncGroupResponse{ErrorCode: code, ErrorMessage: msg}, nil
	}
	return &streamqv1.SyncGroupResponse{Assignment: toProtoAssignment(a)}, nil
}

// Heartbeat implements StreamQServiceServer.
func (s *Server) Heartbeat(_ context.Context, req *streamqv1.HeartbeatRequest) (*streamqv1.HeartbeatResponse, error) {
	err := s.groups.Heartbeat(req.GetGroupId(), req.GetMemberId(), req.GetGenerationId())
	code, msg, gerr := s.groupErr(err)
	if gerr != nil {
		return nil, gerr
	}
	return &streamqv1.HeartbeatResponse{ErrorCode: code, ErrorMessage: msg}, nil
}

// LeaveGroup implements StreamQServiceServer.
func (s *Server) LeaveGroup(_ context.Context, req *streamqv1.LeaveGroupRequest) (*streamqv1.LeaveGroupResponse, error) {
	err := s.groups.Leave(req.GetGroupId(), req.GetMemberId())
	code, msg, gerr := s.groupErr(err)
	if gerr != nil {
		return nil, gerr
	}
	return &streamqv1.LeaveGroupResponse{ErrorCode: code, ErrorMessage: msg}, nil
}

// CommitOffset implements StreamQServiceServer.
func (s *Server) CommitOffset(ctx context.Context, req *streamqv1.CommitOffsetRequest) (*streamqv1.CommitOffsetResponse, error) {
	offsets := make(map[group.TopicPartition]int64, len(req.GetOffsets()))
	for _, o := range req.GetOffsets() {
		if o.GetOffset() < 0 {
			return nil, status.Errorf(codes.InvalidArgument, "negative offset for %s/%d", o.GetTopic(), o.GetPartition())
		}
		offsets[group.TopicPartition{Topic: o.GetTopic(), Partition: o.GetPartition()}] = o.GetOffset()
	}
	perPartition, err := s.groups.Commit(ctx, req.GetGroupId(), req.GetMemberId(), req.GetGenerationId(), offsets)
	if err != nil {
		code, msg, gerr := s.groupErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &streamqv1.CommitOffsetResponse{ErrorCode: code, ErrorMessage: msg}, nil
	}
	resp := &streamqv1.CommitOffsetResponse{}
	for _, o := range req.GetOffsets() {
		tp := group.TopicPartition{Topic: o.GetTopic(), Partition: o.GetPartition()}
		resp.Results = append(resp.Results, &streamqv1.OffsetCommitResult{
			Topic: tp.Topic, Partition: tp.Partition, ErrorCode: s.errorCode(perPartition[tp]),
		})
	}
	return resp, nil
}

// FetchOffset implements StreamQServiceServer.
func (s *Server) FetchOffset(_ context.Context, req *streamqv1.FetchOffsetRequest) (*streamqv1.FetchOffsetResponse, error) {
	var tps []group.TopicPartition
	for _, t := range req.GetPartitions() {
		for _, p := range t.GetPartitions() {
			tps = append(tps, group.TopicPartition{Topic: t.GetTopic(), Partition: p})
		}
	}
	offs, err := s.groups.FetchOffsets(req.GetGroupId(), tps)
	if err != nil {
		code, msg, gerr := s.groupErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &streamqv1.FetchOffsetResponse{ErrorCode: code, ErrorMessage: msg}, nil
	}
	resp := &streamqv1.FetchOffsetResponse{}
	keys := make([]group.TopicPartition, 0, len(offs))
	for tp := range offs {
		keys = append(keys, tp)
	}
	slices.SortFunc(keys, compareTP)
	for _, tp := range keys {
		resp.Offsets = append(resp.Offsets, &streamqv1.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: offs[tp]})
	}
	return resp, nil
}

func compareTP(a, b group.TopicPartition) int {
	if a.Topic != b.Topic {
		if a.Topic < b.Topic {
			return -1
		}
		return 1
	}
	return int(a.Partition) - int(b.Partition)
}

// ListGroups implements StreamQServiceServer.
func (s *Server) ListGroups(_ context.Context, _ *streamqv1.ListGroupsRequest) (*streamqv1.ListGroupsResponse, error) {
	ids, err := s.groups.List()
	if err != nil {
		return &streamqv1.ListGroupsResponse{ErrorCode: s.errorCode(err), ErrorMessage: errMessage(err)}, nil
	}
	return &streamqv1.ListGroupsResponse{GroupIds: ids}, nil
}

// DescribeGroup implements StreamQServiceServer.
func (s *Server) DescribeGroup(_ context.Context, req *streamqv1.DescribeGroupRequest) (*streamqv1.DescribeGroupResponse, error) {
	d, err := s.groups.Describe(req.GetGroupId())
	if err != nil {
		code, msg, gerr := s.groupErr(err)
		if gerr != nil {
			return nil, gerr
		}
		return &streamqv1.DescribeGroupResponse{ErrorCode: code, ErrorMessage: msg, GroupId: req.GetGroupId()}, nil
	}
	resp := &streamqv1.DescribeGroupResponse{GroupId: d.GroupID, State: d.State, GenerationId: d.Generation, Assignor: d.Assignor}
	for _, m := range d.Members {
		resp.Members = append(resp.Members, &streamqv1.GroupMember{MemberId: m.MemberID, Assignment: toProtoAssignment(m.Assignment)})
	}
	keys := make([]group.TopicPartition, 0, len(d.Committed))
	for tp := range d.Committed {
		keys = append(keys, tp)
	}
	slices.SortFunc(keys, compareTP)
	for _, tp := range keys {
		resp.Committed = append(resp.Committed, &streamqv1.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: d.Committed[tp]})
	}
	return resp, nil
}
