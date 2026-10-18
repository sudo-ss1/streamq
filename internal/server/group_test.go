package server_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

func TestGroupHandlers(t *testing.T) {
	c := startNode(t, t.TempDir())
	ctx := ctxT(t)
	_, err := c.CreateTopic(ctx, &streamqv1.CreateTopicRequest{Name: "t", Partitions: 2})
	require.NoError(t, err)

	fc, err := c.FindCoordinator(ctx, &streamqv1.FindCoordinatorRequest{GroupId: "g"})
	require.NoError(t, err)
	require.Equal(t, int32(1), fc.GetCoordinator().GetId())

	// Invalid requests are gRPC InvalidArgument (D29).
	invalid := []func() error{
		func() error { _, err := c.FindCoordinator(ctx, &streamqv1.FindCoordinatorRequest{}); return err },
		func() error {
			_, err := c.JoinGroup(ctx, &streamqv1.JoinGroupRequest{Topics: []string{"t"}})
			return err
		},
		func() error {
			_, err := c.JoinGroup(ctx, &streamqv1.JoinGroupRequest{GroupId: "g", Topics: []string{"t"}, Assignors: []string{"sticky"}})
			return err
		},
		func() error {
			_, err := c.CommitOffset(ctx, &streamqv1.CommitOffsetRequest{GroupId: "g", GenerationId: -1,
				Offsets: []*streamqv1.OffsetCommit{{Topic: "t", Offset: -5}}})
			return err
		},
	}
	for i, call := range invalid {
		require.Equal(t, codes.InvalidArgument, status.Code(call()), "case %d", i)
	}

	// A standalone commit mixing a real and a missing topic: per-partition results.
	resp, err := c.CommitOffset(ctx, &streamqv1.CommitOffsetRequest{GroupId: "g", GenerationId: -1, Offsets: []*streamqv1.OffsetCommit{
		{Topic: "t", Partition: 1, Offset: 7}, {Topic: "gone", Partition: 0, Offset: 1}, {Topic: "t", Partition: 0, Offset: 3},
	}})
	require.NoError(t, err)
	require.Equal(t, codeNone, resp.GetErrorCode())
	codesByTopic := map[string]streamqv1.ErrorCode{}
	for _, r := range resp.GetResults() {
		codesByTopic[r.GetTopic()] = r.GetErrorCode()
	}
	require.Equal(t, codeNone, codesByTopic["t"])
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, codesByTopic["gone"])

	fo, err := c.FetchOffset(ctx, &streamqv1.FetchOffsetRequest{GroupId: "g"})
	require.NoError(t, err)
	require.Len(t, fo.GetOffsets(), 2)
	require.Equal(t, int32(0), fo.GetOffsets()[0].GetPartition(), "sorted by topic, partition")
	require.Equal(t, int64(3), fo.GetOffsets()[0].GetOffset())
	require.Equal(t, int64(7), fo.GetOffsets()[1].GetOffset())

	// Join → sync → heartbeat → commit → describe → leave.
	jr, err := c.JoinGroup(ctx, &streamqv1.JoinGroupRequest{GroupId: "g", Topics: []string{"t"}, SessionTimeoutMs: 10_000})
	require.NoError(t, err)
	require.Equal(t, codeNone, jr.GetErrorCode(), jr.GetErrorMessage())
	require.Equal(t, "range", jr.GetAssignor())
	sr, err := c.SyncGroup(ctx, &streamqv1.SyncGroupRequest{GroupId: "g", MemberId: jr.GetMemberId(), GenerationId: jr.GetGenerationId()})
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1}, sr.GetAssignment()[0].GetPartitions())
	hb, err := c.Heartbeat(ctx, &streamqv1.HeartbeatRequest{GroupId: "g", MemberId: jr.GetMemberId(), GenerationId: jr.GetGenerationId() + 1})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_ILLEGAL_GENERATION, hb.GetErrorCode())
	hb, err = c.Heartbeat(ctx, &streamqv1.HeartbeatRequest{GroupId: "g", MemberId: "nobody", GenerationId: 1})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_MEMBER_ID, hb.GetErrorCode())
	desc, err := c.DescribeGroup(ctx, &streamqv1.DescribeGroupRequest{GroupId: "g"})
	require.NoError(t, err)
	require.Equal(t, "Stable", desc.GetState())
	require.Len(t, desc.GetMembers(), 1)
	require.Len(t, desc.GetCommitted(), 2)
	lg, err := c.ListGroups(ctx, &streamqv1.ListGroupsRequest{})
	require.NoError(t, err)
	require.Equal(t, []string{"g"}, lg.GetGroupIds())
	lv, err := c.LeaveGroup(ctx, &streamqv1.LeaveGroupRequest{GroupId: "g", MemberId: jr.GetMemberId()})
	require.NoError(t, err)
	require.Equal(t, codeNone, lv.GetErrorCode())
}
