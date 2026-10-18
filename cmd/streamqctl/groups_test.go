package main

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// groupsFake extends fakeServer with the group RPCs; ListOffsets uses the embedded fake's
// listOffsets hook. Unset hooks return Unimplemented.
type groupsFake struct {
	*fakeServer

	listGroups    func(*streamqv1.ListGroupsRequest) *streamqv1.ListGroupsResponse
	describeGroup func(*streamqv1.DescribeGroupRequest) *streamqv1.DescribeGroupResponse
}

func (f *groupsFake) ListGroups(ctx context.Context, r *streamqv1.ListGroupsRequest) (*streamqv1.ListGroupsResponse, error) {
	if f.listGroups == nil {
		return f.UnimplementedStreamQServiceServer.ListGroups(ctx, r)
	}
	return f.listGroups(r), nil
}

func (f *groupsFake) DescribeGroup(ctx context.Context, r *streamqv1.DescribeGroupRequest) (*streamqv1.DescribeGroupResponse, error) {
	if f.describeGroup == nil {
		return f.UnimplementedStreamQServiceServer.DescribeGroup(ctx, r)
	}
	return f.describeGroup(r), nil
}

func TestGroupsList(t *testing.T) {
	tests := []struct {
		name    string
		resp    *streamqv1.ListGroupsResponse
		wantOut string
		wantErr string
	}{
		{
			name:    "sorted",
			resp:    &streamqv1.ListGroupsResponse{GroupIds: []string{"zeta", "alpha", "mid"}},
			wantOut: "alpha\nmid\nzeta\n",
		},
		{
			name:    "empty",
			resp:    &streamqv1.ListGroupsResponse{},
			wantOut: "",
		},
		{
			name:    "error code",
			resp:    &streamqv1.ListGroupsResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR, ErrorMessage: "stopped"},
			wantErr: "list groups: NOT_COORDINATOR: stopped",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := &groupsFake{
				fakeServer: &fakeServer{},
				listGroups: func(*streamqv1.ListGroupsRequest) *streamqv1.ListGroupsResponse { return tt.resp },
			}
			res := runCmd(t, srv, "", "groups", "list")
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				require.Empty(t, res.out)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, tt.wantOut, res.out)
		})
	}
}

func TestGroupsDescribe(t *testing.T) {
	tests := []struct {
		name    string
		resp    *streamqv1.DescribeGroupResponse
		wantOut string
		wantErr string
	}{
		{
			name: "members sorted",
			resp: &streamqv1.DescribeGroupResponse{
				GroupId: "g1", State: "Stable", GenerationId: 3, Assignor: "range",
				Members: []*streamqv1.GroupMember{
					{MemberId: "m-b", Assignment: []*streamqv1.TopicPartitions{
						{Topic: "orders", Partitions: []int32{3, 2}},
					}},
					{MemberId: "m-a", Assignment: []*streamqv1.TopicPartitions{
						{Topic: "payments", Partitions: []int32{0}},
						{Topic: "orders", Partitions: []int32{1, 0}},
					}},
					{MemberId: "m-c"},
				},
			},
			wantOut: "GROUP       g1\n" +
				"STATE       Stable\n" +
				"GENERATION  3\n" +
				"ASSIGNOR    range\n" +
				"\n" +
				"MEMBER  TOPIC     PARTITIONS\n" +
				"m-a     orders    0,1\n" +
				"m-a     payments  0\n" +
				"m-b     orders    2,3\n" +
				"m-c     -         -\n",
		},
		{
			name: "no members",
			resp: &streamqv1.DescribeGroupResponse{GroupId: "g1", State: "Empty"},
			wantOut: "GROUP       g1\n" +
				"STATE       Empty\n" +
				"GENERATION  0\n" +
				"ASSIGNOR    -\n" +
				"no active members\n",
		},
		{
			name:    "unknown group",
			resp:    &streamqv1.DescribeGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN, ErrorMessage: "no such group"},
			wantErr: `describe group "g1": UNKNOWN: no such group`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu  sync.Mutex
				got []*streamqv1.DescribeGroupRequest
			)
			srv := &groupsFake{
				fakeServer: &fakeServer{},
				describeGroup: func(r *streamqv1.DescribeGroupRequest) *streamqv1.DescribeGroupResponse {
					mu.Lock()
					defer mu.Unlock()
					got = append(got, r)
					return tt.resp
				},
			}
			res := runCmd(t, srv, "", "groups", "describe", "g1")
			mu.Lock()
			require.Len(t, got, 1)
			require.Equal(t, "g1", got[0].GetGroupId())
			mu.Unlock()
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				require.Empty(t, res.out)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, tt.wantOut, res.out)
		})
	}
}

func TestGroupsDescribeArgs(t *testing.T) {
	res := runCmd(t, &groupsFake{fakeServer: &fakeServer{}}, "", "groups", "describe")
	require.ErrorContains(t, res.err, "accepts 1 arg(s)")
}

func TestGroupsLag(t *testing.T) {
	ok := streamqv1.ErrorCode_ERROR_CODE_NONE
	tests := []struct {
		name      string
		committed []*streamqv1.OffsetCommit
		describe  *streamqv1.DescribeGroupResponse // overrides committed when set
		hws       map[topicPartition]int64         // HW per partition for successful answers
		errs      map[topicPartition]streamqv1.ErrorCode
		rpcErr    error
		wantReq   *streamqv1.ListOffsetsRequest // nil: no ListOffsets expected
		wantOut   string
		wantErr   string
	}{
		{
			name: "lag math and total",
			committed: []*streamqv1.OffsetCommit{
				{Topic: "orders", Partition: 1, Offset: 5},
				{Topic: "audit", Partition: 0, Offset: 7},
				{Topic: "orders", Partition: 0, Offset: 10},
			},
			hws: map[topicPartition]int64{
				{"orders", 0}: 15, {"orders", 1}: 5, {"audit", 0}: 100,
			},
			wantReq: &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{
				{Topic: "audit", Partitions: []*streamqv1.ListOffsetsPartition{{Partition: 0, LeaderEpoch: -1, Timestamp: -1}}},
				{Topic: "orders", Partitions: []*streamqv1.ListOffsetsPartition{
					{Partition: 0, LeaderEpoch: -1, Timestamp: -1},
					{Partition: 1, LeaderEpoch: -1, Timestamp: -1},
				}},
			}},
			wantOut: "TOPIC   PARTITION  COMMITTED  HIGH-WATERMARK  LAG\n" +
				"audit   0          7          100             93\n" +
				"orders  0          10         15              5\n" +
				"orders  1          5          5               0\n" +
				"TOTAL LAG 98\n",
		},
		{
			name:      "committed beyond HW clamps to zero",
			committed: []*streamqv1.OffsetCommit{{Topic: "t", Partition: 0, Offset: 20}},
			hws:       map[topicPartition]int64{{"t", 0}: 12},
			wantReq: &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{
				{Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{{Partition: 0, LeaderEpoch: -1, Timestamp: -1}}},
			}},
			wantOut: "TOPIC  PARTITION  COMMITTED  HIGH-WATERMARK  LAG\n" +
				"t      0          20         12              0\n" +
				"TOTAL LAG 0\n",
		},
		{
			name: "partial ListOffsets errors",
			committed: []*streamqv1.OffsetCommit{
				{Topic: "t", Partition: 0, Offset: 1},
				{Topic: "t", Partition: 1, Offset: 2},
				{Topic: "t", Partition: 2, Offset: 3},
			},
			hws:  map[topicPartition]int64{{"t", 0}: 4},
			errs: map[topicPartition]streamqv1.ErrorCode{{"t", 1}: streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER},
			// t/2 is missing from the response entirely.
			wantReq: &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{
				{Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{
					{Partition: 0, LeaderEpoch: -1, Timestamp: -1},
					{Partition: 1, LeaderEpoch: -1, Timestamp: -1},
					{Partition: 2, LeaderEpoch: -1, Timestamp: -1},
				}},
			}},
			wantOut: "TOPIC  PARTITION  COMMITTED  HIGH-WATERMARK  LAG\n" +
				"t      0          1          4               3\n" +
				"t      1          2          -               -\n" +
				"t      2          3          -               -\n" +
				"TOTAL LAG 3\n",
		},
		{
			name:    "no committed offsets",
			wantOut: "TOPIC  PARTITION  COMMITTED  HIGH-WATERMARK  LAG\nTOTAL LAG 0\n",
		},
		{
			name:      "ListOffsets RPC failure",
			committed: []*streamqv1.OffsetCommit{{Topic: "t", Partition: 0, Offset: 1}},
			rpcErr:    status.Error(codes.Unavailable, "down"),
			wantReq: &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{
				{Topic: "t", Partitions: []*streamqv1.ListOffsetsPartition{{Partition: 0, LeaderEpoch: -1, Timestamp: -1}}},
			}},
			wantErr: "list offsets",
		},
		{
			name:     "describe error code",
			describe: &streamqv1.DescribeGroupResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_COORDINATOR},
			wantErr:  `describe group "g1": NOT_COORDINATOR`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu  sync.Mutex
				got []*streamqv1.ListOffsetsRequest
			)
			describe := tt.describe
			if describe == nil {
				describe = &streamqv1.DescribeGroupResponse{GroupId: "g1", State: "Empty", Committed: tt.committed}
			}
			srv := &groupsFake{
				fakeServer: &fakeServer{},
				describeGroup: func(*streamqv1.DescribeGroupRequest) *streamqv1.DescribeGroupResponse {
					return describe
				},
			}
			srv.listOffsets = func(r *streamqv1.ListOffsetsRequest) *streamqv1.ListOffsetsResponse {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, r)
				resp := &streamqv1.ListOffsetsResponse{}
				for _, rt := range r.GetTopics() {
					tr := &streamqv1.ListOffsetsTopicResponse{Topic: rt.GetTopic()}
					for _, rp := range rt.GetPartitions() {
						tp := topicPartition{rt.GetTopic(), rp.GetPartition()}
						if code, bad := tt.errs[tp]; bad {
							tr.Partitions = append(tr.Partitions, &streamqv1.ListOffsetsPartitionResponse{
								Partition: rp.GetPartition(), ErrorCode: code, Offset: -1, Timestamp: -1,
							})
							continue
						}
						if hw, has := tt.hws[tp]; has {
							tr.Partitions = append(tr.Partitions, &streamqv1.ListOffsetsPartitionResponse{
								Partition: rp.GetPartition(), ErrorCode: ok, Offset: hw, Timestamp: -1,
							})
						}
					}
					resp.Topics = append(resp.Topics, tr)
				}
				return resp
			}
			var lo streamqv1.StreamQServiceServer = srv
			if tt.rpcErr != nil {
				lo = &failingListOffsets{groupsFake: srv, err: tt.rpcErr, record: func(r *streamqv1.ListOffsetsRequest) {
					mu.Lock()
					defer mu.Unlock()
					got = append(got, r)
				}}
			}
			res := runCmd(t, lo, "", "groups", "lag", "g1")

			mu.Lock()
			if tt.wantReq == nil {
				require.Empty(t, got)
			} else {
				require.Len(t, got, 1)
				require.True(t, proto.Equal(tt.wantReq, got[0]), "got %v", got[0])
			}
			mu.Unlock()

			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				require.Empty(t, res.out)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, tt.wantOut, res.out)
		})
	}
}

// failingListOffsets answers ListOffsets with a gRPC error after recording the request.
type failingListOffsets struct {
	*groupsFake
	err    error
	record func(*streamqv1.ListOffsetsRequest)
}

func (f *failingListOffsets) ListOffsets(_ context.Context, r *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
	f.record(r)
	return nil, f.err
}
