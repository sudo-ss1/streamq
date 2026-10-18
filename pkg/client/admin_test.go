package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// adminFake wraps fakeBroker with scriptable CreateTopic/DeleteTopic/ListTopics and records
// their requests. A hook returning (nil, nil) falls through to the fakeBroker behaviour.
type adminFake struct {
	*fakeBroker

	amu        sync.Mutex
	createReqs []*streamqv1.CreateTopicRequest
	deleteReqs []*streamqv1.DeleteTopicRequest
	onCreate   func(context.Context, *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error)
	onDelete   func(context.Context, *streamqv1.DeleteTopicRequest) (*streamqv1.DeleteTopicResponse, error)
	onList     func() (*streamqv1.ListTopicsResponse, error)
}

func (a *adminFake) CreateTopic(ctx context.Context, req *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
	a.amu.Lock()
	a.createReqs = append(a.createReqs, req)
	hook := a.onCreate
	a.amu.Unlock()
	if hook != nil {
		if resp, err := hook(ctx, req); resp != nil || err != nil {
			return resp, err
		}
	}
	return a.fakeBroker.CreateTopic(ctx, req)
}

func (a *adminFake) DeleteTopic(ctx context.Context, req *streamqv1.DeleteTopicRequest) (*streamqv1.DeleteTopicResponse, error) {
	a.amu.Lock()
	a.deleteReqs = append(a.deleteReqs, req)
	hook := a.onDelete
	a.amu.Unlock()
	if hook != nil {
		if resp, err := hook(ctx, req); resp != nil || err != nil {
			return resp, err
		}
	}
	return a.fakeBroker.DeleteTopic(ctx, req)
}

func (a *adminFake) ListTopics(ctx context.Context, req *streamqv1.ListTopicsRequest) (*streamqv1.ListTopicsResponse, error) {
	a.amu.Lock()
	hook := a.onList
	a.amu.Unlock()
	if hook != nil {
		if resp, err := hook(); resp != nil || err != nil {
			return resp, err
		}
	}
	return a.fakeBroker.ListTopics(ctx, req)
}

// newAdminFake starts an adminFake at "b1:9092" with topics and returns it and an Admin.
func newAdminFake(t *testing.T, topics map[string]int32) (*adminFake, *Admin) {
	t.Helper()
	fc := newFakeCluster(t)
	af := &adminFake{fakeBroker: newFakeBroker(1, "b1", 9092)}
	for n, p := range topics {
		af.topics[n] = p
	}
	fc.add("b1:9092", af)
	return af, NewAdmin(fc.newClient("b1:9092"))
}

// adminUnavailableOnce returns a hook that fails its first call with a transport error, after
// running apply (e.g. the real mutation, so the server applied it but the client never heard).
func adminUnavailableOnce[Req, Resp any](apply func(context.Context, Req)) func(context.Context, Req) (Resp, error) {
	var once sync.Once
	return func(ctx context.Context, req Req) (Resp, error) {
		var (
			zero Resp
			err  error
		)
		once.Do(func() {
			if apply != nil {
				apply(ctx, req)
			}
			err = status.Error(codes.Unavailable, "connection reset")
		})
		return zero, err
	}
}

func TestAdminCreateTopic(t *testing.T) {
	af, admin := newAdminFake(t, nil)
	ctx := context.Background()
	cfgs := map[string]string{"retention.ms": "1000"}
	require.NoError(t, admin.CreateTopic(ctx, "orders", 3, 1, cfgs))
	require.Len(t, af.createReqs, 1)
	req := af.createReqs[0]
	require.Equal(t, "orders", req.GetName())
	require.Equal(t, int32(3), req.GetPartitions())
	require.Equal(t, int32(1), req.GetReplicationFactor())
	require.Equal(t, cfgs, req.GetConfigs())

	// Already exists without a prior transport error is an error even if the counts match.
	err := admin.CreateTopic(ctx, "orders", 3, 1, nil)
	require.ErrorIs(t, err, ErrTopicAlreadyExists)

	tests := []struct {
		code streamqv1.ErrorCode
		want error
	}{
		{streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR, ErrInvalidReplicationFactor},
		{streamqv1.ErrorCode_ERROR_CODE_INVALID_CONFIG, ErrInvalidConfig},
		{streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC, ErrInvalidTopic},
	}
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			af.amu.Lock()
			af.onCreate = func(context.Context, *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
				return &streamqv1.CreateTopicResponse{ErrorCode: tt.code, ErrorMessage: "detail"}, nil
			}
			af.amu.Unlock()
			err := admin.CreateTopic(ctx, "x", 1, 1, nil)
			require.ErrorIs(t, err, tt.want)
			require.ErrorContains(t, err, "detail")
		})
	}

	// NOT_CONTROLLER (no controller reachable during forwarding) is retried until ctx ends.
	af.amu.Lock()
	calls := 0
	af.onCreate = func(context.Context, *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
		calls++
		return &streamqv1.CreateTopicResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER, ErrorMessage: "detail"}, nil
	}
	af.amu.Unlock()
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = admin.CreateTopic(short, "x", 1, 1, nil)
	require.Error(t, err, "fails once ctx ends (with NOT_CONTROLLER or the deadline, whichever comes last)")
	af.amu.Lock()
	require.Greater(t, calls, 1, "NOT_CONTROLLER is retried")
	af.amu.Unlock()
}

func TestAdminCreateTopicAfterTransportError(t *testing.T) {
	tests := []struct {
		name      string
		existing  int32 // pre-existing partitions (0 = none)
		create    bool  // the failed attempt really created the topic
		requested int32
		wantErr   error
	}{
		{"created with requested count", 0, true, 3, nil},
		{"created with broker default", 0, true, 0, nil},
		{"pre-existing with other count", 2, false, 3, ErrTopicAlreadyExists},
		{"pre-existing any count accepted", 2, false, -1, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topics := map[string]int32{}
			if tt.existing > 0 {
				topics["t"] = tt.existing
			}
			af, admin := newAdminFake(t, topics)
			var apply func(context.Context, *streamqv1.CreateTopicRequest)
			if tt.create {
				apply = func(ctx context.Context, req *streamqv1.CreateTopicRequest) {
					_, _ = af.fakeBroker.CreateTopic(ctx, req) // never fails
				}
			}
			af.onCreate = adminUnavailableOnce[*streamqv1.CreateTopicRequest, *streamqv1.CreateTopicResponse](apply)
			err := admin.CreateTopic(context.Background(), "t", tt.requested, 1, nil)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Len(t, af.createReqs, 2)
		})
	}
}

func TestAdminDeleteTopic(t *testing.T) {
	af, admin := newAdminFake(t, map[string]int32{"a": 1, "b": 1})
	ctx := context.Background()
	require.NoError(t, admin.DeleteTopic(ctx, "a"))
	require.Equal(t, "a", af.deleteReqs[0].GetName())
	require.ErrorIs(t, admin.DeleteTopic(ctx, "a"), ErrUnknownTopicOrPartition)

	// A delete applied by an attempt whose answer was lost is reported as success.
	af.onDelete = adminUnavailableOnce[*streamqv1.DeleteTopicRequest, *streamqv1.DeleteTopicResponse](
		func(ctx context.Context, req *streamqv1.DeleteTopicRequest) {
			_, _ = af.fakeBroker.DeleteTopic(ctx, req) // "b" exists
		})
	require.NoError(t, admin.DeleteTopic(ctx, "b"))
	require.Len(t, af.deleteReqs, 4)
}

func TestAdminListTopics(t *testing.T) {
	af, admin := newAdminFake(t, map[string]int32{"zeta": 1, "alpha": 2, "mid": 1})
	af.onList = func() (*streamqv1.ListTopicsResponse, error) {
		af.amu.Lock()
		af.onList = nil // fail once with a transport error; the retry succeeds
		af.amu.Unlock()
		return nil, status.Error(codes.Unavailable, "down")
	}
	names, err := admin.ListTopics(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "mid", "zeta"}, names)

	af.amu.Lock()
	af.onList = func() (*streamqv1.ListTopicsResponse, error) {
		return nil, status.Error(codes.Internal, "boom")
	}
	af.amu.Unlock()
	_, err = admin.ListTopics(context.Background())
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestAdminDescribeTopics(t *testing.T) {
	af, admin := newAdminFake(t, nil)
	af.onMetadata = func(req *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		all := map[string]*streamqv1.TopicMetadata{
			"b": {Name: "b", Configs: map[string]string{"retention.ms": "5"}, Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 1, LeaderId: 2, LeaderEpoch: 7, Replicas: []int32{2, 3}, Isr: []int32{2}},
				{Partition: 0, LeaderId: 1, LeaderEpoch: 3, Replicas: []int32{1, 2}, Isr: []int32{1, 2}},
			}},
			"a": {Name: "a", Partitions: []*streamqv1.PartitionMetadata{{Partition: 0, LeaderId: 1}}},
		}
		resp := &streamqv1.MetadataResponse{Brokers: []*streamqv1.Broker{{Id: 1, Host: "b1", Port: 9092}}}
		names := req.GetTopics()
		if len(names) == 0 {
			names = []string{"b", "a"}
		}
		for _, n := range names {
			tm, ok := all[n]
			if !ok {
				tm = &streamqv1.TopicMetadata{Name: n, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION}
			}
			resp.Topics = append(resp.Topics, tm)
		}
		return resp, nil
	}
	ctx := context.Background()

	descs, err := admin.DescribeTopics(ctx)
	require.NoError(t, err)
	require.Len(t, descs, 2)
	require.Equal(t, "a", descs[0].Name)
	require.Equal(t, TopicDescription{
		Name:    "b",
		Configs: map[string]string{"retention.ms": "5"},
		Partitions: []PartitionDescription{
			{Partition: 0, Leader: 1, LeaderEpoch: 3, Replicas: []int32{1, 2}, ISR: []int32{1, 2}},
			{Partition: 1, Leader: 2, LeaderEpoch: 7, Replicas: []int32{2, 3}, ISR: []int32{2}},
		},
	}, descs[1])

	descs, err = admin.DescribeTopics(ctx, "b", "a", "b")
	require.NoError(t, err)
	require.Len(t, descs, 2)
	require.Equal(t, "b", descs[0].Name)
	require.Equal(t, "a", descs[1].Name)

	_, err = admin.DescribeTopics(ctx, "a", "nope")
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
}

func TestAdminListOffsets(t *testing.T) {
	af, admin := newAdminFake(t, map[string]int32{"t": 2})
	tp := TopicPartition{Topic: "t", Partition: 1}
	af.mu.Lock()
	for i := range 4 {
		af.logs[tp] = append(af.logs[tp], record.Record{Offset: int64(i), Timestamp: 100 * int64(i+1)})
	}
	af.mu.Unlock()
	var (
		lmu  sync.Mutex
		reqs []*streamqv1.ListOffsetsRequest
	)
	af.onListOffsets = func(req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		lmu.Lock()
		reqs = append(reqs, req)
		lmu.Unlock()
		return nil, nil
	}
	ctx := context.Background()

	tests := []struct {
		ts   int64
		want int64
	}{
		{-2, 0},
		{-1, 4},
		{250, 2},
		{300, 2},
		{1_000, -1},
	}
	for _, tt := range tests {
		off, err := admin.ListOffsets(ctx, tp, tt.ts)
		require.NoError(t, err)
		require.Equal(t, tt.want, off, "ts %d", tt.ts)
	}
	lmu.Lock()
	require.Len(t, reqs, len(tests))
	for i, req := range reqs {
		require.Equal(t, "t", req.GetTopics()[0].GetTopic())
		p := req.GetTopics()[0].GetPartitions()[0]
		require.Equal(t, int32(1), p.GetPartition())
		require.Equal(t, tests[i].ts, p.GetTimestamp())
	}
	lmu.Unlock()

	_, err := admin.ListOffsets(ctx, TopicPartition{Topic: "missing", Partition: 0}, -1)
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
	_, err = admin.ListOffsets(ctx, TopicPartition{Topic: "t", Partition: 9}, -1)
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
}

func TestAdminListOffsetsErrors(t *testing.T) {
	af, admin := newAdminFake(t, map[string]int32{"t": 1})
	tp := TopicPartition{Topic: "t", Partition: 0}
	var (
		lmu     sync.Mutex
		epochs  []int32
		pending []streamqv1.ErrorCode
	)
	epoch := int32(1)
	af.onMetadata = func(req *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		lmu.Lock()
		defer lmu.Unlock()
		return &streamqv1.MetadataResponse{
			Brokers: []*streamqv1.Broker{{Id: 1, Host: "b1", Port: 9092}},
			Topics: []*streamqv1.TopicMetadata{{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 0, LeaderId: 1, LeaderEpoch: epoch},
			}}},
		}, nil
	}
	af.onListOffsets = func(req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		lmu.Lock()
		defer lmu.Unlock()
		epochs = append(epochs, req.GetTopics()[0].GetPartitions()[0].GetLeaderEpoch())
		if len(pending) == 0 {
			return nil, nil
		}
		code := pending[0]
		pending = pending[1:]
		epoch++
		return &streamqv1.ListOffsetsResponse{Topics: []*streamqv1.ListOffsetsTopicResponse{{
			Topic: "t", Partitions: []*streamqv1.ListOffsetsPartitionResponse{{Partition: 0, ErrorCode: code}},
		}}}, nil
	}
	ctx := context.Background()

	// NOT_LEADER and FENCED refresh metadata and retry with the new epoch.
	lmu.Lock()
	pending = []streamqv1.ErrorCode{
		streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER, streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH,
	}
	lmu.Unlock()
	off, err := admin.ListOffsets(ctx, tp, -1)
	require.NoError(t, err)
	require.Zero(t, off)
	lmu.Lock()
	require.Equal(t, []int32{1, 2, 3}, epochs)
	lmu.Unlock()

	// Non-retriable codes are returned at once.
	lmu.Lock()
	epochs = nil
	pending = []streamqv1.ErrorCode{streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP}
	lmu.Unlock()
	_, err = admin.ListOffsets(ctx, tp, 5)
	require.ErrorIs(t, err, ErrInvalidTimestamp)
	lmu.Lock()
	require.Len(t, epochs, 1)
	lmu.Unlock()

	// A transport error is retried.
	var once sync.Once
	af.mu.Lock()
	af.onListOffsets = func(*streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
		var err error
		once.Do(func() { err = status.Error(codes.Unavailable, "down") })
		return nil, err
	}
	af.mu.Unlock()
	off, err = admin.ListOffsets(ctx, tp, -2)
	require.NoError(t, err)
	require.Zero(t, off)
}
