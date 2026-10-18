package server

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/controller"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/raftstore"
)

// fakeControllerStore is a leader store over a real metadata.State.
type fakeControllerStore struct {
	mu       sync.Mutex
	state    *metadata.State
	index    uint64
	leader   bool
	applyErr error // if set, returned by ApplyForwarded
}

func newFakeControllerStore() *fakeControllerStore {
	return &fakeControllerStore{state: metadata.NewState(nil, 0), leader: true}
}

func (f *fakeControllerStore) Snapshot() metadata.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.state.Snapshot()
	s.Index = f.index
	return s
}

func (f *fakeControllerStore) ApplyForwarded(_ context.Context, cmd *streamqv1.MetadataCommand) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leader {
		return 0, metadata.ErrNotController
	}
	if f.applyErr != nil {
		return 0, f.applyErr
	}
	f.index++
	_, err := f.state.ApplyInTerm(cmd, 1)
	return f.index, err
}

func (f *fakeControllerStore) LeaderID() int32 { return 1 }
func (f *fakeControllerStore) IsLeader() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leader
}
func (f *fakeControllerStore) Term() uint64                         { return 1 }
func (f *fakeControllerStore) Barrier(context.Context) error        { return nil }
func (f *fakeControllerStore) OnLeadershipChange(func(bool)) func() { return func() {} }

type tickerClock struct{}

func (tickerClock) Now() time.Time                                     { return time.Now() }
func (tickerClock) NewTicker(time.Duration) (<-chan time.Time, func()) { return nil, func() {} }

// newInternal returns an InternalServer with an activated controller.
func newInternal(t *testing.T, st *fakeControllerStore) *InternalServer {
	t.Helper()
	ctrl := controller.New(controller.Config{BrokerID: 1, SessionTimeout: time.Minute}, st, tickerClock{}, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- ctrl.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	srv := NewInternal(ctrl, st, slog.New(slog.DiscardHandler))
	require.Eventually(t, func() bool {
		resp, err := srv.RegisterBroker(context.Background(), &streamqv1.RegisterBrokerRequest{BrokerId: 1, Host: "h", Port: 1})
		return err == nil && resp.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_NONE
	}, 5*time.Second, time.Millisecond, "controller activated")
	return srv
}

func TestInternalRegisterAndHeartbeat(t *testing.T) {
	st := newFakeControllerStore()
	srv := newInternal(t, st)
	ctx := context.Background()
	reg, err := srv.RegisterBroker(ctx, &streamqv1.RegisterBrokerRequest{BrokerId: 2, Host: "h", Port: 2, InternalAddr: "i"})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, reg.GetErrorCode())
	require.Equal(t, int64(1), reg.GetBrokerEpoch())
	require.Positive(t, reg.GetCatchUpIndex())
	require.Equal(t, int32(1), reg.GetControllerId())

	hb, err := srv.BrokerHeartbeat(ctx, &streamqv1.BrokerHeartbeatRequest{BrokerId: 2, BrokerEpoch: 1, AppliedIndex: reg.GetCatchUpIndex()})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, hb.GetErrorCode())
	require.False(t, hb.GetFenced())

	hb, err = srv.BrokerHeartbeat(ctx, &streamqv1.BrokerHeartbeatRequest{BrokerId: 2, BrokerEpoch: 9})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_STALE_BROKER_EPOCH, hb.GetErrorCode())

	st.mu.Lock()
	st.leader = false
	st.mu.Unlock()
	reg, err = srv.RegisterBroker(ctx, &streamqv1.RegisterBrokerRequest{BrokerId: 3, Host: "h", Port: 3})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER, reg.GetErrorCode())
	require.Equal(t, int32(1), reg.GetControllerId(), "controller hint")
}

func TestInternalForwardMetadata(t *testing.T) {
	st := newFakeControllerStore()
	srv := newInternal(t, st)
	ctx := context.Background()
	create := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "x", Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}}
	// Broker 1 must be unfenced for CreateTopic to succeed.
	_, err := srv.BrokerHeartbeat(ctx, &streamqv1.BrokerHeartbeatRequest{BrokerId: 1, BrokerEpoch: 1, AppliedIndex: 100})
	require.NoError(t, err)
	resp, err := srv.ForwardMetadata(ctx, &streamqv1.ForwardMetadataRequest{Command: create})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, resp.GetErrorCode(), resp.GetErrorMessage())
	require.Positive(t, resp.GetIndex())
	resp, err = srv.ForwardMetadata(ctx, &streamqv1.ForwardMetadataRequest{Command: create})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS, resp.GetErrorCode())

	for _, cmd := range []*streamqv1.MetadataCommand{
		{Version: 1, Cmd: &streamqv1.MetadataCommand_CommitOffsets{CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: "g"}}},
		{Version: 1, Cmd: &streamqv1.MetadataCommand_RegisterBroker{RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: 5}}},
		{Version: 1, Cmd: &streamqv1.MetadataCommand_FenceBroker{FenceBroker: &streamqv1.FenceBrokerCommand{BrokerId: 1}}},
		{Version: 1, Cmd: &streamqv1.MetadataCommand_UnfenceBroker{UnfenceBroker: &streamqv1.UnfenceBrokerCommand{BrokerId: 1}}},
	} {
		_, err := srv.ForwardMetadata(ctx, &streamqv1.ForwardMetadataRequest{Command: cmd})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "controller-only commands are not forwardable (R20)")
	}

	st.mu.Lock()
	st.applyErr = raftstore.ErrUnknownOutcome
	st.mu.Unlock()
	resp, err = srv.ForwardMetadata(ctx, &streamqv1.ForwardMetadataRequest{Command: create})
	require.NoError(t, err)
	require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT, resp.GetErrorCode(), "unknown outcome is retriable, not invalid")
}

func TestSentinelForRoundTrip(t *testing.T) {
	tests := []struct {
		code streamqv1.ErrorCode
		want error
	}{
		{streamqv1.ErrorCode_ERROR_CODE_NOT_CONTROLLER, metadata.ErrNotController},
		{streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS, metadata.ErrTopicExists},
		{streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, metadata.ErrUnknownTopic},
		{streamqv1.ErrorCode_ERROR_CODE_INVALID_REPLICATION_FACTOR, metadata.ErrInvalidReplicationFactor},
		{streamqv1.ErrorCode_ERROR_CODE_INVALID_TOPIC, metadata.ErrInvalidTopic},
		{streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT, raftstore.ErrUnknownOutcome},
		{streamqv1.ErrorCode_ERROR_CODE_UNKNOWN, errForwardFailed},
	}
	for _, tt := range tests {
		err := sentinelFor(tt.code, "m")
		require.ErrorIs(t, err, tt.want, tt.code.String())
		require.Equal(t, tt.code, errorCode(err), "maps back to the same code")
	}
	require.NoError(t, sentinelFor(streamqv1.ErrorCode_ERROR_CODE_NONE, ""))
}

func TestForwarderRetryClassification(t *testing.T) {
	st := newFakeControllerStore()
	srv := newInternal(t, st)
	_, err := srv.BrokerHeartbeat(context.Background(), &streamqv1.BrokerHeartbeatRequest{BrokerId: 1, BrokerEpoch: 1, AppliedIndex: 100})
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx // test setup
	require.NoError(t, err)
	gs := grpc.NewServer()
	streamqv1.RegisterStreamQInternalServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	fw := NewForwarder()
	defer func() { require.NoError(t, fw.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	create := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "x", Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}}
	idx, err := fw.Forward(ctx, lis.Addr().String(), create)
	require.NoError(t, err)
	require.Positive(t, idx)
	_, err = fw.Forward(ctx, lis.Addr().String(), create)
	require.ErrorIs(t, err, metadata.ErrTopicExists)
	require.NotErrorIs(t, err, raftstore.ErrNotAppended, "a deterministic error is not retried")

	gs.Stop()
	_, err = fw.Forward(ctx, lis.Addr().String(), create)
	require.ErrorIs(t, err, raftstore.ErrNotAppended, "an unreachable controller is safe to retry")
}
