package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

func TestServerErrorMatchesSentinels(t *testing.T) {
	for code := range streamqv1.ErrorCode_name {
		c := streamqv1.ErrorCode(code)
		err := serverError(c, "msg")
		if c == streamqv1.ErrorCode_ERROR_CODE_NONE {
			require.NoError(t, err)
			continue
		}
		require.ErrorIs(t, err, codeError(c), c.String())
		require.ErrorIs(t, fmt.Errorf("wrapped: %w", err), codeError(c))
	}
	require.ErrorIs(t, serverError(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER, ""), ErrNotLeader)
	require.NotErrorIs(t, serverError(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER, ""), ErrFencedLeaderEpoch)
}

func TestIsRetriable(t *testing.T) {
	tests := []struct {
		err       error
		retriable bool
		refresh   bool
	}{
		{nil, false, false},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER, ""), true, true},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_FENCED_LEADER_EPOCH, ""), true, true},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, ""), true, true},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_NOT_ENOUGH_REPLICAS, ""), true, false},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_REQUEST_TIMED_OUT, ""), true, false},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE, ""), false, false},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE, ""), false, false},
		{serverError(streamqv1.ErrorCode_ERROR_CODE_INVALID_TIMESTAMP, ""), false, false},
		{status.Error(codes.Unavailable, "down"), true, true},
		{status.Error(codes.DeadlineExceeded, "slow"), true, true},
		{status.Error(codes.ResourceExhausted, "too big"), false, false},
		{status.Error(codes.InvalidArgument, "bad"), false, false},
		{errors.New("other"), false, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.err), func(t *testing.T) {
			require.Equal(t, tt.retriable, IsRetriable(tt.err))
			require.Equal(t, tt.refresh, NeedsMetadataRefresh(tt.err))
		})
	}
}

func TestBackoff(t *testing.T) {
	b := newBackoff(100*time.Millisecond, time.Second)
	b.rand = func() float64 { return 0.999999 }
	require.InDelta(t, float64(100*time.Millisecond), float64(b.delay(0)), float64(time.Millisecond))
	require.InDelta(t, float64(400*time.Millisecond), float64(b.delay(2)), float64(time.Millisecond))
	require.InDelta(t, float64(time.Second), float64(b.delay(10)), float64(time.Millisecond))
	require.InDelta(t, float64(time.Second), float64(b.delay(1000)), float64(time.Millisecond))
	b.rand = func() float64 { return 0 }
	require.Equal(t, time.Duration(0), b.delay(5))
	b.rand = func() float64 { return 0.5 }
	require.Equal(t, 200*time.Millisecond, b.delay(2))
}

func TestNewValidates(t *testing.T) {
	_, err := New(Config{})
	require.Error(t, err)
}

func TestMetadataCacheAndInvalidate(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 3})
	ctx := context.Background()
	n, err := c.partitionCount(ctx, "t")
	require.NoError(t, err)
	require.Equal(t, int32(3), n)
	_, epoch, err := c.leader(ctx, TopicPartition{Topic: "t", Partition: 2})
	require.NoError(t, err)
	require.Equal(t, int32(0), epoch)
	require.Equal(t, 1, fb.count("Metadata"), "cache hit avoids RPC")

	c.invalidate("t")
	_, err = c.partitionCount(ctx, "t")
	require.NoError(t, err)
	require.Equal(t, 2, fb.count("Metadata"))

	// Expiry by age.
	now := time.Now()
	c.now = func() time.Time { return now.Add(time.Hour) }
	_, err = c.partitionCount(ctx, "t")
	require.NoError(t, err)
	require.Equal(t, 3, fb.count("Metadata"))

	_, err = c.partitionCount(ctx, "missing")
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
	_, _, err = c.leader(ctx, TopicPartition{Topic: "t", Partition: 3})
	require.ErrorIs(t, err, ErrUnknownTopicOrPartition)
}

func TestMetadataBootstrapFailover(t *testing.T) {
	fc := newFakeCluster(t)
	fb := newFakeBroker(2, "b2", 9092)
	fb.topics["t"] = 1
	fc.add("b2:9092", fb)
	c := fc.newClient("down:9092", "b2:9092")
	n, err := c.partitionCount(context.Background(), "t")
	require.NoError(t, err)
	require.Equal(t, int32(1), n)
	api, _, err := c.leader(context.Background(), TopicPartition{Topic: "t"})
	require.NoError(t, err)
	_, err = api.ListTopics(context.Background(), &streamqv1.ListTopicsRequest{})
	require.NoError(t, err, "leader address comes from metadata")
}

func TestMetadataNoBrokerReachable(t *testing.T) {
	fc := newFakeCluster(t)
	c := fc.newClient("down:1", "down:2")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.partitionCount(ctx, "t")
	require.Error(t, err)
	require.True(t, isTransportError(errors.Unwrap(err)) || IsRetriable(err))
}

func TestMetadataRefreshCoalesces(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	release := make(chan struct{})
	entered := make(chan struct{}, 10)
	fb.onMetadata = func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.partitionCount(context.Background(), "t")
			errs <- err
		}()
	}
	<-entered
	// Release the fake only once the other 9 callers have joined the in-flight call.
	require.Eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		for _, call := range c.inflight {
			return call.waiters == 9
		}
		return false
	}, 5*time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, 1, fb.count("Metadata"), "concurrent refreshes share one RPC")
}

func TestLeaderWithoutLeaderIsRetriable(t *testing.T) {
	fb, c := singleBroker(t, map[string]int32{"t": 1})
	fb.onMetadata = func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
		return &streamqv1.MetadataResponse{
			Brokers: []*streamqv1.Broker{{Id: 1, Host: "b1", Port: 9092}},
			Topics: []*streamqv1.TopicMetadata{{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 0, LeaderId: -1},
			}}},
		}, nil
	}
	_, _, err := c.leader(context.Background(), TopicPartition{Topic: "t"})
	require.ErrorIs(t, err, ErrNotLeader)
	require.True(t, IsRetriable(err))
}

func TestClosedClient(t *testing.T) {
	_, c := singleBroker(t, map[string]int32{"t": 1})
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	_, err := c.partitionCount(context.Background(), "t")
	require.ErrorIs(t, err, ErrClosed)
}
