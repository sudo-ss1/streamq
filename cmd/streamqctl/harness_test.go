package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// fixedNowMs is the time (unix ms) returned by the clock injected into every test command.
const fixedNowMs int64 = 1_700_000_000_123

// fakeServer implements only the RPCs a test sets; the rest return Unimplemented.
type fakeServer struct {
	streamqv1.UnimplementedStreamQServiceServer

	createTopic func(*streamqv1.CreateTopicRequest) *streamqv1.CreateTopicResponse
	deleteTopic func(*streamqv1.DeleteTopicRequest) *streamqv1.DeleteTopicResponse
	listTopics  func(*streamqv1.ListTopicsRequest) *streamqv1.ListTopicsResponse
	metadata    func(*streamqv1.MetadataRequest) *streamqv1.MetadataResponse
	produce     func(*streamqv1.ProduceRequest) *streamqv1.ProduceResponse
	fetch       func(*streamqv1.FetchRequest) *streamqv1.FetchResponse
	listOffsets func(*streamqv1.ListOffsetsRequest) *streamqv1.ListOffsetsResponse
}

func (f *fakeServer) CreateTopic(ctx context.Context, r *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
	if f.createTopic == nil {
		return f.UnimplementedStreamQServiceServer.CreateTopic(ctx, r)
	}
	return f.createTopic(r), nil
}

func (f *fakeServer) DeleteTopic(ctx context.Context, r *streamqv1.DeleteTopicRequest) (*streamqv1.DeleteTopicResponse, error) {
	if f.deleteTopic == nil {
		return f.UnimplementedStreamQServiceServer.DeleteTopic(ctx, r)
	}
	return f.deleteTopic(r), nil
}

func (f *fakeServer) ListTopics(ctx context.Context, r *streamqv1.ListTopicsRequest) (*streamqv1.ListTopicsResponse, error) {
	if f.listTopics == nil {
		return f.UnimplementedStreamQServiceServer.ListTopics(ctx, r)
	}
	return f.listTopics(r), nil
}

func (f *fakeServer) Metadata(ctx context.Context, r *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
	if f.metadata == nil {
		return f.UnimplementedStreamQServiceServer.Metadata(ctx, r)
	}
	return f.metadata(r), nil
}

func (f *fakeServer) Produce(ctx context.Context, r *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
	if f.produce == nil {
		return f.UnimplementedStreamQServiceServer.Produce(ctx, r)
	}
	return f.produce(r), nil
}

func (f *fakeServer) Fetch(ctx context.Context, r *streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
	if f.fetch == nil {
		return f.UnimplementedStreamQServiceServer.Fetch(ctx, r)
	}
	return f.fetch(r), nil
}

func (f *fakeServer) ListOffsets(ctx context.Context, r *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
	if f.listOffsets == nil {
		return f.UnimplementedStreamQServiceServer.ListOffsets(ctx, r)
	}
	return f.listOffsets(r), nil
}

// startFake serves srv on a bufconn listener and returns a dialer for it. The server and
// listener are shut down in t.Cleanup.
func startFake(t *testing.T, srv streamqv1.StreamQServiceServer) dialFunc {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(grpc.MaxRecvMsgSize(maxMsgSize), grpc.MaxSendMsgSize(maxMsgSize))
	streamqv1.RegisterStreamQServiceServer(s, srv)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Serve(lis)
	}()
	t.Cleanup(func() {
		s.Stop()
		_ = lis.Close()
		wg.Wait()
	})
	return func(_ context.Context, addr string) (*grpc.ClientConn, error) {
		require.Equal(t, "localhost:9092", addr) // default --bootstrap
		return grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
}

type cmdResult struct {
	out, errOut string
	err         error
}

// runCmd executes streamqctl with args against srv, reading stdin from in.
func runCmd(t *testing.T, srv streamqv1.StreamQServiceServer, in string, args ...string) cmdResult {
	t.Helper()
	return runCmdCtx(context.Background(), t, srv, in, args...)
}

func runCmdCtx(ctx context.Context, t *testing.T, srv streamqv1.StreamQServiceServer, in string, args ...string) cmdResult {
	t.Helper()
	d := startFake(t, srv)
	var out, errOut bytes.Buffer
	cmd := newRootCmdWithOptions(strings.NewReader(in), &out, &errOut, options{
		dial: d,
		now:  func() time.Time { return time.UnixMilli(fixedNowMs) },
	})
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(ctx)
	return cmdResult{out: out.String(), errOut: errOut.String(), err: err}
}
